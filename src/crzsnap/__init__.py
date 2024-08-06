#!/usr/bin/env python3

"""crzsnap- CR1901's ZFS Snapshot Engine."""

from contextlib import contextmanager
from importlib.resources import files
import os
import pwd
import sys
import json
import textwrap

from platformdirs import user_runtime_dir, user_state_dir, user_config_dir
from configclass import Config

from doit import get_var
from doit.exceptions import TaskFailed
from doit.tools import CmdAction, run_once
from doit.cmd_base import ModuleTaskLoader
from doit.doit_cmd import DoitMain

from copy import deepcopy
from functools import partial
from itertools import chain
from pathlib import Path

appname = "crzsnap"

FORCE_ARG = [
    {
        "name": "force_success",
        "short": "f",
        "type": bool,
        "default": False,
        "help": "force task to complete successfully"
    }
]


def show_cmd(task):
    """Global title creator for tasks."""
    if task.name == "create_sender_snapshots":
        return "Create snapshots to send"
    elif task.name == "rotate_receiver":
        return f"Prepare {CONFIG.to} to receive datasets"
    elif task.name.startswith("send_snapshots"):
        # We have to reconstruct send/recv information since we can't pass
        # getargs to title :(...
        dataset = task.name.split(":")[1]
        inc_src = CONFIG.inc_src(dataset)
        bookmark = CONFIG.is_bookmark_safe(dataset)
        inc_trg = CONFIG.inc_trg(dataset)
        dst_trg = CONFIG.dst_trg(dataset)

        preserved = " (with intermediate snapshots)" if not bookmark else ""
        return f"Send {inc_src}..{inc_trg} -> {dst_trg}{preserved}"
    elif task.name.startswith("rotate"):
        dataset = task.name.split(":")[1]
        inc_src = CONFIG.inc_src(dataset)
        inc_trg = CONFIG.inc_trg(dataset)

        return f"Replace {inc_trg} -> {inc_src}"
    elif task.name.startswith("check"):
        return f"Checking preconditions for {task.name.split(':')[1]}"
    elif task.name.startswith("init"):
        return "Initialize dataset for use with this script"
    else:
        return f"??? ({task.name})"


def maybe_echo_or_force(cmd):
    """Possibly insert echo prefix to command action or skip running the command."""  # noqa: E501
    def inner(*args, force_success, **kwargs):
        echo = bool(get_var("echo", False))
        if force_success and not echo:
            return ""
        elif echo:
            return f"echo \"{cmd(*args, **kwargs)}\""
        else:
            return cmd(*args, **kwargs)
        
    return inner


def maybe_echo(cmd):
    """Possibly insert echo prefix to command action.""" 
    def inner(*args, **kwargs):
        return maybe_echo_or_force(cmd)(*args, force_success=False, **kwargs)

    return inner


def run_once_unless_echo_invoked(task, values):
    """Augmented run_once uptodate check.
     
    Check if echo argument was supplied, if yes, unconditionally mark the
    task as not up-to-date. Otherwise, fall back to run_once implementation.
    """
    # If we provided the global echo, then run tasks, as it'll in effect
    # be a dry-run. Otherwise, defer to the run_once implementation.
    echo = get_var("echo", None)
    if echo is None or not bool(echo):
        return run_once(task, values)
    else:
        return False


TASK_DICT_COMMON = {
    "title": show_cmd,
    "uptodate": [
        run_once_unless_echo_invoked
    ],
    "params": FORCE_ARG,
}


# We run zfs list several times- sometimes the same command!- to reduce the
# probability of TOCTOU problems.
def task__zfs_list():
    """Run 'zfs list -H' several times during different phases of snapshot xfer.
    
    Each run returns the raw output of `zfs list -H`. These are meant to be
    used as setup tasks to the check tasks.
    """  # noqa: E501
    yield {
        "name": "pre_create",
        "actions": [CmdAction("zfs list -H -rtsnap,bookmark -oname "
                              f"{CONFIG.from_}", save_out="snapbooks_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_prepare",
        "actions": [CmdAction(f"zfs list -H -rtsnap -oname {CONFIG.to}",
                               save_out="snaps_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_send_from",
        "actions": [CmdAction("zfs list -H -rtsnap,bookmark -oname "
                              f"{CONFIG.from_}", save_out="from_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_send_to",
        "actions": [CmdAction(f"zfs list -H -rtsnap -oname {CONFIG.to}",
                               save_out="to_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_rotate",
        "actions": [CmdAction("zfs list -H -rtsnap,bookmark -oname "
                              f"{CONFIG.from_}", save_out="snapbooks_raw")],
        "verbosity": 0,
    }


def task_check():
    """Check pool and dataset consitency before running."""
    def check_func(check_phase, success_phase, force_msg):
        def inner(*args, force_success, **kwargs):
            if force_success:
                print(force_msg)
                return success_phase()
            else:
                ret = check_phase(*args, **kwargs)
                if isinstance(ret, TaskFailed):
                    return ret
                else:
                    return success_phase()
        
        return inner

    task_dict_common = {
        "title": show_cmd,
        "params": FORCE_ARG,
        "uptodate": [
            run_once
        ],
    }

    def create_precond(snapbooks_raw):
        snapbooks = set(snapbooks_raw.split("\n"))
        snapbooks.remove("")

        missing_books = set(CONFIG.bookmark_inc_sources()) - snapbooks
        missing_snaps = set(CONFIG.snap_inc_sources()) - snapbooks

        if missing_books or missing_snaps:
            msg = "Sender snapshots/bookmarks don't match expected.\n" \
                  "The following snapshots/bookmarks were missing: " \
                  f"{', '.join(missing_books.union(missing_snaps))}.\n" \
                  "Manually run with \"check:create_sender_snapshots -f\" " \
                  "to skip this step when ready."
            return TaskFailed(msg)

    # Calculate w/ getargs b/c it's convenient.
    def create_success():
        snapshots = " ".join(CONFIG.inc_targets())
        return {
            "snapshots": snapshots,
        }

    def prepare_precond(snaps_raw):
        candidates = set(CONFIG.dst_datasets())
        all_snaps = set(snaps_raw.split("\n"))

        if len(candidates - all_snaps) != 0:
            msg = "Receiver snapshots don't match expected.\n" \
                  "The following snapshots were missing: " \
                  f"{', '.join(candidates - all_snaps)}.\n" \
                  "Manually run with \"check:rotate_receiver -f\" to skip " \
                  "this step when ready."
            return TaskFailed(msg)

    def send_precond(from_raw, to_raw):
        from_ = set(from_raw.split("\n"))
        from_.remove("")

        to = set(to_raw.split("\n"))
        to.remove("")

        missing_from_books_src = set(CONFIG.bookmark_inc_sources()) - from_
        missing_from_snaps_src = set(CONFIG.snap_inc_sources())  - from_
        missing_from_snaps_dst = set(CONFIG.inc_targets())  - from_
        missing_to_snaps = set(CONFIG.dst_datasets_prev()) - to

        assert len([*CONFIG.all_datasets()]) == len([*CONFIG.inc_sources()])
        assert len([*CONFIG.inc_sources()]) == len([*CONFIG.inc_targets()])
        assert len([*CONFIG.inc_targets()]) == len([*CONFIG.dst_datasets()])

        missing = missing_from_books_src | missing_from_snaps_src | \
            missing_from_snaps_dst | missing_to_snaps
        if missing:
            msg = "Sender/Receiver snapshots/bookmarks don't match expected.\n" \
                  "The following snapshots/bookmarks were missing: " \
                  f"{', '.join(missing)}.\n" \
                  "Manually run with \"check:send_snapshots -f\" to skip " \
                  "this step when ready."  # noqa: E501
            return TaskFailed(msg)

    def rotate_precond(snapbooks_raw):
        snapbooks = set(snapbooks_raw.split("\n"))
        snapbooks.remove("")

        missing_inc_sources = set(CONFIG.inc_sources()) - snapbooks
        missing_inc_targets = set(CONFIG.inc_targets()) - snapbooks

        if missing_inc_sources or missing_inc_targets:
            msg = "Sender snapshots/bookmarks don't match expected.\n" \
                  "The following snapshots/bookmarks were missing: " \
                  f"{', '.join(missing_inc_sources.union(missing_inc_targets))}.\n" \
                  "Manually run with \"check:rotate_sender -f\" to skip this " \
                  "step when ready."  # noqa: E501
            return TaskFailed(msg)

    yield {
        "name": "create_sender_snapshots",
        "actions": [check_func(create_precond, create_success,
                               "Assuming sender was prepared manually")],
        "getargs": {
            "snapbooks_raw": ("_zfs_list:pre_create", "snapbooks_raw")
        },
        "doc": "Check that incremental sources are available.",
        **deepcopy(task_dict_common)
    }

    yield {
        "name": "rotate_receiver",
        "actions": [check_func(prepare_precond, lambda: None,
                               "Assuming receiver was prepared manually")],
        "getargs": {
            "snaps_raw": ("_zfs_list:pre_prepare", "snaps_raw")
        },
        "doc": "Check that previous receiver targets are available.",  # noqa: E501
        **deepcopy(task_dict_common)
    }

    yield {
        "name": "send_snapshots",
        "actions": [check_func(send_precond, lambda: None,
                               "Assuming snapshots are prepared manually")],
        "getargs": {
            "from_raw": ("_zfs_list:pre_send_from", "from_raw"),
            "to_raw": ("_zfs_list:pre_send_to", "to_raw"),
        },
        "doc": "Check that sender incremental sources/targets and receiver's matched snapshots are available.",  # noqa: E501
        **deepcopy(task_dict_common)
    }

    yield {
        "name": "rotate_sender",
        "actions": [check_func(rotate_precond, lambda: None,
                               "Assuming sender was prepared manually for rotation")],  # noqa: E501
        "getargs": {
            "snapbooks_raw": ("_zfs_list:pre_rotate", "snapbooks_raw")
        },
        "doc": "Check that sender incremental sources and targets are available.",  # noqa: E501
        **deepcopy(task_dict_common)
    }


def task_init_dataset():
    """Initialize a dataset on sender and receiver for future incremental backups."""  # noqa: E501

    # Unlike other commands, this command is potentially destructive, and does
    # not perform checks against the config file (other than grabbing the
    # snapshot suffix and source/dest pool names). Thus, it can destroy
    # existing snapshots/files on receiver, and add datasets not in the config
    # file.
    @maybe_echo
    def mk_create(pos, snapshot):
        return f"sudo zfs snap {pos[0]}@{CONFIG.suffix}"

    @maybe_echo
    def mk_send(pos, snapshot):
        return f"sudo zfs send -pcL {pos[0]}@{CONFIG.suffix} | pv -f | " \
               f"sudo zfs recv -F {pos[0].replace(CONFIG.from_, CONFIG.to, 1)}"

    @maybe_echo    
    def mk_rename(pos, snapshot):
        if snapshot:
            return f"sudo zfs rename {pos[0]}@{CONFIG.suffix} " \
                   f"{pos[0]}@{CONFIG.suffix}-prev"
        else:
            return f"sudo zfs bookmark {pos[0]}@{CONFIG.suffix} " \
                   f"{pos[0]}#{CONFIG.suffix}-prev"

    @maybe_echo
    def mk_maybe_destroy(pos, snapshot):
        if snapshot:
            return ""
        else:
            return f"sudo zfs destroy {pos[0]}@{CONFIG.suffix}"

    return {
        "actions": [
            CmdAction(mk_create),
            CmdAction(mk_send, buffering=1),
            CmdAction(mk_rename),
            CmdAction(mk_maybe_destroy)
        ],
        "title": show_cmd,
        "params": [
            {
                "name": "snapshot",
                "short": "s",
                "type": bool,
                "default": False,
                "help": "preserve snapshot after send (default is to bookmark)"
            }
        ],
        "pos_arg": "pos",
    }
    

def task_create_sender_snapshots():
    """Create new incremental targets on sender."""
    @maybe_echo_or_force
    def mk_cmd(snapshots):
        # Will fail if snapshots already exist 
        return f"sudo zfs snap {snapshots}"

    return {
        "actions": [
            CmdAction(mk_cmd)
        ],
        "getargs": {
            "snapshots": ("check:create_sender_snapshots", "snapshots"),
        },
        # If we don't do deepcopy, then e.g. result_deps added by doit
        # propagate to other tasks b/c of sharing.
        **deepcopy(TASK_DICT_COMMON)
    }


def task_rotate_receiver():
    """Rotate receiver's previous targets -> new matched snapshots."""  # noqa: E501
    @maybe_echo_or_force
    def mk_destroy():
        return f"sudo zfs destroy -r {CONFIG.to}@{CONFIG.suffix}-prev"

    @maybe_echo_or_force
    def mk_rename():
        return f"sudo zfs rename -r {CONFIG.to}@{CONFIG.suffix} " \
               f"{CONFIG.to}@{CONFIG.suffix}-prev"

    return {
        "actions": [
            CmdAction(mk_destroy),
            CmdAction(mk_rename),
        ],
        "setup": [
            "check:rotate_receiver"
        ],
        "task_dep": [
            "create_sender_snapshots"
        ],
        **deepcopy(TASK_DICT_COMMON)
    }


def task_send_snapshots():
    """Incrementally send snapshots from sender to receiver."""
    # These should all succeed, except maybe the root dataset one. E.g.
    # sudo zfs send -pcLi tank#from-raidz-prev | pv | sudo zfs recv -F pipe@from-raidz  # noqa: E501
    # seems to fail with "cannot receive: failed to read from stream" if
    # the snapshot pipe@from-raidz exists already. Don't bother complicating
    # the commands just for that.
    for key in CONFIG.all_datasets():
        @maybe_echo_or_force
        def mk_send(key=key):
            send_src = CONFIG.inc_src(key)
            send_trg = CONFIG.inc_trg(key)
            if CONFIG.is_bookmark_safe(key):
                recv_trg = CONFIG.dst_trg(key)
                flags = "-pcLi"
            else:
                # If snapshot name is kept, we get cannot receive: cannot
                # specify snapshot name for multi-snapshot stream
                recv_trg = CONFIG.dst_trg(key).split("@")[0]
                flags = "-pcLRI"
            
            return f"sudo zfs send {flags} {send_src} {send_trg} | pv -f | " \
                   f"sudo zfs recv -F {recv_trg}"

        send_src = CONFIG.inc_src(key)
        send_trg = CONFIG.inc_trg(key)
        recv_trg = CONFIG.dst_trg(key)
        yield {
            "name": key,
            "actions": [
                # buffering=1 so pv works
                CmdAction(mk_send, buffering=1)
            ],
            "setup": [
                "check:send_snapshots"
            ],
            "task_dep": [
                "rotate_receiver"
            ],
            "doc": f"Send {send_src}..{send_trg} -> {recv_trg}",
            **deepcopy(TASK_DICT_COMMON),
        }


# This script assumes run from start to finish. Once a previous step has
# completed, the script will not know about filesystem modifications, and
# single previous steps that have completed cannot be run again. Instead, _all_
# tasks can be reset via "forget", and tasks that do not need to rerun can be
# forced to complete
# successfully without doing anything by passing the "-f" option to tasks.
# echo=1 recommended- it is a noop
# forget resets the script to the beginning to attempt another incremental
# backup.
# Run init_dataset _before_ first run of script or after a successful
# completion and the script has been reset.
# Otherwise, manual modifications to your source and dest pools may be needed
# if init_dataset is run in the middle of the interrupted script. Not tracking
# fs changes prevents accidentally overwriting successfully sent snapshots
# and the previously-send one. I consider this preferable to automatic
# retrying.
def task_rotate_sender():
    """Rotate senders's previous incremental targets -> new incremental sources."""  # noqa: E501
    @maybe_echo_or_force
    def mk_destroy(snapbook):
        return f"sudo zfs destroy {snapbook}"

    @maybe_echo_or_force
    def mk_rename(src, dst):
        return f"sudo zfs rename {src} {dst}"
    
    @maybe_echo_or_force
    def mk_bookmark(snapbook, newbook):
        return f"sudo zfs bookmark {snapbook} {newbook}"

    for key in CONFIG.all_datasets():
        send_src = CONFIG.inc_src(key)
        send_trg = CONFIG.inc_trg(key)

        if CONFIG.is_bookmark_safe(key):
            yield {
                "name": key,
                "actions": [
                    CmdAction(partial(mk_destroy, snapbook=send_src)),
                    CmdAction(partial(mk_bookmark, snapbook=send_trg,
                                      newbook=send_src)),
                    CmdAction(partial(mk_destroy, snapbook=send_trg)),
                ],
                "setup": [
                    "check:rotate_sender"
                ],
                "task_dep": [
                    "send_snapshots"
                ],
                "doc": f"Rotate {send_trg} -> {send_src}",
                **deepcopy(TASK_DICT_COMMON),
            }
        else:
            yield {
                "name": key,
                "actions": [
                    CmdAction(partial(mk_destroy, snapbook=send_src)),
                    CmdAction(partial(mk_rename, src=send_trg, dst=send_src)),
                ],
                "setup": [
                    "check:rotate_sender"
                ],
                "task_dep": [
                    "send_snapshots"
                ],
                "doc": f"Rotate {send_trg} -> {send_src}",
                **deepcopy(TASK_DICT_COMMON),
            }


def task_all():
    """Run entire snapshot rotations and xfers in one go."""
    return {
        "actions": [],
        "task_dep": [
            "create_sender_snapshots",
            "rotate_receiver",
            "send_snapshots",
            "rotate_sender"
        ],
        "uptodate": [
            run_once
        ],
    }


class ZFSConfig(Config):
    """`Config` helper class to calculate zfs datasets and snapshots.
    
    This is intended to be a singleton shared between all task loader functions
    and classes in crzsnap. It should never be modified more than once.
    """

    # TODO: Enforce set-once behavior somehow...

    # Basic getters- plays nicer with f-strings.
    @property
    def to(self):
        """CONFIG["to"] getter."""
        return self["to"]
    
    @property
    def from_(self):
        """CONFIG["from"] getter."""
        return self["from"]
    
    @property
    def suffix(self):
        """CONFIG["suffix"] getter."""
        return self["suffix"]

    @property
    def bookmark_safe_datasets(self):
        """CONFIG["bookmarks"] getter."""
        return self["bookmark"]

    @property
    def snap_req_datasets(self):
        """CONFIG["snapshots"] getter."""
        return self["snapshot"]

    def all_datasets(self):
        """Return iterable of all datasets in CONFIG."""
        return chain(self.bookmark_safe_datasets, self.snap_req_datasets)

    def inc_src(self, ds):
        """Return iterable of all the sender's incremental sources in CONFIG.
        
        Given a dataset "ds" in pools "tank" and "pipe", an incremental source
        is the "src" snapshot (@) or bookmark (#) in:

        `zfs send -i tank/ds{#,@}src tank/ds@dst | zfs recv pipe/ds@dst`.
        """
        splitter = "#" if self.is_bookmark_safe(ds) else "@"
        return f"{ds}{splitter}{self.suffix}-prev" 
    
    def inc_trg(self, ds):
        """Return iterable of all the sender's incremental targets in CONFIG.
        
        Given a dataset "ds" in pools "tank" and "pipe", an incremental target
        is the "dst" snapshot of tank in:

        `zfs send -i tank/ds{#,@}src tank/ds@dst | zfs recv pipe/ds@dst`.
        """
        return f"{ds}@{self.suffix}"
    
    def dst_trg(self, ds):
        """Return iterable of all the receiver's targets in CONFIG.
        
        Given a dataset "ds" in pools "tank" and "pipe", the receiver's
        target is the "dst" snapshot of pipe in:

        `zfs send -i tank/ds{#,@}src tank/ds@dst | zfs recv pipe/ds@dst`.
        """
        return f"{ds.replace(self.from_, self.to, 1)}@{self.suffix}"

    def dst_trg_prev(self, ds):
        """Return iterable of all the receiver's matched snapshots in CONFIG.
        
        Given a dataset "ds" in pools "tank" and "pipe", the receiver's
        matched snapshot (matched to the sender pool) is the implied "src"
        snapshot of pipe in `zfs send -i tank/ds{#,@}src tank/ds@dst | zfs recv pipe/ds@dst`.
        """ # noqa: E501
        return f"{ds.replace(self.from_, self.to, 1)}@{CONFIG.suffix}-prev"

    def is_bookmark_safe(self, ds):
        """Predicate which returns True if a dataset's incremental source is a bookmark."""  # noqa: E501
        return ds in self.bookmark_safe_datasets

    def bookmark_inc_sources(self):
        """Return all sender's incremental sources which are bookmarks."""
        return map(self.inc_src, self.bookmark_safe_datasets)

    def bookmark_inc_targets(self):
        """Return all sender's incremental targets whose sources are bookmarks."""  # noqa: E501
        return map(self.inc_trg, self.bookmark_safe_datasets)

    def snap_inc_sources(self):
        """Return all sender's incremental sources which are snapshots."""
        return map(self.inc_src, self.snap_req_datasets)

    def snap_inc_targets(self):
        """Return all sender's incremental targets whose sources are snapshots."""  # noqa: E501
        return map(self.inc_trg, self.snap_req_datasets)

    def inc_sources(self):
        """Return all sender's incremental sources."""
        return map(self.inc_src, self.all_datasets())

    def inc_targets(self):
        """Return all sender's incremental targets."""
        return map(self.inc_trg, self.all_datasets())

    def dst_datasets(self):
        # FIXME: make sure the string begins with self.from_ before
        # substituting.
        """Return all receivers's targets."""
        return map(self.dst_trg, self.all_datasets())

    def dst_datasets_prev(self):
        # FIXME: make sure the string begins with self.from_ before
        # substituting.
        """Return all receivers's matched snapshots.
        
        The receiver's matched snapshots (matched to the sender) are not
        specified in an incremental send and recv command pipeline; they are
        implied. However, they should still be checked for existence.
        """
        return map(self.dst_trg_prev, self.all_datasets())


# TODO: I want these scrub helpers to work with Illumos and *BSD, but can't
# currently test.
def _create_copy_zedlet_task(pools):
    """Create a task which registers crzsnap's scrub helpers with ZED."""
    event = "scrub_finish"  # Change to e.g. history_event for testing.
    user = pwd.getpwuid(os.getuid()).pw_name
    fn = f"/etc/zfs/zed.d/{event}-health-{user}.sh"

    import jinja2
    template_path = files("crzsnap").joinpath(f"{event}-template.jinja")

    def mk_copy():
        fifos = [Path(user_runtime_dir()) / f"zfs-health-{pool}.pipe"
                 for pool in pools]
        script = jinja2.Template(template_path.read_text()).render({
            "filename": os.path.realpath(__file__),
            "pools": pools,
            "pipes": fifos,
            "zip": zip
        })

        # TODO: I don't love this...
        return textwrap.dedent(
f"""sudo dd of={fn} <<EOF
{script}
EOF""")

    return {
        "basename": "_copy_zedlet",
        "file_dep": [template_path],
        "targets": [fn],
        "actions": [CmdAction(mk_copy),
                    CmdAction(f"sudo chmod 755 {fn}")]
    }


def create_scrub_tasks(pools):
    """Create doit tasks for scrubbing and waiting for pools.

    Arguments
    ---------
    pools: list(str)
        Pools to be scrubbed under control of this script. Controls
        the ``name`` argument of the ``scrub`` sub-tasks.
     
    Yields
    ------
    _copy_zedlet: dict
        Copy a script to /etc/zfs/zed.d/ that ZED will execute
        for each pool when a scrub is finished.
    scrub: dict
        Start ZFS scrubs and wait for them to finish. One scrub sub-task will
        start for each argument provided to ``pools``. Each scrub sub-task
        returns an argument saved under ``status``, which contains the output
        of ``zpool status -x $pool``.
    """
    yield _create_copy_zedlet_task(pools)

    @contextmanager
    def fifo(pool):
        fifo = Path(user_runtime_dir()) / f"zfs-health-{pool}.pipe"
        os.mkfifo(fifo)
        try:
            yield fifo
        finally:
            os.unlink(fifo)

    def wait(pool):
        # Basically, once scrub has been invoked (which should happen in the
        # first action), open a FIFO and wait for ZED to contact us that the
        # scrub is done.
        with fifo(pool) as fifo_name, open(fifo_name, "r") as fp:
            fp.read()

    for pool in pools:
        yield {
            "basename": "scrub",
            "name": pool,
            "uptodate": [False],
            "actions": [
                CmdAction(f"sudo zfs scrub {pool}"),
                (wait, (), {"pool": pool}),
                CmdAction(f"zpool status -x {pool}", save_out="status")
            ],
            "setup": ["_copy_zedlet"],
        }


# CONFIG Singleton. The task loader populates it from the file specified by
# the --dataset-config argument to crzsnap (or a system-specific defaault).
CONFIG = ZFSConfig({
    "from": "",
    "to": "",
    "suffix": "",
    "bookmark": [
    ],
    "snapshot": [
    ]
})


# Add a CLI parameter to our loader.
opt_dataset_config = {
    "section": "crzsnap",
    "name": "dataset_config",
    "long": "dataset_config",
    "type": str,
    "default": str(Path(user_config_dir("crzsnap")) / "crzsnap.json"),
    "help": "location of JSON file with dataset config [default: %(default)s]",
}


class CrZSnapTaskLoader(ModuleTaskLoader):
    """ModuleTaskLoader that takes a config file config file argument.
    
    Parameters
    ----------
    * mod_dict: Module dictionary or `globals()`. Pass to `ModuleTaskLoader`
      unaltered.
    * zfs_config: Reference to a `ZFSConfig` global. To be populated by
      `doit` loader initialization using the file provided by
      `opt_dataset_config`.
    """

    cmd_options = (opt_dataset_config,)

    def __init__(self, mod_dict, zfs_config):
        self.zfs_config = zfs_config
        super().__init__(mod_dict)

    def setup(self, opt_values):
        """Populate config singleton with data from `dataset_config` argument."""  # noqa: E501
        with open(opt_values["dataset_config"]) as fp:
            zfs_config = json.load(fp)

        self.zfs_config.update(zfs_config)


# https://github.com/pydoit/doit/issues/469
DOIT_CONFIG = {
    "default_tasks": [],
}


def main():
    """Main entry point."""  # noqa: D401
    sys.exit(DoitMain(CrZSnapTaskLoader(globals(), CONFIG), extra_config={
        "GLOBAL": {
            "dep_file": str(Path(user_state_dir("crzsnap")) /
                            ".crzsnap.doit.db"),
            "action_string_formatting": "new",
            "verbosity": 2,
            "forget_all": True
        }
    }).run(sys.argv[1:]))


if __name__ == "__main__":
    main()
