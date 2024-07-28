#!/usr/bin/env python3

import sys
import json
import doit

from appdirs import user_state_dir, user_config_dir
from configclass import Config
appname = "crzsnap"

from doit import task_params, get_var
from doit.exceptions import TaskFailed
from doit.tools import Interactive, config_changed, CmdAction, result_dep, run_once
from doit.cmd_base import ModuleTaskLoader
from doit.doit_cmd import DoitMain

from copy import deepcopy
from functools import partial
from itertools import chain, repeat
from dataclasses import dataclass
from pathlib import Path


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
    if task.name == "create_snapshots":
        return "Create snapshots to send"
    elif task.name == "prepare_receiver":
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
    elif task.name.startswith("make_bookmarks"):
        book = task.meta["book"]
        snap = task.meta["snap"]
        return f"Replace {snap} -> {book}"
    elif task.name.startswith("move_snapshots"):
        prev = task.meta["prev"]
        snap = task.meta["snap"]
        return f"Replace {snap} -> {prev}"
    elif task.name.startswith("check"):
        return f"Checking preconditions for {task.name.split(':')[1]}"
    elif task.name.startswith("init"):
        return "Initialize dataset for use with this script"
    else:
        return f"??? ({task.name})"


def maybe_echo_or_force(cmd):
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
    def inner(*args, **kwargs):
        return maybe_echo_or_force(cmd)(*args, force_success=False, **kwargs)

    return inner


def run_once_unless_echo_invoked(task, values):
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
    yield {
        "name": "pre_create",
        "actions": [CmdAction(f"zfs list -H -rtsnap,bookmark -oname {CONFIG.from_}", save_out="snapbooks_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_prepare",
        "actions": [CmdAction(f"zfs list -H -rtsnap -oname {CONFIG.to}", save_out="snaps_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_send_from",
        "actions": [CmdAction(f"zfs list -H -rtsnap,bookmark -oname {CONFIG.from_}", save_out="from_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_send_to",
        "actions": [CmdAction(f"zfs list -H -rtsnap -oname {CONFIG.to}", save_out="to_raw")],
        "verbosity": 0,
    }


def task_check():
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

        books_required = set(CONFIG.bookmark_inc_sources())
        snapshots_required = set(CONFIG.snap_inc_sources())

        missing_books = books_required - snapbooks
        missing_snaps = snapshots_required - snapbooks

        if missing_books or missing_snaps:
            return TaskFailed("Sender snapshots/bookmarks don't match expected.\n"
                            f"The following snapshots/bookmarks were missing: {', '.join(missing_books.union(missing_snaps))}.\n"
                            "Manually run with \"check:create_snapshots -f\" to skip this step when ready.")

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
            return TaskFailed("Receiver snapshots don't match expected.\n"
                            f"The following snapshots were missing: {', '.join(candidates - all_snaps)}.\n"
                            "Manually run with \"check:prepare_receiver -f\" to skip this step when ready.")

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
                return TaskFailed("Sender/Receiver snapshots/bookmarks don't match expected.\n"
                                f"The following snapshots/bookmarks were missing: {', '.join(missing)}.\n"
                                "Manually run with \"check:send_snapshots -f\" to skip this step when ready.")

    yield {
        "name": "create_snapshots",
        "actions": [check_func(create_precond, create_success,
                               "Assuming sender was prepared manually")],
        "getargs": {
            "snapbooks_raw": ("_zfs_list:pre_create", "snapbooks_raw")
        },
        **deepcopy(task_dict_common)
    }

    yield {
        "name": "prepare_receiver",
        "actions": [check_func(prepare_precond, lambda: None,
                               "Assuming receiver was prepared manually")],
        "getargs": {
            "snaps_raw": ("_zfs_list:pre_prepare", "snaps_raw")
        },
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
        **deepcopy(task_dict_common)
    }

    # yield {
    #     "name": "pre_prepare",
    #     "actions": [CmdAction(f"zfs list -H -rtsnap -oname {CONFIG.to}", save_out="snaps_raw")],
    #     "verbosity": 0,
    # }

def task_init_dataset():
    """Initialize a dataset on sender and receiver for future incremental backups.
       
       Unlike other commands, this command is potentially destructive, and does
       not perform checks against the config file (other than grabbing the
       snapshot suffix and source/dest pool names). Thus, it can destroy existing
       snapshots/files on receiver, and add datasets not in the config file."""

    @maybe_echo
    def mk_create(pos, snapshot):
        return f"sudo zfs snap {pos[0]}@{CONFIG.suffix}"

    @maybe_echo
    def mk_send(pos, snapshot):
        return f"sudo zfs send -pcL {pos[0]}@{CONFIG.suffix} | pv -f | sudo zfs recv -F {pos[0].replace(CONFIG.from_, CONFIG.to, 1)}"

    @maybe_echo    
    def mk_rename(pos, snapshot):
        if snapshot:
            return f"sudo zfs rename {pos[0]}@{CONFIG.suffix} {pos[0]}@{CONFIG.suffix}-prev"
        else:
            return f"sudo zfs bookmark {pos[0]}@{CONFIG.suffix} {pos[0]}#{CONFIG.suffix}-prev"

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
                "type": str,
                "default": "",
                "help": "preserve snapshot after send (default is to bookmark)"
            }
        ],
        "pos_arg": "pos",
    }
    

def task_create_snapshots():
    @maybe_echo_or_force
    def mk_cmd(snapshots):
        # Will fail if snapshots already exist 
        return f"sudo zfs snap {snapshots}"

    return {
        "actions": [
            CmdAction(mk_cmd)
        ],
        "getargs": {
            "snapshots": ("check:create_snapshots", "snapshots"),
        },
        # If we don't do deepcopy, then e.g. result_deps added by doit
        # propagate to other tasks b/c of sharing.
        **deepcopy(TASK_DICT_COMMON)
    }


def task_prepare_receiver():
    @maybe_echo_or_force
    def mk_destroy():
        return f"sudo zfs destroy -r {CONFIG.to}@{CONFIG.suffix}-prev"

    @maybe_echo_or_force
    def mk_rename():
        return f"sudo zfs rename -r {CONFIG.to}@{CONFIG.suffix} {CONFIG.to}@{CONFIG.suffix}-prev"

    return {
        "actions": [
            CmdAction(mk_destroy),
            CmdAction(mk_rename),
        ],
        "setup": [
            "check:prepare_receiver"
        ],
        "task_dep": [
            "create_snapshots"
        ],
        **deepcopy(TASK_DICT_COMMON)
    }


def task_send_snapshots():
    # These should all succeed, except maybe the root dataset one. E.g.
    # sudo zfs send -pcLi tank#from-raidz-prev | pv | sudo zfs recv -F pipe@from-raidz
    # seems to fail with "cannot receive: failed to read from stream" if
    # the snapshot pipe@from-raidz exists already. Don't bother complicating
    # the commands just for that.
    for key in CONFIG.all_datasets():
        @maybe_echo_or_force
        def mk_send(key=key):
            send_src = CONFIG.inc_src(key)
            send_trg = CONFIG.inc_trg(key)
            recv_trg = CONFIG.dst_trg(key)
            flags = "-pcLi" if CONFIG.is_bookmark_safe(key) else "-pcLRI"
            
            return f"sudo zfs send {flags} {send_src} {send_trg} | pv -f | sudo zfs recv -F {recv_trg}"

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
                "prepare_receiver"
            ],
            **deepcopy(TASK_DICT_COMMON),
        }

# This script assumes run from start to finish. Once a previous step has
# completed, the script will not know about filesystem modifications, and
# single previous steps that have completed cannot be run again. Instead, _all_
# tasks can be reset via "forget", and tasks that do not need to rerun can be forced to complete
# successfully without doing anything by passing the "-f" option to tasks.
# echo=1 recommended- it is a noop
# forget resets the script to the beginning to attempt another incremental
# backup.
# Run init_dataset _before_ first run of script or after a successful completion
# and the script has been reset.
# Otherwise, manual modifications to your source and dest pools may be needed
# if init_dataset is run in the middle of the interrupted script. Not tracking
# fs changes prevents accidentally overwriting successfully sent snapshots
# and the previously-send one. I consider this preferable to automatic retrying.
def task_rotate_sender():
    pass

def task_make_bookmarks():
    for b, bs in zip(CONFIG.bookmark_inc_sources(), CONFIG.bookmark_inc_targets()):
        yield {
            "name": b.replace("/", "-"),
            "actions": [
                CmdAction(f"sudo zfs destroy {b} && sudo zfs bookmark {bs} {b} && sudo zfs destroy {bs}")
            ],
            "title": show_cmd,
            "uptodate": [
                run_once
            ],
            "meta": {
                "book": b,
                "snap": bs
            }
        }


def task_move_snapshots():
    for p, s in zip(CONFIG.snap_inc_sources(), CONFIG.snap_inc_targets()):
        yield {
            "name": p.replace("/", "-"),
            "actions": [
                CmdAction(f"sudo zfs destroy {p} && sudo zfs rename {s} {p}")
            ],
            "title": show_cmd,
            "uptodate": [
                run_once
            ],
            "meta": {
                "prev": p,
                "snap": s
            }
        }


def task_all():        
    return {
        "actions": [],
        "task_dep": [
            "create_snapshots",
            "prepare_receiver",
            "send_snapshots"
        ],
        "uptodate": [
            run_once
        ],
    }


class ZFSConfig(Config):
    # Basic getters- plays nicer with f-strings.
    @property
    def to(self):
        return self["to"]
    
    @property
    def from_(self):
        return self["from"]
    
    @property
    def suffix(self):
        return self["suffix"]

    @property
    def bookmark_safe_datasets(self):
        return self["bookmark"]

    @property
    def snap_req_datasets(self):
        return self["snapshot"]

    def all_datasets(self):
        return chain(self.bookmark_safe_datasets, self.snap_req_datasets)

    def inc_src(self, ds):
        splitter = "#" if self.is_bookmark_safe(ds) else "@"
        return f"{ds}{splitter}{self.suffix}-prev" 
    
    def inc_trg(self, ds):
        return f"{ds}@{self.suffix}"
    
    def dst_trg(self, ds):
        return f"{ds.replace(self.from_, self.to, 1)}@{self.suffix}"

    def dst_trg_prev(self, ds):
        return f"{ds.replace(self.from_, self.to, 1)}@{CONFIG.suffix}-prev"

    def is_bookmark_safe(self, ds):
        return ds in self.bookmark_safe_datasets

    def bookmark_inc_sources(self):
        return map(self.inc_src, self.bookmark_safe_datasets)

    def bookmark_inc_targets(self):
        return map(self.inc_trg, self.bookmark_safe_datasets)

    def snap_inc_sources(self):
        return map(self.inc_src, self.snap_req_datasets)

    def snap_inc_targets(self):
        return map(self.inc_trg, self.snap_req_datasets)

    def inc_sources(self):
        return map(self.inc_src, self.all_datasets())

    def inc_targets(self):
        return map(self.inc_trg, self.all_datasets())

    def dst_datasets(self):
        # FIXME: make sure the string begins with self.from_ before
        # substituting.
        return map(self.dst_trg, self.all_datasets())

    def dst_datasets_prev(self):
        # FIXME: make sure the string begins with self.from_ before
        # substituting.
        return map(self.dst_trg_prev, self.all_datasets())


# The Config is injected into all task creator classes as an empty config.
# Then the task loader populates each entry.
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
    cmd_options = (opt_dataset_config,)
    """ModuleTaskLoader that takes an argument to change the config file
    where datasets are loaded.
    """

    def __init__(self, mod_dict, zfs_config):
        self.zfs_config = zfs_config
        super().__init__(mod_dict)

    def setup(self, opt_values):
        with open(opt_values["dataset_config"]) as fp:
            zfs_config = json.load(fp)

        self.zfs_config.update(zfs_config)


# https://github.com/pydoit/doit/issues/469
DOIT_CONFIG = {
    "default_tasks": [],
}

def main():
    sys.exit(DoitMain(CrZSnapTaskLoader(globals(), CONFIG), extra_config={
        "GLOBAL": {
            "dep_file": str(Path(user_state_dir("crzsnap")) / ".crzsnap.doit.db"),
            "action_string_formatting": "new",
            "verbosity": 2,
            "forget_all": True
        }
    }).run(sys.argv[1:]))


if __name__ == "__main__":
    main()
