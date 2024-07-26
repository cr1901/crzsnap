#!/usr/bin/env python3

import sys
import json
import doit

from appdirs import user_state_dir, user_config_dir
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


DOIT_CONFIG = {
    "dep_file": str(Path(user_state_dir("crzsnap")) / ".crzsnap.doit.db"),
    "action_string_formatting": "new",
    "verbosity": 2,
    "default_tasks": [],
    "forget_all": True
}

with open(Path(user_config_dir("crzsnap")) / "crzsnap.json") as fp:
    ZFS_CONFIG = json.load(fp)

ZFS_BOOKMARK_SAFE=ZFS_CONFIG["bookmark"]
ZFS_PREV_SNAPSHOTS_REQUIRED=ZFS_CONFIG["snapshot"]
FROM_POOL=ZFS_CONFIG["from"]
FROM_SNAP=ZFS_CONFIG["suffix"]
TO_POOL=ZFS_CONFIG["to"]

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
        return f"Prepare {TO_POOL} to receive datasets"
    elif task.name.startswith("send_snapshots"):
        # We have to reconstruct send/recv information since we can't pass
        # getargs to title :(...
        bookmark = not task.meta["intr"]

        inc_src_root = task.name.split(":")[1]
        if bookmark:
            src = f"{inc_src_root}#{FROM_SNAP}-prev"
        else:
            src = f"{inc_src_root}@{FROM_SNAP}-prev"

        snap = f"{inc_src_root}@{FROM_SNAP}"
        dst = f"{inc_src_root.replace(FROM_POOL, TO_POOL, 1)}"
        preserved = " (with intermediate snapshots)" if not bookmark else ""
        return f"Send {src}..{snap} -> {dst}{preserved}"
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
    else:
        return f"??? ({task.name})"


def maybe_echo_or_force(cmd, force_success):
    echo = bool(get_var("echo", False))
    if force_success and not echo:
        return ""
    elif echo:
        return f"echo \"{cmd}\""
    else:
        return cmd


def maybe_echo(cmd):
    return maybe_echo_or_force(cmd, False)


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
        "actions": [CmdAction(f"zfs list -H -rtsnap,bookmark -oname {FROM_POOL}", save_out="snapbooks_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_prepare",
        "actions": [CmdAction(f"zfs list -H -rtsnap -oname {TO_POOL}", save_out="snaps_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_send_from",
        "actions": [CmdAction(f"zfs list -H -rtsnap,bookmark -oname {FROM_POOL}", save_out="from_raw")],
        "verbosity": 0,
    }

    yield {
        "name": "pre_send_to",
        "actions": [CmdAction(f"zfs list -H -rtsnap -oname {TO_POOL}", save_out="to_raw")],
        "verbosity": 0,
    }


def task_check():
    def task_dict_common():
        return {
            "title": show_cmd,
            "params": FORCE_ARG,
            "uptodate": [
                run_once
            ],
        }

    def create_precond(force_success, snapbooks_raw):
        if force_success:
            print("Assuming sender was prepared manually")
        else:
            snapbooks = set(snapbooks_raw.split("\n"))
            snapbooks.remove("")

            books_required = set([s for s in map(lambda ds: f"{ds}#{FROM_SNAP}-prev", ZFS_BOOKMARK_SAFE)])
            snapshots_required = set([s for s in map(lambda ds: f"{ds}@{FROM_SNAP}-prev", ZFS_PREV_SNAPSHOTS_REQUIRED)])

            missing_books = books_required - snapbooks
            missing_snaps = snapshots_required - snapbooks

            if missing_books or missing_snaps:
                return TaskFailed("Sender snapshots/bookmarks don't match expected.\n"
                                f"The following snapshots/bookmarks were missing: {', '.join(missing_books.union(missing_snaps))}.\n"
                                "Manually run with \"check:create_snapshots -f\" to skip this step when ready.")
        
        snapshots = " ".join([s for s in map(lambda ds: f"{ds}@{FROM_SNAP}",
                             chain(ZFS_BOOKMARK_SAFE, ZFS_PREV_SNAPSHOTS_REQUIRED))])

        return {
            "snapshots": snapshots,
        }
    
    def prepare_precond(force_success, snaps_raw):
        if force_success:
            print("Assuming receiver was prepared manually")
        else:
            candidates = [s for s in map(lambda ds: f"{ds.replace(FROM_POOL, TO_POOL, 1)}@{FROM_SNAP}",
                          chain(ZFS_BOOKMARK_SAFE, ZFS_PREV_SNAPSHOTS_REQUIRED))]

            for rs in snaps_raw.split("\n"):
                if rs and rs in candidates:
                    candidates.remove(rs)

            if len(candidates) != 0:
                return TaskFailed("Receiver snapshots don't match expected.\n"
                                f"The following snapshots were missing: {', '.join(candidates)}.\n"
                                "Manually run with \"check:prepare_receiver -f\" to skip this step when ready.")
            
    def send_precond(force_success, from_raw, to_raw):
        from_ = set(from_raw.split("\n"))
        from_.remove("")

        to = set(to_raw.split("\n"))
        to.remove("")

        from_books_src_required = [s for s in map(lambda ds: f"{ds}#{FROM_SNAP}-prev", ZFS_BOOKMARK_SAFE)]
        from_snaps_src_required = [s for s in map(lambda ds: f"{ds}@{FROM_SNAP}-prev", ZFS_PREV_SNAPSHOTS_REQUIRED)]
        from_snaps_dst_required = [s for s in map(lambda ds: f"{ds}@{FROM_SNAP}",
                                    chain(ZFS_BOOKMARK_SAFE, ZFS_PREV_SNAPSHOTS_REQUIRED))]
        to_datasets = [s for s in map(lambda ds: f"{ds.replace(FROM_POOL, TO_POOL, 1)}",
                                    chain(ZFS_BOOKMARK_SAFE, ZFS_PREV_SNAPSHOTS_REQUIRED))]
        to_snaps_required = [f"{ds.replace(FROM_POOL, TO_POOL, 1)}@{FROM_SNAP}-prev" for ds in to_datasets]

        if force_success:
            print("Assuming snapshots are prepared manually")
        else:
            missing_from_books_src = set(from_books_src_required) - from_
            missing_from_snaps_src = set(from_snaps_src_required)  - from_
            missing_from_snaps_dst = set(from_snaps_dst_required)  - from_
            missing_to_snaps = set(to_snaps_required) - to

            missing = missing_from_books_src | missing_from_snaps_src | \
                missing_from_snaps_dst | missing_to_snaps
            if missing:
                    return TaskFailed("Sender/Receiver snapshots/bookmarks don't match expected.\n"
                                    f"The following snapshots/bookmarks were missing: {', '.join(missing)}.\n"
                                    "Manually run with \"check:send_snapshots -f\" to skip this step when ready.")

        assert len(from_books_src_required + from_snaps_src_required) == len(from_snaps_dst_required)
        assert len(from_snaps_dst_required) == len(to_snaps_required)
        send_args = dict()
        for title, from_src, from_dst, to_src, bookmark in zip(
                                               chain(ZFS_BOOKMARK_SAFE, ZFS_PREV_SNAPSHOTS_REQUIRED),
                                               chain(from_books_src_required, from_snaps_src_required),
                                               from_snaps_dst_required,
                                               to_datasets,
                                               chain(repeat(True, len(from_books_src_required)),
                                                     repeat(False, len(from_snaps_src_required)))):
            send_args[title] = dict(send_src=from_src,
                                        send_trg=from_dst,
                                        recv_src=to_src,
                                        bookmark=bookmark)
            
        return send_args

    yield {
        "name": "create_snapshots",
        "actions": [create_precond],
        "getargs": {
            "snapbooks_raw": ("_zfs_list:pre_create", "snapbooks_raw")
        },
        **task_dict_common()
    }

    yield {
        "name": "prepare_receiver",
        "actions": [prepare_precond],
        "getargs": {
            "snaps_raw": ("_zfs_list:pre_prepare", "snaps_raw")
        },
        **task_dict_common()
    }

    yield {
        "name": "send_snapshots",
        "actions": [send_precond],
        "getargs": {
            "from_raw": ("_zfs_list:pre_send_from", "from_raw"),
            "to_raw": ("_zfs_list:pre_send_to", "to_raw"),
        },
        **task_dict_common()
    }

    # yield {
    #     "name": "pre_prepare",
    #     "actions": [CmdAction(f"zfs list -H -rtsnap -oname {TO_POOL}", save_out="snaps_raw")],
    #     "verbosity": 0,
    # }

def task_init_dataset():
    """Initialize a dataset on sender and receiver for future incremental backups.
       
       Unlike other commands, this command is potentially destructive, and does
       not perform checks against the config file. Thus, it can destroy existing
       snapshots/files on receiver, and add datasets not in the config file."""

    def mk_create(pos):
        return maybe_echo(f"sudo zfs snap {pos[0]}@{FROM_SNAP}")

    def mk_send(pos):
        return maybe_echo(f"sudo zfs send -pcL {pos[0]}@{FROM_SNAP} | pv -f | sudo zfs recv -F {pos[0].replace(FROM_POOL, TO_POOL, 1)}")
    
    def mk_rename(pos, snapshot):
        if snapshot:
            return maybe_echo(f"sudo zfs rename {pos[0]}@{FROM_SNAP} {pos[0]}@{FROM_SNAP}-prev")
        else:
            return maybe_echo(f"sudo zfs bookmark {pos[0]}@{FROM_SNAP} {pos[0]}#{FROM_SNAP}-prev")


    def mk_maybe_destroy(pos, snapshot):
        if snapshot:
            return ""
        else:
            return maybe_echo(f"sudo zfs destroy {pos[0]}@{FROM_SNAP}")

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
    

def task_create_snapshots():
    def mk_cmd(snapshots, force_success):
        # Will fail if snapshots already exist 
        return maybe_echo_or_force(f"sudo zfs snap {snapshots}", force_success)

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
    def mk_destroy(force_success):
        return maybe_echo_or_force(f"sudo zfs destroy -r {TO_POOL}@{FROM_SNAP}-prev", force_success)

    def mk_rename(force_success):
        return maybe_echo_or_force(f"sudo zfs rename -r {TO_POOL}@{FROM_SNAP} {TO_POOL}@{FROM_SNAP}-prev", force_success)

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
    for key, bookmark_safe in zip(chain(ZFS_BOOKMARK_SAFE, ZFS_PREV_SNAPSHOTS_REQUIRED),
                                    chain(repeat(True, len(ZFS_BOOKMARK_SAFE)),
                                          repeat(False, len(ZFS_PREV_SNAPSHOTS_REQUIRED)))):
        def mk_send(force_success, send_info):
            send_src = send_info["send_src"]
            send_trg = send_info["send_trg"]
            recv_src = send_info["recv_src"]

            if send_info["bookmark"]:
                return maybe_echo_or_force(f"sudo zfs send -pcLi {send_src} {send_trg} | pv -f | sudo zfs recv -F {recv_src}", force_success)
            else:
                return maybe_echo_or_force(f"sudo zfs send -pcLRI {send_src} {send_trg} | pv -f | sudo zfs recv -F {recv_src}", force_success) 

        yield {
            "name": key,
            "actions": [
                # buffering=1 so pv works
                CmdAction(mk_send, buffering=1)
            ],
            "getargs": {
                "send_info": ("check:send_snapshots", key),
            },
            "task_dep": [
                "prepare_receiver"
            ],
            "meta": {
                "intr": not bookmark_safe
            },
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
    bookmarks = [s for s in map(lambda ds: f"{ds}#{FROM_SNAP}-prev", ZFS_BOOKMARK_SAFE)]
    book_snap = [s for s in map(lambda ds: f"{ds}@{FROM_SNAP}", ZFS_BOOKMARK_SAFE)]
    for b, bs in zip(bookmarks, book_snap):
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
    prev_snaps = [s for s in map(lambda ds: f"{ds}@{FROM_SNAP}-prev", ZFS_PREV_SNAPSHOTS_REQUIRED)]
    snaps = [s for s in map(lambda ds: f"{ds}@{FROM_SNAP}", ZFS_PREV_SNAPSHOTS_REQUIRED)]
    for p, s in zip(prev_snaps, snaps):
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


def main():
    sys.exit(DoitMain(ModuleTaskLoader(globals())).run(sys.argv[1:]))


if __name__ == "__main__":
    main()
