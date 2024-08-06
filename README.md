# crzsnap

`crzsnap` is my (cr1901) attempt at a ZFS snapshotting engine built upon
the [`doit`](https://pydoit.org) Python task automater. It is built
specifically for my use case, and is designed to back up a ZFS pool from one
set of disks to another using incremental `zfs send` and `zfs recv`.

## Quick Start

* Either install this script with `pip install .`, or copy/symlink `__init__.py`
  to/from your personal `bin` directory; either works!

* Fill out a [JSON file](#json-config-file-format) under `~/.config/crzsnap/crzsnap.json`.
  All datasets in the `bookmark` and `snapshot` arrays should include the pool
  name specified in the `from` key in the JSON file.

* Run `crzsnap init_dataset [-s] [dataset]` for each dataset named in the JSON
  file. Use `-s` for each dataset where intermediate `snapshots` should be
  preserved. The dataset should include the pool name specified in the `from`
  key in the JSON file.

* (Optional): If you want to see what would be executed, run the following
  to simulate a "dry run", ignoring checks that are expected to fail if you
  just want to print out all results (TODO: can this be incorporated into
  the script?):

  ```
  crzsnap echo=1 all check:send_snapshots -f check:rotate_sender -f | grep -v -- ^[.-] && crzsnap forget > /dev/null
  ```

* `crzsnap init_dataset` does a full backup of datasets, and prepares the pools
  for subsequent runs of itself. After some amount of time has passed and you
  want to do another backup (your first incremental `send` and `recv` for those
  datasets), run `crzsnap all`.

* After `crzsnap all` has run, wait some more time before another backup. Then
  run `crzsnap forget` and then `crzsnap all` (_you must run `crzsnap forget`!_).

  * If you want to add a new dataset to this script, modify the JSON file with
    the new dataset, run `crzsnap forget`, and then run `crzsnap init_dataset [-s] [dataset]`
    as appropriate. The script will be ready for another backup using
    `crzsnap all` with your newly added dataset. _I do not suggest running
    `init_dataset` without running `crzsnap forget` first, but at present I
    don't enforce this._

## Help

Since `crzsnap` is built upon `doit`, `crzsnap` inherits all of `doit`'s
[subcommands](https://pydoit.org/cmd-other.html). _This includes how to
invoke help, which is done with `crzsnap help` instead of e.g. `crzsnap -h`._

* `crzsnap help help` gives help on global command line parameters to `crzsnap`.

* `crzsnap list` lists all public-facing tasks created by `doit`, along
  with some basic help text. `crzsnap --all` will list sub-tasks of
  public-facing tasks ("tasks with a colon"), and `crzsnap --all -p` will
  additionally include private tasks and subtasks.

* Usually you will be invoking the [`run` subcommand](https://pydoit.org/cmd-run.html),
  which is the default if no other `doit` subcommand is not specified. The
  above `list` command will give the name of tasks to run. `crzsnap help [task]`
  will give help for the any task displayed from the `list` command, along with
  command-line arguments it takes.

* `doit`'s command line parameter format is reminiscent of `make`; e.g.
  `make PREFIX=$HOME/.local all install`. Unlike `make`, `doit` tasks can take
  additional flag arguments. If you want to pass command-line args to multiple
  tasks at once, you must specify all those tasks on the command-line. See the
  "dry-run" example above, which also demonstrates the `make`-like way to pass
  the `echo` global variable to `doit`.

* All `check` tasks can be forced to complete by passing in an `-f` option; the
  script assumes you will manually fix problems for failing checks instead.
  See the "dry run" example above; in a "dry run" just before doing a backup,
  the `check:send_snapshots` and `check:rotate_sender` tasks are generally
  expected to fail.

* `crzsnap forget` needs to be run to "reset" the script state just prior to
  starting another backup. _If you do not run `crzsnap forget` after a
  successful backup, the script will think there's nothing to do when
  `crzsnap all` is run again!_

### Quick Terminology

ZFS has a bit of terminology to describe the snapshots that I also use (see [`zfs send`](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-send.8.html) and [`zfs recv`](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-recv.8.html)
pages for more info). Given the following command:

`zfs send -i tank/ds{#,@}src tank/ds@dst | zfs recv pipe/ds@dst`

I'm using the following naming scheme, mostly derived from the OpenZFS man
pages:

* Incremental source: the `tank/ds{#,@}src` snapshot/bookmark.
* Incremental target: the `tank/ds@dst` snapshot.
* Sender: the `tank` pool.
* Recevier: the `pipe` pool.
* Source: the `tank/ds` dataset.
* Destination: the `pipe/ds` dataset.
* Target: the `pipe/ds@dst` snapshot; OpenZFS doesn't seem to use "incremental"
  descriptor for the receiver/destination.
* Matched snapshots: During an incremental send, a "source" snapshot is implied
  in the `zfs recv` command. OpenZFS doesn't seem to give a special name to it.
  The man page for `zfs recv` describes this snapshot as the "most recent
  snapshot" and "must match the incremental stream's source". So I propose the
  name "matched snapshots", as in "receiver snapshots matched to sender's
  incremental source".

### JSON Config File Format

A config file is a single JSON [object](https://www.json.org/json-en.html)
with the following key-value pairs:

* `from` (string): Name of pool used in `zfs send`.
* `to` (string): Name of pool used in `zfs recv`.
* `suffix` (string): Basename of snapshots and bookmarks created by `crzsnap`.
  when rotating snapshots, snapshots named `$suffix-prev` will be destroyed,
  and snapshots named `$suffix` will be renamed to `$suffix-prev`.
* `bookmark` (array[string]): Datasets on the sending pool which will not have
  snapshots (beyond those created by `crzsnap`) preserved on the receiver.
  Instead, `zfs` [bookmarks](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-bookmark.8.html)
  are leveraged.
* `bookmark` (array[string]): Datasets on the sending pool which _will_ have
  snapshots preserved on the receiver.

Datasets which are not specified in the JSON file will be ignored by `crzsnap`.

```json
{
    "from": "tank",
    "to": "pipe",
    "suffix": "from-main",
    "bookmark": [
        "tank/backups",
        "tank/cat-videos"
    ],
    "snapshot": [
        "tank/workdir"
    ]
}
```

## How It Works

`crzsnap` is essentially an orchestrator of a sequence of `zfs` `destroy`, `rename`,
`bookmark`, `snapshot`, `send`, and `recv` shell commands generated from a JSON
input file. In this sense, `crzsnap` is a shell script generator and executor
(using [`subprocess`](https://docs.python.org/3/library/subprocess.html) with
`shell=True`). It leverages `doit` for its database/dependency support, so that
the script knows where to pick up from if it is interrupted, something tedious
to create in a raw shell script.

The script is divided into 4 main tasks:

* `create_snapshots`: create `$suffix` snapshots on sender.
* `rotate_receiver`: delete `$suffix-prev` snapshots on receiver, rename
  `$suffix` -> `$suffix-prev`.
* `send_snapshots`: incremental send of `$suffix-prev` to `$suffix` to receiver.
   Receiver datasets match sender after this step.
* `rotate_sender`: delete `$suffix-prev` snapshots on sender, rename
  `$suffix` -> `$suffix-prev`.

Each one of these tasks has a corresponding `check` subtask, e.g.
`check:create_snapshots`. These tasks essentially check whether the state
of the sending and receiving pools match what is expected before each task
runs, and bails if not. Combined with `doit`'s database, these checks are
intended to make it difficult to accidentally forget to create (or accidentally
delete!) the required snapshots/bookmarks.

_A fifth task, `all`, is the intended entry point for normal operation:_
`crzsnap all`.

## Example Usage

### Context

**FIXME: This is all how `crzsnap` is _supposed_ to work. I need to write an
integration test for it with an on-disk ZFS pool populated with fake files.**

Suppose you have three datasets in pool `tank`: `backups`, `workdir`, and
`cat-videos`. You don't care about keeping snapshots on `backups` because e.g.
you're using you backup software's rotation scheme. On the other hand, you want
to keep snapshots of `workdir` because you're testing software you wrote and
need to quickly revert to a pristine state. `cat-videos` is a recent addition,
and recently populated with all your favorite feline goodness.

You have successfully set up `backups` and `workdir` previously to be
transferred from `tank` to `pipe` in the JSON config. Additionally, you have run
`crzsnap all` successfully once (and _not_ `crzsnap forget` yet). Now you want
to add the `cat-videos` dataset to be managed by `crzsnap`. You don't care
about snapshotting `cat-videos`, so you add `cat-videos` to the `bookmarks`
section of your JSON config.

Assuming your snapshot suffix is `from-main`, right now your JSON config should
match the config in the [JSON example section](#json-config-file-format). We'll
start preparing a backup from here.

### Initial State

Right now, your `zfs` datasets should look (something) like this:

```sh
user@system$ zfs list -tfilesystem,snap,bookmark -oname
pipe/backups
pipe/backups@from-main-prev
pipe/backups@from-main
pipe/workdir
pipe/workdir@from-main-prev
pipe/workdir@from-main
tank/backups
tank/backups#from-main-prev
tank/cat-videos
tank/workdir
tank/workdir@from-main-prev
tank/workdir@weekly
```

Given the context above:

* The `from-main-prev` snapshots on `pipe` reflect the contents of the
  equivalent `tank` datasets just after `init_dataset {workdir,backups}` was
  run.
* The `from-main` snapshots on `pipe` and `from-main-prev` snapshots/bookmarks
  on `tank` reflect the contents of `tank` just after `crzsnap all` was run.
* The `tank/cat-videos` dataset has no snapshots because it was just recently
  created and was not registered with `crzsnap`.
* `tank/workdir@weekly` is a snapshot created between the run of `crzsnap all`
  and now (whether it was created before/after `tank/cat-videos` is irrelevant).

### After `crzsnap forget` and `init_dataset`

After we run `init_dataset tank/cat-videos`, your `zfs` datasets should look 
(something) like this:

```sh
user@system$ crzsnap init_dataset tank/cat-videos
...
user@system$ zfs list -tfilesystem,snap,bookmark -oname
pipe/backups
pipe/backups@from-main-prev
pipe/backups@from-main
pipe/cat-videos
pipe/cat-videos@from-main
pipe/workdir
pipe/workdir@from-main-prev
pipe/workdir@from-main
tank/backups
tank/backups#from-main-prev
tank/cat-videos
tank/cat-videos#from-main-prev
tank/workdir
tank/workdir@from-main-prev
tank/workdir@weekly
```

`init_dataset` does a "dummy" full `send` and `recv` to prepare future
incremental backups to be managed by `crzsnap`:

* `pipe/cat-videos`, `pipe/cat-videos@from-main`, and `tank/cat-videos`, should
  have very similar, if not identical, content. Likewise, the `tank/cat-videos#from-main-prev`
  bookmark should point very close to the current state of `tank/cat-videos`.
* All other datasets, snapshots, and bookmarks are the same as [previously](#after-crzsnap-forget-and-init_dataset).

### Start `crzsnap all`- After `create_snapshots`

Note that `crzsnap all` is equivalent to `crzsnap create_snapshots rotate_receiver send_snapshots rotate_sender`.
The worked example goes through each task one at a time.

```sh
user@system$ crzsnap create_snapshots
...
user@system$ zfs list -tfilesystem,snap,bookmark -oname
pipe/backups
pipe/backups@from-main-prev
pipe/backups@from-main
pipe/cat-videos
pipe/cat-videos@from-main
pipe/workdir
pipe/workdir@from-main-prev
pipe/workdir@from-main
tank/backups
tank/backups#from-main-prev
tank/backups@from-main
tank/cat-videos
tank/cat-videos#from-main-prev
tank/cat-videos@from-main
tank/workdir
tank/workdir@from-main-prev
tank/workdir@weekly
tank/workdir@from-main
```

`from-main` snapshots are created on `tank` with the current contents of each
dataset. Because we just initialized `tank/cat-videos` to be managed by `crzsnap`,
the state of the `tank/cat-videos@from-main` is likely to be very close
(if not identical) to `tank/cat-videos#from-main-prev` and friends, as
[above](#after-init_dataset). An incremental send will still work just fine.

### After `rotate_receiver`

```sh
user@system$ crzsnap rotate_receiver
...
user@system$ zfs list -tfilesystem,snap,bookmark -oname
pipe/backups
pipe/backups@from-main-prev
pipe/cat-videos
pipe/cat-videos@from-main-prev
pipe/workdir
pipe/workdir@from-main-prev
tank/backups
tank/backups#from-main-prev
tank/backups@from-main
tank/cat-videos
tank/cat-videos#from-main-prev
tank/cat-videos@from-main
tank/workdir
tank/workdir@from-main-prev
tank/workdir@weekly
tank/workdir@from-main
```

`tank` is unmodified compared to the [previous step](#start-crzsnap-all--after-create_snapshots);
the `from-main` snapshots on `pipe` are moved to `from-main-prev`, and the
snapshots previously named `from-main-prev` are destroyed. `pipe` is now ready
for an incremental `send` and `recv`.

### After `send_snapshots`

```sh
user@system$ crzsnap send_snapshots
...
user@system$ zfs list -tfilesystem,snap,bookmark -oname
pipe/backups
pipe/backups@from-main-prev
pipe/backups@from-main
pipe/cat-videos
pipe/cat-videos@from-main-prev
pipe/cat-videos@from-main
pipe/workdir
pipe/workdir@from-main-prev
pipe/workdir@weekly
pipe/workdir@from-main
tank/backups
tank/backups#from-main-prev
tank/backups@from-main
tank/cat-videos
tank/cat-videos#from-main-prev
tank/cat-videos@from-main
tank/workdir
tank/workdir@from-main-prev
tank/workdir@weekly
tank/workdir@from-main
```

`tank` datasets (via the `from-main` snapshots) are incrementally copied from
to `pipe`. _The `pipe` datasets are rolled back automatically to the
`from-main-prev` snapshot before `recv`._ Upon receipt, datasets on `pipe` will
match the corresponding datasets on `tank` (as of the `from-main` snapshots).

We could [delete](#after-rotate_receiver) the `-prev` snapshots on `pipe` now,
as they will not be needed again after the `send`/`recv` is done. However, I
opt to keep the `-prev` snapshots afterwards _just in case_. This way, a user
has time from the end of backup `X` all the way to the `rotate_receiver` step
of backup `X + 2` to grab data they realize they in fact still need!

Right now (8/5/2024), _I have no plans to add keeping additional backups on the
receiver._ `$suffix` and `$suffix-prev` are sufficient for my needs to
efficiently copy datasets after one full `send`/`recv`.

### Backup done- After `rotate_sender`

```sh
user@system$ crzsnap rotate_sender
...
user@system$ zfs list -tfilesystem,snap,bookmark -oname
pipe/backups
pipe/backups@from-main-prev
pipe/backups@from-main
pipe/cat-videos
pipe/cat-videos@from-main-prev
pipe/cat-videos@from-main
pipe/workdir
pipe/workdir@from-main-prev
pipe/workdir@weekly
pipe/workdir@from-main
tank/backups
tank/backups#from-main-prev
tank/cat-videos
tank/cat-videos#from-main-prev
tank/workdir
tank/workdir@weekly
tank/workdir@from-main-prev
```

After snapshots `from-main` snapshots on `tank` have been moved to
`from-main-prev` snapshots and bookmarks, the backup is done. Subsequent runs
of `crzsnap all` (or running any of the individual tasks that `all` composes)
report "up to date". You must run `crzsnap forget` to "reset" the script. At
this point, the backup cycle [restarts](#initial-state), and now is a good time
to add/initialize new datasets.

At this point in time (8/5/2024), swapping between snapshot and bookmark
strategies requires `destroy`ing the relevant dataset and children on `pipe`
as well as the matching dataset's bookmarks and snapshots on `tank`.
Running `init_dataset` on said dataset will then reinitialize the pools so
that `crzsnap` uses the alternate strategy.
