<p align="center">
  <img src="assets/icon.svg" alt="rift logo" width="160">
</p>

<h1 align="center">rift</h1>

rift is a blazing fast file transfer tool that moves the data over several SSH connections at
once, splits big files across them with work stealing, and uses content-defined chunking
for deltas. On a source tree it is several times faster than rsync, and a sync
where nothing changed finishes in a fraction of a second.

```sh
rift -av --delete ./ me@server:/code/project
```

If you already have an rsync command, swap the binary name:

```sh
rift -rav --delete \
    --filter='P /build/***' \
    --filter=':- .gitignore' \
    ./ me@server:/code/project
```

Nothing needs to be installed on the server. The first time rift connects to
a host, it copies a small static server binary into `~/.rift/bin` over the
same SSH connection. The remote only needs `sh` and `gzip`.

## Wasn't rsync good enough?

Yes, sure. `rsync` is just good enough and it works. But when I had to sync a local git repo to my
server (which I hadn't done in a while), all the `.git` files started becoming a bottleneck. It would
take upwards of 10 minutes for me to simply sync all the `pack` and `idx` files, at which point
I would just stop it and think of resuming this activity it some other day.

## Benchmarks

The Go source tree (12,000 files, 156 MB), pushed to a local sshd on an Apple
Silicon MacBook. Every run is warm: the page cache is hot, and rift reuses its
persistent SSH connections.

| Scenario                         | rsync |  rift |       |
| -------------------------------- | ----: | ----: | ----: |
| Fresh copy                       | 5.23s | 1.87s |  2.8x |
| Nothing changed                  | 2.01s | 0.17s | 11.8x |
| 2,000 files touched (mtime only) | 3.16s | 0.44s |  7.2x |
| 50 files edited                  | 2.00s | 0.19s | 10.5x |
| A subtree deleted                | 1.97s | 0.26s |  7.6x |
| Small edit inside a 381 MB file  | 1.74s |  0.6s |  2.9x |

The rsync here is the one that ships with macOS (openrsync). A localhost
benchmark also hides network latency, which is exactly where rift's design
pays off most. Don't take these numbers on faith; run
[`rift-bench`](#benchmark-it-yourself) against your own machine and your own
repository.

## How it works

A sync is a short conversation between the rift client and a `rift serve`
process started over SSH on the remote end. Both are the same binary.

**Scanning.** The local tree is walked by a pool of goroutines sharing a
stack of directories. Each directory is read through its own file descriptor
and its entries are stat'ed with `fstatat`, so paths are never resolved from
the root again. This walk runs while SSH is still connecting, so the file
list is usually ready before the server is. The server walks its side the
same way and sends the listing back in batches. Protected trees that you have
nothing to send to, such as a remote-only `build/`, are skipped entirely.

**Planning.** The client diffs the two listings by size and mtime. A file
with the same size but a different mtime is the classic `git checkout` or
build-tool case. Both sides hash such files in parallel with XXH3-128, and if
the content matches, only the mtime is fixed and no data is sent. Files too
big to hash cheaply go through the delta path, which notices identical
content the same way.

**Connections.** rift opens up to 8 SSH sessions (`-j`). Each is a real,
separate TCP connection with its own `ssh` process, so encryption spreads
across cores and every connection gets its own congestion window. Extra
connections are opened only when the plan calls for them; a one-file change
uses one. Each connection keeps its own ControlMaster socket alive for 15
minutes, so the next sync skips the SSH handshake. rift also asks SSH for
AES-GCM, which is hardware-accelerated on any recent CPU.

**Scheduling.** Small files (up to 1 MiB) travel as a single message carrying
path, mode, mtime and contents. They are sent in batches, and the server
writes new files in place with their final permissions. Large files are
queued largest first and cut into 1 MiB ranges, which any connection can
write straight into a shared temp file on the server. When the queues run
dry, an idle connection steals the back half of the biggest file still in
flight. The last large file is therefore finished by all connections
together, not by whichever one happened to pick it up.

**Deltas.** For a changed file of at least 128 KiB, the server computes a
FastCDC signature: chunk lengths and hashes. Chunk boundaries depend on the
content, so the client chunks its copy in one linear pass and sends back
"copy this range" and "here are new bytes" instructions. The server rebuilds
the file next to the original and checks the result against the client's
whole-file hash. If anything doesn't match, the file is resent in full.

**Wire format.** Both directions are a framed binary protocol inside a zstd
stream. Frames are batched and flushed explicitly, so hundreds of small files
become a few compressed writes.

**Finishing.** Once every connection confirms that its writes have landed,
deletions run (children before parents) and directory mtimes are restored
last.

## Install

You need Go 1.27 or newer to build.

```sh
git clone https://github.com/shxntanu/rift
cd rift
make install            # into ~/.local/bin, or: make install PREFIX=/usr/local
```

`make` builds `bin/rift` together with server binaries for linux/amd64,
linux/arm64, darwin/amd64 and darwin/arm64. Keep those `rift-<os>-<arch>`
files next to `rift`, because they are what gets uploaded to a remote. For
any other platform, rift cross-compiles a server from the source tree when Go
is available.

## Usage

```
rift [OPTIONS] SRC [USER@]HOST:DEST
rift [OPTIONS] SRC DEST
```

The trailing slash works as in rsync: `src/` copies the contents of `src`
into `DEST`, while `src` copies the directory itself to `DEST/src`.

rift always syncs recursively and preserves symlinks, permissions and
modification times. So `-a`, `-r`, `-l`, `-p`, `-t`, `-g`, `-o` and `-D` are
accepted, but they don't change anything. File ownership is not synced.

Supported rsync options:

```
-v, --verbose, -i       list what changes
-q, --quiet             no progress line or summary
-n, --dry-run           show what would change
-c, --checksum          compare by content instead of size and mtime
-u, --update            skip files that are newer on the receiver
    --ignore-existing   skip files that already exist on the receiver
    --size-only         skip files whose size matches
    --modify-window=N   treat mtimes within N seconds as equal
-W, --whole-file        never use delta transfer
    --delete            delete receiver files that are gone on the sender
-f, --filter=RULE       rsync filter rules, including dir-merge (see below)
    --exclude, --include, --exclude-from, --include-from, -F
-e, --rsh=CMD           ssh command, e.g. "ssh -p 2222"
-z, --compress          accepted; zstd compression is on by default
    --compress-level=N  zstd level 1-4, 0 disables
    --no-compress
```

Options that rift can't honour yet are rejected rather than silently
ignored: `-H`, `-A`, `-X`, `-L`, `-R`, `-b`, `--delete-excluded`,
`--existing`, `--bwlimit`, `--remove-source-files` and `--cvs-exclude`.

rift's own options:

```
-j, --conns=N           parallel SSH connections (default 8)
    --gitignore         send exactly what git would (see below)
    --delta-min=SIZE    smallest file to delta-transfer (default 128K)
    --no-fast-cipher    don't ask SSH for AES-GCM
    --no-persist        don't keep SSH connections open between runs
```

### Filters

rift implements rsync's filter rules: `-`, `+`, `P` (protect), `R` (risk),
`H` (hide) and `S` (show), along with their modifiers, the `***` suffix, `.`
merge files and `:` per-directory merge files. The first matching rule wins,
just as in rsync, and excluded files are also protected from `--delete`.

`--filter=':- .gitignore'` reads `.gitignore` files with real gitignore
semantics. That covers `!` negation, patterns anchored by a slash, and deeper
files overriding shallower ones.

`--gitignore` is a separate mode that asks git directly. It sends every file
that `git ls-files --cached --others --exclude-standard` lists. With
`--delete`, it never removes ignored files or `.git` on the remote, so build
outputs that live there survive.

### Debugging

`RIFT_DEBUG=1` prints a timestamped trace of each phase: connect, scans,
plan, transfer and teardown. `RIFT_PROFILE=dir` writes CPU profiles for the
client and any local server processes into `dir`.

Exit status is 0 on success, 23 if some files failed, and 1 on a fatal error.

## Benchmark it yourself

`rift-bench` (built by `make`) runs both tools against your own data:

```sh
rift-bench ~/code/myrepo me@server -- --filter=':- .gitignore'
```

It pushes the directory into a fresh scratch directory under `/tmp` on the
server with rsync and with rift. It times a fresh copy, a no-op, touched
files, edited files, deleted files, a new 256 MiB file, and small edits
inside that file. Each scenario runs three times, the order alternates
between runs, and the median is reported. After every scenario it checks
that both destinations match: same files, sizes, modes, mtimes and
checksums.

Your source directory is never modified. Scenarios that change files work on
a scratch copy, cloned on APFS so it costs almost nothing. The remote scratch
directory is removed at the end, and that still happens on Ctrl-C.

Both tools get the same SSH cipher settings and both reuse a warm connection,
so the comparison is between the tools and not SSH configurations. Pass
`-cold` to make every run pay for a full handshake. The rsync in macOS is
openrsync. For a fair fight, run `brew install rsync` and pass
`-rsync /opt/homebrew/bin/rsync`. Run `rift-bench -h` for the full option
list.

## Limitations

rift is young, and it does one direction of rsync's job.

- Push only: the source must be a local directory. There's no pulling from a
  remote, and no multiple or single-file sources.
- No hard links, ACLs, extended attributes or ownership.
- Linux and macOS, on both ends.
- The wire protocol isn't stable. That only matters if you mix builds: each
  client installs a server keyed to its own build, so both ends always match.

## License

[MIT](LICENSE)
