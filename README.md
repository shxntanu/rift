<p align="center">
  <img src="assets/icon.svg" alt="rift logo" width="160">
</p>

<h1 align="center">rift</h1>

<p align="center">
  <a href="https://github.com/shxntanu/rift/stargazers"><img src="https://img.shields.io/github/stars/shxntanu/rift" alt="GitHub stars"></a>
  <a href="https://github.com/shxntanu/rift/blob/main/LICENSE"><img src="https://img.shields.io/github/license/shxntanu/rift" alt="MIT license"></a>
  <img src="https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go&logoColor=white" alt="Go 1.27+">
</p>

rift is a blazing fast file transfer tool that moves the data over several SSH connections at
once, splits big files across them with work stealing, and uses content-defined chunking
for deltas. On a source tree it is **6x** times faster than rsync, and a sync
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

## Quickstart

On Linux or macOS (Intel/AMD and ARM64), install the latest release without Go:

```sh
curl -fsSL https://github.com/shxntanu/rift/releases/latest/download/install.sh | sh
```

## Wasn't rsync good enough?

Yes, sure. `rsync` is just good enough and it works. But when I had to sync a local git repo to my
server (which I hadn't done in a while), all the `.git` files started becoming a bottleneck. It would
take upwards of 10 minutes for me to simply sync all the `pack` and `idx` files, at which point
I would just stop it and think of resuming this activity it some other day.

## Benchmarks

Every run is warm: the page cache is hot, and rift reuses its
persistent SSH connections.

| Scenario   |   rsync |    rift |      |
| ---------- | ------: | ------: | ---: |
| Fresh copy | 626.41s | 108.00s | 5.8x |

The rsync here is the one that ships with macOS (openrsync). A localhost
benchmark also hides network latency, which is exactly where rift's design
pays off most. Don't take these numbers on faith; run
[`rift-bench`](#benchmark-it-yourself) against your own machine and your own
repository.

## How it works

| Optimization                | What it does                                                                                                                                                 |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Automatic deployment        | Starts the same binary on the remote over SSH; installs a matching server when needed.                                                                       |
| Persistent agent            | Reuses server processes, compression streams and watched scans; exits after 15 idle minutes. Disable with `--no-persist`.                                    |
| Safe session reuse          | Queues simultaneous syncs, resets connections when destinations change, and starts a matching agent after rebuilds.                                          |
| Watch mode                  | `rift watch --delete ./ me@server:/code/project` syncs after saves, checking every 100 ms.                                                                   |
| Incremental scans           | Uses inotify on Linux and FSEvents on macOS to re-scan changed directories; falls back to full walks when needed.                                            |
| Parallel directory walks    | Scans directories concurrently and uses `fstatat` relative to open directory descriptors to avoid repeated path resolution.                                  |
| Overlapped setup            | Scans locally while SSH connects, pipelines the remote scan behind hello, and warms worker connections during the remote scan.                               |
| Pruned scans                | Skips protected remote-only trees; `--gitignore` uses git to determine file membership.                                                                      |
| Metadata fast path          | Compares size and mtime first; hashes candidates in parallel with XXH3-128 to avoid sending unchanged contents.                                              |
| Content-defined deltas      | FastCDC finds reusable chunks in changed files from 128 KiB; sends copy instructions and only the new bytes.                                                 |
| Delta verification          | Checks the reconstructed file against the source hash; resends in full if verification fails.                                                                |
| Adaptive connection count   | Uses planned work, hello RTT and the SSH window to size the pool: up to 8 on low-latency links, 32 otherwise.                                                |
| SSH window detection        | Reads advertised windows when available; otherwise assumes 2 MiB, or skips fixed-window sizing for identified HPN builds.                                    |
| Independent SSH connections | Gives each connection its own TCP window and encryption process; prefers hardware-accelerated AES-GCM.                                                       |
| Warm SSH fallback           | Keeps separate ControlMaster sockets for 15 minutes so restarting a server can skip SSH handshakes.                                                          |
| Small-file read-ahead       | 24 readers feed connection workers independently, bounded by 64 MiB and 1,024 files.                                                                         |
| Batched small files         | Sends files up to 1 MiB with their metadata in one message; creates new files with final permissions.                                                        |
| Parallel large files        | Schedules largest files first and writes 1 MiB chunks into shared temporary files across connections.                                                        |
| Work stealing               | Idle connections take the back half of unfinished ranges to shorten the transfer tail.                                                                       |
| Double-buffered reads       | Overlaps large-file disk reads with compression and network writes.                                                                                          |
| Parallel server applies     | 16 lanes per connection apply independent paths concurrently, preserve same-path order, and bound queued payloads to 64 MiB.                                 |
| Ordering fences             | Drains applies before namespace changes, reads, directory metadata and sync acknowledgements; keeps delta sequences sequential.                              |
| Batched compression         | Packs binary frames into zstd streams and flushes batches, turning many small messages into fewer writes.                                                    |
| Fewer round trips           | Avoids redundant barriers and server-exit waits; warm no-ops need one scan round trip, simple size-changing edits usually two.                               |
| Ordered finishing           | Confirms file writes before deleting children then parents and restoring directory mtimes last.                                                              |
| Optional direct TCP         | Experimental `--direct` uses SSH-bootstrapped AES-256-GCM streams to bypass SSH data overhead, with SSH fallback. [Security notes](docs/direct-security.md). |

## Install

On Linux or macOS (Intel/AMD and ARM64), install the latest release without Go:

```sh
curl -fsSL https://github.com/shxntanu/rift/releases/latest/download/install.sh | sh
```

The installer verifies the bundle's SHA-256 checksum and installs `rift` and its
four server binaries into `~/.local/bin`. Add that directory to your `PATH` if
needed. Run the same command to upgrade. It requires `curl`, `tar`, and
`sha256sum` (Linux) or `shasum` (macOS), and does not need sudo.

To pin a version or choose another directory:

```sh
curl -fsSL https://github.com/shxntanu/rift/releases/latest/download/install.sh \
  | RIFT_INSTALL_DIR="$HOME/bin" sh -s -- v0.1.0
rift --version
```

You can also download the bundle and `checksums.txt` from
[GitHub Releases](https://github.com/shxntanu/rift/releases). Each bundle contains
all four platforms so a client can deploy a matching server to a different
platform without needing Go. Keep the `rift-<os>-<arch>` files beside `rift`.

### Build from source

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

### Publish a release

Versions use Semantic Versioning with stable tags such as `v0.1.0`. Bump the
patch for fixes, minor for features, and major for breaking changes. Protocol
versions are tracked separately in `internal/proto/proto.go`.

Releases are manual. After merging these changes to `main`, open the repository's
**Actions → CI and releases → Run workflow**, select `main`, enter a new version
(for example, `v0.1.0`), and run it. There is no scheduled release or automatic
release on push. You do not need to create a tag yourself.

GitHub Actions runs vet, race tests, and installer checks on Linux and macOS,
then cross-compiles static binaries for all four platforms. It creates the
version tag at the tested commit and publishes a
GitHub Release with generated release notes, the versioned bundle,
`checksums.txt`, and `install.sh`. The tag is embedded in `rift --version`;
all binaries share the commit ID so clients deploy matching servers. No
additional release secrets are needed beyond the workflow's `GITHUB_TOKEN`.
The curl command becomes available after the first release is published.

To build the same assets locally without publishing:

```sh
make release VERSION=v0.1.0  # outputs to dist/
```

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
-j, --conns=N           maximum connections, 1-32 (default 32; auto-sized)
    --direct           experimental AES-GCM TCP data path, with SSH fallback
    --gitignore         send exactly what git would (see below)
    --delta-min=SIZE    smallest file to delta-transfer (default 128K)
    --no-fast-cipher    don't ask SSH for AES-GCM
    --no-persist        disable the agent and warm server sessions
```

### Direct TCP (experimental)

```sh
rift --direct ./ me@server:/code/project
```

SSH authenticates the remote and starts a server listening on an ephemeral TCP
port. It returns the port and a fresh session secret through SSH. Data then
travels over mutually authenticated AES-256-GCM TCP streams, bypassing the SSH
channel window and SSH data encryption processes. The original SSH session
stays open to own the listener; disconnecting it closes the listener and streams.
Local destinations also support this mode for testing.
With the persistent agent, watched local scans remain cached, but direct
connections and their SSH bootstrap are recreated for each sync.

If the listener cannot start or the initial TCP probe fails, rift uses the
existing SSH connection. Failed extra TCP connections try SSH instead. This
works through SSH host aliases; destinations reachable only through ProxyJump,
port forwarding or a firewall generally fall back. Failures after a data stream
starts abort the sync, rather than replaying partially applied operations.

This is a new, opt-in cryptographic protocol. The implementation review, limits,
and regression checks are in [the direct transport security review](docs/direct-security.md).
It has not had an independent cryptographic audit. Keep the default SSH data
path where that assurance is required.

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
