# Direct transport implementation review

Status: experimental and opt-in (`--direct`). This document records the code
review and automated checks for this implementation; it is not an independent
cryptographic audit or a claim of production security certification.

## Trust and authorization

The existing SSH connection authenticates the remote and user, deploys the
matching binary, and selects the destination root and session ID. Only this
channel carries the fresh 256-bit random session secret and ephemeral listening
port. Neither secrets nor proofs are printed in diagnostics. Direct connections
must use that same absolute root and session ID and cannot open another listener.
The listener lasts for the SSH control session and closes all accepted sockets
when that session ends. Direct streams reject `TNewSession` and are not retained
between agent runs; each sync receives a fresh SSH bootstrap. Ordinary SSH
streams retain the existing session reuse and watched scans.

The service runs as the SSH user, with the same filesystem permissions as the
SSH transport. Root binding prevents a direct handshake from choosing a new root;
it is not a filesystem sandbox. Existing path resolution follows intermediate
symlinks and assumes the destination tree is not controlled by a hostile local
writer. The protocol does not make concurrent syncs to the same tree safe.

## Authentication and key separation

A listener offers 32 numbered connection slots. For each connection:

1. The client sends its one-byte slot number.
2. The server sends a fresh 32-byte random challenge.
3. The client sends a fresh 32-byte challenge and an HMAC-SHA256 proof over
   `slot || server challenge || client challenge`.
4. After verifying the proof in constant time, the server atomically consumes
   the slot and replies with its own HMAC proof over that transcript.
5. Each side derives separate client-to-server and server-to-client AES-256
   keys with HMAC-SHA256. Proofs and keys use distinct, versioned domain labels.

A successful slot cannot be reused. Failed authentication does not consume a
slot. A captured proof cannot authenticate against a fresh server challenge.
An attacker cannot impersonate either endpoint without the SSH-delivered key.
The key derivation is a PRF keyed by a uniformly random secret; transcript values
are public. It does not add independent forward secrecy beyond the SSH bootstrap:
compromising the session secret permits decryption of recorded direct streams.

## Record integrity

Each record contains a four-byte big-endian ciphertext length followed by
AES-GCM ciphertext and its 16-byte tag. The length is authenticated as additional
data. Plaintexts are capped at 64 KiB; lengths are checked before allocation.
Each direction uses its own key and an implicit monotonically increasing counter
in a 96-bit nonce (four zero bytes plus the 64-bit counter). The transcript gives
each connection distinct keys. Replays, modifications, reordering and reflection
between directions fail authentication.

An authenticated empty record ends the stream. Bare TCP EOF or a partial record
is a fatal error, not a successful application EOF. Read and write failures are
sticky. Each direction refuses to exceed 2^32 records (at most 256 TiB of plaintext
per connection); there is no in-stream rekeying. Record sizes and traffic timing
remain visible. Compression has the same trust boundary as the SSH data path.

## Resource and failure handling

TCP dial and authentication attempts have five-second timeouts. The server caps
accepted sockets and concurrent handshakes at 32; untrusted peers can still deny
service by occupying those slots until their deadlines. The listener binds all
interfaces, so the ephemeral port must be treated as exposed to the network.
No file protocol messages are processed before authentication. Authentication is
followed by the normal versioned hello and its root/session checks.

The receiver bounds protocol frames at 64 MiB, and the apply queue adds at most
64 MiB of copied payloads and 256 operations per connection. These budgets do
not include decoder windows, file-system caches, or protocol receive buffers.
An attacker who possesses the bootstrap secret has the authority of the sync
client; this is not a multi-tenant service.

The initial probe can fall back to SSH on firewall, routing, authentication or
listener failure. This never falls back to unencrypted TCP. Extra connections can
also use SSH, with the same session IDs and temp-file scheme. An established data
stream failure aborts the sync. It does not automatically replay operations that
may already have modified the destination. File atomicity remains as in SSH mode:
new small files may be visible before the sync completes; overwritten files use
temporary files and rename.

## Checks and remaining validation

Automated tests cover wrong keys, one-use slots, all 32 slots in parallel, record
corruption, oversized/truncated frames, missing authenticated EOF, record replay,
direction reflection, counter exhaustion, pending-handshake shutdown, direct
root/session binding, and a failed TCP probe preserving SSH fallback state.
End-to-end tests run real client/server processes with one and four connections,
with and without compression, over process pipes and direct TCP. They verify
contents, modes, times, symlinks, metadata updates, delta reconstruction, recursive
replacement and deletion. Race detection covers the pools and encrypted streams.

Independent cryptographic review, remote firewall/NAT/ProxyJump interoperability,
and throughput benchmarks on real high-RTT links remain deployment validation.

## SSH window evidence

The locally installed client during implementation was OpenSSH_10.0p2
(LibreSSL 3.3.6). Its upstream
[`channels.h`](https://github.com/openssh/openssh-portable/blob/V_10_0_P2/channels.h)
defines `CHAN_SES_WINDOW_DEFAULT` as `64 * 32 KiB = 2 MiB`. This is source evidence
for the default, not proof of a particular server's active window.

At runtime, the primary SSH connection writes owner-readable temporary debug
output. Rift extracts the local and remote version strings and the first positive
channel-open window or initial window adjustment. `RIFT_DEBUG=1` reports this
observation. The log is removed after connection setup. Reused ControlMaster
sessions and alternate SSH clients may omit these diagnostics; rift labels the
2 MiB fallback as an assumption. Identified HPN builds without an observed window
skip fixed-window scaling; observed larger windows reduce the estimated connection
count. Version strings alone cannot establish vendor patches or HPN behavior.
No user-specified remote endpoint was available for verification during this work.
