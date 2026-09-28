# DurableDeliveryQueue

DurableDeliveryQueue (DDQ) is a small POSIX C11 append-only queue for
asynchronous delivery. The intended use is an application outbox:

~~~text
enqueue -> claim with a lease -> send -> ack
                              \-> retry
                              \-> quarantine
~~~

The first prototype is intended for the IoTSharp/DurableDeliveryQueue
repository and can later be included in LaneApp as a Git submodule. It is
single-process and single-consumer. It does not change the existing LaneApp
queue ABI.

## Properties

- Binary segmented log, with fixed little-endian fields.
- Every record contains magic, version, type, message sequence, payload
  length, CRC32, and a commit footer.
- fsync is issued after every appended record in v0.1.
- A lock file prevents two queue instances from mutating one directory.
- On open/recover, complete records are replayed in segment order.
- A short or incomplete tail in the final segment, after the manifest's
  committed snapshot boundary, is truncated and counted. An incomplete
  selected snapshot or corruption in an earlier segment is reported.
- Claim, retry, ack, and quarantine are durable log events.
- A claim has a wall-clock lease. Recovery preserves its claimed state,
  including an expired lease, so the original consumer can finish ack or
  quarantine. The next claim operation makes expired leases available.
- Queue size is bounded by configured item, payload, segment, and segment-count
  limits. An enqueue that reaches a limit returns an error.

## API

The public API is in ddq.h:

~~~c
ddq_options_t options;
ddq_t *queue = NULL;
uint64_t sequence = 0;
ddq_claim_t claim;
char payload[4096];
size_t payload_length = 0;

ddq_options_default(&options);
if (ddq_open(&queue, "/var/lib/my-app/outbox", &options) == DDQ_OK) {
    ddq_enqueue(queue, payload, payload_length, &sequence);
    if (ddq_claim(queue, 30000, &claim) == DDQ_OK) {
        if (ddq_read(queue, claim.seq, payload, sizeof(payload),
                     &payload_length) == DDQ_OK) {
            /* Send and validate the business-level success response. */
            ddq_ack(queue, claim.seq);
        } else {
            ddq_retry(queue, claim.seq, 1000);
        }
    }
    ddq_close(queue);
}
~~~

ddq_ack must be called only after the external service reports business
success. A transport success without a valid application response should be
retried or quarantined according to the caller's policy.

## Build and test

The Makefile uses a POSIX C11 compiler:

~~~sh
make
make test
make clean
~~~

The test source covers lock exclusion, state transitions, restart replay,
and recovery from a deliberately incomplete final record. The additional
capacity fixtures cover repeated enqueue/ack, retry-history reclamation
without any ack, mixed state and quarantine preservation, control-record
headroom, and manifest recovery guards. Test loops and temporary-directory
cleanup are bounded.

## Capacity and recovery (2026-09-28)

Capacity pressure automatically removes acknowledged items and redundant
claim/retry history. Unconfirmed payloads, original message sequences,
retry attempts and deadlines, active leases, and quarantined payloads and
reasons are retained. Reclamation can shorten retry history without any
acknowledged item. It publishes a new generation only when the resulting
log is smaller and has room for the requested operation. The sequence
high-water mark survives an empty snapshot; reclaimed sequences are never
reused. Quarantine is not an ack. Repeating quarantine on the same item
returns `DDQ_ALREADY_DONE` and keeps its first reason.

Enqueue conservatively reserves space for claim and terminal records,
state snapshots, maximum quarantine reasons, and record-boundary gaps.
The reason budget is `min(4096, max_payload_bytes)`. It checks both the
current append space and a worst-case compacted record layout. This can
return `DDQ_FULL` before the configured item or raw byte limit is reached;
very small configurations may accept no payload at all. A rejected enqueue
does not accept a payload or consume its sequence. Dense logs accepted by
an older binary without these reserves can still lack transition space;
the new admission rule does not repair such directories automatically.

Legacy logs use `segment-N.log`. Compacted logs use
`generation-G-segment-N.log`, with 20-digit decimal numbers. The
authoritative `queue.manifest` selects one generation and records its
committed snapshot boundary and sequence high-water mark. Staged segments
and `queue.manifest.tmp` are synchronized before the manifest is atomically
renamed and its directory synchronized. The old generation remains until
publication succeeds. A persistent `queue.format` marker prevents a
missing manifest from being mistaken for a fresh empty queue. Recovery
validates the selected snapshot before cleaning inactive generations.
An uncertain publication or cleanup failure stops further staging until
recovery succeeds. Downgrading to a binary that ignores this format after
the first compaction is unsupported and can lose queue visibility.

An append, synchronization, or publication failure can mean a complete
record reached disk even though the operation reported failure. The
instance then stops public payload reads and mutations. The caller must
recover or reopen before continuing delivery; recovery does not itself
expire claims. Statistics remain available for diagnostics. All calls on
one instance must be serialized by its owner; the file lock does not
provide thread synchronization. Public declarations and structure layouts
are unchanged, and no explicit compaction API is added.

For an initially valid directory exclusively owned by DDQ, with unchanged
options, active and staged or retained generations occupy at most
`2 * max_segments * segment_bytes` logical log bytes. The manifest and its
temporary file add at most two 48-byte files. This is not a filesystem
quota: allocation and metadata overhead, external or preexisting artifacts,
and other queue directories are outside this bound. Retained quarantine
evidence consumes real capacity. User-space loops have count and monotonic
time limits; blocking storage-driver calls such as `fsync` still require
host monitoring.

As of 2026-09-28, these changes passed whitespace, strict CP936
round-trip, delimiter, single-exit, and unchanged-public-header static
checks. Compilation, executable C tests, target ARM/eMMC validation, and
power-cut tests for these changes are **NOT_RUN**. The fixtures above are
test source, not runtime acceptance evidence.

## Historical v0.1 / v0.1.1 limitations

This is a POC baseline for integration experiments, not a completed
power-cut certification. Filesystem and storage hardware must honor flushes;
no software queue can compensate for a controller that lies about fsync or
loses acknowledged writes. Validation on the target ARM32/eMMC device still
requires controlled power-cut testing.

The historical prototype and v0.1.1 baseline had no segment compaction or
deletion; acknowledged and quarantined records remained in their logs.
The 2026-09-28 implementation adds the capacity changes described above, and
does not rewrite that release history. Group commit, multiple writers,
multiple consumers, and priority scheduling remain unsupported. One fsync
per state change still favors durability over throughput.

## License

BSD 3-Clause. See LICENSE.
