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
- A short or incomplete tail in the final segment is truncated and counted.
  Corruption in an earlier segment is reported instead of silently discarded.
- Claim, retry, ack, and quarantine are durable log events.
- A claim has a wall-clock lease. An expired lease becomes available after
  recovery or the next claim operation.
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

The test program covers lock exclusion, state transitions, restart replay,
and recovery from a deliberately incomplete final record. The test uses a
bounded temporary directory cleanup loop.

## v0.1 limitations

This is a POC baseline for integration experiments, not a completed
power-cut certification. Filesystem and storage hardware must honor flushes;
no software queue can compensate for a controller that lies about fsync or
loses acknowledged writes. Validation on the target ARM32/eMMC device still
requires controlled power-cut testing.

The prototype has no segment compaction or deletion, no group commit, no
multi-writer support, no multi-consumer protocol, and no priority scheduling.
Acknowledged and quarantined records remain in the log until a future,
separately tested compaction feature is added. The current implementation
performs one fsync per state change, which favors durability over throughput.

## License

BSD 3-Clause. See LICENSE.
