#ifndef DURABLE_DELIVERY_QUEUE_H
#define DURABLE_DELIVERY_QUEUE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ddq ddq_t;

typedef enum {
    DDQ_OK = 0,
    DDQ_EMPTY = 1,
    DDQ_ALREADY_DONE = 2,
    DDQ_INVALID_ARGUMENT = -1,
    DDQ_IO_ERROR = -2,
    DDQ_CORRUPT = -3,
    DDQ_LOCKED = -4,
    DDQ_NO_MEMORY = -5,
    DDQ_TOO_LARGE = -6,
    DDQ_FULL = -7,
    DDQ_NOT_FOUND = -8,
    DDQ_BAD_STATE = -9,
    DDQ_BUFFER_TOO_SMALL = -10,
    DDQ_LIMIT = -11
} ddq_status_t;

typedef struct {
    size_t segment_bytes;
    size_t max_payload_bytes;
    uint32_t max_items;
    uint32_t max_segments;
} ddq_options_t;

typedef struct {
    uint64_t seq;
    uint32_t attempts;
    uint32_t payload_len;
    uint64_t lease_until_ms;
} ddq_claim_t;

typedef struct {
    uint64_t total_items;
    uint64_t available_items;
    uint64_t claimed_items;
    uint64_t acknowledged_items;
    uint64_t quarantined_items;
    uint64_t stored_bytes;
    uint32_t segment_count;
    uint64_t next_seq;
    uint64_t recovered_tail_bytes;
} ddq_stats_t;

int ddq_options_default(ddq_options_t *options);
int ddq_open(ddq_t **out_queue, const char *directory,
             const ddq_options_t *options);
void ddq_close(ddq_t *queue);

int ddq_recover(ddq_t *queue);
int ddq_enqueue(ddq_t *queue, const void *payload, size_t payload_len,
                uint64_t *out_seq);
int ddq_claim(ddq_t *queue, uint32_t lease_ms, ddq_claim_t *out_claim);
int ddq_read(ddq_t *queue, uint64_t seq, void *buffer, size_t buffer_cap,
             size_t *out_len);
int ddq_ack(ddq_t *queue, uint64_t seq);
int ddq_retry(ddq_t *queue, uint64_t seq, uint32_t delay_ms);
int ddq_quarantine(ddq_t *queue, uint64_t seq, const char *reason);
int ddq_stats(ddq_t *queue, ddq_stats_t *out_stats);

const char *ddq_strerror(int status);

#ifdef __cplusplus
}
#endif

#endif
