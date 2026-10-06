#ifndef DURABLE_DELIVERY_QUEUE_H
#define DURABLE_DELIVERY_QUEUE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ddq ddq_t;

/* 一个实例的所有调用须由宿主串行化，文件锁不替代线程互斥。
 * 容量压力会自动回收已确认项；未确认项、隔离原文及原因始终保留。
 * 入队另为控制记录和分段空隙保守预留容量，可能早于条目或字节上限返回满。
 * 未留控制空间的旧饱和目录不能凭新准入规则自动恢复可消费能力。
 * 首次回收后目录使用代清单格式，禁止降级到不识别清单的旧二进制。
 * 同步或发布失败后投递读取和状态写入均停止，须恢复或重开，不能假定未落盘。
 * 次数和单调时间限制约束用户态工作，存储驱动阻塞的同步调用仍需宿主监控。
 */

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
    /* 统计只含当前保留项；回收后的确认序号不会复用，也不再可读取。 */
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
/* 已回收的确认项读取或再次确认返回不存在，隔离项仍占真实容量。 */
int ddq_enqueue(ddq_t *queue, const void *payload, size_t payload_len,
                uint64_t *out_seq);
int ddq_claim(ddq_t *queue, uint32_t lease_ms, ddq_claim_t *out_claim);
/* 严格顺序领取不越过未到期租约或退避队头；DDQ_EMPTY 可表示队头暂未就绪。 */
int ddq_claim_ordered(ddq_t *queue, uint32_t lease_ms,
                       ddq_claim_t *out_claim);
int ddq_read(ddq_t *queue, uint64_t seq, void *buffer, size_t buffer_cap,
             size_t *out_len);
int ddq_ack(ddq_t *queue, uint64_t seq);
int ddq_retry(ddq_t *queue, uint64_t seq, uint32_t delay_ms);
/* 已隔离序号重复隔离返回已完成，首次隔离原因和原文保持不变。 */
int ddq_quarantine(ddq_t *queue, uint64_t seq, const char *reason);
int ddq_stats(ddq_t *queue, ddq_stats_t *out_stats);

const char *ddq_strerror(int status);

#ifdef __cplusplus
}
#endif

#endif
