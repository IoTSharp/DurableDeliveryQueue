#define _POSIX_C_SOURCE 200809L

#include "ddq.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* 测试扫描和重复用例均设单调时钟边界，不依赖投递租约的实时时钟。 */
static uint64_t test_deadline(void)
{
    struct timespec ts;
    uint64_t deadline;
    deadline = 0ULL;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        deadline = (uint64_t)ts.tv_sec * 1000ULL +
                   (uint64_t)ts.tv_nsec / 1000000ULL + 30000ULL;
    }
    return deadline;
}

/* 信号可取消测试进程，任何时钟失败均停止后续扫描。 */
static int test_expired(uint64_t deadline)
{
    struct timespec ts;
    uint64_t now;
    int expired;
    expired = 1;
    if (deadline != 0ULL && clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        now = (uint64_t)ts.tv_sec * 1000ULL +
              (uint64_t)ts.tv_nsec / 1000000ULL;
        expired = now >= deadline;
    }
    return expired;
}

static int check_status(const char *label, int actual, int expected)
{
    int status;
    status = 0;
    if (actual != expected) {
        fprintf(stderr, "%s: got %s (%d), expected %s (%d)\n",
                label, ddq_strerror(actual), actual,
                ddq_strerror(expected), expected);
        status = 1;
    }
    return status;
}

static int check_true(const char *label, int condition)
{
    int status;
    status = 0;
    if (condition == 0) {
        fprintf(stderr, "%s: condition failed\n", label);
        status = 1;
    }
    return status;
}

static int find_last_segment(const char *directory, char *out_path,
                             size_t out_capacity)
{
    /* 文件名复制到自持缓冲，兼容旧代和压缩代，避免复用目录项指针。 */
    DIR *dir;
    struct dirent *entry;
    char candidate[PATH_MAX];
    char best[PATH_MAX];
    uint64_t deadline;
    uint32_t iterations;
    int done;
    int status;
    int written;
    dir = NULL;
    entry = NULL;
    best[0] = '\0';
    done = 0;
    deadline = test_deadline();
    status = DDQ_IO_ERROR;
    dir = opendir(directory);
    if (dir != NULL) {
        status = DDQ_NOT_FOUND;
        for (iterations = 0U; iterations < 256U; ++iterations) {
            if (test_expired(deadline) != 0) {
                status = DDQ_LIMIT;
                break;
            }
            entry = readdir(dir);
            if (entry == NULL) {
                done = 1;
                break;
            }
            if ((strncmp(entry->d_name, "segment-", 8U) != 0 &&
                 strncmp(entry->d_name, "generation-", 11U) != 0) ||
                strstr(entry->d_name, ".log") == NULL) {
                continue;
            }
            if (best[0] == '\0' || strcmp(entry->d_name, best) > 0) {
                memcpy(best, entry->d_name, strlen(entry->d_name) + 1U);
            }
        }
        if (done == 0 && status == DDQ_NOT_FOUND) {
            status = DDQ_LIMIT;
        }
        if (status != DDQ_LIMIT && best[0] != '\0') {
            written = snprintf(candidate, sizeof(candidate), "%s/%s",
                               directory, best);
            if (written < 0 || (size_t)written >= sizeof(candidate) ||
                (size_t)written >= out_capacity) {
                status = DDQ_TOO_LARGE;
            } else {
                memcpy(out_path, candidate, (size_t)written + 1U);
                status = DDQ_OK;
            }
        }
        closedir(dir);
    }
    return status;
}

static void remove_test_directory(const char *directory)
{
    /* 只回收本测试创建的临时目录，清理受条目数和时间双重限制。 */
    DIR *dir;
    struct dirent *entry;
    char path[PATH_MAX];
    uint32_t iterations;
    uint64_t deadline;
    int written;
    deadline = test_deadline();
    dir = directory != NULL ? opendir(directory) : NULL;
    if (dir != NULL) {
        for (iterations = 0U; iterations < 256U; ++iterations) {
            if (test_expired(deadline) != 0) {
                break;
            }
            entry = readdir(dir);
            if (entry == NULL) {
                break;
            }
            if (strcmp(entry->d_name, ".") == 0 ||
                strcmp(entry->d_name, "..") == 0) {
                continue;
            }
            written = snprintf(path, sizeof(path), "%s/%s",
                               directory, entry->d_name);
            if (written >= 0 && (size_t)written < sizeof(path)) {
                unlink(path);
            }
        }
        closedir(dir);
    }
    if (directory != NULL) {
        rmdir(directory);
    }
}

/* 比较磁盘读取的完整原始载荷，覆盖零字节和反斜杠字节。 */
static int check_payload(ddq_t *queue, uint64_t seq,
                         const uint8_t *expected, size_t expected_length)
{
    /* 定向载荷夹具包含208字节原文，独立读回缓冲区须覆盖完整长度。 */
    uint8_t buffer[256];
    size_t length;
    int status;
    status = check_status("读取保留原文",
        ddq_read(queue, seq, buffer, sizeof(buffer), &length), DDQ_OK);
    if (status == 0) {
        status |= check_true("保留原文字节一致",
            length == expected_length &&
            memcmp(buffer, expected, expected_length) == 0);
    }
    return status;
}

/* 只检查本测试小段文件中的唯一原因原文，不把统计结果当作原文证据。 */
static int count_reason_bytes(const char *directory, const char *reason)
{
    DIR *dir;
    struct dirent *entry;
    struct stat st;
    char path[PATH_MAX];
    uint8_t bytes[4096];
    uint64_t deadline;
    uint32_t iteration;
    size_t index;
    size_t reason_length;
    ssize_t amount;
    int fd;
    int written;
    int count;
    int done;
    count = 0;
    done = 0;
    fd = -1;
    reason_length = strlen(reason);
    deadline = test_deadline();
    dir = opendir(directory);
    if (dir == NULL || reason_length == 0U ||
        reason_length > sizeof(bytes)) {
        count = -1;
    }
    if (dir != NULL) {
        for (iteration = 0U; iteration < 256U && count >= 0; ++iteration) {
            if (test_expired(deadline) != 0) {
                count = -1;
                break;
            }
            entry = readdir(dir);
            if (entry == NULL) {
                done = 1;
                break;
            }
            if (strstr(entry->d_name, ".log") == NULL) {
                continue;
            }
            written = snprintf(path, sizeof(path), "%s/%s", directory,
                               entry->d_name);
            if (written < 0 || (size_t)written >= sizeof(path)) {
                count = -1;
                break;
            }
            fd = open(path, O_RDONLY);
            if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
                st.st_size < 0 || st.st_size > (off_t)sizeof(bytes)) {
                count = -1;
            } else {
                amount = pread(fd, bytes, (size_t)st.st_size, 0);
                if (amount != st.st_size) {
                    count = -1;
                } else {
                    for (index = 0U;
                         index + reason_length <= (size_t)amount;
                         ++index) {
                        if (test_expired(deadline) != 0) {
                            count = -1;
                            break;
                        }
                        if (memcmp(bytes + index, reason,
                                   reason_length) == 0) {
                            count += 1;
                        }
                    }
                }
            }
            if (fd >= 0) {
                close(fd);
                fd = -1;
            }
        }
        closedir(dir);
        if (done == 0) {
            count = -1;
        }
    }
    return count;
}

/* 故障夹具只向已创建的专用测试目录写入至多二百五十六字节。 */
static int write_fixture(const char *directory, const char *name,
                         const uint8_t *bytes, size_t length)
{
    char path[PATH_MAX];
    int written;
    int fd;
    int status;
    fd = -1;
    status = DDQ_OK;
    written = snprintf(path, sizeof(path), "%s/%s", directory, name);
    if (written < 0 || (size_t)written >= sizeof(path) || length > 256U) {
        status = DDQ_TOO_LARGE;
    } else {
        fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (fd < 0 || write(fd, bytes, length) != (ssize_t)length ||
            fsync(fd) != 0) {
            status = DDQ_IO_ERROR;
        }
    }
    if (fd >= 0) {
        close(fd);
    }
    return status;
}

/* 分别用条目上限和段上限触发自动回收，确认高水位不被空快照重置。 */
static int test_capacity_reuse(uint32_t max_items)
{
    char directory_template[] = "/tmp/ddq-reuse-XXXXXX";
    char *directory;
    const uint8_t payload[] = {0x00U, 0x7fU, 0x5cU, 0x80U};
    ddq_options_t options;
    ddq_claim_t claim;
    ddq_stats_t stats;
    ddq_t *queue;
    uint64_t sequence;
    uint64_t deadline;
    uint32_t iteration;
    int status;
    queue = NULL;
    sequence = 0ULL;
    status = 0;
    directory = mkdtemp(directory_template);
    deadline = test_deadline();
    status |= check_true("创建容量测试目录", directory != NULL);
    if (status == 0) {
        ddq_options_default(&options);
        options.segment_bytes = 256U;
        options.max_payload_bytes = 16U;
        options.max_items = max_items;
        options.max_segments = 2U;
        status |= check_status("打开小容量队列",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    for (iteration = 0U; iteration < 12U && status == 0; ++iteration) {
        status |= check_true("容量重复测试未超时",
                             test_expired(deadline) == 0);
        if (status == 0) {
            status |= check_status("连续接收",
                ddq_enqueue(queue, payload, sizeof(payload), &sequence),
                DDQ_OK);
            status |= check_true("序号单调递增",
                sequence == (uint64_t)iteration + 1ULL);
            status |= check_status("连续领取",
                ddq_claim(queue, 600000U, &claim), DDQ_OK);
            status |= check_true("领取归属一致", claim.seq == sequence);
            status |= check_payload(queue, sequence, payload,
                                    sizeof(payload));
            status |= check_status("连续确认", ddq_ack(queue, sequence),
                                   DDQ_OK);
        }
    }
    if (status == 0) {
        status |= check_status("容量回收后统计", ddq_stats(queue, &stats),
                               DDQ_OK);
        status |= check_true("容量与高水位",
            stats.total_items <= max_items && stats.segment_count <= 2U &&
            stats.next_seq == 13ULL);
        ddq_close(queue);
        queue = NULL;
        status |= check_status("容量回收后重开",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("重开后仍能接收",
            ddq_enqueue(queue, payload, sizeof(payload), &sequence), DDQ_OK);
        status |= check_true("重开不复用序号", sequence == 13ULL);
        status |= check_status("重开后领取", ddq_claim(queue, 600000U,
                                                     &claim), DDQ_OK);
        status |= check_true("重开后归属", claim.seq == sequence);
        status |= check_status("重开后确认", ddq_ack(queue, sequence),
                               DDQ_OK);
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    remove_test_directory(directory);
    return status;
}

/* 数据贴段边界时仍须保留终结空间；饱和拒绝不能改变已接受项和序号。 */
static int test_control_headroom(uint32_t max_segments)
{
    char directory_template[] = "/tmp/ddq-control-XXXXXX";
    char *directory;
    char reason[209];
    uint8_t payload[208];
    ddq_options_t options;
    ddq_claim_t claim;
    ddq_stats_t stats;
    ddq_t *queue;
    uint64_t seq[3] = {0ULL, 0ULL, 0ULL};
    int status;
    queue = NULL;
    status = 0;
    memset(payload, 0x53, sizeof(payload));
    memset(reason, 'Q', sizeof(reason) - 1U);
    reason[sizeof(reason) - 1U] = '\0';
    directory = mkdtemp(directory_template);
    status |= check_true("创建控制预留目录", directory != NULL);
    if (status == 0) {
        ddq_options_default(&options);
        options.segment_bytes = 256U;
        options.max_payload_bytes = sizeof(payload);
        options.max_items = 32U;
        options.max_segments = max_segments;
        status |= check_status("打开控制预留队列",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0 && max_segments == 1U) {
        status |= check_status("单段无终结空间拒绝接收",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[0]), DDQ_FULL);
        status |= check_status("拒绝后统计", ddq_stats(queue, &stats), DDQ_OK);
        status |= check_true("拒绝不接受数据和序号",
            seq[0] == 0ULL && stats.total_items == 0ULL &&
            stats.stored_bytes == 0ULL && stats.next_seq == 1ULL);
    } else if (status == 0) {
        status |= check_status("接收贴边第一项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[0]), DDQ_OK);
        status |= check_status("接收贴边第二项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[1]), DDQ_OK);
        status |= check_status("控制预算饱和拒绝第三项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[2]), DDQ_FULL);
        status |= check_status("饱和统计", ddq_stats(queue, &stats), DDQ_OK);
        status |= check_true("饱和仍保留两项及高水位",
            seq[0] == 1ULL && seq[1] == 2ULL && seq[2] == 0ULL &&
            stats.total_items == 2ULL && stats.available_items == 2ULL &&
            stats.next_seq == 3ULL && stats.segment_count == 2U);
        status |= check_payload(queue, seq[0], payload, sizeof(payload));
        status |= check_payload(queue, seq[1], payload, sizeof(payload));
        status |= check_status("饱和后仍能领取第一项",
            ddq_claim(queue, 600000U, &claim), DDQ_OK);
        status |= check_true("第一项领取归属", claim.seq == seq[0]);
        status |= check_status("饱和后仍能确认第一项",
            ddq_ack(queue, seq[0]), DDQ_OK);
        status |= check_status("饱和后仍能领取第二项",
            ddq_claim(queue, 600000U, &claim), DDQ_OK);
        status |= check_true("第二项领取归属", claim.seq == seq[1]);
        status |= check_status("饱和后最大原因可终结",
            ddq_quarantine(queue, seq[1], reason), DDQ_OK);
        status |= check_payload(queue, seq[1], payload, sizeof(payload));
        status |= check_true("最大隔离原因留存",
            count_reason_bytes(directory, reason) == 1);
        status |= check_status("回收确认项后再次接受",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[2]), DDQ_OK);
        status |= check_true("回收后不复用序号", seq[2] == 3ULL);
        /* 期望载荷不作读取目标，长度和字节均由独立缓冲区核对。 */
        status |= check_payload(queue, seq[1], payload, sizeof(payload));
        status |= check_true("回收后最大原因仍保留",
            count_reason_bytes(directory, reason) == 1);
        status |= check_status("新项可领取",
            ddq_claim(queue, 600000U, &claim), DDQ_OK);
        status |= check_true("新项领取归属", claim.seq == seq[2]);
        status |= check_status("新项可确认", ddq_ack(queue, seq[2]), DDQ_OK);
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    remove_test_directory(directory);
    return status;
}

/* 没有任何确认时也可回收冗余领取和重试历史，仍保留原文和尝试次数。 */
static int test_retry_history_reclaim(void)
{
    char directory_template[] = "/tmp/ddq-retry-XXXXXX";
    char *directory;
    const uint8_t payload[] = {0x52U, 0x00U, 0x5cU, 0x83U};
    ddq_options_t options;
    ddq_claim_t claim;
    ddq_stats_t stats;
    ddq_t *queue;
    uint64_t sequence;
    uint64_t deadline;
    uint32_t iteration;
    int status;
    queue = NULL;
    sequence = 0ULL;
    status = 0;
    directory = mkdtemp(directory_template);
    deadline = test_deadline();
    status |= check_true("创建重试历史目录", directory != NULL);
    if (status == 0) {
        ddq_options_default(&options);
        options.segment_bytes = 256U;
        options.max_payload_bytes = 16U;
        options.max_items = 4U;
        options.max_segments = 2U;
        status |= check_status("打开无确认队列",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("接收无确认原文",
            ddq_enqueue(queue, payload, sizeof(payload), &sequence), DDQ_OK);
    }
    for (iteration = 0U; iteration < 12U && status == 0; ++iteration) {
        status |= check_true("重试历史测试未超时",
                             test_expired(deadline) == 0);
        if (status == 0) {
            status |= check_status("重复领取原项",
                ddq_claim(queue, 600000U, &claim), DDQ_OK);
            status |= check_true("重试归属和次数保留",
                claim.seq == sequence && claim.attempts == iteration);
            status |= check_status("重复重试原项",
                ddq_retry(queue, sequence, 0U), DDQ_OK);
            status |= check_payload(queue, sequence, payload,
                                    sizeof(payload));
        }
    }
    if (status == 0) {
        status |= check_status("无确认回收统计", ddq_stats(queue, &stats),
                               DDQ_OK);
        status |= check_true("无确认项未丢弃",
            stats.total_items == 1ULL && stats.acknowledged_items == 0ULL &&
            stats.available_items == 1ULL && stats.segment_count <= 2U &&
            stats.next_seq == 2ULL);
        ddq_close(queue);
        queue = NULL;
        status |= check_status("无确认回收后重开",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("重开领取原项",
            ddq_claim(queue, 600000U, &claim), DDQ_OK);
        status |= check_true("重开次数原样保留",
            claim.seq == sequence && claim.attempts == 12U);
        status |= check_status("恢复后首次确认",
            ddq_ack(queue, sequence), DDQ_OK);
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    remove_test_directory(directory);
    return status;
}

/* 混合回收保留待投递、活动租约、重试次数及隔离的原文和原因。 */
static int test_mixed_preservation(void)
{
    char directory_template[] = "/tmp/ddq-mixed-XXXXXX";
    char *directory;
    const uint8_t payload[] = {0x80U, 0x00U, 0x5cU, 0xffU};
    const char reason[] = "隔离原文必须保留\\\"证据";
    ddq_options_t options;
    ddq_claim_t claim;
    ddq_stats_t stats;
    ddq_t *queue;
    uint64_t seq[6] = {0ULL, 0ULL, 0ULL, 0ULL, 0ULL, 0ULL};
    uint64_t deadline;
    uint32_t index;
    size_t length;
    uint8_t buffer[16];
    int status;
    queue = NULL;
    status = 0;
    directory = mkdtemp(directory_template);
    deadline = test_deadline();
    status |= check_true("创建混合测试目录", directory != NULL);
    if (status == 0) {
        ddq_options_default(&options);
        options.segment_bytes = 256U;
        options.max_payload_bytes = 64U;
        options.max_items = 5U;
        /* 混合证据场景也计入最大隔离原因及快照预留，不假设控制记录免费。 */
        options.max_segments = 16U;
        status |= check_status("打开混合队列",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    for (index = 0U; index < 5U && status == 0; ++index) {
        status |= check_true("混合准备未超时",
                             test_expired(deadline) == 0);
        if (status == 0) {
            status |= check_status("准备混合原文",
                ddq_enqueue(queue, payload, sizeof(payload), &seq[index]),
                DDQ_OK);
        }
    }
    if (status == 0) {
        status |= check_status("领取可回收项", ddq_claim(queue, 600000U,
                                                       &claim), DDQ_OK);
        status |= check_status("确认可回收项", ddq_ack(queue, seq[0]),
                               DDQ_OK);
        status |= check_status("领取延迟项", ddq_claim(queue, 600000U,
                                                     &claim), DDQ_OK);
        status |= check_status("设置延迟重试", ddq_retry(queue, seq[1],
                                                         600000U), DDQ_OK);
        status |= check_status("保留活动租约", ddq_claim(queue, 600000U,
                                                       &claim), DDQ_OK);
        status |= check_true("活动租约归属", claim.seq == seq[2]);
        status |= check_status("领取隔离项", ddq_claim(queue, 600000U,
                                                     &claim), DDQ_OK);
        status |= check_status("隔离并保留原因",
            ddq_quarantine(queue, seq[3], reason), DDQ_OK);
        /* 第一次隔离提交后，重复交接不得再追加或替换原因。 */
        status |= check_status("首次隔离后幂等完成",
            ddq_quarantine(queue, seq[3], "重复原因不得覆盖"),
            DDQ_ALREADY_DONE);
        status |= check_status("领取次数项", ddq_claim(queue, 600000U,
                                                     &claim), DDQ_OK);
        status |= check_status("记录重试次数", ddq_retry(queue, seq[4], 0U),
                               DDQ_OK);
        status |= check_status("触发混合回收",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[5]), DDQ_OK);
        status |= check_true("混合回收不复用序号", seq[5] == 6ULL);
        status |= check_status("混合回收统计", ddq_stats(queue, &stats),
                               DDQ_OK);
        status |= check_true("混合状态保留",
            stats.total_items == 5ULL && stats.available_items == 3ULL &&
            stats.claimed_items == 1ULL && stats.quarantined_items == 1ULL &&
            stats.acknowledged_items == 0ULL && stats.next_seq == 7ULL);
        status |= check_status("确认项已回收",
            ddq_read(queue, seq[0], buffer, sizeof(buffer), &length),
            DDQ_NOT_FOUND);
    }
    for (index = 1U; index < 6U && status == 0; ++index) {
        status |= check_true("原文检查未超时",
                             test_expired(deadline) == 0);
        if (status == 0) {
            status |= check_payload(queue, seq[index], payload,
                                    sizeof(payload));
        }
    }
    if (status == 0) {
        status |= check_true("隔离原因原文保留",
            count_reason_bytes(directory, reason) == 1);
        status |= check_status("回收后隔离幂等完成",
            ddq_quarantine(queue, seq[3], "重复原因不得覆盖"),
            DDQ_ALREADY_DONE);
        status |= check_true("重复原因未落盘",
            count_reason_bytes(directory, "重复原因不得覆盖") == 0);
        status |= check_status("领取保留次数项",
            ddq_claim(queue, 600000U, &claim), DDQ_OK);
        status |= check_true("重试次数与顺序保留",
            claim.seq == seq[4] && claim.attempts == 1U);
        status |= check_status("确认次数项", ddq_ack(queue, seq[4]), DDQ_OK);
        status |= check_status("领取回收后新项",
            ddq_claim(queue, 600000U, &claim), DDQ_OK);
        status |= check_true("新项归属", claim.seq == seq[5]);
        status |= check_status("确认新项", ddq_ack(queue, seq[5]), DDQ_OK);
        ddq_close(queue);
        queue = NULL;
        status |= check_status("混合状态重开",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("重开后期限仍阻止领取",
            ddq_claim(queue, 600000U, &claim), DDQ_EMPTY);
        status |= check_payload(queue, seq[1], payload, sizeof(payload));
        status |= check_payload(queue, seq[2], payload, sizeof(payload));
        status |= check_payload(queue, seq[3], payload, sizeof(payload));
        status |= check_true("重开后隔离原因保留",
            count_reason_bytes(directory, reason) == 1);
        status |= check_status("重开后隔离幂等完成",
            ddq_quarantine(queue, seq[3], "重复原因不得覆盖"),
            DDQ_ALREADY_DONE);
        status |= check_true("重开后重复原因未落盘",
            count_reason_bytes(directory, "重复原因不得覆盖") == 0);
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    remove_test_directory(directory);
    return status;
}

/* 发布前故障须停止续写；发布后旧代和未发布代不能覆盖活动快照。 */
static int test_manifest_recovery(void)
{
    char directory_template[] = "/tmp/ddq-recover-XXXXXX";
    char *directory;
    char path[PATH_MAX];
    const uint8_t payload[] = {0x41U, 0x00U, 0x5cU, 0x81U};
    const uint8_t partial[] = {0xdeU, 0xadU, 0xbeU, 0xefU, 0x01U};
    const char future[] =
        "generation-00000000000000000999-segment-00000000000000000000.log";
    struct timespec pause;
    ddq_options_t options;
    ddq_claim_t claim;
    ddq_t *queue;
    uint64_t seq[3] = {0ULL, 0ULL, 0ULL};
    size_t length;
    uint8_t buffer[16];
    int written;
    int blocked;
    int status;
    queue = NULL;
    blocked = 0;
    status = 0;
    directory = mkdtemp(directory_template);
    status |= check_true("创建清单故障目录", directory != NULL);
    if (status == 0) {
        ddq_options_default(&options);
        options.segment_bytes = 256U;
        options.max_payload_bytes = 16U;
        options.max_items = 2U;
        options.max_segments = 4U;
        status |= check_status("打开清单故障队列",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("准备确认项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[0]), DDQ_OK);
        status |= check_status("准备保留项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[1]), DDQ_OK);
        status |= check_status("故障前领取", ddq_claim(queue, 600000U,
                                                     &claim), DDQ_OK);
        status |= check_status("故障前确认", ddq_ack(queue, seq[0]), DDQ_OK);
        written = snprintf(path, sizeof(path), "%s/queue.manifest.tmp",
                           directory);
        status |= check_true("故障路径有界",
            written > 0 && (size_t)written < sizeof(path));
        if (status == 0) {
            blocked = mkdir(path, 0700) == 0;
            status |= check_true("注入发布前故障", blocked != 0);
        }
    }
    if (status == 0) {
        status |= check_status("发布前失败不接受新项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[2]),
            DDQ_IO_ERROR);
        status |= check_true("失败序号不可用", seq[2] == 0ULL);
        status |= check_status("失败实例停止续写",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[2]),
            DDQ_IO_ERROR);
        /* 所有状态变更都暴露需恢复状态，不返回旧状态掩盖未知提交。 */
        status |= check_status("失败实例停止领取",
            ddq_claim(queue, 600000U, &claim), DDQ_IO_ERROR);
        status |= check_status("失败实例停止确认",
            ddq_ack(queue, seq[0]), DDQ_IO_ERROR);
        status |= check_status("失败实例停止重试",
            ddq_retry(queue, seq[1], 0U), DDQ_IO_ERROR);
        status |= check_status("失败实例停止隔离",
            ddq_quarantine(queue, seq[1], "故障状态不可写入"), DDQ_IO_ERROR);
        /* 停写同时停止公共投递读取，成功恢复后才核对原载荷。 */
        status |= check_status("失败实例停止投递读取",
            ddq_read(queue, seq[1], buffer, sizeof(buffer), &length),
            DDQ_IO_ERROR);
        status |= check_true("停写读取不返回旧长度", length == 0U);
        ddq_close(queue);
        queue = NULL;
    }
    if (blocked != 0) {
        status |= check_true("撤销本测试故障", rmdir(path) == 0);
        blocked = 0;
    }
    if (status == 0) {
        status |= check_status("故障后重开旧代",
            ddq_open(&queue, directory, &options), DDQ_OK);
        status |= check_payload(queue, seq[1], payload, sizeof(payload));
    }
    if (status == 0) {
        status |= check_status("准备过期租约", ddq_claim(queue, 1U, &claim),
                               DDQ_OK);
        status |= check_true("过期租约原车归属", claim.seq == seq[1]);
        /* 单次十毫秒等待有固定边界；中断直接失败，不形成重试循环。 */
        pause.tv_sec = 0;
        pause.tv_nsec = 10000000L;
        status |= check_true("等待租约过期", nanosleep(&pause, NULL) == 0);
        status |= check_status("故障恢复后接受新项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[2]), DDQ_OK);
        status |= check_true("故障不复用序号", seq[2] == 3ULL);
        ddq_close(queue);
        queue = NULL;
    }
    if (status == 0) {
        /* 模拟清单已发布但旧代未清理，以及下一代写到一半未发布。 */
        status |= check_status("注入未发布代",
            write_fixture(directory, future, partial, sizeof(partial)),
            DDQ_OK);
        status |= check_status("注入未发布清单",
            write_fixture(directory, "queue.manifest.tmp", partial,
                          sizeof(partial)), DDQ_OK);
        status |= check_status("注入发布后旧代",
            write_fixture(directory, "segment-00000000000000000000.log",
                          partial, sizeof(partial)), DDQ_OK);
        status |= check_status("按活动清单恢复",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_payload(queue, seq[1], payload, sizeof(payload));
        status |= check_payload(queue, seq[2], payload, sizeof(payload));
        written = snprintf(path, sizeof(path), "%s/%s", directory, future);
        status |= check_true("非活动代被回收",
            written > 0 && (size_t)written < sizeof(path) &&
            access(path, F_OK) != 0 && errno == ENOENT);
        /* 恢复本身不释放原领取状态，当前消费者可直接补过期原项的确认。 */
        status |= check_status("过期原领取可直接补确认",
            ddq_ack(queue, seq[1]), DDQ_OK);
        status |= check_status("领取后续短租约",
            ddq_claim(queue, 1U, &claim), DDQ_OK);
        status |= check_true("后续原序号不变", claim.seq == seq[2]);
        status |= check_true("等待后续租约过期", nanosleep(&pause, NULL) == 0);
        ddq_close(queue);
        queue = NULL;
        status |= check_status("后续过期租约重开",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("过期租约可重新领取",
            ddq_claim(queue, 600000U, &claim), DDQ_OK);
        status |= check_true("过期后原序号不变", claim.seq == seq[2]);
        status |= check_status("确认后续项", ddq_ack(queue, seq[2]), DDQ_OK);
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    remove_test_directory(directory);
    return status;
}

/* 选定代的清单缺失、校验错误或快照短尾都必须拒绝，不能回到旧空队列。 */
static int test_manifest_guard(void)
{
    char directory_template[] = "/tmp/ddq-guard-XXXXXX";
    char *directory;
    char manifest_path[PATH_MAX];
    char saved_path[PATH_MAX];
    char segment_path[PATH_MAX];
    uint8_t manifest[48];
    uint8_t changed;
    const uint8_t payload[] = {0x47U, 0x00U, 0x5cU, 0x82U};
    struct stat st;
    ddq_options_t options;
    ddq_claim_t claim;
    ddq_t *queue;
    uint64_t seq[3] = {0ULL, 0ULL, 0ULL};
    int written;
    int moved;
    int fd;
    int status;
    queue = NULL;
    fd = -1;
    moved = 0;
    status = 0;
    directory = mkdtemp(directory_template);
    status |= check_true("创建清单保护目录", directory != NULL);
    if (status == 0) {
        ddq_options_default(&options);
        options.segment_bytes = 256U;
        options.max_payload_bytes = 16U;
        options.max_items = 2U;
        options.max_segments = 4U;
        status |= check_status("打开清单保护队列",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("保护测试第一项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[0]), DDQ_OK);
        status |= check_status("保护测试保留项",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[1]), DDQ_OK);
        status |= check_status("保护测试领取", ddq_claim(queue, 600000U,
                                                       &claim), DDQ_OK);
        status |= check_status("保护测试确认", ddq_ack(queue, seq[0]), DDQ_OK);
        status |= check_status("保护测试回收",
            ddq_enqueue(queue, payload, sizeof(payload), &seq[2]), DDQ_OK);
        ddq_close(queue);
        queue = NULL;
        written = snprintf(manifest_path, sizeof(manifest_path),
                           "%s/queue.manifest", directory);
        status |= check_true("清单路径有界",
            written > 0 && (size_t)written < sizeof(manifest_path));
        written = snprintf(saved_path, sizeof(saved_path),
                           "%s/saved.manifest", directory);
        status |= check_true("保存路径有界",
            written > 0 && (size_t)written < sizeof(saved_path));
    }
    if (status == 0) {
        moved = rename(manifest_path, saved_path) == 0;
        status |= check_true("注入清单缺失", moved != 0);
        if (moved != 0) {
            status |= check_status("清单缺失拒绝打开",
                ddq_open(&queue, directory, &options), DDQ_CORRUPT);
            if (queue != NULL) {
                ddq_close(queue);
                queue = NULL;
            }
        }
    }
    if (moved != 0) {
        status |= check_true("恢复原清单",
            rename(saved_path, manifest_path) == 0);
        moved = 0;
    }
    if (status == 0) {
        fd = open(manifest_path, O_RDWR);
        status |= check_true("读取故障夹具清单",
            fd >= 0 && pread(fd, manifest, sizeof(manifest), 0) ==
                           (ssize_t)sizeof(manifest));
        if (status == 0) {
            /* 独立原始夹具故意改变四十字节后的校验字段，生产不按偏移解析。 */
            changed = manifest[40] ^ 1U;
            status |= check_true("注入清单校验故障",
                pwrite(fd, &changed, 1U, 40) == 1 && fsync(fd) == 0);
            status |= check_status("校验错误拒绝打开",
                ddq_open(&queue, directory, &options), DDQ_CORRUPT);
            status |= check_true("还原清单字节",
                pwrite(fd, manifest, sizeof(manifest), 0) ==
                    (ssize_t)sizeof(manifest) && fsync(fd) == 0);
        }
        if (fd >= 0) {
            close(fd);
            fd = -1;
        }
        if (queue != NULL) {
            ddq_close(queue);
            queue = NULL;
        }
    }
    if (status == 0) {
        status |= check_status("还原后重开",
            ddq_open(&queue, directory, &options), DDQ_OK);
    }
    if (status == 0) {
        status |= check_payload(queue, seq[1], payload, sizeof(payload));
        ddq_close(queue);
        queue = NULL;
        written = snprintf(segment_path, sizeof(segment_path),
            "%s/generation-00000000000000000001-segment-00000000000000000000.log",
            directory);
        status |= check_true("快照路径有界",
            written > 0 && (size_t)written < sizeof(segment_path));
        fd = open(segment_path, O_WRONLY);
        /* 默认可投递状态由五十二字节数据表达，夹具切在完整数据边界前。 */
        status |= check_true("注入已选快照短尾",
            fd >= 0 && ftruncate(fd, 51) == 0 && fsync(fd) == 0);
        status |= check_status("快照不完整拒绝恢复",
            ddq_open(&queue, directory, &options), DDQ_CORRUPT);
        status |= check_true("不完整快照未被截断伪恢复",
            fd >= 0 && fstat(fd, &st) == 0 && st.st_size == 51);
    }
    if (fd >= 0) {
        close(fd);
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    remove_test_directory(directory);
    return status;
}

int main(void)
{
    char directory_template[] = "/tmp/ddq-test-XXXXXX";
    char *directory;
    char segment_path[PATH_MAX];
    uint8_t partial_tail[7] = {0xdeU, 0xadU, 0xbeU, 0xefU, 0x01U, 0x02U,
                               0x03U};
    const char first_payload[] = "alpha";
    const char second_payload[] = "beta";
    const char third_payload[] = "gamma";
    char buffer[32];
    size_t payload_length;
    uint64_t first_seq;
    uint64_t second_seq;
    uint64_t third_seq;
    ddq_options_t options;
    ddq_claim_t claim;
    ddq_stats_t stats;
    ddq_t *queue;
    ddq_t *second_queue;
    int segment_fd;
    ssize_t written;
    int status;

    directory = NULL;
    queue = NULL;
    second_queue = NULL;
    segment_fd = -1;
    status = 0;
    directory = mkdtemp(directory_template);
    status |= check_true("mkdtemp", directory != NULL);
    if (status == 0) {
        status |= check_status("options", ddq_options_default(&options),
                               DDQ_OK);
        options.segment_bytes = 256U;
        options.max_payload_bytes = 128U;
        options.max_items = 64U;
        /* 原状态回归按新准入规则保留最大原因控制空间。 */
        options.max_segments = 16U;
        status |= check_status("open", ddq_open(&queue, directory, &options),
                               DDQ_OK);
    }
    if (status == 0) {
        status |= check_status(
            "second open lock", ddq_open(&second_queue, directory, &options),
            DDQ_LOCKED);
        status |= check_status(
            "enqueue first",
            ddq_enqueue(queue, first_payload, strlen(first_payload),
                        &first_seq),
            DDQ_OK);
        status |= check_status(
            "enqueue second",
            ddq_enqueue(queue, second_payload, strlen(second_payload),
                        &second_seq),
            DDQ_OK);
        status |= check_status(
            "enqueue third",
            ddq_enqueue(queue, third_payload, strlen(third_payload),
                        &third_seq),
            DDQ_OK);
    }
    if (status == 0) {
        status |= check_status("claim first", ddq_claim(queue, 100000U, &claim),
                               DDQ_OK);
        status |= check_true("claim first sequence", claim.seq == first_seq);
        status |= check_status(
            "read first",
            ddq_read(queue, first_seq, buffer, sizeof(buffer),
                     &payload_length),
            DDQ_OK);
        status |= check_true(
            "read first bytes",
            payload_length == strlen(first_payload) &&
                memcmp(buffer, first_payload, payload_length) == 0);
        status |= check_status("ack first", ddq_ack(queue, first_seq), DDQ_OK);
        status |= check_status("retry second", ddq_claim(queue, 100000U,
                                                          &claim),
                               DDQ_OK);
        status |= check_true("claim second sequence", claim.seq == second_seq);
        status |= check_status("retry second now", ddq_retry(queue, second_seq,
                                                              0U),
                               DDQ_OK);
        status |= check_status("claim second again",
                               ddq_claim(queue, 100000U, &claim), DDQ_OK);
        status |= check_true("claim second again sequence",
                             claim.seq == second_seq);
        status |= check_status("quarantine second",
                               ddq_quarantine(queue, second_seq,
                                              "permanent test failure"),
                               DDQ_OK);
        status |= check_status("stats before reopen", ddq_stats(queue, &stats),
                               DDQ_OK);
        status |= check_true(
            "stats before reopen values",
            stats.available_items == 1ULL &&
                stats.claimed_items == 0ULL &&
                stats.acknowledged_items == 1ULL &&
                stats.quarantined_items == 1ULL);
        ddq_close(queue);
        queue = NULL;
    }
    if (status == 0) {
        status |= check_status("reopen", ddq_open(&queue, directory, &options),
                               DDQ_OK);
        status |= check_status("claim third after reopen",
                               ddq_claim(queue, 100000U, &claim), DDQ_OK);
        status |= check_true("claim third sequence", claim.seq == third_seq);
        status |= check_status("ack third after reopen",
                               ddq_ack(queue, third_seq), DDQ_OK);
        status |= check_status("stats after reopen", ddq_stats(queue, &stats),
                               DDQ_OK);
        status |= check_true(
            "stats after reopen values",
            stats.available_items == 0ULL &&
                stats.claimed_items == 0ULL &&
                stats.acknowledged_items == 2ULL &&
                stats.quarantined_items == 1ULL);
        ddq_close(queue);
        queue = NULL;
    }
    if (status == 0) {
        status |= check_status("find last segment",
                               find_last_segment(directory, segment_path,
                                                 sizeof(segment_path)),
                               DDQ_OK);
        if (status == 0) {
            segment_fd = open(segment_path, O_WRONLY | O_APPEND);
            status |= check_true("open tail segment", segment_fd >= 0);
        }
        if (status == 0) {
            written = write(segment_fd, partial_tail, sizeof(partial_tail));
            status |= check_true("write partial tail",
                                 written == (ssize_t)sizeof(partial_tail));
            close(segment_fd);
            segment_fd = -1;
        }
    }
    if (status == 0) {
        status |= check_status("reopen after torn tail",
                               ddq_open(&queue, directory, &options), DDQ_OK);
        status |= check_status("stats after torn tail",
                               ddq_stats(queue, &stats), DDQ_OK);
        status |= check_true("tail was recovered",
                             stats.recovered_tail_bytes >=
                                 sizeof(partial_tail));
        status |= check_status("empty after all messages",
                               ddq_claim(queue, 100000U, &claim), DDQ_EMPTY);
    }
    if (segment_fd >= 0) {
        close(segment_fd);
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    remove_test_directory(directory);
    if (status == 0) {
        /* 容量回收测试独立于旧状态测试，均由各自临时目录负责清理。 */
        status |= test_capacity_reuse(2U);
        status |= test_capacity_reuse(32U);
        status |= test_control_headroom(1U);
        status |= test_control_headroom(8U);
        status |= test_retry_history_reclaim();
        status |= test_mixed_preservation();
        status |= test_manifest_recovery();
        status |= test_manifest_guard();
    }
    if (status == 0) {
        printf("DDQ 测试通过\n");
    }
    return status;
}
