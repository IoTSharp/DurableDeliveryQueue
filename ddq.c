#define _POSIX_C_SOURCE 200809L

#include "ddq.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef O_DIRECTORY
#define O_DIRECTORY 0
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define DDQ_MAGIC 0x31475144U
#define DDQ_VERSION 1U
#define DDQ_COMMIT 0x54494D43U
#define DDQ_MANIFEST_MAGIC 0x4d514444U
#define DDQ_MANIFEST_VERSION 1U
#define DDQ_OPERATION_TIMEOUT_MS 120000ULL
#define DDQ_IO_TIMEOUT_MS 30000ULL

/* 线缆结构只使用单字节数组，禁止依赖主机端序、对齐或结构体填充。 */
typedef struct {
    uint8_t version[2];
    uint8_t type[2];
    uint8_t seq[8];
    uint8_t arg[8];
    uint8_t payload_length[4];
    uint8_t record_length[4];
} ddq_record_fields_wire_t;

typedef struct {
    uint8_t magic[4];
    ddq_record_fields_wire_t fields;
    uint8_t crc[4];
    uint8_t reserved[4];
} ddq_record_header_wire_t;

typedef struct {
    uint8_t commit[4];
    uint8_t record_length[4];
} ddq_record_footer_wire_t;

typedef struct {
    uint8_t state[4];
    uint8_t attempts[4];
    uint8_t lease_until_ms[8];
    uint8_t not_before_ms[8];
} ddq_state_wire_t;

typedef struct {
    uint8_t magic[4];
    uint8_t version[2];
    uint8_t bytes[2];
    uint8_t generation[8];
    uint8_t next_seq[8];
    uint8_t segment_count[4];
    uint8_t item_count[4];
    uint8_t end_offset[8];
} ddq_manifest_body_wire_t;

typedef struct {
    ddq_manifest_body_wire_t body;
    uint8_t crc[4];
    uint8_t reserved[4];
} ddq_manifest_wire_t;

/* 固定线缆尺寸必须在编译期核对，诊断仅说明结构及长度约束。 */
_Static_assert(sizeof(ddq_record_fields_wire_t) == 28U, "记录字段尺寸必须为28字节");
_Static_assert(sizeof(ddq_record_header_wire_t) == 40U, "记录头尺寸必须为40字节");
_Static_assert(sizeof(ddq_record_footer_wire_t) == 8U, "提交尾尺寸必须为8字节");
_Static_assert(sizeof(ddq_state_wire_t) == 24U, "状态快照尺寸必须为24字节");
_Static_assert(sizeof(ddq_manifest_body_wire_t) == 40U, "代清单业务体尺寸必须为40字节");
_Static_assert(sizeof(ddq_manifest_wire_t) == 48U, "代清单尺寸必须为48字节");

#define DDQ_HEADER_SIZE ((uint32_t)sizeof(ddq_record_header_wire_t))
#define DDQ_FOOTER_SIZE ((uint32_t)sizeof(ddq_record_footer_wire_t))
#define DDQ_MIN_RECORD_SIZE (DDQ_HEADER_SIZE + DDQ_FOOTER_SIZE)
#define DDQ_DEFAULT_SEGMENT_BYTES (4U * 1024U * 1024U)
#define DDQ_DEFAULT_MAX_PAYLOAD (1U * 1024U * 1024U)
#define DDQ_DEFAULT_MAX_ITEMS 100000U
#define DDQ_DEFAULT_MAX_SEGMENTS 1024U
#define DDQ_MIN_SEGMENT_BYTES 256U
#define DDQ_MAX_SEGMENT_BYTES (64U * 1024U * 1024U)
#define DDQ_MAX_ITEMS_LIMIT 1000000U
#define DDQ_MAX_SEGMENTS_LIMIT 4096U
#define DDQ_MAX_REASON_BYTES 4096U
#define DDQ_IO_ITER_LIMIT 1048576U
#define DDQ_SCAN_RECORD_LIMIT 4000000U
#define DDQ_MAX_LEASE_MS (7ULL * 24ULL * 60ULL * 60ULL * 1000ULL)

/* 记录采用定宽小端头部、CRC 和提交尾标记，恢复只接受完整提交记录。 */

enum {
    DDQ_RECORD_DATA = 1,
    DDQ_RECORD_CLAIM = 2,
    DDQ_RECORD_ACK = 3,
    DDQ_RECORD_RETRY = 4,
    DDQ_RECORD_QUARANTINE = 5,
    DDQ_RECORD_STATE = 6
};

enum {
    DDQ_ITEM_AVAILABLE = 1,
    DDQ_ITEM_CLAIMED = 2,
    DDQ_ITEM_ACKED = 3,
    DDQ_ITEM_QUARANTINED = 4
};

typedef struct {
    uint64_t id;
    char path[PATH_MAX];
    uint64_t size;
} ddq_segment_t;

typedef struct {
    uint64_t seq;
    uint32_t payload_len;
    uint32_t attempts;
    uint32_t segment_index;
    uint64_t offset;
    uint64_t lease_until_ms;
    uint64_t not_before_ms;
    uint32_t reason_segment_index;
    uint32_t reason_length;
    uint64_t reason_offset;
    uint8_t state;
    uint8_t snapshot_seen;
    uint8_t reason_recorded;
} ddq_item_t;

/* 容量预检复用追加记录的分段规则，不实际创建文件或修改条目。 */
typedef struct {
    uint32_t segment_count;
    uint64_t offset;
} ddq_capacity_plan_t;

struct ddq {
    int lock_fd;
    int current_fd;
    char directory[PATH_MAX];
    ddq_options_t options;
    ddq_segment_t *segments;
    uint32_t segment_count;
    ddq_item_t *items;
    uint32_t item_count;
    uint32_t item_capacity;
    uint64_t next_seq;
    uint64_t current_offset;
    uint64_t recovered_tail_bytes;
    uint64_t generation;
    uint64_t checkpoint_next_seq;
    uint64_t checkpoint_offset;
    uint32_t checkpoint_segments;
    uint32_t checkpoint_items;
    int checkpoint_seen;
    int has_manifest;
    int staging;
};

/* 容量回收只复制未确认和隔离记录，不增加新的公共投递接口。 */
static int compact_queue(ddq_t *queue, uint32_t reserve_bytes,
                         uint32_t required_items);

static void put_u16(uint8_t *dst, uint16_t value)
{
    dst[0] = (uint8_t)(value & 0xffU);
    dst[1] = (uint8_t)((value >> 8) & 0xffU);
}

static void put_u32(uint8_t *dst, uint32_t value)
{
    dst[0] = (uint8_t)(value & 0xffU);
    dst[1] = (uint8_t)((value >> 8) & 0xffU);
    dst[2] = (uint8_t)((value >> 16) & 0xffU);
    dst[3] = (uint8_t)((value >> 24) & 0xffU);
}

static void put_u64(uint8_t *dst, uint64_t value)
{
    uint32_t index;
    for (index = 0U; index < 8U; ++index) {
        dst[index] = (uint8_t)((value >> (index * 8U)) & 0xffU);
    }
}

static uint16_t get_u16(const uint8_t *src)
{
    uint16_t value;
    value = (uint16_t)src[0] | (uint16_t)((uint16_t)src[1] << 8);
    return value;
}

static uint32_t get_u32(const uint8_t *src)
{
    uint32_t value;
    value = (uint32_t)src[0] |
            ((uint32_t)src[1] << 8) |
            ((uint32_t)src[2] << 16) |
            ((uint32_t)src[3] << 24);
    return value;
}

static uint64_t get_u64(const uint8_t *src)
{
    uint64_t value;
    uint32_t index;
    value = 0ULL;
    for (index = 0U; index < 8U; ++index) {
        value |= (uint64_t)src[index] << (index * 8U);
    }
    return value;
}

static uint32_t crc32_update(uint32_t crc, const uint8_t *data, size_t length)
{
    size_t index;
    uint32_t bit;
    for (index = 0U; index < length; ++index) {
        crc ^= data[index];
        for (bit = 0U; bit < 8U; ++bit) {
            if ((crc & 1U) != 0U) {
                crc = (crc >> 1) ^ 0xedb88320U;
            } else {
                crc >>= 1;
            }
        }
    }
    return crc;
}

static uint32_t record_crc(const ddq_record_header_wire_t *header,
                           const uint8_t *payload, uint32_t payload_len)
{
    /* 校验范围保持旧版字节兼容，字段通过命名结构访问。 */
    uint32_t crc;
    crc = 0xffffffffU;
    crc = crc32_update(crc, (const uint8_t *)&header->fields,
                       sizeof(header->fields));
    crc = crc32_update(crc, header->reserved, sizeof(header->reserved));
    crc = crc32_update(crc, payload, payload_len);
    crc ^= 0xffffffffU;
    return crc;
}

/* 单调时钟仅限制本次工作，持久租约继续使用原有实时时钟。 */
static uint64_t operation_deadline(uint64_t timeout_ms)
{
    struct timespec ts;
    uint64_t deadline;
    uint64_t now;
    deadline = 0ULL;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0 && ts.tv_sec >= 0) {
        now = (uint64_t)ts.tv_sec * 1000ULL +
              (uint64_t)ts.tv_nsec / 1000000ULL;
        if (now <= UINT64_MAX - timeout_ms) {
            deadline = now + timeout_ms;
        }
    }
    return deadline;
}

/* 时钟失败或超时均停止新增操作，调用方可取消后重新恢复。 */
static int deadline_expired(uint64_t deadline)
{
    struct timespec ts;
    uint64_t now;
    int expired;
    expired = 1;
    if (deadline != 0ULL && clock_gettime(CLOCK_MONOTONIC, &ts) == 0 &&
        ts.tv_sec >= 0) {
        now = (uint64_t)ts.tv_sec * 1000ULL +
              (uint64_t)ts.tv_nsec / 1000000ULL;
        expired = now >= deadline;
    }
    return expired;
}

static uint64_t wall_clock_ms(void)
{
    struct timespec ts;
    uint64_t value;
    value = 0ULL;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
        value = (uint64_t)ts.tv_sec * 1000ULL;
        value += (uint64_t)ts.tv_nsec / 1000000ULL;
    }
    return value;
}

static int write_full(int fd, const uint8_t *data, size_t length,
                      size_t *out_written)
{
    /* 限制分次写入次数和时间；中断作为取消失败交由恢复处理。 */
    size_t written;
    uint32_t iteration;
    uint64_t deadline;
    ssize_t result;
    int status;
    written = 0U;
    status = DDQ_OK;
    deadline = operation_deadline(DDQ_IO_TIMEOUT_MS);
    for (iteration = 0U;
         written < length && iteration < DDQ_IO_ITER_LIMIT;
         ++iteration) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
            break;
        }
        result = write(fd, data + written, length - written);
        if (result < 0) {
            status = DDQ_IO_ERROR;
            break;
        }
        if (result == 0) {
            status = DDQ_IO_ERROR;
            break;
        }
        written += (size_t)result;
    }
    if (written < length && status == DDQ_OK) {
        status = DDQ_LIMIT;
    }
    if (out_written != NULL) {
        *out_written = written;
    }
    return status;
}

static int pread_full(int fd, uint8_t *data, size_t length, uint64_t offset)
{
    /* 短读受项目数和单调时钟限制，信号中断不会形成忙重试。 */
    size_t read_bytes;
    uint32_t iteration;
    uint64_t deadline;
    ssize_t result;
    int status;
    read_bytes = 0U;
    status = DDQ_OK;
    deadline = operation_deadline(DDQ_IO_TIMEOUT_MS);
    for (iteration = 0U;
         read_bytes < length && iteration < DDQ_IO_ITER_LIMIT;
         ++iteration) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
            break;
        }
        result = pread(fd, data + read_bytes, length - read_bytes,
                       (off_t)(offset + read_bytes));
        if (result < 0) {
            status = DDQ_IO_ERROR;
            break;
        }
        if (result == 0) {
            status = DDQ_IO_ERROR;
            break;
        }
        read_bytes += (size_t)result;
    }
    if (read_bytes < length && status == DDQ_OK) {
        status = DDQ_LIMIT;
    }
    return status;
}

static int make_segment_path(char *path, size_t capacity,
                             const char *directory, uint64_t generation,
                             uint64_t id)
{
    /* 零代保留旧文件名，新代文件只有清单发布后才成为恢复依据。 */
    int written;
    int status;
    if (generation == 0ULL) {
        written = snprintf(path, capacity, "%s/segment-%020llu.log",
                           directory, (unsigned long long)id);
    } else {
        written = snprintf(path, capacity,
                           "%s/generation-%020llu-segment-%020llu.log",
                           directory, (unsigned long long)generation,
                           (unsigned long long)id);
    }
    status = DDQ_OK;
    if (written < 0 || (size_t)written >= capacity) {
        status = DDQ_TOO_LARGE;
    }
    return status;
}

/* 文件名中的整数只接受完整二十位十进制，末位也须检查溢出。 */
static int parse_decimal_20(const char *digits, uint64_t *out_value)
{
    size_t index;
    uint64_t digit;
    uint64_t value;
    uint64_t deadline;
    int status;
    value = 0ULL;
    status = DDQ_OK;
    deadline = operation_deadline(DDQ_IO_TIMEOUT_MS);
    for (index = 0U; index < 20U && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else if (digits[index] < '0' || digits[index] > '9') {
            status = DDQ_CORRUPT;
        } else {
            digit = (uint64_t)(digits[index] - '0');
            if (value > (UINT64_MAX - digit) / 10ULL) {
                status = DDQ_CORRUPT;
            } else {
                value = value * 10ULL + digit;
            }
        }
    }
    if (status == DDQ_OK) {
        *out_value = value;
    }
    return status;
}

/* 按完整文件名识别队列自有段，清理时不碰其他文件。 */
static int parse_segment_name(const char *name, uint64_t *out_generation,
                              uint64_t *out_id)
{
    const size_t old_prefix = sizeof("segment-") - 1U;
    const size_t gen_prefix = sizeof("generation-") - 1U;
    const size_t middle = sizeof("-segment-") - 1U;
    const size_t suffix = sizeof(".log") - 1U;
    size_t length;
    uint64_t generation;
    uint64_t id;
    int status;
    generation = 0ULL;
    id = 0ULL;
    status = DDQ_CORRUPT;
    length = strnlen(name, PATH_MAX);
    if (length == old_prefix + 20U + suffix &&
        memcmp(name, "segment-", old_prefix) == 0 &&
        memcmp(name + old_prefix + 20U, ".log", suffix) == 0) {
        status = parse_decimal_20(name + old_prefix, &id);
    } else if (length == gen_prefix + 20U + middle + 20U + suffix &&
               memcmp(name, "generation-", gen_prefix) == 0 &&
               memcmp(name + gen_prefix + 20U, "-segment-", middle) == 0 &&
               memcmp(name + gen_prefix + 20U + middle + 20U,
                      ".log", suffix) == 0) {
        status = parse_decimal_20(name + gen_prefix, &generation);
        if (status == DDQ_OK && generation != 0ULL) {
            status = parse_decimal_20(name + gen_prefix + 20U + middle,
                                      &id);
        } else {
            status = DDQ_CORRUPT;
        }
    }
    if (status == DDQ_OK) {
        *out_generation = generation;
        *out_id = id;
    }
    return status;
}

static int compare_segments(const void *left, const void *right)
{
    const ddq_segment_t *a;
    const ddq_segment_t *b;
    int result;
    a = (const ddq_segment_t *)left;
    b = (const ddq_segment_t *)right;
    result = 0;
    if (a->id < b->id) {
        result = -1;
    } else if (a->id > b->id) {
        result = 1;
    }
    return result;
}

static int validate_options(ddq_options_t *options)
{
    /* 载荷长度写入定宽字段前必须拒绝截断，不能仅依赖段大小限制。 */
    int status;
    status = DDQ_OK;
    if (options == NULL ||
        options->segment_bytes < DDQ_MIN_SEGMENT_BYTES ||
        options->segment_bytes > DDQ_MAX_SEGMENT_BYTES ||
        options->max_payload_bytes == 0U ||
        options->max_payload_bytes > UINT32_MAX ||
        (options->segment_bytes >= DDQ_MIN_RECORD_SIZE &&
         options->max_payload_bytes >
             options->segment_bytes - DDQ_MIN_RECORD_SIZE) ||
        options->max_items == 0U ||
        options->max_items > DDQ_MAX_ITEMS_LIMIT ||
        options->max_segments == 0U ||
        options->max_segments > DDQ_MAX_SEGMENTS_LIMIT) {
        status = DDQ_INVALID_ARGUMENT;
    }
    return status;
}

static int ensure_item_capacity(ddq_t *queue, uint32_t needed)
{
    /* 元数据扩容同时受次数和单调时间约束，超限不发布新容量。 */
    uint32_t capacity;
    uint32_t growth;
    uint64_t deadline;
    ddq_item_t *items;
    int status;
    capacity = queue->item_capacity;
    status = DDQ_OK;
    deadline = operation_deadline(DDQ_IO_TIMEOUT_MS);
    if (needed > queue->options.max_items) {
        status = DDQ_FULL;
    } else if (capacity < needed) {
        if (capacity == 0U) {
            capacity = 64U;
        }
        for (growth = 0U;
             growth < 32U && capacity < needed && status == DDQ_OK;
             ++growth) {
            if (deadline_expired(deadline) != 0) {
                status = DDQ_LIMIT;
            } else if (capacity > queue->options.max_items / 2U) {
                capacity = queue->options.max_items;
            } else {
                capacity *= 2U;
            }
        }
        if (status == DDQ_OK && capacity < needed) {
            status = DDQ_LIMIT;
        } else if (status == DDQ_OK) {
            items = (ddq_item_t *)realloc(
                queue->items, (size_t)capacity * sizeof(*items));
            if (items == NULL) {
                status = DDQ_NO_MEMORY;
            } else {
                queue->items = items;
                queue->item_capacity = capacity;
            }
        }
    }
    return status;
}

static int find_item(const ddq_t *queue, uint64_t seq)
{
    /* 数据序号严格递增，二分定位避免大队列重放时二次方扫描。 */
    uint32_t low;
    uint32_t high;
    uint32_t middle;
    uint32_t iteration;
    uint64_t deadline;
    int result;
    result = -1;
    low = 0U;
    high = queue->item_count;
    deadline = operation_deadline(DDQ_IO_TIMEOUT_MS);
    for (iteration = 0U; iteration < 32U && low < high; ++iteration) {
        if (deadline_expired(deadline) != 0) {
            break;
        }
        middle = low + (high - low) / 2U;
        if (queue->items[middle].seq == seq) {
            result = (int)middle;
            break;
        } else if (queue->items[middle].seq < seq) {
            low = middle + 1U;
        } else {
            high = middle;
        }
    }
    return result;
}

static int sync_directory(const char *directory)
{
    int fd;
    int status;
    fd = open(directory, O_RDONLY | O_DIRECTORY);
    status = DDQ_OK;
    if (fd < 0) {
        status = DDQ_IO_ERROR;
    } else {
        if (fsync(fd) != 0) {
            status = DDQ_IO_ERROR;
        }
        close(fd);
    }
    return status;
}

static int ensure_directory(const char *directory)
{
    struct stat st;
    int status;
    status = DDQ_OK;
    if (stat(directory, &st) != 0) {
        /* 队列载荷可能包含车辆和交易信息，新建目录只允许运行用户访问。 */
        if (errno == ENOENT && mkdir(directory, 0700) == 0) {
            status = DDQ_OK;
        } else {
            status = DDQ_IO_ERROR;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        status = DDQ_INVALID_ARGUMENT;
    }
    return status;
}

/* 元数据名称由队列固定定义，不接受外部路径片段。 */
static int make_metadata_path(char *path, size_t capacity,
                              const char *directory, const char *name)
{
    int written;
    int status;
    status = DDQ_OK;
    written = snprintf(path, capacity, "%s/%s", directory, name);
    if (written < 0 || (size_t)written >= capacity) {
        status = DDQ_TOO_LARGE;
    }
    return status;
}

/* 标记存在时缺失清单必须停机，不能把已压缩队列误认为新空队列。 */
static int load_manifest(ddq_t *queue)
{
    char path[PATH_MAX];
    char marker_path[PATH_MAX];
    struct stat st;
    ddq_manifest_wire_t manifest;
    uint8_t bytes[sizeof(manifest)];
    uint32_t crc;
    int marker;
    int fd;
    int status;
    fd = -1;
    marker = 0;
    status = make_metadata_path(path, sizeof(path), queue->directory,
                                "queue.manifest");
    queue->generation = 0ULL;
    queue->checkpoint_next_seq = 1ULL;
    queue->checkpoint_segments = 0U;
    queue->checkpoint_items = 0U;
    queue->checkpoint_offset = 0ULL;
    queue->checkpoint_seen = 1;
    queue->has_manifest = 0;
    if (status == DDQ_OK) {
        status = make_metadata_path(marker_path, sizeof(marker_path),
                                    queue->directory, "queue.format");
    }
    if (status == DDQ_OK) {
        if (lstat(marker_path, &st) == 0) {
            if (!S_ISREG(st.st_mode) || st.st_size != 0) {
                status = DDQ_CORRUPT;
            } else {
                marker = 1;
            }
        } else if (errno != ENOENT) {
            status = DDQ_IO_ERROR;
        }
    }
    if (status == DDQ_OK) {
        fd = open(path, O_RDONLY);
        if (fd < 0) {
            if (errno != ENOENT) {
                status = DDQ_IO_ERROR;
            } else if (marker != 0) {
                status = DDQ_CORRUPT;
            }
        } else if (fstat(fd, &st) != 0) {
            status = DDQ_IO_ERROR;
        } else if (!S_ISREG(st.st_mode) ||
                   st.st_size != (off_t)sizeof(manifest)) {
            status = DDQ_CORRUPT;
        } else {
            status = pread_full(fd, bytes, sizeof(bytes), 0ULL);
            if (status == DDQ_OK) {
                memcpy(&manifest, bytes, sizeof(manifest));
                crc = crc32_update(0xffffffffU,
                    (const uint8_t *)&manifest.body,
                    sizeof(manifest.body)) ^ 0xffffffffU;
                if (get_u32(manifest.body.magic) != DDQ_MANIFEST_MAGIC ||
                    get_u16(manifest.body.version) != DDQ_MANIFEST_VERSION ||
                    get_u16(manifest.body.bytes) != sizeof(manifest) ||
                    get_u32(manifest.reserved) != 0U ||
                    get_u32(manifest.crc) != crc) {
                    status = DDQ_CORRUPT;
                } else {
                    queue->generation = get_u64(manifest.body.generation);
                    queue->checkpoint_next_seq =
                        get_u64(manifest.body.next_seq);
                    queue->checkpoint_segments =
                        get_u32(manifest.body.segment_count);
                    queue->checkpoint_items =
                        get_u32(manifest.body.item_count);
                    queue->checkpoint_offset =
                        get_u64(manifest.body.end_offset);
                    if (queue->checkpoint_next_seq == 0ULL ||
                        queue->checkpoint_segments >
                            queue->options.max_segments ||
                        queue->checkpoint_items > queue->options.max_items ||
                        queue->checkpoint_offset >
                            queue->options.segment_bytes ||
                        queue->checkpoint_segments == 0U ||
                        (queue->generation != 0ULL &&
                         (marker == 0 || queue->checkpoint_segments == 0U))) {
                        status = DDQ_CORRUPT;
                    } else {
                        queue->has_manifest = 1;
                        queue->checkpoint_seen = 0;
                    }
                }
            }
        }
    }
    if (fd >= 0) {
        close(fd);
    }
    return status;
}

/* 清单先写临时文件并同步，再原子替换；目录同步失败不得删旧代。 */
static int publish_manifest(const ddq_t *snapshot, int *out_published)
{
    char path[PATH_MAX];
    char temp_path[PATH_MAX];
    ddq_manifest_wire_t manifest;
    size_t written;
    int fd;
    int created;
    int status;
    fd = -1;
    created = 0;
    *out_published = 0;
    memset(&manifest, 0, sizeof(manifest));
    put_u32(manifest.body.magic, DDQ_MANIFEST_MAGIC);
    put_u16(manifest.body.version, DDQ_MANIFEST_VERSION);
    put_u16(manifest.body.bytes, (uint16_t)sizeof(manifest));
    put_u64(manifest.body.generation, snapshot->generation);
    put_u64(manifest.body.next_seq, snapshot->next_seq);
    put_u32(manifest.body.segment_count, snapshot->segment_count);
    put_u32(manifest.body.item_count, snapshot->item_count);
    put_u64(manifest.body.end_offset, snapshot->current_offset);
    put_u32(manifest.crc, crc32_update(0xffffffffU,
        (const uint8_t *)&manifest.body, sizeof(manifest.body)) ^
        0xffffffffU);
    status = make_metadata_path(path, sizeof(path), snapshot->directory,
                                "queue.manifest");
    if (status == DDQ_OK) {
        status = make_metadata_path(temp_path, sizeof(temp_path),
                                    snapshot->directory,
                                    "queue.manifest.tmp");
    }
    if (status == DDQ_OK) {
        fd = open(temp_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (fd < 0) {
            status = DDQ_IO_ERROR;
        } else {
            created = 1;
            status = write_full(fd, (const uint8_t *)&manifest,
                                sizeof(manifest), &written);
            if (status == DDQ_OK && fsync(fd) != 0) {
                status = DDQ_IO_ERROR;
            }
            if (close(fd) != 0 && status == DDQ_OK) {
                status = DDQ_IO_ERROR;
            }
            fd = -1;
        }
    }
    if (status == DDQ_OK) {
        if (rename(temp_path, path) != 0) {
            status = DDQ_IO_ERROR;
        } else {
            *out_published = 1;
            status = sync_directory(snapshot->directory);
        }
    }
    if (created != 0 && *out_published == 0) {
        if (unlink(temp_path) != 0 ||
            sync_directory(snapshot->directory) != DDQ_OK) {
            status = DDQ_IO_ERROR;
        }
    }
    return status;
}

/* 先保留零代清单，再创建持久标记，迁移中断仍能恢复旧格式日志。 */
static int prepare_manifest_format(ddq_t *queue)
{
    char path[PATH_MAX];
    struct stat st;
    int published;
    int fd;
    int status;
    fd = -1;
    published = 0;
    status = DDQ_OK;
    if (queue->has_manifest == 0) {
        status = publish_manifest(queue, &published);
        if (status == DDQ_OK) {
            queue->has_manifest = 1;
        }
    }
    if (status == DDQ_OK) {
        status = make_metadata_path(path, sizeof(path), queue->directory,
                                    "queue.format");
    }
    if (status == DDQ_OK) {
        if (lstat(path, &st) == 0) {
            if (!S_ISREG(st.st_mode) || st.st_size != 0) {
                status = DDQ_CORRUPT;
            }
        } else if (errno != ENOENT) {
            status = DDQ_IO_ERROR;
        } else {
            fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
            if (fd < 0 || fsync(fd) != 0) {
                status = DDQ_IO_ERROR;
            }
            if (fd >= 0) {
                close(fd);
                fd = -1;
            }
            if (status == DDQ_OK) {
                status = sync_directory(queue->directory);
            }
        }
    }
    if (status != DDQ_OK && queue->current_fd >= 0) {
        close(queue->current_fd);
        queue->current_fd = -1;
    }
    return status;
}

/* 仅在选定代已完整恢复或发布后，清除队列命名的非活动代和临时清单。 */
static int clean_obsolete_files(const ddq_t *queue)
{
    DIR *directory;
    struct dirent *entry;
    struct stat st;
    char path[PATH_MAX];
    uint64_t generation;
    uint64_t id;
    uint64_t deadline;
    uint32_t iteration;
    uint32_t limit;
    int done;
    int removed;
    int obsolete;
    int status;
    directory = NULL;
    done = 0;
    removed = 0;
    status = DDQ_OK;
    limit = queue->options.max_segments * 8U + 1024U;
    deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
    directory = opendir(queue->directory);
    if (directory == NULL) {
        status = DDQ_IO_ERROR;
    } else {
        for (iteration = 0U; iteration < limit && status == DDQ_OK;
             ++iteration) {
            if (deadline_expired(deadline) != 0) {
                status = DDQ_LIMIT;
                break;
            }
            errno = 0;
            entry = readdir(directory);
            if (entry == NULL) {
                if (errno == 0) {
                    done = 1;
                } else {
                    status = DDQ_IO_ERROR;
                }
                break;
            }
            obsolete = strcmp(entry->d_name, "queue.manifest.tmp") == 0;
            if (parse_segment_name(entry->d_name, &generation, &id) ==
                    DDQ_OK && generation != queue->generation) {
                obsolete = 1;
            }
            if (obsolete != 0) {
                status = make_metadata_path(path, sizeof(path),
                                            queue->directory, entry->d_name);
                if (status == DDQ_OK) {
                    if (lstat(path, &st) != 0) {
                        status = DDQ_IO_ERROR;
                    } else if (!S_ISREG(st.st_mode)) {
                        status = DDQ_CORRUPT;
                    } else if (unlink(path) != 0) {
                        status = DDQ_IO_ERROR;
                    } else {
                        removed = 1;
                    }
                }
            }
        }
        closedir(directory);
        if (status == DDQ_OK && done == 0) {
            status = DDQ_LIMIT;
        }
    }
    if (removed != 0 && sync_directory(queue->directory) != DDQ_OK) {
        status = DDQ_IO_ERROR;
    }
    return status;
}

static int create_segment(ddq_t *queue, uint64_t id)
{
    /* 暂存代由最终清单发布前统一同步目录，运行代仍逐段同步。 */
    char path[PATH_MAX];
    int fd;
    int status;
    status = DDQ_OK;
    fd = -1;
    if (queue->segment_count >= queue->options.max_segments) {
        status = DDQ_LIMIT;
    } else if (make_segment_path(path, sizeof(path), queue->directory,
                                 queue->generation, id) !=
               DDQ_OK) {
        status = DDQ_TOO_LARGE;
    } else {
        /* 段文件和恢复锁只保存本进程的投递数据，禁止其他用户读取。 */
        fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_APPEND, 0600);
        if (fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (queue->staging == 0 &&
                   sync_directory(queue->directory) != DDQ_OK) {
            status = DDQ_IO_ERROR;
        }
    }
    if (status == DDQ_OK) {
        memcpy(queue->segments[queue->segment_count].path, path,
               strlen(path) + 1U);
        queue->segments[queue->segment_count].id = id;
        queue->segments[queue->segment_count].size = 0ULL;
        queue->segment_count += 1U;
        queue->current_fd = fd;
        queue->current_offset = 0ULL;
    } else if (fd >= 0) {
        close(fd);
    }
    return status;
}

static int rotate_segment(ddq_t *queue)
{
    /* 达到段上限时保留当前描述符，由回收或恢复决定后续写入。 */
    uint64_t next_id;
    int status;
    status = DDQ_OK;
    next_id = 0ULL;
    if (queue->current_fd < 0 || queue->segment_count == 0U) {
        status = DDQ_IO_ERROR;
    } else if (queue->segment_count >= queue->options.max_segments) {
        status = DDQ_LIMIT;
    } else if (fsync(queue->current_fd) != 0) {
        status = DDQ_IO_ERROR;
    } else {
        close(queue->current_fd);
        queue->current_fd = -1;
        next_id = queue->segments[queue->segment_count - 1U].id;
        if (next_id == UINT64_MAX) {
            status = DDQ_LIMIT;
        } else {
            next_id += 1ULL;
            status = create_segment(queue, next_id);
        }
    }
    return status;
}

static int scan_segments(ddq_t *queue)
{
    /* 只重放清单选择的代，未发布的暂存代不能提供已接受数据。 */
    DIR *directory;
    struct dirent *entry;
    uint64_t id;
    uint64_t generation;
    uint64_t deadline;
    uint32_t iteration;
    uint32_t iteration_limit;
    int done;
    uint32_t index;
    int status;
    status = DDQ_OK;
    queue->segment_count = 0U;
    deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
    directory = opendir(queue->directory);
    if (directory == NULL) {
        status = DDQ_IO_ERROR;
    } else {
        iteration_limit = queue->options.max_segments * 8U + 1024U;
        done = 0;
        entry = NULL;
        for (iteration = 0U;
             iteration < iteration_limit && status == DDQ_OK;
             ++iteration) {
            if (deadline_expired(deadline) != 0) {
                status = DDQ_LIMIT;
                break;
            }
            errno = 0;
            entry = readdir(directory);
            if (entry == NULL) {
                if (errno == 0) {
                    done = 1;
                } else {
                    status = DDQ_IO_ERROR;
                }
                break;
            }
            if (parse_segment_name(entry->d_name, &generation, &id) !=
                    DDQ_OK || generation != queue->generation) {
                continue;
            }
            if (queue->segment_count >= queue->options.max_segments) {
                status = DDQ_LIMIT;
                break;
            }
            if (make_segment_path(queue->segments[queue->segment_count].path,
                                  sizeof(queue->segments[0].path),
                                  queue->directory, generation, id) !=
                    DDQ_OK) {
                status = DDQ_TOO_LARGE;
                break;
            }
            queue->segments[queue->segment_count].id = id;
            queue->segments[queue->segment_count].size = 0ULL;
            queue->segment_count += 1U;
        }
        if (status == DDQ_OK && done == 0) {
            status = DDQ_LIMIT;
        }
        closedir(directory);
    }
    if (status == DDQ_OK && queue->segment_count > 1U) {
        qsort(queue->segments, queue->segment_count,
              sizeof(queue->segments[0]), compare_segments);
    }
    for (index = 0U;
         index < queue->segment_count && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else if (queue->segments[index].id != (uint64_t)index) {
            /* 已选代必须从零连续编号，不能掩盖被删除的已接受段。 */
            status = DDQ_CORRUPT;
        }
    }
    return status;
}

static int validate_record_bytes(const ddq_t *queue, const uint8_t *record,
                                 uint32_t record_length, uint16_t *out_type,
                                 uint64_t *out_seq, uint64_t *out_arg,
                                 uint32_t *out_payload_length)
{
    /* 先复制自持线缆头和尾，所有消费者共用命名字段及完整边界校验。 */
    ddq_record_header_wire_t header;
    ddq_record_footer_wire_t footer;
    uint16_t type;
    uint64_t seq;
    uint64_t arg;
    uint32_t payload_length;
    uint32_t total_length;
    int status;
    status = DDQ_OK;
    payload_length = 0U;
    total_length = 0U;
    type = 0U;
    seq = 0ULL;
    arg = 0ULL;
    if (record_length < DDQ_MIN_RECORD_SIZE) {
        status = DDQ_CORRUPT;
    } else {
        memcpy(&header, record, sizeof(header));
        type = get_u16(header.fields.type);
        seq = get_u64(header.fields.seq);
        arg = get_u64(header.fields.arg);
        payload_length = get_u32(header.fields.payload_length);
        total_length = get_u32(header.fields.record_length);
        if (get_u32(header.magic) != DDQ_MAGIC ||
            get_u16(header.fields.version) != DDQ_VERSION ||
            get_u32(header.reserved) != 0U ||
            type < DDQ_RECORD_DATA || type > DDQ_RECORD_STATE ||
            (payload_length > queue->options.max_payload_bytes &&
             !(type == DDQ_RECORD_STATE &&
               payload_length == sizeof(ddq_state_wire_t))) ||
            (uint64_t)total_length !=
                (uint64_t)DDQ_MIN_RECORD_SIZE + payload_length ||
            total_length != record_length ||
            total_length > queue->options.segment_bytes) {
            status = DDQ_CORRUPT;
        } else {
            memcpy(&footer, record + sizeof(header) + payload_length,
                   sizeof(footer));
            if (get_u32(footer.commit) != DDQ_COMMIT ||
                get_u32(footer.record_length) != total_length ||
                get_u32(header.crc) != record_crc(
                    &header, record + sizeof(header), payload_length) ||
                seq == 0ULL ||
                (type == DDQ_RECORD_DATA && arg != 0ULL) ||
                (type == DDQ_RECORD_CLAIM &&
                 (arg == 0ULL || payload_length != 0U)) ||
                (type == DDQ_RECORD_ACK &&
                 (arg != 0ULL || payload_length != 0U)) ||
                (type == DDQ_RECORD_RETRY && payload_length != 0U) ||
                (type == DDQ_RECORD_QUARANTINE &&
                 (arg != 0ULL || payload_length > DDQ_MAX_REASON_BYTES)) ||
                (type == DDQ_RECORD_STATE &&
                 (arg != 0ULL ||
                  payload_length != sizeof(ddq_state_wire_t)))) {
                status = DDQ_CORRUPT;
            }
        }
    }
    if (status == DDQ_OK) {
        if (out_type != NULL) {
            *out_type = type;
        }
        if (out_seq != NULL) {
            *out_seq = seq;
        }
        if (out_arg != NULL) {
            *out_arg = arg;
        }
        if (out_payload_length != NULL) {
            *out_payload_length = payload_length;
        }
    }
    return (int)status;
}

static int apply_record(ddq_t *queue, uint32_t segment_index,
                        uint64_t offset, const uint8_t *record,
                        uint16_t type, uint64_t seq, uint64_t arg,
                        uint32_t payload_length)
{
    /* 状态事件按日志顺序重放，未知消息或非法状态转换视为损坏。 */
    int item_index;
    int capacity_status;
    int status;
    int prefix;
    uint32_t state;
    uint64_t lease;
    ddq_state_wire_t snapshot;
    ddq_item_t *item;
    status = DDQ_OK;
    item_index = -1;
    prefix = queue->generation != 0ULL && queue->checkpoint_seen == 0;
    if (type == DDQ_RECORD_DATA) {
        capacity_status = DDQ_OK;
        if (seq == 0ULL || seq == UINT64_MAX ||
            (queue->item_count != 0U &&
             seq <= queue->items[queue->item_count - 1U].seq) ||
            (prefix == 0 && seq != queue->next_seq) ||
            (prefix != 0 && queue->item_count != 0U &&
             queue->items[queue->item_count - 1U].state ==
                 DDQ_ITEM_QUARANTINED &&
             queue->items[queue->item_count - 1U].reason_recorded == 0U)) {
            status = DDQ_CORRUPT;
        } else {
            capacity_status = ensure_item_capacity(queue,
                                                  queue->item_count + 1U);
            if (capacity_status != DDQ_OK) {
                status = capacity_status;
            }
        }
        if (status == DDQ_OK) {
            item = &queue->items[queue->item_count];
            memset(item, 0, sizeof(*item));
            item->seq = seq;
            item->payload_len = payload_length;
            item->segment_index = segment_index;
            item->offset = offset;
            item->state = DDQ_ITEM_AVAILABLE;
            queue->item_count += 1U;
            if (seq >= queue->next_seq) {
                if (seq == UINT64_MAX) {
                    queue->next_seq = UINT64_MAX;
                } else {
                    queue->next_seq = seq + 1ULL;
                }
            }
        }
    } else {
        item_index = find_item(queue, seq);
        if (item_index < 0) {
            status = DDQ_CORRUPT;
        } else {
            item = &queue->items[item_index];
            if (type == DDQ_RECORD_STATE && prefix != 0 &&
                payload_length == sizeof(snapshot) &&
                (uint32_t)item_index + 1U == queue->item_count &&
                item->snapshot_seen == 0U) {
                /* 快照保留尝试次数和绝对期限，不把隔离条目变回可投递。 */
                memcpy(&snapshot, record + DDQ_HEADER_SIZE,
                       sizeof(snapshot));
                state = get_u32(snapshot.state);
                lease = get_u64(snapshot.lease_until_ms);
                if ((state != DDQ_ITEM_AVAILABLE &&
                     state != DDQ_ITEM_CLAIMED &&
                     state != DDQ_ITEM_QUARANTINED) ||
                    (state == DDQ_ITEM_CLAIMED && lease == 0ULL) ||
                    (state != DDQ_ITEM_CLAIMED && lease != 0ULL)) {
                    status = DDQ_CORRUPT;
                } else {
                    item->state = (uint8_t)state;
                    item->attempts = get_u32(snapshot.attempts);
                    item->lease_until_ms = lease;
                    item->not_before_ms = get_u64(snapshot.not_before_ms);
                    item->snapshot_seen = 1U;
                }
            } else if (type == DDQ_RECORD_QUARANTINE && prefix != 0 &&
                       item->state == DDQ_ITEM_QUARANTINED &&
                       item->snapshot_seen != 0U &&
                       item->reason_recorded == 0U &&
                       (uint32_t)item_index + 1U == queue->item_count) {
                item->reason_segment_index = segment_index;
                item->reason_offset = offset;
                item->reason_length = payload_length;
                item->reason_recorded = 1U;
            } else if (prefix != 0) {
                status = DDQ_CORRUPT;
            } else if (type == DDQ_RECORD_CLAIM &&
                item->state != DDQ_ITEM_ACKED &&
                item->state != DDQ_ITEM_QUARANTINED) {
                item->state = DDQ_ITEM_CLAIMED;
                item->lease_until_ms = arg;
            } else if (type == DDQ_RECORD_ACK &&
                       item->state == DDQ_ITEM_CLAIMED) {
                item->state = DDQ_ITEM_ACKED;
                item->lease_until_ms = 0ULL;
            } else if (type == DDQ_RECORD_RETRY &&
                       item->state == DDQ_ITEM_CLAIMED &&
                       item->attempts < UINT32_MAX) {
                item->state = DDQ_ITEM_AVAILABLE;
                item->attempts += 1U;
                item->lease_until_ms = 0ULL;
                item->not_before_ms = arg;
            } else if (type == DDQ_RECORD_QUARANTINE &&
                       item->state == DDQ_ITEM_CLAIMED) {
                item->state = DDQ_ITEM_QUARANTINED;
                item->lease_until_ms = 0ULL;
                item->reason_segment_index = segment_index;
                item->reason_offset = offset;
                item->reason_length = payload_length;
                item->reason_recorded = 1U;
            } else {
                status = DDQ_CORRUPT;
            }
        }
    }
    return status;
}

/* 快照必须完整到清单给定的边界，边界前的短尾不能作为可修复尾部。 */
static int checkpoint_boundary(ddq_t *queue, uint32_t segment_index,
                               uint64_t offset)
{
    uint32_t index;
    uint64_t deadline;
    int status;
    status = DDQ_OK;
    deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
    if (queue->checkpoint_seen == 0 &&
        segment_index + 1U == queue->checkpoint_segments) {
        if (offset > queue->checkpoint_offset) {
            status = DDQ_CORRUPT;
        } else if (offset == queue->checkpoint_offset) {
            if (queue->item_count != queue->checkpoint_items ||
                queue->next_seq > queue->checkpoint_next_seq ||
                (queue->generation == 0ULL &&
                 queue->next_seq != queue->checkpoint_next_seq)) {
                status = DDQ_CORRUPT;
            }
            for (index = 0U;
                 queue->generation != 0ULL &&
                 index < queue->item_count && status == DDQ_OK; ++index) {
                if (deadline_expired(deadline) != 0) {
                    status = DDQ_LIMIT;
                } else if ((queue->items[index].snapshot_seen == 0U &&
                            (queue->items[index].state !=
                                 DDQ_ITEM_AVAILABLE ||
                             queue->items[index].attempts != 0U ||
                             queue->items[index].lease_until_ms != 0ULL ||
                             queue->items[index].not_before_ms != 0ULL)) ||
                           (queue->items[index].state ==
                                DDQ_ITEM_QUARANTINED &&
                            queue->items[index].reason_recorded == 0U)) {
                    status = DDQ_CORRUPT;
                }
            }
            if (status == DDQ_OK) {
                queue->next_seq = queue->checkpoint_next_seq;
                queue->checkpoint_seen = 1;
            }
        }
    }
    return status;
}

static int replay_segment(ddq_t *queue, uint32_t segment_index, int is_last,
                          uint64_t deadline)
{
    /* 只有最后一段允许截断不完整尾部；中间段损坏必须停止恢复。 */
    int fd;
    struct stat st;
    uint64_t file_size;
    uint64_t offset;
    uint32_t record_count;
    ddq_record_header_wire_t header;
    uint8_t header_bytes[sizeof(header)];
    uint8_t *record;
    uint32_t total_length;
    uint16_t type;
    uint64_t seq;
    uint64_t arg;
    uint32_t payload_length;
    int status;
    int tail;
    fd = -1;
    record = NULL;
    status = DDQ_OK;
    offset = 0ULL;
    file_size = 0ULL;
    fd = open(queue->segments[segment_index].path, O_RDWR | O_APPEND);
    if (fd < 0) {
        status = DDQ_IO_ERROR;
    } else if (fstat(fd, &st) != 0 || st.st_size < 0) {
        status = DDQ_IO_ERROR;
    } else {
        file_size = (uint64_t)st.st_size;
        if (file_size > queue->options.segment_bytes) {
            status = DDQ_CORRUPT;
        }
        if (status == DDQ_OK) {
            status = checkpoint_boundary(queue, segment_index, offset);
        }
        for (record_count = 0U;
             record_count < DDQ_SCAN_RECORD_LIMIT &&
                 offset < file_size && status == DDQ_OK;
             ++record_count) {
            if (deadline_expired(deadline) != 0) {
                status = DDQ_LIMIT;
                break;
            }
            if (file_size - offset < sizeof(header)) {
                if (is_last != 0 && queue->checkpoint_seen != 0) {
                    if (ftruncate(fd, (off_t)offset) != 0 ||
                        fsync(fd) != 0) {
                        status = DDQ_IO_ERROR;
                    } else {
                        queue->recovered_tail_bytes += file_size - offset;
                        file_size = offset;
                    }
                } else {
                    status = DDQ_CORRUPT;
                }
                break;
            }
            status = pread_full(fd, header_bytes, sizeof(header_bytes),
                                offset);
            if (status != DDQ_OK) {
                break;
            }
            memcpy(&header, header_bytes, sizeof(header));
            total_length = get_u32(header.fields.record_length);
            tail = 0;
            if (get_u32(header.magic) != DDQ_MAGIC ||
                get_u16(header.fields.version) != DDQ_VERSION ||
                total_length < DDQ_MIN_RECORD_SIZE ||
                total_length > queue->options.segment_bytes) {
                status = DDQ_CORRUPT;
                break;
            }
            if (total_length > file_size - offset) {
                tail = 1;
            }
            if (tail != 0) {
                if (is_last != 0 && queue->checkpoint_seen != 0) {
                    if (ftruncate(fd, (off_t)offset) != 0 ||
                        fsync(fd) != 0) {
                        status = DDQ_IO_ERROR;
                    } else {
                        queue->recovered_tail_bytes += file_size - offset;
                        file_size = offset;
                    }
                } else {
                    status = DDQ_CORRUPT;
                }
                break;
            }
            record = (uint8_t *)malloc(total_length);
            if (record == NULL) {
                status = DDQ_NO_MEMORY;
                break;
            }
            memcpy(record, &header, sizeof(header));
            status = pread_full(fd, record + DDQ_HEADER_SIZE,
                               total_length - DDQ_HEADER_SIZE,
                               offset + DDQ_HEADER_SIZE);
            if (status == DDQ_OK) {
                status = validate_record_bytes(queue, record, total_length,
                                               &type, &seq, &arg,
                                               &payload_length);
            }
            if (status == DDQ_OK) {
                status = apply_record(queue, segment_index, offset, record,
                                      type, seq, arg, payload_length);
            }
            if (status == DDQ_OK) {
                status = checkpoint_boundary(queue, segment_index,
                                             offset + total_length);
            }
            free(record);
            record = NULL;
            if (status == DDQ_OK) {
                offset += total_length;
            }
        }
        if (status == DDQ_OK && record_count == DDQ_SCAN_RECORD_LIMIT &&
            offset < file_size) {
            status = DDQ_LIMIT;
        }
        if (status == DDQ_OK) {
            queue->segments[segment_index].size = offset;
            if (is_last != 0) {
                queue->current_fd = fd;
                queue->current_offset = offset;
                fd = -1;
            }
        }
    }
    if (record != NULL) {
        free(record);
    }
    if (fd >= 0) {
        close(fd);
    }
    return status;
}

static int append_record(ddq_t *queue, uint16_t type, uint64_t seq,
                         uint64_t arg, const void *payload,
                         uint32_t payload_length, uint64_t *out_offset)
{
    /* 每条记录写完立即 fsync，失败后关闭描述符，要求调用方重新恢复。 */
    uint32_t total_length;
    ddq_record_header_wire_t header;
    ddq_record_footer_wire_t footer;
    uint8_t *record;
    size_t written;
    uint64_t offset;
    int status;
    record = NULL;
    total_length = 0U;
    written = 0U;
    offset = queue->current_offset;
    status = DDQ_OK;
    if ((payload_length > queue->options.max_payload_bytes &&
         !(type == DDQ_RECORD_STATE &&
           payload_length == sizeof(ddq_state_wire_t))) ||
        payload_length > UINT32_MAX - DDQ_MIN_RECORD_SIZE) {
        status = DDQ_TOO_LARGE;
    } else if (payload_length > 0U && payload == NULL) {
        status = DDQ_INVALID_ARGUMENT;
    } else {
        total_length = DDQ_MIN_RECORD_SIZE + payload_length;
        if ((uint64_t)total_length > queue->options.segment_bytes) {
            status = DDQ_TOO_LARGE;
        } else if (queue->current_fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (queue->current_offset > 0ULL &&
                   queue->current_offset + total_length >
                       queue->options.segment_bytes) {
            /* 状态或确认历史挤占段预算时先压缩；暂存代不能递归压缩。 */
            if (queue->segment_count >= queue->options.max_segments &&
                queue->staging == 0) {
                status = compact_queue(queue, total_length, 0U);
                if (status == DDQ_FULL) {
                    status = DDQ_LIMIT;
                }
            }
            if (status == DDQ_OK && queue->current_offset > 0ULL &&
                queue->current_offset + total_length >
                    queue->options.segment_bytes) {
                status = rotate_segment(queue);
            }
            offset = queue->current_offset;
        }
    }
    if (status == DDQ_OK) {
        record = (uint8_t *)calloc(1U, total_length);
        if (record == NULL) {
            status = DDQ_NO_MEMORY;
        } else {
            memset(&header, 0, sizeof(header));
            memset(&footer, 0, sizeof(footer));
            put_u32(header.magic, DDQ_MAGIC);
            put_u16(header.fields.version, DDQ_VERSION);
            put_u16(header.fields.type, type);
            put_u64(header.fields.seq, seq);
            put_u64(header.fields.arg, arg);
            put_u32(header.fields.payload_length, payload_length);
            put_u32(header.fields.record_length, total_length);
            if (payload_length > 0U) {
                memcpy(record + DDQ_HEADER_SIZE, payload, payload_length);
            }
            put_u32(header.crc, record_crc(&header,
                (const uint8_t *)payload, payload_length));
            put_u32(footer.commit, DDQ_COMMIT);
            put_u32(footer.record_length, total_length);
            memcpy(record, &header, sizeof(header));
            memcpy(record + sizeof(header) + payload_length,
                   &footer, sizeof(footer));
            status = write_full(queue->current_fd, record, total_length,
                                &written);
            queue->current_offset += written;
            queue->segments[queue->segment_count - 1U].size =
                queue->current_offset;
            if (status == DDQ_OK && queue->staging == 0 &&
                fsync(queue->current_fd) != 0) {
                status = DDQ_IO_ERROR;
            }
            if (status != DDQ_OK && queue->current_fd >= 0) {
                close(queue->current_fd);
                queue->current_fd = -1;
            }
            if (status == DDQ_OK && out_offset != NULL) {
                *out_offset = offset;
            }
        }
    }
    if (record != NULL) {
        free(record);
    }
    return status;
}

static int read_item_payload(const ddq_t *queue, const ddq_item_t *item,
                             int reason, void *buffer)
{
    /* 原始数据和隔离原因共用完整记录校验，压缩不重新编码业务原文。 */
    int fd;
    ddq_record_header_wire_t header;
    uint8_t header_bytes[sizeof(header)];
    uint8_t *record;
    uint32_t segment_index;
    uint32_t expected_length;
    uint16_t expected_type;
    uint64_t offset;
    uint32_t total_length;
    uint16_t type;
    uint64_t seq;
    uint64_t arg;
    uint32_t payload_length;
    int status;
    fd = -1;
    record = NULL;
    status = DDQ_OK;
    segment_index = reason != 0 ? item->reason_segment_index :
                                 item->segment_index;
    offset = reason != 0 ? item->reason_offset : item->offset;
    expected_length = reason != 0 ? item->reason_length : item->payload_len;
    expected_type = reason != 0 ? DDQ_RECORD_QUARANTINE : DDQ_RECORD_DATA;
    if (segment_index >= queue->segment_count ||
        (reason != 0 && item->reason_recorded == 0U)) {
        status = DDQ_CORRUPT;
    } else {
        fd = open(queue->segments[segment_index].path, O_RDONLY);
        if (fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (offset > queue->segments[segment_index].size ||
                   queue->segments[segment_index].size - offset <
                       sizeof(header)) {
            status = DDQ_CORRUPT;
        } else if (pread_full(fd, header_bytes, sizeof(header_bytes),
                              offset) != DDQ_OK) {
            status = DDQ_CORRUPT;
        } else {
            memcpy(&header, header_bytes, sizeof(header));
            total_length = get_u32(header.fields.record_length);
            if (total_length < DDQ_MIN_RECORD_SIZE ||
                total_length > queue->options.segment_bytes ||
                total_length > queue->segments[segment_index].size - offset) {
                status = DDQ_CORRUPT;
            } else {
                record = (uint8_t *)malloc(total_length);
                if (record == NULL) {
                    status = DDQ_NO_MEMORY;
                } else {
                    memcpy(record, &header, sizeof(header));
                    status = pread_full(fd, record + DDQ_HEADER_SIZE,
                                        total_length - DDQ_HEADER_SIZE,
                                        offset + DDQ_HEADER_SIZE);
                    if (status == DDQ_OK) {
                        status = validate_record_bytes(
                            queue, record, total_length, &type, &seq, &arg,
                            &payload_length);
                    }
                    if (status == DDQ_OK &&
                        (type != expected_type || seq != item->seq ||
                         payload_length != expected_length)) {
                        status = DDQ_CORRUPT;
                    }
                    if (status == DDQ_OK && payload_length > 0U) {
                        memcpy(buffer, record + DDQ_HEADER_SIZE,
                               payload_length);
                    }
                }
            }
        }
    }
    if (record != NULL) {
        free(record);
    }
    if (fd >= 0) {
        close(fd);
    }
    return status;
}

/* 暂存文件逐个登记；未发布失败时只回收本次创建的文件。 */
static int discard_staging(ddq_t *staged, int remove_files)
{
    struct stat st;
    uint32_t index;
    uint64_t deadline;
    int status;
    status = DDQ_OK;
    deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
    if (staged->current_fd >= 0) {
        close(staged->current_fd);
        staged->current_fd = -1;
    }
    for (index = 0U; remove_files != 0 &&
         index < staged->segment_count && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else if (lstat(staged->segments[index].path, &st) != 0) {
            status = DDQ_IO_ERROR;
        } else if (!S_ISREG(st.st_mode)) {
            status = DDQ_CORRUPT;
        } else if (unlink(staged->segments[index].path) != 0) {
            status = DDQ_IO_ERROR;
        }
    }
    if (remove_files != 0 && staged->segment_count != 0U &&
        sync_directory(staged->directory) != DDQ_OK) {
        status = DDQ_IO_ERROR;
    }
    free(staged->segments);
    staged->segments = NULL;
    free(staged->items);
    staged->items = NULL;
    return status;
}

/* 回收确认项和冗余状态历史；新代同步及清单发布成功前始终保留旧代。 */
static int compact_queue(ddq_t *queue, uint32_t reserve_bytes,
                         uint32_t required_items)
{
    ddq_t staged;
    ddq_state_wire_t state;
    ddq_item_t *item;
    ddq_item_t *copied;
    uint8_t *payload;
    size_t capacity;
    uint32_t index;
    uint32_t acknowledged;
    uint64_t deadline;
    uint64_t data_offset;
    uint64_t reason_offset;
    uint64_t active_bytes;
    uint64_t staged_bytes;
    int published;
    int cleanup_status;
    int status;
    memset(&staged, 0, sizeof(staged));
    staged.current_fd = -1;
    staged.lock_fd = -1;
    staged.staging = 1;
    payload = NULL;
    acknowledged = 0U;
    active_bytes = 0ULL;
    staged_bytes = 0ULL;
    published = 0;
    status = DDQ_OK;
    deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
    if (queue->current_fd < 0) {
        status = DDQ_IO_ERROR;
    }
    for (index = 0U;
         index < queue->item_count && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else if (queue->items[index].state == DDQ_ITEM_ACKED) {
            acknowledged += 1U;
        }
    }
    if (status == DDQ_OK && acknowledged == 0U && required_items != 0U) {
        status = DDQ_FULL;
    }
    if (status == DDQ_OK && queue->generation == UINT64_MAX) {
        status = DDQ_LIMIT;
    }
    if (status == DDQ_OK) {
        status = prepare_manifest_format(queue);
    }
    if (status == DDQ_OK) {
        status = clean_obsolete_files(queue);
    }
    if (status == DDQ_OK) {
        staged.options = queue->options;
        memcpy(staged.directory, queue->directory,
               strlen(queue->directory) + 1U);
        staged.generation = queue->generation + 1ULL;
        staged.next_seq = queue->next_seq;
        staged.item_capacity = queue->item_capacity;
        staged.segments = (ddq_segment_t *)calloc(
            staged.options.max_segments, sizeof(*staged.segments));
        staged.items = (ddq_item_t *)calloc(
            staged.item_capacity, sizeof(*staged.items));
        if (staged.segments == NULL || staged.items == NULL) {
            status = DDQ_NO_MEMORY;
        } else {
            status = create_segment(&staged, 0ULL);
        }
    }
    for (index = 0U;
         index < queue->item_count && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else if (queue->items[index].state != DDQ_ITEM_ACKED) {
            item = &queue->items[index];
            capacity = item->payload_len;
            if (capacity < item->reason_length) {
                capacity = item->reason_length;
            }
            if (capacity == 0U) {
                capacity = 1U;
            }
            payload = (uint8_t *)malloc(capacity);
            if (payload == NULL) {
                status = DDQ_NO_MEMORY;
            } else {
                status = read_item_payload(queue, item, 0, payload);
            }
            if (status == DDQ_OK) {
                status = append_record(&staged, DDQ_RECORD_DATA,
                    item->seq, 0ULL, payload, item->payload_len,
                    &data_offset);
            }
            if (status == DDQ_OK) {
                copied = &staged.items[staged.item_count];
                *copied = *item;
                copied->segment_index = staged.segment_count - 1U;
                copied->offset = data_offset;
                copied->snapshot_seen = 0U;
                memset(&state, 0, sizeof(state));
                put_u32(state.state, item->state);
                put_u32(state.attempts, item->attempts);
                put_u64(state.lease_until_ms, item->lease_until_ms);
                put_u64(state.not_before_ms, item->not_before_ms);
                /* 默认可投递状态由原数据记录表达，避免无变化项增加快照空间。 */
                if (item->state != DDQ_ITEM_AVAILABLE ||
                    item->attempts != 0U || item->lease_until_ms != 0ULL ||
                    item->not_before_ms != 0ULL) {
                    status = append_record(&staged, DDQ_RECORD_STATE,
                        item->seq, 0ULL, &state, (uint32_t)sizeof(state), NULL);
                    if (status == DDQ_OK) {
                        copied->snapshot_seen = 1U;
                    }
                }
                if (status == DDQ_OK &&
                    item->state == DDQ_ITEM_QUARANTINED) {
                    /* 隔离载荷与原因原样复制，不能借回收删除审计证据。 */
                    status = read_item_payload(queue, item, 1, payload);
                    if (status == DDQ_OK) {
                        status = append_record(&staged,
                            DDQ_RECORD_QUARANTINE, item->seq, 0ULL,
                            payload, item->reason_length, &reason_offset);
                    }
                    if (status == DDQ_OK) {
                        copied->reason_segment_index =
                            staged.segment_count - 1U;
                        copied->reason_offset = reason_offset;
                        copied->reason_recorded = 1U;
                    }
                }
                if (status == DDQ_OK) {
                    staged.item_count += 1U;
                }
            }
            free(payload);
            payload = NULL;
        }
    }
    if (status == DDQ_OK && deadline_expired(deadline) != 0) {
        status = DDQ_LIMIT;
    }
    for (index = 0U;
         index < queue->segment_count && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else {
            active_bytes += queue->segments[index].size;
        }
    }
    for (index = 0U;
         index < staged.segment_count && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else {
            staged_bytes += staged.segments[index].size;
        }
    }
    if (status == DDQ_OK &&
        (staged_bytes >= active_bytes ||
         staged.item_count > staged.options.max_items - required_items ||
         (staged.segment_count >= staged.options.max_segments &&
          staged.current_offset + reserve_bytes >
              staged.options.segment_bytes))) {
        /* 无确认但状态历史可缩短时也能回收；没有实际收益不能发布新代。 */
        status = DDQ_FULL;
    }
    if (status == DDQ_OK) {
        if (fsync(staged.current_fd) != 0) {
            status = DDQ_IO_ERROR;
        } else {
            status = sync_directory(staged.directory);
        }
    }
    if (status == DDQ_OK) {
        status = publish_manifest(&staged, &published);
    }
    if (status == DDQ_OK) {
        /* 发布完成后才替换内存归属，锁描述符始终归原实例。 */
        close(queue->current_fd);
        free(queue->segments);
        free(queue->items);
        queue->segments = staged.segments;
        queue->segment_count = staged.segment_count;
        queue->items = staged.items;
        queue->item_count = staged.item_count;
        queue->item_capacity = staged.item_capacity;
        queue->current_fd = staged.current_fd;
        queue->current_offset = staged.current_offset;
        queue->generation = staged.generation;
        queue->next_seq = staged.next_seq;
        queue->checkpoint_next_seq = staged.next_seq;
        queue->checkpoint_segments = staged.segment_count;
        queue->checkpoint_items = staged.item_count;
        queue->checkpoint_offset = staged.current_offset;
        queue->checkpoint_seen = 1;
        queue->has_manifest = 1;
        staged.current_fd = -1;
        staged.segments = NULL;
        staged.items = NULL;
        staged.segment_count = 0U;
        status = clean_obsolete_files(queue);
    }
    cleanup_status = discard_staging(&staged, published == 0);
    if (cleanup_status != DDQ_OK) {
        /* 无收益退出若清理失败仍须停写，禁止在残留暂存代上叠加第三代。 */
        status = cleanup_status;
    }
    if (status != DDQ_OK && status != DDQ_FULL &&
        queue->current_fd >= 0) {
        /* 发布或同步失败可能处于未知代，必须重新恢复后才能接受写入。 */
        close(queue->current_fd);
        queue->current_fd = -1;
    }
    return status;
}

/* 按完整记录规划段边界，尺寸包含固定头和提交尾，绝不拆开单条记录。 */
static int plan_capacity_record(const ddq_t *queue,
                                ddq_capacity_plan_t *plan,
                                uint32_t record_bytes)
{
    int status;
    status = DDQ_OK;
    if (record_bytes > queue->options.segment_bytes) {
        status = DDQ_TOO_LARGE;
    } else if (plan->segment_count == 0U ||
               plan->offset + record_bytes > queue->options.segment_bytes) {
        if (plan->segment_count >= queue->options.max_segments) {
            status = DDQ_FULL;
        } else {
            plan->segment_count += 1U;
            plan->offset = 0ULL;
        }
    }
    if (status == DDQ_OK) {
        plan->offset += record_bytes;
    }
    return status;
}

/* 入队同时预留领取、终结、快照和最大隔离原因，禁止纯数据堵死消费路径。 */
static int check_enqueue_capacity(const ddq_t *queue, uint32_t payload_bytes)
{
    ddq_capacity_plan_t physical;
    ddq_capacity_plan_t snapshot;
    const ddq_item_t *item;
    uint32_t index;
    uint32_t reason_bytes;
    uint32_t terminal_bytes;
    uint32_t state_bytes;
    uint32_t control_max;
    uint64_t pending;
    uint64_t quarantined;
    uint64_t reserved_segments;
    uint64_t remaining_segments;
    uint64_t control_bytes;
    uint64_t guaranteed_bytes;
    uint64_t remaining_bytes;
    uint64_t deadline;
    int status;
    physical.segment_count = queue->segment_count;
    physical.offset = queue->current_offset;
    memset(&snapshot, 0, sizeof(snapshot));
    reason_bytes = (uint32_t)queue->options.max_payload_bytes;
    if (reason_bytes > DDQ_MAX_REASON_BYTES) {
        reason_bytes = DDQ_MAX_REASON_BYTES;
    }
    terminal_bytes = DDQ_MIN_RECORD_SIZE + reason_bytes;
    state_bytes = DDQ_MIN_RECORD_SIZE + (uint32_t)sizeof(ddq_state_wire_t);
    pending = 1ULL;
    quarantined = 0ULL;
    reserved_segments = 0ULL;
    deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
    status = plan_capacity_record(queue, &physical,
                                 DDQ_MIN_RECORD_SIZE + payload_bytes);
    for (index = 0U;
         index < queue->item_count && status == DDQ_OK; ++index) {
        if (deadline_expired(deadline) != 0) {
            status = DDQ_LIMIT;
        } else if (queue->items[index].state != DDQ_ITEM_ACKED) {
            item = &queue->items[index];
            if (item->state == DDQ_ITEM_QUARANTINED) {
                quarantined += 1ULL;
            } else {
                pending += 1ULL;
            }
            /* 快照最坏布局保留原数据、状态和原因；默认状态省略只会缩短布局。 */
            status = plan_capacity_record(queue, &snapshot,
                DDQ_MIN_RECORD_SIZE + item->payload_len);
            if (status == DDQ_OK) {
                status = plan_capacity_record(queue, &snapshot, state_bytes);
            }
            if (status == DDQ_OK) {
                status = plan_capacity_record(queue, &snapshot,
                    item->state == DDQ_ITEM_QUARANTINED ?
                        DDQ_MIN_RECORD_SIZE + item->reason_length :
                        terminal_bytes);
            }
        }
    }
    if (status == DDQ_OK) {
        status = plan_capacity_record(queue, &snapshot,
                                     DDQ_MIN_RECORD_SIZE + payload_bytes);
    }
    if (status == DDQ_OK) {
        status = plan_capacity_record(queue, &snapshot, state_bytes);
    }
    if (status == DDQ_OK) {
        status = plan_capacity_record(queue, &snapshot, terminal_bytes);
    }
    if (status == DDQ_OK) {
        /* 历史回收后仍须容纳一轮领取和最大原因终结，不以快照完整代替可消费。 */
        status = plan_capacity_record(queue, &snapshot, DDQ_MIN_RECORD_SIZE);
    }
    if (status == DDQ_OK) {
        status = plan_capacity_record(queue, &snapshot, terminal_bytes);
    }
    if (status == DDQ_OK) {
        control_max = state_bytes;
        control_bytes = pending * (state_bytes + DDQ_MIN_RECORD_SIZE) +
                        quarantined * state_bytes;
        remaining_segments = queue->options.max_segments -
                             physical.segment_count;
        if (terminal_bytes > queue->options.segment_bytes / 2U) {
            /* 大原因按两整段保守计费，一段容纳记录，一段承担任意交错造成的空隙。 */
            reserved_segments = pending * 2ULL;
        } else {
            control_bytes += pending * terminal_bytes;
            if (terminal_bytes > control_max) {
                control_max = terminal_bytes;
            }
        }
        if (reserved_segments > remaining_segments) {
            status = DDQ_FULL;
        } else {
            remaining_segments -= reserved_segments;
            remaining_bytes = queue->options.segment_bytes - physical.offset;
            guaranteed_bytes = 0ULL;
            if (remaining_bytes >= control_max) {
                guaranteed_bytes = remaining_bytes - (control_max - 1U);
            }
            /* 每次小控制记录换段最多空出最大记录减一字节，扣除此界后才准入。 */
            guaranteed_bytes += remaining_segments *
                (queue->options.segment_bytes - (control_max - 1U));
            if (control_bytes > guaranteed_bytes) {
                status = DDQ_FULL;
            }
        }
    }
    return status;
}

/* 压缩可能改变内存下标，落盘成功后始终按原序号重新定位。 */
static int resolve_written_item(ddq_t *queue, uint64_t seq, int *out_index)
{
    int status;
    *out_index = find_item(queue, seq);
    status = DDQ_OK;
    if (*out_index < 0) {
        status = DDQ_CORRUPT;
        if (queue->current_fd >= 0) {
            close(queue->current_fd);
            queue->current_fd = -1;
        }
    }
    return status;
}

int ddq_options_default(ddq_options_t *options)
{
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (options != NULL) {
        options->segment_bytes = DDQ_DEFAULT_SEGMENT_BYTES;
        options->max_payload_bytes = DDQ_DEFAULT_MAX_PAYLOAD;
        options->max_items = DDQ_DEFAULT_MAX_ITEMS;
        options->max_segments = DDQ_DEFAULT_MAX_SEGMENTS;
        status = DDQ_OK;
    }
    return status;
}

int ddq_recover(ddq_t *queue)
{
    /* 每次恢复重新读取代清单和磁盘目录，处理发布结果未知的实例。 */
    uint32_t index;
    uint64_t deadline;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL) {
        deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
        if (queue->current_fd >= 0) {
            close(queue->current_fd);
            queue->current_fd = -1;
        }
        queue->current_offset = 0ULL;
        queue->item_count = 0U;
        queue->next_seq = 1ULL;
        queue->recovered_tail_bytes = 0ULL;
        status = load_manifest(queue);
        if (status == DDQ_OK) {
            status = scan_segments(queue);
        }
        if (status == DDQ_OK && queue->has_manifest != 0 &&
            queue->segment_count < queue->checkpoint_segments) {
            status = DDQ_CORRUPT;
        }
        if (status == DDQ_OK && queue->segment_count == 0U) {
            status = create_segment(queue, 0ULL);
        } else if (status == DDQ_OK) {
            for (index = 0U;
                 index < queue->segment_count && status == DDQ_OK;
                 ++index) {
                if (deadline_expired(deadline) != 0) {
                    status = DDQ_LIMIT;
                } else {
                    status = replay_segment(queue, index,
                        index + 1U == queue->segment_count, deadline);
                }
            }
            if (status == DDQ_OK && queue->current_fd < 0) {
                status = DDQ_IO_ERROR;
            }
        }
        if (status == DDQ_OK && queue->checkpoint_seen == 0) {
            status = DDQ_CORRUPT;
        }
        if (status == DDQ_OK) {
            /* 先确认选定代目录持久，再回收非活动代，不赌上次同步结果。 */
            status = sync_directory(queue->directory);
        }
        if (status == DDQ_OK) {
            status = clean_obsolete_files(queue);
        }
        if (status != DDQ_OK && queue->current_fd >= 0) {
            close(queue->current_fd);
            queue->current_fd = -1;
        }
    }
    return status;
}

int ddq_open(ddq_t **out_queue, const char *directory,
             const ddq_options_t *options)
{
    /* 锁文件保证一个目录只由一个进程执行写入和投递状态转换。 */
    ddq_options_t selected;
    ddq_t *queue;
    char lock_path[PATH_MAX];
    int status;
    int written;
    size_t directory_length;
    queue = NULL;
    directory_length = 0U;
    status = DDQ_INVALID_ARGUMENT;
    if (out_queue != NULL) {
        *out_queue = NULL;
        if (directory != NULL) {
            directory_length = strnlen(directory, PATH_MAX);
        }
        if (directory != NULL && options != NULL &&
            directory_length < sizeof(queue->directory)) {
            selected = *options;
            status = validate_options(&selected);
            if (status == DDQ_OK) {
                status = ensure_directory(directory);
            }
            if (status == DDQ_OK) {
                queue = (ddq_t *)calloc(1U, sizeof(*queue));
                if (queue == NULL) {
                    status = DDQ_NO_MEMORY;
                } else {
                    queue->lock_fd = -1;
                    queue->current_fd = -1;
                    queue->options = selected;
                    memcpy(queue->directory, directory,
                           directory_length + 1U);
                    queue->segments = (ddq_segment_t *)calloc(
                        selected.max_segments, sizeof(*queue->segments));
                    if (queue->segments == NULL) {
                        status = DDQ_NO_MEMORY;
                    }
                }
            }
            if (status == DDQ_OK) {
                written = snprintf(lock_path, sizeof(lock_path),
                                    "%s/queue.lock", directory);
                if (written < 0 || (size_t)written >= sizeof(lock_path)) {
                    status = DDQ_TOO_LARGE;
                } else {
                    queue->lock_fd = open(lock_path, O_CREAT | O_RDWR, 0600);
                    if (queue->lock_fd < 0) {
                        status = DDQ_IO_ERROR;
                    } else if (flock(queue->lock_fd,
                                     LOCK_EX | LOCK_NB) != 0) {
                        status = DDQ_LOCKED;
                    }
                }
            }
            if (status == DDQ_OK) {
                status = ddq_recover(queue);
            }
            if (status == DDQ_OK) {
                *out_queue = queue;
                queue = NULL;
            }
        }
    }
    if (queue != NULL) {
        ddq_close(queue);
    }
    return status;
}

void ddq_close(ddq_t *queue)
{
    if (queue != NULL) {
        if (queue->current_fd >= 0) {
            fsync(queue->current_fd);
            close(queue->current_fd);
            queue->current_fd = -1;
        }
        if (queue->lock_fd >= 0) {
            flock(queue->lock_fd, LOCK_UN);
            close(queue->lock_fd);
            queue->lock_fd = -1;
        }
        free(queue->segments);
        free(queue->items);
        free(queue);
    }
}

int ddq_enqueue(ddq_t *queue, const void *payload, size_t payload_len,
                uint64_t *out_seq)
{
    /* 容量不足先回收已确认历史；未确认和隔离数据仍占用真实容量。 */
    uint64_t seq;
    uint64_t offset;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL && out_seq != NULL &&
        (payload_len == 0U || payload != NULL)) {
        *out_seq = 0ULL;
        if (payload_len > queue->options.max_payload_bytes) {
            status = DDQ_TOO_LARGE;
        } else if (queue->current_fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (queue->next_seq == UINT64_MAX) {
            status = DDQ_FULL;
        } else {
            status = check_enqueue_capacity(queue, (uint32_t)payload_len);
            if (status == DDQ_FULL ||
                (status == DDQ_OK &&
                 queue->item_count >= queue->options.max_items)) {
                /* 状态历史也会占用预留空间，回收后重算，不能先接受再发现无法终结。 */
                status = compact_queue(queue,
                    DDQ_MIN_RECORD_SIZE + (uint32_t)payload_len,
                    queue->item_count >= queue->options.max_items ? 1U : 0U);
                if (status == DDQ_OK) {
                    status = check_enqueue_capacity(queue,
                                                    (uint32_t)payload_len);
                }
            }
            if (status == DDQ_OK) {
                status = ensure_item_capacity(queue, queue->item_count + 1U);
            }
            if (status == DDQ_OK) {
                seq = queue->next_seq;
                status = append_record(queue, DDQ_RECORD_DATA, seq, 0ULL,
                                       payload, (uint32_t)payload_len,
                                       &offset);
                if (status == DDQ_OK) {
                    memset(&queue->items[queue->item_count], 0,
                           sizeof(queue->items[queue->item_count]));
                    queue->items[queue->item_count].seq = seq;
                    queue->items[queue->item_count].payload_len =
                        (uint32_t)payload_len;
                    queue->items[queue->item_count].segment_index =
                        queue->segment_count - 1U;
                    queue->items[queue->item_count].offset = offset;
                    queue->items[queue->item_count].state =
                        DDQ_ITEM_AVAILABLE;
                    queue->item_count += 1U;
                    queue->next_seq += 1ULL;
                    *out_seq = seq;
                }
            }
        }
    }
    return status;
}

int ddq_claim(ddq_t *queue, uint32_t lease_ms, ddq_claim_t *out_claim)
{
    /* claim 先落盘租约，再更新内存状态，重启后过期租约重新可投递。 */
    uint64_t now;
    uint64_t lease_until;
    uint64_t selected_seq;
    uint64_t deadline;
    uint32_t index;
    int selected;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    selected = -1;
    if (queue != NULL && out_claim != NULL && lease_ms > 0U &&
        (uint64_t)lease_ms <= DDQ_MAX_LEASE_MS) {
        memset(out_claim, 0, sizeof(*out_claim));
        now = wall_clock_ms();
        /* 负写描述符即需恢复，不能用旧内存返回空队列来掩盖未知提交。 */
        if (queue->current_fd < 0 || now == 0ULL) {
            status = DDQ_IO_ERROR;
        } else if (now > UINT64_MAX - (uint64_t)lease_ms) {
            status = DDQ_LIMIT;
        } else {
            lease_until = now + (uint64_t)lease_ms;
            status = DDQ_OK;
            deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
            for (index = 0U;
                 index < queue->item_count && status == DDQ_OK; ++index) {
                if (deadline_expired(deadline) != 0) {
                    status = DDQ_LIMIT;
                    break;
                }
                if (queue->items[index].state == DDQ_ITEM_CLAIMED &&
                    queue->items[index].lease_until_ms <= now) {
                    queue->items[index].state = DDQ_ITEM_AVAILABLE;
                    queue->items[index].lease_until_ms = 0ULL;
                }
                if (selected < 0 &&
                    queue->items[index].state == DDQ_ITEM_AVAILABLE &&
                    queue->items[index].not_before_ms <= now) {
                    selected = (int)index;
                }
            }
            if (status == DDQ_OK && selected < 0) {
                status = DDQ_EMPTY;
            } else if (status == DDQ_OK) {
                selected_seq = queue->items[selected].seq;
                status = append_record(
                    queue, DDQ_RECORD_CLAIM,
                    selected_seq, lease_until, NULL, 0U,
                    NULL);
                if (status == DDQ_OK) {
                    status = resolve_written_item(queue, selected_seq,
                                                  &selected);
                }
                if (status == DDQ_OK) {
                    queue->items[selected].state = DDQ_ITEM_CLAIMED;
                    queue->items[selected].lease_until_ms = lease_until;
                    out_claim->seq = queue->items[selected].seq;
                    out_claim->attempts = queue->items[selected].attempts;
                    out_claim->payload_len =
                        queue->items[selected].payload_len;
                    out_claim->lease_until_ms = lease_until;
                }
            }
        }
    }
    return status;
}

int ddq_read(ddq_t *queue, uint64_t seq, void *buffer, size_t buffer_cap,
             size_t *out_len)
{
    /* 读取压缩后保留的原始载荷，已回收的确认项按不存在处理。 */
    int item_index;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (out_len != NULL) {
        *out_len = 0U;
    }
    if (queue != NULL && out_len != NULL) {
        /* 停写实例也停止公共投递读取，避免未知重试提交后继续使用旧内存归属。 */
        item_index = queue->current_fd < 0 ? -1 : find_item(queue, seq);
        if (queue->current_fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (item_index < 0) {
            status = DDQ_NOT_FOUND;
        } else if (queue->items[item_index].payload_len > buffer_cap ||
                   (queue->items[item_index].payload_len > 0U &&
                    buffer == NULL)) {
            *out_len = queue->items[item_index].payload_len;
            status = DDQ_BUFFER_TOO_SMALL;
        } else {
            status = read_item_payload(queue, &queue->items[item_index],
                                       0, buffer);
            if (status == DDQ_OK) {
                *out_len = queue->items[item_index].payload_len;
            }
        }
    }
    return status;
}

int ddq_ack(ddq_t *queue, uint64_t seq)
{
    /* 仅明确业务成功的已领取项可确认，自动压缩后重新定位原序号。 */
    int item_index;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL) {
        /* 未知写入结果先恢复，普通确认不能重复追加同一状态记录。 */
        item_index = queue->current_fd < 0 ? -1 : find_item(queue, seq);
        if (queue->current_fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (item_index < 0) {
            status = DDQ_NOT_FOUND;
        } else if (queue->items[item_index].state == DDQ_ITEM_ACKED) {
            status = DDQ_ALREADY_DONE;
        } else if (queue->items[item_index].state != DDQ_ITEM_CLAIMED) {
            status = DDQ_BAD_STATE;
        } else {
            status = append_record(queue, DDQ_RECORD_ACK, seq, 0ULL,
                                   NULL, 0U, NULL);
            if (status == DDQ_OK) {
                status = resolve_written_item(queue, seq, &item_index);
            }
            if (status == DDQ_OK) {
                queue->items[item_index].state = DDQ_ITEM_ACKED;
                queue->items[item_index].lease_until_ms = 0ULL;
            }
        }
    }
    return status;
}

int ddq_retry(ddq_t *queue, uint64_t seq, uint32_t delay_ms)
{
    /* 重试期限和次数写盘后再更新，回收不得改变原重试归属。 */
    int item_index;
    uint64_t not_before;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL) {
        /* 停写实例禁止重试事件，避免已完整写入但同步失败的记录被重复提交。 */
        item_index = queue->current_fd < 0 ? -1 : find_item(queue, seq);
        if (queue->current_fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (item_index < 0) {
            status = DDQ_NOT_FOUND;
        } else if (queue->items[item_index].state != DDQ_ITEM_CLAIMED) {
            status = DDQ_BAD_STATE;
        } else if (queue->items[item_index].attempts == UINT32_MAX) {
            status = DDQ_LIMIT;
        } else {
            not_before = wall_clock_ms();
            if (not_before == 0ULL) {
                status = DDQ_IO_ERROR;
            } else if (not_before > UINT64_MAX - (uint64_t)delay_ms) {
                status = DDQ_LIMIT;
            } else {
                not_before += (uint64_t)delay_ms;
                status = append_record(queue, DDQ_RECORD_RETRY, seq,
                                       not_before, NULL, 0U, NULL);
                if (status == DDQ_OK) {
                    status = resolve_written_item(queue, seq, &item_index);
                }
                if (status == DDQ_OK) {
                    queue->items[item_index].state = DDQ_ITEM_AVAILABLE;
                    queue->items[item_index].attempts += 1U;
                    queue->items[item_index].lease_until_ms = 0ULL;
                    queue->items[item_index].not_before_ms = not_before;
                }
            }
        }
    }
    return status;
}

int ddq_quarantine(ddq_t *queue, uint64_t seq, const char *reason)
{
    /* 隔离不是确认，记录原文位置供后续容量回收完整保留证据。 */
    int item_index;
    size_t reason_length;
    uint64_t reason_offset;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    reason_length = 0U;
    if (queue != NULL) {
        /* 需恢复时不能再隔离或确认，保留未知提交及原记录供重放判断。 */
        item_index = queue->current_fd < 0 ? -1 : find_item(queue, seq);
        if (queue->current_fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (item_index < 0) {
            status = DDQ_NOT_FOUND;
        } else if (queue->items[item_index].state == DDQ_ITEM_QUARANTINED) {
            /* 恢复后发现首次隔离已提交即可完成交接，重复原因不得覆盖原证据。 */
            status = DDQ_ALREADY_DONE;
        } else if (queue->items[item_index].state != DDQ_ITEM_CLAIMED) {
            status = DDQ_BAD_STATE;
        } else {
            if (reason != NULL) {
                reason_length = strnlen(reason, DDQ_MAX_REASON_BYTES + 1U);
            }
            if (reason_length > DDQ_MAX_REASON_BYTES ||
                reason_length > queue->options.max_payload_bytes) {
                status = DDQ_TOO_LARGE;
            } else {
                status = append_record(
                    queue, DDQ_RECORD_QUARANTINE, seq, 0ULL, reason,
                    (uint32_t)reason_length, &reason_offset);
                if (status == DDQ_OK) {
                    status = resolve_written_item(queue, seq, &item_index);
                }
                if (status == DDQ_OK) {
                    queue->items[item_index].state =
                        DDQ_ITEM_QUARANTINED;
                    queue->items[item_index].lease_until_ms = 0ULL;
                    queue->items[item_index].reason_segment_index =
                        queue->segment_count - 1U;
                    queue->items[item_index].reason_length =
                        (uint32_t)reason_length;
                    queue->items[item_index].reason_offset = reason_offset;
                    queue->items[item_index].reason_recorded = 1U;
                }
            }
        }
    }
    return status;
}

int ddq_stats(ddq_t *queue, ddq_stats_t *out_stats)
{
    /* 统计只读已保留元数据，扫描超限时清空结果，不能把部分扫描当成完整统计。 */
    uint32_t index;
    uint64_t deadline;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL && out_stats != NULL) {
        status = DDQ_OK;
        deadline = operation_deadline(DDQ_OPERATION_TIMEOUT_MS);
        memset(out_stats, 0, sizeof(*out_stats));
        out_stats->total_items = queue->item_count;
        out_stats->segment_count = queue->segment_count;
        out_stats->next_seq = queue->next_seq;
        out_stats->recovered_tail_bytes = queue->recovered_tail_bytes;
        for (index = 0U;
             index < queue->segment_count && status == DDQ_OK; ++index) {
            if (deadline_expired(deadline) != 0) {
                status = DDQ_LIMIT;
            } else {
                out_stats->stored_bytes += queue->segments[index].size;
            }
        }
        for (index = 0U;
             index < queue->item_count && status == DDQ_OK; ++index) {
            if (deadline_expired(deadline) != 0) {
                status = DDQ_LIMIT;
            } else if (queue->items[index].state == DDQ_ITEM_AVAILABLE) {
                out_stats->available_items += 1ULL;
            } else if (queue->items[index].state == DDQ_ITEM_CLAIMED) {
                out_stats->claimed_items += 1ULL;
            } else if (queue->items[index].state == DDQ_ITEM_ACKED) {
                out_stats->acknowledged_items += 1ULL;
            } else if (queue->items[index].state == DDQ_ITEM_QUARANTINED) {
                out_stats->quarantined_items += 1ULL;
            }
        }
        if (status != DDQ_OK) {
            memset(out_stats, 0, sizeof(*out_stats));
        }
    }
    return status;
}

const char *ddq_strerror(int status)
{
    const char *message;
    message = "unknown status";
    if (status == DDQ_OK) {
        message = "ok";
    } else if (status == DDQ_EMPTY) {
        message = "queue is empty";
    } else if (status == DDQ_ALREADY_DONE) {
        /* 已完成可以是确认或隔离，不能把隔离误描述为业务投递成功。 */
        message = "记录已完成确认或隔离";
    } else if (status == DDQ_INVALID_ARGUMENT) {
        message = "invalid argument";
    } else if (status == DDQ_IO_ERROR) {
        message = "I/O error";
    } else if (status == DDQ_CORRUPT) {
        message = "queue data is corrupt";
    } else if (status == DDQ_LOCKED) {
        message = "queue is locked by another process";
    } else if (status == DDQ_NO_MEMORY) {
        message = "out of memory";
    } else if (status == DDQ_TOO_LARGE) {
        message = "payload or path is too large";
    } else if (status == DDQ_FULL) {
        message = "queue is full";
    } else if (status == DDQ_NOT_FOUND) {
        message = "message was not found";
    } else if (status == DDQ_BAD_STATE) {
        message = "message is in an invalid state";
    } else if (status == DDQ_BUFFER_TOO_SMALL) {
        message = "output buffer is too small";
    } else if (status == DDQ_LIMIT) {
        message = "configured limit was reached";
    }
    return message;
}
