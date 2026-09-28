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
#define DDQ_HEADER_SIZE 40U
#define DDQ_FOOTER_SIZE 8U
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
    DDQ_RECORD_QUARANTINE = 5
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
    uint8_t state;
} ddq_item_t;

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
};

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

static uint32_t record_crc(const uint8_t *record, uint32_t payload_len)
{
    uint32_t crc;
    crc = 0xffffffffU;
    crc = crc32_update(crc, record + 4U, 28U);
    crc = crc32_update(crc, record + 36U, 4U);
    crc = crc32_update(crc, record + DDQ_HEADER_SIZE, payload_len);
    crc ^= 0xffffffffU;
    return crc;
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
    size_t written;
    uint32_t iteration;
    ssize_t result;
    int status;
    written = 0U;
    status = DDQ_OK;
    for (iteration = 0U;
         written < length && iteration < DDQ_IO_ITER_LIMIT;
         ++iteration) {
        result = write(fd, data + written, length - written);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
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
    size_t read_bytes;
    uint32_t iteration;
    ssize_t result;
    int status;
    read_bytes = 0U;
    status = DDQ_OK;
    for (iteration = 0U;
         read_bytes < length && iteration < DDQ_IO_ITER_LIMIT;
         ++iteration) {
        result = pread(fd, data + read_bytes, length - read_bytes,
                       (off_t)(offset + read_bytes));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
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
                             const char *directory, uint64_t id)
{
    int written;
    int status;
    written = snprintf(path, capacity, "%s/segment-%020llu.log",
                       directory, (unsigned long long)id);
    status = DDQ_OK;
    if (written < 0 || (size_t)written >= capacity) {
        status = DDQ_TOO_LARGE;
    }
    return status;
}

static int parse_segment_name(const char *name, uint64_t *out_id)
{
    const size_t prefix_len = 8U;
    const size_t digits_len = 20U;
    const size_t suffix_len = 4U;
    size_t length;
    size_t index;
    uint64_t value;
    int status;
    length = strnlen(name, PATH_MAX);
    value = 0ULL;
    status = DDQ_CORRUPT;
    if (length == prefix_len + digits_len + suffix_len &&
        memcmp(name, "segment-", prefix_len) == 0 &&
        memcmp(name + prefix_len + digits_len, ".log", suffix_len) == 0) {
        status = DDQ_OK;
        for (index = 0U; index < digits_len; ++index) {
            if (name[prefix_len + index] < '0' ||
                name[prefix_len + index] > '9') {
                status = DDQ_CORRUPT;
                break;
            }
            if (value > (UINT64_MAX / 10ULL)) {
                status = DDQ_CORRUPT;
                break;
            }
            value = value * 10ULL +
                    (uint64_t)(name[prefix_len + index] - '0');
        }
    }
    if (status == DDQ_OK && out_id != NULL) {
        *out_id = value;
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
    int status;
    status = DDQ_OK;
    if (options == NULL ||
        options->segment_bytes < DDQ_MIN_SEGMENT_BYTES ||
        options->segment_bytes > DDQ_MAX_SEGMENT_BYTES ||
        options->max_payload_bytes == 0U ||
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
    uint32_t capacity;
    uint32_t growth;
    ddq_item_t *items;
    int status;
    capacity = queue->item_capacity;
    status = DDQ_OK;
    if (needed > queue->options.max_items) {
        status = DDQ_FULL;
    } else if (capacity < needed) {
        if (capacity == 0U) {
            capacity = 64U;
        }
        for (growth = 0U; growth < 32U && capacity < needed; ++growth) {
            if (capacity > queue->options.max_items / 2U) {
                capacity = queue->options.max_items;
            } else {
                capacity *= 2U;
            }
        }
        if (capacity < needed) {
            status = DDQ_LIMIT;
        } else {
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
    uint32_t index;
    int result;
    result = -1;
    for (index = 0U; index < queue->item_count; ++index) {
        if (queue->items[index].seq == seq) {
            result = (int)index;
            break;
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
        if (errno == ENOENT && mkdir(directory, 0755) == 0) {
            status = DDQ_OK;
        } else {
            status = DDQ_IO_ERROR;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        status = DDQ_INVALID_ARGUMENT;
    }
    return status;
}

static int create_segment(ddq_t *queue, uint64_t id)
{
    char path[PATH_MAX];
    int fd;
    int status;
    status = DDQ_OK;
    fd = -1;
    if (queue->segment_count >= queue->options.max_segments) {
        status = DDQ_LIMIT;
    } else if (make_segment_path(path, sizeof(path), queue->directory, id) !=
               DDQ_OK) {
        status = DDQ_TOO_LARGE;
    } else {
        fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_APPEND, 0644);
        if (fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (sync_directory(queue->directory) != DDQ_OK) {
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
    uint64_t next_id;
    int status;
    status = DDQ_OK;
    next_id = 0ULL;
    if (queue->current_fd < 0 || queue->segment_count == 0U) {
        status = DDQ_IO_ERROR;
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
    DIR *directory;
    struct dirent *entry;
    uint64_t id;
    uint32_t iteration;
    uint32_t iteration_limit;
    int done;
    int duplicate;
    uint32_t index;
    int status;
    status = DDQ_OK;
    queue->segment_count = 0U;
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
            entry = readdir(directory);
            if (entry == NULL) {
                done = 1;
                break;
            }
            if (parse_segment_name(entry->d_name, &id) != DDQ_OK) {
                continue;
            }
            if (queue->segment_count >= queue->options.max_segments) {
                status = DDQ_LIMIT;
                break;
            }
            duplicate = 0;
            for (index = 0U; index < queue->segment_count; ++index) {
                if (queue->segments[index].id == id) {
                    duplicate = 1;
                    break;
                }
            }
            if (duplicate != 0) {
                status = DDQ_CORRUPT;
                break;
            }
            if (make_segment_path(queue->segments[queue->segment_count].path,
                                  sizeof(queue->segments[0].path),
                                  queue->directory, id) != DDQ_OK) {
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
    return status;
}

static int validate_record_bytes(const ddq_t *queue, const uint8_t *record,
                                 uint32_t record_length, uint16_t *out_type,
                                 uint64_t *out_seq, uint64_t *out_arg,
                                 uint32_t *out_payload_length)
{
    uint32_t payload_length;
    uint32_t total_length;
    uint32_t footer_offset;
    int status;
    status = DDQ_OK;
    payload_length = 0U;
    total_length = 0U;
    if (record_length < DDQ_MIN_RECORD_SIZE ||
        get_u32(record) != DDQ_MAGIC ||
        get_u16(record + 4U) != DDQ_VERSION) {
        status = DDQ_CORRUPT;
    } else {
        payload_length = get_u32(record + 24U);
        total_length = get_u32(record + 28U);
        if (payload_length > queue->options.max_payload_bytes ||
            total_length != DDQ_HEADER_SIZE + payload_length +
                DDQ_FOOTER_SIZE ||
            total_length != record_length ||
            total_length > queue->options.segment_bytes) {
            status = DDQ_CORRUPT;
        } else {
            footer_offset = DDQ_HEADER_SIZE + payload_length;
            if (get_u32(record + footer_offset) != DDQ_COMMIT ||
                get_u32(record + footer_offset + 4U) != total_length ||
                get_u32(record + 32U) != record_crc(record, payload_length)) {
                status = DDQ_CORRUPT;
            }
        }
    }
    if (status == DDQ_OK) {
        if (out_type != NULL) {
            *out_type = get_u16(record + 6U);
        }
        if (out_seq != NULL) {
            *out_seq = get_u64(record + 8U);
        }
        if (out_arg != NULL) {
            *out_arg = get_u64(record + 16U);
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
    int existing_index;
    int capacity_status;
    int status;
    ddq_item_t *item;
    status = DDQ_OK;
    item_index = -1;
    if (type == DDQ_RECORD_DATA) {
        existing_index = find_item(queue, seq);
        capacity_status = ensure_item_capacity(queue, queue->item_count + 1U);
        if (seq == 0ULL || existing_index >= 0) {
            status = DDQ_CORRUPT;
        } else if (capacity_status != DDQ_OK) {
            status = capacity_status;
        } else {
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
            if (type == DDQ_RECORD_CLAIM &&
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
            } else {
                status = DDQ_CORRUPT;
            }
        }
    }
    (void)record;
    return status;
}

static int replay_segment(ddq_t *queue, uint32_t segment_index, int is_last)
{
    /* 只有最后一段允许截断不完整尾部；中间段损坏必须停止恢复。 */
    int fd;
    struct stat st;
    uint64_t file_size;
    uint64_t offset;
    uint32_t record_count;
    uint8_t header[DDQ_HEADER_SIZE];
    uint8_t *record;
    uint32_t total_length;
    uint16_t type;
    uint64_t seq;
    uint64_t arg;
    uint32_t payload_length;
    ssize_t header_bytes;
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
        for (record_count = 0U;
             record_count < DDQ_SCAN_RECORD_LIMIT &&
                 offset < file_size && status == DDQ_OK;
             ++record_count) {
            header_bytes = pread(fd, header, DDQ_HEADER_SIZE,
                                 (off_t)offset);
            if (header_bytes < 0) {
                status = DDQ_IO_ERROR;
                break;
            }
            if ((size_t)header_bytes < DDQ_HEADER_SIZE) {
                if (is_last != 0) {
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
            total_length = get_u32(header + 28U);
            tail = 0;
            if (get_u32(header) != DDQ_MAGIC ||
                get_u16(header + 4U) != DDQ_VERSION ||
                total_length < DDQ_MIN_RECORD_SIZE ||
                total_length > queue->options.segment_bytes) {
                status = DDQ_CORRUPT;
                break;
            }
            if (total_length > file_size - offset) {
                tail = 1;
            }
            if (tail != 0) {
                if (is_last != 0) {
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
            memcpy(record, header, DDQ_HEADER_SIZE);
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
    uint8_t *record;
    size_t written;
    uint64_t offset;
    int status;
    record = NULL;
    written = 0U;
    offset = queue->current_offset;
    status = DDQ_OK;
    if (payload_length > queue->options.max_payload_bytes ||
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
            status = rotate_segment(queue);
            offset = queue->current_offset;
        }
    }
    if (status == DDQ_OK) {
        record = (uint8_t *)calloc(1U, total_length);
        if (record == NULL) {
            status = DDQ_NO_MEMORY;
        } else {
            put_u32(record, DDQ_MAGIC);
            put_u16(record + 4U, DDQ_VERSION);
            put_u16(record + 6U, type);
            put_u64(record + 8U, seq);
            put_u64(record + 16U, arg);
            put_u32(record + 24U, payload_length);
            put_u32(record + 28U, total_length);
            if (payload_length > 0U) {
                memcpy(record + DDQ_HEADER_SIZE, payload, payload_length);
            }
            put_u32(record + 32U, record_crc(record, payload_length));
            put_u32(record + DDQ_HEADER_SIZE + payload_length, DDQ_COMMIT);
            put_u32(record + DDQ_HEADER_SIZE + payload_length + 4U,
                    total_length);
            status = write_full(queue->current_fd, record, total_length,
                                &written);
            queue->current_offset += written;
            queue->segments[queue->segment_count - 1U].size =
                queue->current_offset;
            if (status == DDQ_OK && fsync(queue->current_fd) != 0) {
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

static int read_data_payload(const ddq_t *queue, const ddq_item_t *item,
                             void *buffer)
{
    int fd;
    uint8_t header[DDQ_HEADER_SIZE];
    uint8_t *record;
    uint32_t total_length;
    uint16_t type;
    uint64_t seq;
    uint64_t arg;
    uint32_t payload_length;
    int status;
    fd = -1;
    record = NULL;
    status = DDQ_OK;
    if (item->segment_index >= queue->segment_count) {
        status = DDQ_CORRUPT;
    } else {
        fd = open(queue->segments[item->segment_index].path, O_RDONLY);
        if (fd < 0) {
            status = DDQ_IO_ERROR;
        } else if (pread_full(fd, header, DDQ_HEADER_SIZE,
                              item->offset) != DDQ_OK) {
            status = DDQ_CORRUPT;
        } else {
            total_length = get_u32(header + 28U);
            if (total_length < DDQ_MIN_RECORD_SIZE ||
                total_length > queue->options.segment_bytes) {
                status = DDQ_CORRUPT;
            } else {
                record = (uint8_t *)malloc(total_length);
                if (record == NULL) {
                    status = DDQ_NO_MEMORY;
                } else {
                    memcpy(record, header, DDQ_HEADER_SIZE);
                    status = pread_full(fd, record + DDQ_HEADER_SIZE,
                                        total_length - DDQ_HEADER_SIZE,
                                        item->offset + DDQ_HEADER_SIZE);
                    if (status == DDQ_OK) {
                        status = validate_record_bytes(
                            queue, record, total_length, &type, &seq, &arg,
                            &payload_length);
                    }
                    if (status == DDQ_OK &&
                        (type != DDQ_RECORD_DATA || seq != item->seq ||
                         payload_length != item->payload_len)) {
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
    uint32_t index;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL) {
        if (queue->current_fd >= 0) {
            close(queue->current_fd);
            queue->current_fd = -1;
        }
        queue->current_offset = 0ULL;
        queue->item_count = 0U;
        queue->next_seq = 1ULL;
        queue->recovered_tail_bytes = 0ULL;
        if (queue->segment_count == 0U) {
            status = create_segment(queue, 0ULL);
        } else {
            status = DDQ_OK;
            for (index = 0U;
                 index < queue->segment_count && status == DDQ_OK;
                 ++index) {
                status = replay_segment(
                    queue, index, index + 1U == queue->segment_count);
            }
            if (status == DDQ_OK && queue->current_fd < 0) {
                status = DDQ_IO_ERROR;
            }
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
                    queue->lock_fd = open(lock_path, O_CREAT | O_RDWR, 0644);
                    if (queue->lock_fd < 0) {
                        status = DDQ_IO_ERROR;
                    } else if (flock(queue->lock_fd,
                                     LOCK_EX | LOCK_NB) != 0) {
                        status = DDQ_LOCKED;
                    }
                }
            }
            if (status == DDQ_OK) {
                status = scan_segments(queue);
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
    uint64_t seq;
    uint64_t offset;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL && out_seq != NULL &&
        (payload_len == 0U || payload != NULL)) {
        *out_seq = 0ULL;
        if (payload_len > queue->options.max_payload_bytes) {
            status = DDQ_TOO_LARGE;
        } else if (queue->item_count >= queue->options.max_items ||
                   queue->next_seq == UINT64_MAX) {
            status = DDQ_FULL;
        } else {
            status = ensure_item_capacity(queue, queue->item_count + 1U);
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
    uint32_t index;
    int selected;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    selected = -1;
    if (queue != NULL && out_claim != NULL && lease_ms > 0U &&
        (uint64_t)lease_ms <= DDQ_MAX_LEASE_MS) {
        memset(out_claim, 0, sizeof(*out_claim));
        now = wall_clock_ms();
        if (now > UINT64_MAX - (uint64_t)lease_ms) {
            status = DDQ_LIMIT;
        } else {
            lease_until = now + (uint64_t)lease_ms;
            for (index = 0U; index < queue->item_count; ++index) {
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
            if (selected < 0) {
                status = DDQ_EMPTY;
            } else {
                status = append_record(
                    queue, DDQ_RECORD_CLAIM,
                    queue->items[selected].seq, lease_until, NULL, 0U,
                    NULL);
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
    int item_index;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (out_len != NULL) {
        *out_len = 0U;
    }
    if (queue != NULL && out_len != NULL) {
        item_index = find_item(queue, seq);
        if (item_index < 0) {
            status = DDQ_NOT_FOUND;
        } else if (queue->items[item_index].payload_len > buffer_cap ||
                   (queue->items[item_index].payload_len > 0U &&
                    buffer == NULL)) {
            *out_len = queue->items[item_index].payload_len;
            status = DDQ_BUFFER_TOO_SMALL;
        } else {
            status = read_data_payload(queue, &queue->items[item_index],
                                       buffer);
            if (status == DDQ_OK) {
                *out_len = queue->items[item_index].payload_len;
            }
        }
    }
    return status;
}

int ddq_ack(ddq_t *queue, uint64_t seq)
{
    int item_index;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL) {
        item_index = find_item(queue, seq);
        if (item_index < 0) {
            status = DDQ_NOT_FOUND;
        } else if (queue->items[item_index].state == DDQ_ITEM_ACKED) {
            status = DDQ_ALREADY_DONE;
        } else if (queue->items[item_index].state != DDQ_ITEM_CLAIMED) {
            status = DDQ_BAD_STATE;
        } else {
            status = append_record(queue, DDQ_RECORD_ACK, seq, 0ULL,
                                   NULL, 0U, NULL);
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
    int item_index;
    uint64_t not_before;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL) {
        item_index = find_item(queue, seq);
        if (item_index < 0) {
            status = DDQ_NOT_FOUND;
        } else if (queue->items[item_index].state != DDQ_ITEM_CLAIMED) {
            status = DDQ_BAD_STATE;
        } else if (queue->items[item_index].attempts == UINT32_MAX) {
            status = DDQ_LIMIT;
        } else {
            not_before = wall_clock_ms();
            if (not_before > UINT64_MAX - (uint64_t)delay_ms) {
                status = DDQ_LIMIT;
            } else {
                not_before += (uint64_t)delay_ms;
                status = append_record(queue, DDQ_RECORD_RETRY, seq,
                                       not_before, NULL, 0U, NULL);
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
    int item_index;
    size_t reason_length;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    reason_length = 0U;
    if (queue != NULL) {
        item_index = find_item(queue, seq);
        if (item_index < 0) {
            status = DDQ_NOT_FOUND;
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
                    (uint32_t)reason_length, NULL);
                if (status == DDQ_OK) {
                    queue->items[item_index].state =
                        DDQ_ITEM_QUARANTINED;
                    queue->items[item_index].lease_until_ms = 0ULL;
                }
            }
        }
    }
    return status;
}

int ddq_stats(ddq_t *queue, ddq_stats_t *out_stats)
{
    uint32_t index;
    int status;
    status = DDQ_INVALID_ARGUMENT;
    if (queue != NULL && out_stats != NULL) {
        memset(out_stats, 0, sizeof(*out_stats));
        out_stats->total_items = queue->item_count;
        out_stats->segment_count = queue->segment_count;
        out_stats->next_seq = queue->next_seq;
        out_stats->recovered_tail_bytes = queue->recovered_tail_bytes;
        for (index = 0U; index < queue->segment_count; ++index) {
            out_stats->stored_bytes += queue->segments[index].size;
        }
        for (index = 0U; index < queue->item_count; ++index) {
            if (queue->items[index].state == DDQ_ITEM_AVAILABLE) {
                out_stats->available_items += 1ULL;
            } else if (queue->items[index].state == DDQ_ITEM_CLAIMED) {
                out_stats->claimed_items += 1ULL;
            } else if (queue->items[index].state == DDQ_ITEM_ACKED) {
                out_stats->acknowledged_items += 1ULL;
            } else if (queue->items[index].state == DDQ_ITEM_QUARANTINED) {
                out_stats->quarantined_items += 1ULL;
            }
        }
        status = DDQ_OK;
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
        message = "message is already acknowledged";
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
