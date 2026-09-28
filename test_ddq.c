#define _POSIX_C_SOURCE 200809L

#include "ddq.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

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
    DIR *dir;
    struct dirent *entry;
    char candidate[PATH_MAX];
    const char *best;
    uint32_t iterations;
    int status;
    int written;
    dir = NULL;
    entry = NULL;
    best = NULL;
    status = DDQ_IO_ERROR;
    dir = opendir(directory);
    if (dir != NULL) {
        status = DDQ_NOT_FOUND;
        for (iterations = 0U; iterations < 256U; ++iterations) {
            entry = readdir(dir);
            if (entry == NULL) {
                break;
            }
            if (strncmp(entry->d_name, "segment-", 8U) != 0 ||
                strstr(entry->d_name, ".log") == NULL) {
                continue;
            }
            if (best == NULL || strcmp(entry->d_name, best) > 0) {
                best = entry->d_name;
            }
        }
        if (best != NULL) {
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
    DIR *dir;
    struct dirent *entry;
    char path[PATH_MAX];
    uint32_t iterations;
    int written;
    dir = opendir(directory);
    if (dir != NULL) {
        for (iterations = 0U; iterations < 256U; ++iterations) {
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
    rmdir(directory);
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
        options.max_segments = 8U;
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
        printf("ddq tests passed\n");
    }
    return status;
}
