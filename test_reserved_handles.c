#define _POSIX_C_SOURCE 200809L
#include <ddq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <limits.h>

static int inject_enfile;
static int inject_sync;
static int opens_under_injection;
static long long test_deadline;
int __real_open(const char *, int, ...);
int __real_fsync(int);
/* 整机句柄耗尽只注入错误，不对其他进程制造资源压力。 */
int __wrap_open(const char *path, int flags, ...)
{
    va_list args;
    mode_t mode = 0;
    int result;
    if ((flags & O_CREAT) != 0) { va_start(args, flags); mode = (mode_t)va_arg(args, int); va_end(args); }
    if (inject_enfile != 0) { opens_under_injection++; errno = ENFILE; result = -1; }
    else result = __real_open(path, flags, mode);
    return result;
}
/* 同步失败绝不能当作已持久保存。 */
int __wrap_fsync(int fd)
{
    int result;
    if (inject_sync != 0) { inject_sync = 0; errno = EIO; result = -1; }
    else result = __real_fsync(fd);
    return result;
}
/* 所有夹具循环共用单调时钟总界限。 */
static long long now_ms(void)
{
    struct timespec t;
    long long result = 0;
    if (clock_gettime(CLOCK_MONOTONIC, &t) == 0) result = (long long)t.tv_sec * 1000 + t.tv_nsec / 1000000;
    return result;
}
/* 失败输出具体条件，正文只有测试编号。 */
static int expect(const char *label, int actual, int expected)
{
    int result = 0;
    if (actual != expected || now_ms() >= test_deadline) { fprintf(stderr, "%s actual=%d expected=%d\n", label, actual, expected); result = 1; }
    return result;
}
/* 仅扫描进程自身 fd 目录，最多一千项及一秒，不扫描文件系统。 */
static int fd_count(void)
{
    DIR *d = opendir("/proc/self/fd");
    struct dirent *entry;
    unsigned int i;
    long long deadline = now_ms() + 1000;
    int result = 0;
    if (d == NULL) result = -1;
    else {
        for (i = 0; i < 1024U && now_ms() < deadline; i++) {
            entry = readdir(d);
            if (entry == NULL) break;
            if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) result++;
        }
        if (i == 1024U || now_ms() >= deadline) result = -1;
        (void)closedir(d);
    }
    return result;
}
/* 每条记录通过公开领取、读取、确认接口；核对最早序号和原文不变。 */
static int consume(ddq_t *queue, uint64_t expected)
{
    ddq_claim_t claim;
    char payload[120];
    size_t length = 0;
    int result = 0;
    memset(&claim, 0, sizeof(claim));
    result |= expect("claim", ddq_claim_ordered(queue, 1000, &claim), DDQ_OK);
    result |= expect("FIFO", claim.seq == expected, 1);
    result |= expect("read", ddq_read(queue, claim.seq, payload, sizeof(payload), &length), DDQ_OK);
    result |= expect("payload length", (int)length, (int)sizeof(payload));
    result |= expect("payload", length == sizeof(payload) && payload[0] == 'R' && payload[119] == 'R', 1);
    result |= expect("ack", ddq_ack(queue, claim.seq), DDQ_OK);
    return result;
}
/* 最小真实 EMFILE 仅限制本测试进程，预留模式必须跨段且完成读回和 ACK。 */
static int emfile_case(const char *path)
{
    ddq_options_t options;
    ddq_t *queue = NULL;
    ddq_stats_t stats;
    struct rlimit original;
    struct rlimit limited;
    int filler[256];
    unsigned int used = 0;
    unsigned int i;
    int saved_errno = 0;
    long long cleanup_deadline = 0;
    int before = fd_count();
    int result = 0;
    char payload[120];
    uint64_t seq = 0;
    memset(payload, 'R', sizeof(payload));
    (void)ddq_options_default(&options);
    options.segment_bytes = 256; options.max_payload_bytes = 120; options.max_items = 64; options.max_segments = 32;
    result |= expect("open", ddq_open(&queue, path, &options), DDQ_OK);
    if (result == 0) result |= expect("reserve", ddq_reserve_handles(queue, 32), DDQ_OK);
    result |= expect("read original limits", getrlimit(RLIMIT_NOFILE, &original), 0);
    limited = original;
    if (limited.rlim_cur > 128) limited.rlim_cur = 128;
    result |= expect("limit this process", setrlimit(RLIMIT_NOFILE, &limited), 0);
    for (i = 0; i < 256U && now_ms() < test_deadline; i++) {
        int fd = open("/dev/null", O_RDONLY);
        if (fd < 0) { saved_errno = errno; break; }
        filler[used++] = fd;
    }
    result |= expect("real EMFILE", saved_errno, EMFILE);
    if (result == 0) {
        result |= expect("enqueue first", ddq_enqueue(queue, payload, sizeof(payload), &seq), DDQ_OK);
        result |= expect("first seq", (int)seq, 1);
        result |= expect("enqueue next segment", ddq_enqueue(queue, payload, sizeof(payload), &seq), DDQ_OK);
        result |= consume(queue, 1);
        result |= consume(queue, 2);
        result |= expect("crossed segments", ddq_stats(queue, &stats), DDQ_OK);
        result |= expect("more than two active segments", stats.segment_count >= 3, 1);
    }
    /* 此夹具最多占用二百五十六个句柄，回收也有独立一秒墙钟界限。 */
    cleanup_deadline = now_ms() + 1000;
    for (i = 0; i < used && i < 256U && now_ms() < cleanup_deadline; i++) (void)close(filler[i]);
    result |= expect("filler cleanup complete", (int)i, (int)used);
    result |= expect("restore process limit", setrlimit(RLIMIT_NOFILE, &original), 0);
    ddq_close(queue); queue = NULL;
    result |= expect("close releases every fd", fd_count(), before);
    /* 排空重建走已有全 ACK 压缩，必须保留严格递增序号并补足真实储备。 */
    if (result == 0) result |= expect("reopen", ddq_open(&queue, path, &options), DDQ_OK);
    if (result == 0) result |= expect("rearm drained", ddq_reserve_handles(queue, 32), DDQ_OK);
    if (result == 0) {
        result |= expect("drained stats", ddq_stats(queue, &stats), DDQ_OK);
        result |= expect("drained history compacted", stats.segment_count == 1 && stats.total_items == 0 && stats.next_seq == 3, 1);
        result |= expect("next enqueue", ddq_enqueue(queue, payload, sizeof(payload), &seq), DDQ_OK);
        result |= expect("no seq reuse", (int)seq, 3);
        result |= consume(queue, 3);
    }
    ddq_close(queue);
    result |= expect("rearm close releases every fd", fd_count(), before);
    return result;
}
/* ENFILE 注入覆盖满容量与预留控制空间，期间一次新 open 也不允许。 */
static int enfile_case(const char *path)
{
    ddq_options_t options;
    ddq_t *queue = NULL;
    ddq_stats_t stats;
    uint64_t seq = 0;
    unsigned int count = 0;
    unsigned int i;
    int rc = DDQ_OK;
    int result = 0;
    int before = fd_count();
    char payload[120];
    memset(payload, 'R', sizeof(payload));
    (void)ddq_options_default(&options);
    options.segment_bytes = 256; options.max_payload_bytes = 120; options.max_items = 64; options.max_segments = 32;
    result |= expect("ENFILE open", ddq_open(&queue, path, &options), DDQ_OK);
    if (result == 0) result |= expect("ENFILE reserve", ddq_reserve_handles(queue, 32), DDQ_OK);
    inject_enfile = 1; opens_under_injection = 0;
    for (i = 0; i < 64U && result == 0 && rc == DDQ_OK && now_ms() < test_deadline; i++) {
        rc = ddq_enqueue(queue, payload, sizeof(payload), &seq);
        if (rc == DDQ_OK) count++;
    }
    result |= expect("reservoir bounded FULL", rc, DDQ_FULL);
    result |= expect("reserved multiple messages", count > 2, 1);
    for (i = 0; i < count && i < 64U && result == 0 && now_ms() < test_deadline; i++) result |= consume(queue, i + 1U);
    result |= expect("no new open even at FULL", opens_under_injection, 0);
    result |= expect("control space survives FULL", ddq_stats(queue, &stats), DDQ_OK);
    result |= expect("every accepted message acknowledged", (int)stats.acknowledged_items, (int)count);
    inject_enfile = 0;
    ddq_close(queue);
    result |= expect("ENFILE close no fd leak", fd_count(), before);
    printf("ENFILE accepted=%u, active_segments=%u, new_open_calls=%d\n", count, stats.segment_count, opens_under_injection);
    return result;
}
/* 重启跳过空预留，同 inode 激活别名回收后仍按原序号重放，未知非空预留保留。 */
static int restart_case(const char *path)
{
    ddq_options_t options;
    ddq_t *queue = NULL;
    char payload[120];
    char active[PATH_MAX];
    char spare[PATH_MAX];
    uint64_t seq = 0;
    struct stat st;
    int fd;
    int result = 0;
    int before = fd_count();
    memset(payload, 'R', sizeof(payload));
    (void)ddq_options_default(&options);
    options.segment_bytes = 256; options.max_payload_bytes = 120; options.max_items = 64; options.max_segments = 32;
    result |= expect("restart open", ddq_open(&queue, path, &options), DDQ_OK);
    if (result == 0) result |= expect("restart reserve", ddq_reserve_handles(queue, 32), DDQ_OK);
    if (result == 0) result |= expect("restart enqueue 1", ddq_enqueue(queue, payload, sizeof(payload), &seq), DDQ_OK);
    if (result == 0) result |= expect("restart enqueue 2", ddq_enqueue(queue, payload, sizeof(payload), &seq), DDQ_OK);
    ddq_close(queue); queue = NULL;
    (void)snprintf(active, sizeof(active), "%.4000s/segment-%020u.log", path, 0U);
    (void)snprintf(spare, sizeof(spare), "%.4000s/segment-%020u.log.reserve", path, 0U);
    result |= expect("make accepted inode alias", link(active, spare), 0);
    if (result == 0) result |= expect("recover linked alias", ddq_open(&queue, path, &options), DDQ_OK);
    if (result == 0) result |= expect("alias removed", lstat(spare, &st), -1);
    if (result == 0) result |= expect("restart re-reserve", ddq_reserve_handles(queue, 32), DDQ_OK);
    if (result == 0) result |= consume(queue, 1);
    if (result == 0) result |= consume(queue, 2);
    ddq_close(queue); queue = NULL;
    (void)snprintf(spare, sizeof(spare), "%.4000s/segment-%020u.log.reserve", path, 30U);
    fd = open(spare, O_CREAT | O_EXCL | O_WRONLY, 0600);
    result |= expect("make orphan reserved data", fd >= 0, 1);
    if (fd >= 0) { result |= expect("write orphan", (int)write(fd, "R", 1), 1); (void)close(fd); }
    result |= expect("orphan is not silently deleted", ddq_open(&queue, path, &options), DDQ_CORRUPT);
    result |= expect("orphan preserved", lstat(spare, &st), 0);
    result |= expect("orphan still nonempty", (int)st.st_size, 1);
    ddq_close(queue);
    result |= expect("restart no fd leak", fd_count(), before);
    return result;
}
/* fsync 错误必须停写，不能返回成功序号；恢复由明确的资源恢复阶段执行。 */
static int sync_case(const char *path)
{
    ddq_options_t options;
    ddq_t *queue = NULL;
    uint64_t seq = 99;
    int result = 0;
    int before = fd_count();
    (void)ddq_options_default(&options);
    options.max_segments = 32;
    result |= expect("sync open", ddq_open(&queue, path, &options), DDQ_OK);
    if (result == 0) result |= expect("sync reserve", ddq_reserve_handles(queue, 32), DDQ_OK);
    inject_sync = 1;
    if (result == 0) result |= expect("fsync failure", ddq_enqueue(queue, "R", 1, &seq), DDQ_IO_ERROR);
    result |= expect("not acknowledged durable", (int)seq, 0);
    result |= expect("stops accepting writes", ddq_enqueue(queue, "R", 1, &seq), DDQ_IO_ERROR);
    ddq_close(queue);
    result |= expect("sync failure no fd leak", fd_count(), before);
    return result;
}
/* 主入口最多四种夹具及二十五秒；输出本进程身份供脚本审计。 */
int main(int argc, char **argv)
{
    char path[PATH_MAX];
    int result = 0;
    test_deadline = now_ms() + 25000;
    /* 路径预算为固定段名保留空间，拒绝可能截断的测试根目录。 */
    if (argc != 3) result = 1;
    else if (strlen(argv[1]) > 3900U) result = 1;
    printf("test pid=%ld ppid=%ld start_ms=%lld\n", (long)getpid(), (long)getppid(), now_ms());
    if (result == 0) { (void)snprintf(path, sizeof(path), "%s/emfile", argv[1]); result |= emfile_case(path); }
    if (result == 0 && strcmp(argv[2], "full") == 0) { (void)snprintf(path, sizeof(path), "%s/enfile", argv[1]); result |= enfile_case(path); }
    if (result == 0 && strcmp(argv[2], "full") == 0) { (void)snprintf(path, sizeof(path), "%s/restart", argv[1]); result |= restart_case(path); }
    if (result == 0 && strcmp(argv[2], "full") == 0) { (void)snprintf(path, sizeof(path), "%s/sync", argv[1]); result |= sync_case(path); }
    if (result == 0) puts("Reserved descriptor tests passed.");
    return result;
}