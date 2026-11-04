/*
 * Low-level POSIX helpers for pubsubr.
 *
 * R's base library has no equivalent for a few system calls used by the
 * Python implementation (os.kill(pid, 0), os.mkfifo, os.open with O_NONBLOCK,
 * os.read and signal.signal). They are provided here as thin wrappers.
 */

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* ---------------------------------------------------------------- process */

SEXP pubsubr_is_process_running(SEXP pid_) {
    pid_t pid = (pid_t) asReal(pid_);
    if (pid <= 0) return ScalarLogical(FALSE);
    /* Signal 0 doesn't send a signal but checks if the process exists. */
    if (kill(pid, 0) == 0) return ScalarLogical(TRUE);
    return ScalarLogical(FALSE);
}

/* ------------------------------------------------------------------- fifo */

SEXP pubsubr_mkfifo(SEXP path_) {
    const char *path = R_ExpandFileName(translateChar(STRING_ELT(path_, 0)));
    if (mkfifo(path, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP) != 0) {
        error("%s", strerror(errno));
    }
    /* os.chmod: mkfifo is subject to the umask. */
    if (chmod(path, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP) != 0) {
        error("%s", strerror(errno));
    }
    return R_NilValue;
}

/* mode: 0 = O_RDONLY | O_NONBLOCK, 1 = O_WRONLY | O_NONBLOCK */
SEXP pubsubr_open_nonblock(SEXP path_, SEXP mode_) {
    const char *path = R_ExpandFileName(translateChar(STRING_ELT(path_, 0)));
    int flags = (asInteger(mode_) == 1 ? O_WRONLY : O_RDONLY) | O_NONBLOCK;
    int fd = open(path, flags);
    if (fd == -1) error("%s", strerror(errno));
    return ScalarInteger(fd);
}

SEXP pubsubr_close_fd(SEXP fd_) {
    if (close(asInteger(fd_)) != 0) error("%s", strerror(errno));
    return R_NilValue;
}

/* Returns a raw vector, or NULL when the read would block (BlockingIOError). */
SEXP pubsubr_read_fd(SEXP fd_, SEXP n_) {
    int n = asInteger(n_);
    unsigned char buffer[64];
    if (n < 0 || n > (int) sizeof(buffer)) error("invalid read size %d", n);
    ssize_t got = read(asInteger(fd_), buffer, (size_t) n);
    if (got < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return R_NilValue;
        error("%s", strerror(errno));
    }
    SEXP out = PROTECT(allocVector(RAWSXP, got));
    if (got > 0) memcpy(RAW(out), buffer, (size_t) got);
    UNPROTECT(1);
    return out;
}

SEXP pubsubr_write_fd(SEXP fd_, SEXP data_) {
    ssize_t written = write(asInteger(fd_), RAW(data_), (size_t) XLENGTH(data_));
    if (written < 0) error("%s", strerror(errno));
    return ScalarInteger((int) written);
}

/* ---------------------------------------------------------------- signals */

static volatile sig_atomic_t shutdown_signal = 0;
static struct sigaction original_sigterm;
static struct sigaction original_sigint;
static int handlers_installed = 0;

static void signal_handler(int signum) {
    shutdown_signal = signum;
}

SEXP pubsubr_install_signal_handlers(void) {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = signal_handler;
    sigemptyset(&action.sa_mask);
    shutdown_signal = 0;
    sigaction(SIGTERM, &action, &original_sigterm);
    sigaction(SIGINT, &action, &original_sigint);
    handlers_installed = 1;
    return R_NilValue;
}

SEXP pubsubr_shutdown_signal(void) {
    return ScalarInteger((int) shutdown_signal);
}

/*
 * Restore the original handlers. As in the Python implementation, the original
 * handler is called afterwards when a signal was received and that handler is
 * a function (SIG_DFL and SIG_IGN are not called).
 */
SEXP pubsubr_restore_signal_handlers(void) {
    if (!handlers_installed) return R_NilValue;
    int signum = (int) shutdown_signal;
    sigaction(SIGTERM, &original_sigterm, NULL);
    sigaction(SIGINT, &original_sigint, NULL);
    handlers_installed = 0;
    shutdown_signal = 0;

    struct sigaction *original = NULL;
    if (signum == SIGTERM) original = &original_sigterm;
    else if (signum == SIGINT) original = &original_sigint;
    if (original != NULL && !(original->sa_flags & SA_SIGINFO) &&
        original->sa_handler != SIG_DFL && original->sa_handler != SIG_IGN) {
        original->sa_handler(signum);
    }
    return R_NilValue;
}

/* ----------------------------------------------------------- registration */

static const R_CallMethodDef CallEntries[] = {
    {"C_pubsubr_is_process_running", (DL_FUNC) &pubsubr_is_process_running, 1},
    {"C_pubsubr_mkfifo", (DL_FUNC) &pubsubr_mkfifo, 1},
    {"C_pubsubr_open_nonblock", (DL_FUNC) &pubsubr_open_nonblock, 2},
    {"C_pubsubr_close_fd", (DL_FUNC) &pubsubr_close_fd, 1},
    {"C_pubsubr_read_fd", (DL_FUNC) &pubsubr_read_fd, 2},
    {"C_pubsubr_write_fd", (DL_FUNC) &pubsubr_write_fd, 2},
    {"C_pubsubr_install_signal_handlers", (DL_FUNC) &pubsubr_install_signal_handlers, 0},
    {"C_pubsubr_shutdown_signal", (DL_FUNC) &pubsubr_shutdown_signal, 0},
    {"C_pubsubr_restore_signal_handlers", (DL_FUNC) &pubsubr_restore_signal_handlers, 0},
    {NULL, NULL, 0}
};

void R_init_pubsubr(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
    R_forceSymbols(dll, TRUE);
}
