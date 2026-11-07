/*
 * SIGTERM detection for pubsubr.
 *
 * Base R cannot catch SIGTERM: it ends the R process. This file installs a
 * SIGTERM handler that only sets a flag, which subscribe() polls to exit its
 * loop gracefully.
 *
 * Adapted from the 'sigterm' R package, src/sigterm.c
 * (https://github.com/atheriel/sigterm, commit d742e32).
 * The code is embedded here, instead of depending on the 'sigterm' package,
 * because that package is not available on CRAN, so a CRAN package cannot
 * depend on it.
 * Copyright (c) Aaron Jacobs.
 * Licensed under the Apache License, Version 2.0; see
 * inst/licenses/LICENSE-APACHE-2.0 and inst/COPYRIGHTS.
 *
 * Modifications from the original, by Jean-Philippe Gravel (2026):
 *   - The handler is installed only while subscribe() runs, and the previous
 *     SIGTERM handler is restored afterwards (the original installed it for
 *     the whole session when the package was loaded).
 *   - The flag is reset each time the handler is installed.
 *   - As in the Python formix-pubsub library, the previous handler is called
 *     after it is restored when a signal was received, if it is a function
 *     (SIG_DFL and SIG_IGN are not called).
 *   - Routines renamed and registered under pubsubr.
 */

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include <signal.h>
#include <string.h>

static volatile sig_atomic_t flag = 0;
static struct sigaction previous_action;
static int installed = 0;

static void handler(int signum)
{
  (void) signum;
  flag = 1;
}

SEXP pubsubr_sigterm_install(void)
{
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  action.sa_handler = handler;
  sigemptyset(&action.sa_mask);
  flag = 0;
  if (!installed) {
    if (sigaction(SIGTERM, &action, &previous_action) != 0) {
      Rf_error("cannot install the SIGTERM handler");
    }
    installed = 1;
  }
  return R_NilValue;
}

SEXP pubsubr_sigterm_received(void)
{
  return Rf_ScalarLogical(flag != 0);
}

SEXP pubsubr_sigterm_restore(void)
{
  if (!installed) return R_NilValue;
  int received = flag != 0;
  sigaction(SIGTERM, &previous_action, NULL);
  installed = 0;
  flag = 0;
  if (received && !(previous_action.sa_flags & SA_SIGINFO) &&
      previous_action.sa_handler != SIG_DFL &&
      previous_action.sa_handler != SIG_IGN) {
    previous_action.sa_handler(SIGTERM);
  }
  return R_NilValue;
}

static const R_CallMethodDef call_entries[] = {
  {"C_pubsubr_sigterm_install", (DL_FUNC) &pubsubr_sigterm_install, 0},
  {"C_pubsubr_sigterm_received", (DL_FUNC) &pubsubr_sigterm_received, 0},
  {"C_pubsubr_sigterm_restore", (DL_FUNC) &pubsubr_sigterm_restore, 0},
  {NULL, NULL, 0}
};

void R_init_pubsubr(DllInfo *dll)
{
  R_registerRoutines(dll, NULL, call_entries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  R_forceSymbols(dll, TRUE);
}
