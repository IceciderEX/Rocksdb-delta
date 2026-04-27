//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "delta/diag_log.h"

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <mutex>

static FILE* g_diag_file = nullptr;
static std::mutex g_diag_mutex;
static std::atomic<bool> g_diag_stderr_enabled{false};

void DiagLogSetStderrEnabled(bool enabled) {
  g_diag_stderr_enabled.store(enabled, std::memory_order_relaxed);
}

void DiagLogOpen(const char* path) {
  std::lock_guard<std::mutex> lock(g_diag_mutex);
  if (g_diag_file != nullptr) {
    fclose(g_diag_file);
    g_diag_file = nullptr;
  }
  g_diag_file = fopen(path, "a");
}

void DiagLogf(const char* fmt, ...) {
  char buffer[8192];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);

  if (g_diag_stderr_enabled.load(std::memory_order_relaxed)) {
    fputs(buffer, stderr);
  }

  std::lock_guard<std::mutex> lock(g_diag_mutex);
  if (g_diag_file != nullptr) {
    fputs(buffer, g_diag_file);
    fflush(g_diag_file);
  }
}

static FILE* g_lifecycle_file = nullptr;
static std::mutex g_lifecycle_mutex;

void LifecycleLogOpen(const char* path) {
  std::lock_guard<std::mutex> lock(g_lifecycle_mutex);
  if (g_lifecycle_file != nullptr) {
    fclose(g_lifecycle_file);
    g_lifecycle_file = nullptr;
  }
  g_lifecycle_file = fopen(path, "a");
}

void LifecycleLogf(const char* fmt, ...) {
  std::lock_guard<std::mutex> lock(g_lifecycle_mutex);
  if (g_lifecycle_file == nullptr) {
    return;
  }

  char buffer[4096];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);

  fputs(buffer, g_lifecycle_file);
  fflush(g_lifecycle_file);
}