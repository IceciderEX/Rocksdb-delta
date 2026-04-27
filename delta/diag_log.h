//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef __GNUC__
#define DIAG_PRINTF_ATTR __attribute__((format(printf, 1, 2)))
#else
#define DIAG_PRINTF_ATTR
#endif

void DiagLogOpen(const char* path);
void DiagLogf(const char* fmt, ...) DIAG_PRINTF_ATTR;
void DiagLogSetStderrEnabled(bool enabled);

void LifecycleLogOpen(const char* path);
void LifecycleLogf(const char* fmt, ...) DIAG_PRINTF_ATTR;