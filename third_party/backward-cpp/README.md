# backward-cpp Integration

This directory contains the backward-cpp header for improved stack traces with line numbers.

## Overview

[backward-cpp](https://github.com/bombela/backward-cpp) is a header-only library that provides
beautiful stack traces with source file and line number information. This replaces the current
Abseil-based stack trace implementation which does not show line numbers even in debug builds.

## Requirements

On Linux, backward-cpp requires the following libraries for full functionality:

```bash
sudo apt-get install libdw-dev libunwind-dev
```

- **libunwind**: For stack unwinding
- **libdw**: For DWARF debug info parsing (provides line numbers)

## Required Changes to helio Submodule

The following changes need to be made in the helio submodule:

### 1. helio/base/CMakeLists.txt

Replace the Abseil failure signal handler with backward-cpp libraries:

```cmake
add_library(base cpu_features.cc hash.cc histogram.cc init.cc logging.cc proc_util.cc
    pthread_utils.cc varz_node.cc cuckoo_map.cc io_buf.cc segment_pool.cc)

# Add include path for backward-cpp header
target_include_directories(base PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../..)

if (LEGACY_GLOG) 
  set(LOG_LIBS glog::glog)
else()
  target_compile_definitions(base PUBLIC -DUSE_ABSL_LOG=1)
  set(LOG_LIBS absl::check absl::log -Wl,--whole-archive absl::log_flags
      absl::log_initialize)
endif()

# backward-cpp requires libdw (for DWARF debug info) and libunwind for stack traces
# with line numbers. We find and link these libraries on Linux.
if (NOT APPLE)
  find_library(LIBDW_LIBRARY dw)
  find_library(LIBUNWIND_LIBRARY unwind)
  if (LIBDW_LIBRARY AND LIBUNWIND_LIBRARY)
    set(BACKWARD_LIBS ${LIBDW_LIBRARY} ${LIBUNWIND_LIBRARY})
    message(STATUS "backward-cpp: using libdw (${LIBDW_LIBRARY}) and libunwind (${LIBUNWIND_LIBRARY}) for stack traces with line numbers")
  else()
    message(WARNING "backward-cpp: libdw or libunwind not found, stack traces may lack line numbers")
    set(BACKWARD_LIBS "")
  endif()
else()
  # On macOS, backward-cpp uses different mechanisms
  set(BACKWARD_LIBS "")
endif()

# Note: Removed absl::failure_signal_handler as we now use backward-cpp
cxx_link(base ${LOG_LIBS} absl::flags_parse 
    absl::strings absl::symbolize absl::time
    TRDP::xxhash TRDP::expected ${BACKWARD_LIBS})
```

### 2. helio/base/init.cc

Replace the Abseil failure signal handler initialization with backward-cpp:

```cpp
// At the top of the file, replace:
// #include <absl/debugging/failure_signal_handler.h>

// Add these includes and definitions:
// Use backward-cpp for stack traces with line numbers.
#define BACKWARD_HAS_UNWIND 1
#define BACKWARD_HAS_DW 1
#include "third_party/backward-cpp/backward.hpp"

namespace {

// Global backward-cpp signal handling - provides stack traces with line numbers
// for SIGSEGV, SIGABRT, SIGFPE, SIGILL, SIGBUS, and SIGTERM.
backward::SignalHandling g_signal_handling;

}  // namespace

// In MainInitGuard constructor, remove:
//   absl::FailureSignalHandlerOptions options;
//   absl::InstallFailureSignalHandler(options);
// 
// Replace with a comment:
//   // backward-cpp signal handler is initialized globally via g_signal_handling.
//   // It provides stack traces with line numbers using libunwind + libdw.
```

### 3. helio/util/fibers/CMakeLists.txt

Add the include path for backward-cpp:

```cmake
# After the add_library(fibers2 ...) line, add:
target_include_directories(fibers2 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../..)
```

### 4. helio/util/fibers/stacktrace.cc

Replace the Abseil-based stacktrace implementation with backward-cpp:

```cpp
// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/stacktrace.h"

#include <sstream>

// Use backward-cpp for stack traces with line numbers.
#define BACKWARD_HAS_UNWIND 1
#define BACKWARD_HAS_DW 1
#include "third_party/backward-cpp/backward.hpp"

#ifdef NDEBUG
#define SKIP_COUNT 2
#else
#define SKIP_COUNT 5
#endif

std::string util::fb2::GetStacktrace() {
  backward::StackTrace st;
  st.load_here(32);

  // Skip internal frames
  st.skip_n_firsts(SKIP_COUNT);

  backward::TraceResolver resolver;
  resolver.load_stacktrace(st);

  std::ostringstream oss;
  for (size_t i = 0; i < st.size(); ++i) {
    backward::ResolvedTrace trace = resolver.resolve(st[i]);

    oss << "#" << i << " " << trace.addr;
    if (!trace.object_filename.empty()) {
      oss << " in " << trace.object_filename;
    }
    if (!trace.object_function.empty()) {
      oss << " " << trace.object_function;
    }

    // Print source location if available (this is the key improvement over abseil)
    if (!trace.source.filename.empty()) {
      oss << " at " << trace.source.filename << ":" << trace.source.line;
      if (!trace.source.function.empty()) {
        oss << " in " << trace.source.function;
      }
    }
    oss << "\n";

    // Print inlined calls if any
    for (size_t j = 0; j < trace.inliners.size(); ++j) {
      const backward::ResolvedTrace::SourceLoc& inliner = trace.inliners[j];
      oss << "    (inlined) " << inliner.filename << ":" << inliner.line;
      if (!inliner.function.empty()) {
        oss << " in " << inliner.function;
      }
      oss << "\n";
    }
  }

  return oss.str();
}
```

## Benefits of backward-cpp over Abseil

1. **Line Numbers**: backward-cpp shows source file line numbers when debug info is available
2. **Inlined Functions**: Shows inlined function calls
3. **Better Formatting**: Cleaner, more readable stack trace output
4. **No Runtime Overhead**: Header-only, no performance impact until a crash occurs

## Example Output

Before (Abseil):
```
0x5555555a1234  my_function
0x5555555a5678  main
```

After (backward-cpp):
```
#0 0x5555555a1234 in /path/to/binary my_function at /src/my_file.cc:42 in my_function
#1 0x5555555a5678 in /path/to/binary main at /src/main.cc:15 in main
```

## Building

Ensure you have the required libraries installed:

```bash
# Ubuntu/Debian
sudo apt-get install libdw-dev libunwind-dev

# Fedora/RHEL
sudo dnf install elfutils-devel libunwind-devel
```

Then build with:

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j4
```

The CMake configuration will automatically detect libdw and libunwind and enable
line number support in stack traces.
