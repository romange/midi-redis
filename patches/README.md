# Helio Patches for backward-cpp Integration

This directory contains patches to integrate backward-cpp into the helio submodule
for improved stack traces with line numbers.

## Quick Start

Apply all patches to helio:

```bash
cd helio
git apply ../patches/helio-backward-cpp-all.patch
```

Or apply individual patches:

```bash
cd helio
git apply ../patches/helio-base-cmake.patch
git apply ../patches/helio-init.patch
git apply ../patches/helio-fibers-cmake.patch
git apply ../patches/helio-stacktrace.patch
```

## Patch Descriptions

- **helio-backward-cpp-all.patch**: Combined patch containing all changes
- **helio-base-cmake.patch**: Updates CMakeLists.txt to find and link libdw/libunwind
- **helio-init.patch**: Replaces Abseil failure signal handler with backward-cpp
- **helio-fibers-cmake.patch**: Adds include path for backward-cpp
- **helio-stacktrace.patch**: Replaces GetStacktrace() implementation with backward-cpp

## Requirements

Ensure these libraries are installed:

```bash
# Ubuntu/Debian
sudo apt-get install libdw-dev libunwind-dev

# Fedora/RHEL
sudo dnf install elfutils-devel libunwind-devel
```
