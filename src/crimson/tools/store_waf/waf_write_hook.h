// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <iostream>

namespace crimson::tools::waf {

// TODO: multi devices support
seastar::future<> register_device(uint64_t total_bytes) __attribute__((weak));

seastar::future<> open_ruh(uint16_t handle, bool initially_isolated) __attribute__((weak));

seastar::future<> record_write(uint64_t offset,
                  uint64_t bytes,
                  uint16_t stream) __attribute__((weak));

seastar::future<> record_discard(uint64_t offset,
                   uint64_t bytes) __attribute__((weak));

}
