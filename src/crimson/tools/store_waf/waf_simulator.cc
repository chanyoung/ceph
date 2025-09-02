// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/tools/store_waf/waf_write_hook.h"
#include "include/ceph_assert.h"

#include <cstring>
#include <unordered_map>
#include <vector>

namespace {

// Based on Open-Channel SSD (OCSSD) 1.2 geometry.
// RAW = 64 GiB, USER = 59.52 GB, OP = 7 %.
constexpr uint32_t nchannels        = 2;
constexpr uint32_t luns_per_channel = 2;
constexpr uint32_t planes_per_lun   = 4;
constexpr uint32_t blocks_per_plane = 256;
constexpr uint32_t pages_per_block  = 4096;
// On modern SSDs, page sizes of 16KB or 32KB are common. However,
// using such sizes would complicate WAF simulation by requiring
// additional concepts, since the page size would no longer align with
// the mapping table. As this tool’s sole purpose is to derive WAF
// values, it adopts a 4KB page size which is directly matching the
// mapping table, to keep the implementation as simple as possible.
constexpr uint32_t page_nbytes      = 4ULL * 1024;
constexpr uint64_t user_capacity    = 59.52 * 1000 * 1000 * 1000;

// Summary:
//
// WAF simulator supporting FDP on/off modes.
//
// (1) FDP disabled (full-stripe):
//   - Each line stripes across all Parallel Units (PUs = LUNs).
//   - Operates with a single Reclaim Unit Handle (RUH) and a single Write Pointer (WP).
//
// (2) FDP enabled:
//   - Each line stripes across all Parallel Units (PUs = LUNs).
//   - Operates with multiple Reclaim Unit Handles (RUHs) and multiple Write Pointers (WPs).
//   - Each RUH opens its own line.
//
// Detailed description:
//
// (1) FDP disabled (full-stripe):
//
// RUHs: count = 1, WPs: count = 1.
// pages_per_line = nchannels * luns_per_channel * planes_per_lun * pages_per_block
//
//  +----------+
//  |  RUH[0]  |
//  +-----+----+
//        |
//        v
//  +----------+
//  |  WP[0]   |
//  +-----+----+
//        |
//        v
//  +---------------------------------------+
//  | Lines (nlines = blocks_per_plane)     |
//  |                                       |
//  |  Line[0]: [ page × pages_per_line ]   |
//  |  Line[1]: [ page × pages_per_line ]   |
//  |   ...                                 |
//  |  Line[N-1]: [ page × pages_per_line ] |
//  +---------------------------------------+
//
// (2) FDP enabled:
//
// (Max supported) RUHs: count = 8, WPs: count = 8.
// pages_per_line = nchannels * luns_per_channel * planes_per_lun * pages_per_block
//
//  +----------+ +----------+ +----------+
//  |  RUH[0]  | |  RUH[1]  | |  RUH[2]  | ...
//  +-------+--+ +-------+--+ +-------+--+
//          |            |            |
//          v            v            v
//     +-------+    +-------+    +-------+
//     | WP[0] |    | WP[1] |    | WP[2] | ...
//     +----+--+    +----+--+    +----+--+
//          |            |            |
//          v            v            v
//  +---------------------------------------+
//  | Lines (nlines = blocks_per_plane)     |
//  |                                       |
//  |  Line[0]: [ page × pages_per_line ]   |
//  |  Line[1]: [ page × pages_per_line ]   |
//  |   ...                                 |
//  |  Line[N-1]: [ page × pages_per_line ] |
//  +---------------------------------------+

class waf_simulator {
  // INVALID collectively refers to INVALID_LPN/PPA/INDEX.
  static constexpr uint32_t INVALID = UINT32_MAX;

public:
  explicit waf_simulator(bool fdp_enable)
    : fdp_enabled(fdp_enable),
      full_stripe(!fdp_enabled),
      nlines(blocks_per_plane),
      pages_per_line(nchannels * luns_per_channel * planes_per_lun * pages_per_block),
      ruh_count(full_stripe ? 1 : 8),
      lpn_count(user_capacity / page_nbytes + 1),
      ruhs(ruh_count),
      lines(nlines),
      l2p(lpn_count),
      host_wps(ruh_count),
      gc_wps(ruh_count)
  {
    for (auto &l : lines) {
      l.used = l.writing = false;
      l.handle = l.ipc = l.vpc = 0;
      l.pages.resize(pages_per_line);
      for (auto &pg : l.pages) {
	pg.lpn = INVALID;
	pg.valid = false;
      }
    }
    for (auto &r : ruhs) {
      r.opened = r.initially_isolated = false;
      r.used_bytes = r.total_writes = page_nbytes; // prevent divide by zero.
    }
    for (auto &e : l2p) {
      e.addr = INVALID;
    }
    for (auto &w : host_wps) {
      w.line = w.page = INVALID;
    }
    for (auto &w : gc_wps) {
      w.line = w.page = INVALID;
    }
    free_line_count = nlines;
    used_bytes = nand_writes = host_writes = 0;
  }

  void calc_waf() {
    std::cout << "[WAF - fdp " << fdp_enabled << "] " <<
      nand_writes * 100 / host_writes << " % (disk usage:" <<
      used_bytes * 100ULL / user_capacity << " %)" << std::endl;
    nand_writes = host_writes = 0;
    for (int i = 0; i < ruh_count; i++) {
      if (!ruhs[i].opened) {
	continue;
      }
      std::cout << "[RU" << i <<
	" - " << ruhs[i].used_bytes <<
	" - " << ruhs[i].total_writes <<
	" - " << ruhs[i].used_bytes * 100ULL / user_capacity << " %" <<
	" - " << ruhs[i].total_writes * 100ULL / ruhs[i].used_bytes << " %" << std::endl;
    }
  }

  void register_device(uint64_t total_bytes) {
    ceph_assert(total_bytes == user_capacity);
    open_ruh(0, true);
  }

  void open_ruh(uint16_t handle, bool initially_isolated) {
    ceph_assert(handle < ruh_count);
    if (!ruhs[handle].opened) {
      ruhs[handle].opened = true;
      host_wps[handle].line = get_next_free_line();
      host_wps[handle].page = 0;
      if (handle == 0 || !initially_isolated) {
	gc_wps[handle].line = get_next_free_line();
	gc_wps[handle].page = 0;
      }
    }
    ruhs[handle].initially_isolated = initially_isolated;
  }

  void record_write(uint64_t offset, uint64_t bytes, uint16_t handle) {
    const uint64_t start_lpn = offset / page_nbytes;
    const uint64_t last_lpn  = (offset + bytes) / page_nbytes;

    for (uint64_t lpn = start_lpn; lpn < last_lpn; ++lpn) {
      invalidate_lpn(lpn);
      used_bytes += page_nbytes;
      ruhs[handle].used_bytes += page_nbytes;
      ruhs[handle].total_writes += page_nbytes;

      lines[host_wps[handle].line].pages[host_wps[handle].page].lpn   = lpn;
      lines[host_wps[handle].line].pages[host_wps[handle].page].valid = true;
      ++lines[host_wps[handle].line].vpc;
      ceph_assert(lines[host_wps[handle].line].vpc <= pages_per_line);

      l2p[lpn].line = host_wps[handle].line;
      l2p[lpn].page = host_wps[handle].page;
      advance_write_pointer(handle, true);

      if (++host_writes % 100000 == 0) {
	calc_waf();
      }
    }

    if (free_line_count == 1) {
      do_gc();
    }
  }

  void record_discard(uint64_t offset, uint64_t bytes) {
    const uint64_t start_lpn = offset / page_nbytes;
    const uint64_t last_lpn  = (offset + bytes) / page_nbytes;
    for (uint64_t lpn = start_lpn; lpn < last_lpn; ++lpn) {
      invalidate_lpn(lpn);
    }
  }

private:
  typedef struct physical_page_address {
    union {
      struct {
	uint32_t line : 12;
	uint32_t page : 20;
      };
      uint32_t addr;
    };
  } ppa;

  typedef struct reclaim_unit_handle {
    bool opened;
    bool initially_isolated;
    uint64_t used_bytes;
    uint64_t total_writes;
  } ruh;

  typedef struct nand_page {
    uint32_t lpn;   // logical page number
    bool     valid;
  } page;

  typedef struct parallel_unit_stripe {
    std::vector<page> pages;
    uint32_t          ipc;     // invalid page count
    uint32_t          vpc;     // valid page count
    int               handle;
    bool              writing;
    bool              used;
  } line;

  typedef struct write_pointer {
    uint32_t line;
    uint32_t page;
  } wp;

  const bool      fdp_enabled;
  const bool      full_stripe;
  const uint32_t  nlines;
  const uint32_t  pages_per_line;
  const uint16_t  ruh_count;
  const uint64_t  lpn_count;

  std::vector<ruh>  ruhs;
  std::vector<line> lines;
  std::vector<ppa>  l2p;
  std::vector<wp>   host_wps;
  std::vector<wp>   gc_wps;
  uint32_t          free_line_count;

  uint64_t          nand_writes;
  uint64_t          host_writes;
  uint64_t          used_bytes;

  void invalidate_lpn(uint64_t lpn) {
    ceph_assert(lpn < lpn_count);
    ppa mapped = l2p[lpn];
    if (mapped.addr != INVALID) {
      ceph_assert(mapped.page < pages_per_line);
      ceph_assert(mapped.line < nlines);
      ceph_assert(lines[mapped.line].pages[mapped.page].lpn == lpn);

      lines[mapped.line].pages[mapped.page].valid = false;
      ++lines[mapped.line].ipc;
      ceph_assert(lines[mapped.line].vpc > 0);
      --lines[mapped.line].vpc;
      ceph_assert(lines[mapped.line].vpc + lines[mapped.line].ipc
                  <= pages_per_line);

      l2p[lpn].addr = INVALID;
      used_bytes -= page_nbytes;
      ruhs[lines[mapped.line].handle].used_bytes -= page_nbytes;
    }
  }

  void advance_write_pointer(int handle, bool host) {
    auto &wp = host ? host_wps[handle] : gc_wps[handle];
    ++wp.page;
    if (wp.page == pages_per_line) {
      wp.page = 0;
      lines[wp.line].writing = false;
      ceph_assert(lines[wp.line].vpc +
                  lines[wp.line].ipc == pages_per_line);
      wp.line = get_next_free_line();
      lines[wp.line].handle = handle;
    }
    ++nand_writes;
  }

  uint32_t get_next_free_line() {
    ceph_assert(free_line_count > 0);
    for (uint32_t i = 0; i < nlines; ++i) {
      if (!lines[i].used) {
	ceph_assert(lines[i].ipc == 0 && lines[i].vpc == 0);
	lines[i].used = true;
	lines[i].writing = true;
	--free_line_count;
	return i;
      }
    }
    ceph_abort();
  }

  void do_gc() {
    uint32_t victim_line_n = INVALID;
    uint32_t victim_ipc  = 0;

    for (uint32_t i = 0; i < nlines; ++i) {
      if (lines[i].writing) continue;
      if (lines[i].ipc > victim_ipc) {
	victim_line_n = i;
	victim_ipc = lines[i].ipc;
      }
    }
    ceph_assert(victim_line_n != INVALID);
    ceph_assert(lines[victim_line_n].ipc +
                lines[victim_line_n].vpc == pages_per_line);

    auto &victim_line = lines[victim_line_n];
    std::cout << "[GC - fdp " << fdp_enabled << " - "
      << victim_line.handle
      << "] invalid ratio: "
      << victim_line.ipc * 100 / pages_per_line
      << " %" << std::endl;

    int handle = victim_line.handle;
    bool initially_isolated = ruhs[handle].initially_isolated;
    ceph_assert(ruhs[handle].opened);

    for (uint32_t i = 0; i < pages_per_line; ++i) {
      auto &pg = victim_line.pages[i];
      if (pg.valid) {
	const int dst_handle = initially_isolated ? 0 : handle;
	auto &wp = gc_wps[dst_handle];
	auto &ln = lines[wp.line];

        ln.pages[wp.page].lpn   = pg.lpn;
        ln.pages[wp.page].valid = true;
	ln.vpc++;

	l2p[pg.lpn].line = wp.line;
	l2p[pg.lpn].page = wp.page;

	advance_write_pointer(dst_handle, false);
	pg.valid = false;
	pg.lpn = INVALID;
      }
    }

    ceph_assert(!victim_line.writing);
    victim_line.used   = false;
    victim_line.ipc    = 0;
    victim_line.vpc    = 0;
    victim_line.handle = 0;
    ++free_line_count;
  }
};

struct multiplexer {
  waf_simulator base{false /* FDP disable */ };
  waf_simulator fdp{true /* FDP enable */ };

  void register_device(uint64_t total_bytes) {
    base.register_device(total_bytes);
    fdp.register_device(total_bytes);
  }

  void open_ruh(uint16_t handle, bool initially_isolated) {
    return fdp.open_ruh(handle, initially_isolated);
  }

  void record_write(uint64_t off, uint64_t bytes, uint16_t handle) {
    base.record_write(off, bytes, 0);
    fdp.record_write(off, bytes, handle);
  }

  void record_discard(uint64_t off, uint64_t bytes) {
    base.record_discard(off, bytes);
    fdp.record_discard(off, bytes);
  }
};

static multiplexer mux;
}

namespace crimson::tools::waf {
  void register_device(uint64_t total_bytes) {
    mux.register_device(total_bytes);
  }

  void open_ruh(uint16_t handle, bool initially_isolated) {
    mux.open_ruh(handle, initially_isolated);
  }

  void record_write(uint64_t offset, uint64_t bytes, uint16_t handle) {
    mux.record_write(offset, bytes, handle);
  }

  void record_discard(uint64_t offset, uint64_t bytes) {
    mux.record_discard(offset, bytes);
  }
}
