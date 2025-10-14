// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"

namespace crimson::os::seastore {

void Transaction::push_laddr_internal_list(CachedExtentRef ref)
{
  if (ref->template cast<lba::LBAInternalNode>()->get_meta().depth != 2) {
    return;
  }
  if (!laddr_internal_list.empty() && laddr_internal_list.back() == ref) {
    return;
  }
  laddr_internal_list.push_back(ref);
}

bool Transaction::is_hot(laddr_t laddr)
{
  CachedExtentRef ex = nullptr;

  for (auto& e : laddr_internal_list) {
    if (e->template cast<lba::LBAInternalNode>()->is_in_range(laddr)) {
      ex = e;
      break;
    }
  }

  if (ex == nullptr) {
    return false;
  }

  if (!ex->get_cms()) {
    ex->make_cms();
  }
  auto cms = ex->get_cms();

  // 16777216: default logical address space reservation for seastore objects' data
  constexpr auto tt = 16777216;
  laddr_offset_t cur{laddr, 0};
  uint64_t aligned_laddr = std::hash<laddr_t>()(cur.get_aligned_laddr(tt));

  static thread_local uint64_t hit = 0;

  bool hot = (cms->query_and_add(aligned_laddr) > 0);
  if (hot) {
    hit++;
  }

  if (ex->get_last_transaction_id() == TRANS_ID_NULL) {
    ex->set_last_transaction_id(get_trans_id());
    return false;
  }

  auto distance = get_trans_id() - ex->get_last_transaction_id();
  if (distance >= threshold) {
    if (seastar::this_shard_id() == 0) {
      std::cout << "[halving] decay distance: " << distance << std::endl;
    }
    cms->halve();
    ex->set_last_transaction_id(get_trans_id());
  }

  static thread_local uint64_t n = 0;
  static uint64_t min_threshold = 1000;
  uint64_t max_threshold = 1000000000;
  uint64_t update_threshold = 10000;
  uint64_t target_hit = 7900;
  double step = 0.02;

  if (++n == update_threshold) {
    if (hit > target_hit) {
      threshold *= (1 - step);
      if (threshold < min_threshold) {
        threshold = min_threshold;
      }
    } else {
      threshold *= (1 + step);
      if (threshold > max_threshold) {
        threshold = max_threshold;
      }
    }
    if (seastar::this_shard_id() == 0) {
      std::cout << "[hit ratio: " << hit / 100 << " %] decay distance: " << threshold << std::endl;
    }
    n = hit = 0;
  }
  return hot;
}

}
