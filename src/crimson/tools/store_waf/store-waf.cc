// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

/**
 * crimson-store-waf
 *
 * A simulation tool built on crimson’s store with waf_write_hook,
 * designed to observe write patterns and measure Write Amplification
 * Factor (WAF). It enables small-scale comparison of placement policies
 * for random block SSDs such as FDP.
 *
 * Example usage:
 *  $ ./bin/crimson-store-waf --store-path <path>
 */
#include <boost/program_options/variables_map.hpp>
#include <experimental/random>

#include <seastar/core/app-template.hh>

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

// From tools/store_bench/store-bench.cc
ghobject_t create_hobj(unsigned id, bool rbd) {
  if (rbd) {
    return ghobject_t(shard_id_t::NO_SHARD, seastar::this_shard_id(),
      id, "rbd", "", 0, ghobject_t::NO_GEN);
  } else {
    return ghobject_t(shard_id_t::NO_SHARD, seastar::this_shard_id(),
      id, "rgw", "", 0, ghobject_t::NO_GEN);
  }
};

// From tools/store_bench/store-bench.cc
coll_t make_cid(int obj_id, int num_objects_per_collection, bool rbd) {
  int pg_id = obj_id / num_objects_per_collection;
  if (rbd) {
    return coll_t(spg_t(pg_t(pg_id, 0)));
  } else {
    return coll_t(spg_t(pg_t(pg_id, 1)));
  }
}

seastar::future<bufferptr> generate_random_bp(uint64_t size)
{
  bufferptr bp(ceph::buffer::create_page_aligned(size));
  auto f = co_await seastar::open_file_dma(
    "/dev/urandom", seastar::open_flags::ro);
  static constexpr uint64_t STRIDE = 256<<10;
  for (uint64_t off = 0; off < size; off += STRIDE) {
    co_await f.dma_read(off, bp.c_str() + off, STRIDE);
  }
  co_return bp;
}

std::string generate_random_string(int key_size) {
  std::string res = "";
  for (int i = 0; i < key_size; ++i) {
    char letter = char(std::rand() % 26 + 97);
    res += letter;
  }
  return res;
}

// From tools/store_bench/store-bench.cc
seastar::future<> cbw_workload(crimson::os::FuturizedStore &global_store, std::string workload) {
  uint64_t prefill_size = 128<<10;
  uint64_t rbd_size_per_shard;
  uint64_t rgw_size_per_shard;
  uint64_t rbd_size_per_obj = 4<<20;
  uint64_t rgw_size_per_obj = 4<<19;  // 2MB. Actual distribution is 1.9MB.
  uint64_t colls_per_shard = 16;
  uint64_t io_concurrency_per_shard = 16;

  if (workload == "rbd") {
    // 100%
    // rbd_size_per_shard = 5500ULL<<20;
    // rgw_size_per_shard = 0;

    // 80%
    rbd_size_per_shard = 3660ULL<<20;
    rgw_size_per_shard = 0;
  } else if (workload == "rgw") {
    // 100%
    // rbd_size_per_shard = 0;
    // rgw_size_per_shard = 5200ULL<<20;

    // 80%
    rbd_size_per_shard = 0;
    rgw_size_per_shard = 3460ULL<<20;
  } else if (workload == "mix") {
    // 100%
    // rbd_size_per_shard = 2750ULL<<20;
    // rgw_size_per_shard = 2600ULL<<20;

    // 80%
    rbd_size_per_shard = 1830ULL<<20;
    rgw_size_per_shard = 1730ULL<<20;
  } else {
    ceph_abort();
  }

  auto get_obj_per_shard = [&](bool rbd) {
    if (rbd) {
      return (rbd_size_per_shard + rbd_size_per_obj - 1) / rbd_size_per_obj;
    } else {
      return (rgw_size_per_shard + rgw_size_per_obj - 1) / rgw_size_per_obj;
    }
  };
  auto get_obj_per_coll = [&](bool rbd) {
    if (rbd) {
      return (get_obj_per_shard(rbd) + colls_per_shard - 1) / colls_per_shard;
    } else {
      return (get_obj_per_shard(rbd) + colls_per_shard - 1) / colls_per_shard;
    }
  };
  auto random_buffer = co_await generate_random_bp(16<<20);
  auto get_random_buffer = [&random_buffer](uint64_t size) {
    assert((size % CEPH_PAGE_SIZE) == 0);
    bufferptr bp(
      random_buffer,
      std::experimental::randint<uint64_t>(
	0,
	(random_buffer.length() - size) / CEPH_PAGE_SIZE) *
        CEPH_PAGE_SIZE,
	size);
    assert(bp.is_page_aligned());
    bufferlist bl;
    bl.append(bp);
    return bl;
  };
  // Any size smaller than CEPH_PAGE_SIZE (4KB) is rounded up to 4KB.
  static const int rbd_dist_table[100] = {
    //  0.5B  ( 4%)
    // 0,0,0,0,
    7,7,7,7,
    //  1.0KB ( 1%)
    // 1,
    7,
    //  1.5KB ( 1%)
    // 2,
    7,
    //  2.0KB ( 1%)
    // 3,
    7,
    //  2.5KB ( 1%)
    // 4,
    7,
    //  3.0KB ( 1%)
    // 5,
    7,
    //  3.5KB ( 1%)
    // 6,
    7,
    //  4.0KB (67%)
    7,7,7,7,7,7,7,7,7,7,    7,7,7,7,7,7,7,7,7,7,
    7,7,7,7,7,7,7,7,7,7,    7,7,7,7,7,7,7,7,7,7,
    7,7,7,7,7,7,7,7,7,7,    7,7,7,7,7,7,7,7,7,7,
    7,7,7,7,7,7,7,
    //  8.0KB (10%)
    8,8,8,8,8,8,8,8,8,8,
    // 16.0KB ( 7%)
    9,9,9,9,9,9,9,
    // 32.0KB ( 3%)
    10,10,10,
    // 64.0KB ( 3%)
    11,11,11,
  };
  static const size_t rbd_sizes[] = {
    512, 1024, 1536, 2048, 2560,
    3072, 3584, 4096, 8192,
    16384, 32768, 65536
  };
  static const int rgw_dist_table[100] = {
    //  4.0KB (30%)
    0,0,0,0,0,0,0,0,0,0,    0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,
    // 64.0KB (20%)
    1,1,1,1,1,1,1,1,1,1,    1,1,1,1,1,1,1,1,1,1,
    //  2.0MB (35%)
    2,2,2,2,2,2,2,2,2,2,    2,2,2,2,2,2,2,2,2,2,
    2,2,2,2,2,2,2,2,2,2,    2,2,2,2,2,
    //  8.0MB (15%)
    3,3,3,3,3,3,3,3,3,3,    3,3,3,3,3,
  };
  static const size_t rgw_sizes[] = {
    4096, 65536, 2097152, 8388608
  };

  auto &local_store = global_store.get_sharded_store();

  std::vector<std::pair<coll_t, crimson::os::CollectionRef>> rbd_coll_refs;
  for (uint64_t collidx = 0; collidx < colls_per_shard; ++collidx) {
    coll_t cid(
      spg_t(pg_t(0, (seastar::this_shard_id() * colls_per_shard) + collidx))
    );
    auto ref = co_await local_store.create_new_collection(cid);
    rbd_coll_refs.emplace_back(std::make_pair(cid, std::move(ref)));
  }
  std::vector<std::pair<coll_t, crimson::os::CollectionRef>> rgw_coll_refs;
  for (uint64_t collidx = 0; collidx < colls_per_shard; ++collidx) {
    coll_t cid(
      spg_t(pg_t(0, (seastar::this_shard_id() * colls_per_shard) + collidx))
    );
    auto ref = co_await local_store.create_new_collection(cid);
    rgw_coll_refs.emplace_back(std::make_pair(cid, std::move(ref)));
  }

  auto get_coll_id = [&](uint64_t obj_id, bool rbd) {
    if (rbd) {
      return rbd_coll_refs[obj_id / get_obj_per_coll(rbd)].first;
    } else {
      return rgw_coll_refs[obj_id / get_obj_per_coll(rbd)].first;
    }
  };
  auto get_coll_ref = [&](uint64_t obj_id, bool rbd) {
    if (rbd) {
      return rbd_coll_refs[obj_id / get_obj_per_coll(rbd)].second;
    } else {
      return rgw_coll_refs[obj_id / get_obj_per_coll(rbd)].second;
    }
  };

  unsigned running = 0;
  std::optional<seastar::promise<>> complete;

  seastar::semaphore sem{io_concurrency_per_shard};
  auto submit_transaction = [&](
    crimson::os::CollectionRef &col_ref,
    ceph::os::Transaction &&t) -> seastar::future<> {
    ++running;
    co_await sem.wait(1);
    std::ignore = local_store.do_transaction(
      col_ref,
      std::move(t)
    ).finally([&, start = ceph::mono_clock::now()] {
      --running;
      if (running == 0 && complete) {
        complete->set_value();
      }
      sem.signal(1);
    });
  };

  auto omap_object_per_rgw_objects = 50;
  auto target_keys_per_bucket = 60000;
  auto key_size = 50;
  auto value_size = 50;
  std::vector<std::set<std::string>> keys_per_bucket(get_obj_per_shard(false) / omap_object_per_rgw_objects + 1);

  for (uint64_t obj_id = 0; obj_id < get_obj_per_shard(true); ++obj_id) {
    auto hobj = create_hobj(obj_id, true);
    auto coll_id = get_coll_id(obj_id, true);
    auto coll_ref = get_coll_ref(obj_id, true);

    {
      ceph::os::Transaction t;
      t.create(coll_id, hobj);
      co_await submit_transaction(coll_ref, std::move(t));
    }
    for (uint64_t off = 0; off < rbd_size_per_obj; off += prefill_size) {
      ceph::os::Transaction t;
      t.write(coll_id, hobj, off, prefill_size, get_random_buffer(prefill_size));
      co_await submit_transaction(coll_ref, std::move(t));
    }

    std::cout << "[" << seastar::this_shard_id() << "] wrote rbd obj " << obj_id << " of " << get_obj_per_shard(true) << std::endl;
  }

  for (uint64_t obj_id = 0; obj_id < get_obj_per_shard(false); ++obj_id) {
    auto hobj = create_hobj(obj_id, false);
    auto coll_id = get_coll_id(obj_id, false);
    auto coll_ref = get_coll_ref(obj_id, false);

    {
      ceph::os::Transaction t;
      t.create(coll_id, hobj);
      co_await submit_transaction(coll_ref, std::move(t));
    }
    {
      uint64_t io_size = rgw_sizes[rgw_dist_table[std::experimental::randint<int>(0, 99)]];
      ceph::os::Transaction t;
      t.write(coll_id, hobj, 0, io_size, get_random_buffer(io_size));
      co_await submit_transaction(coll_ref, std::move(t));
    }

    if (keys_per_bucket[obj_id/omap_object_per_rgw_objects].size() == 0) {
      auto omap_obj_id = obj_id / omap_object_per_rgw_objects;

      hobj = create_hobj(omap_obj_id, false);
      coll_id = get_coll_id(omap_obj_id, false);
      coll_ref = get_coll_ref(omap_obj_id, false);

      std::map<std::string, bufferlist> omap_for_this_bucket;
      std::set<std::string> keys_in_this_bucket;
      for (int j = 0; j < target_keys_per_bucket; ++j) {
        std::string possible_key = generate_random_string(key_size);
        while (keys_in_this_bucket.count(possible_key) > 0) {
          possible_key = generate_random_string(key_size);
        }
        keys_in_this_bucket.insert(possible_key);
        bufferlist val_for_poss_key;
        val_for_poss_key.append_zero(value_size);
        omap_for_this_bucket[possible_key] = val_for_poss_key;
      }
      keys_per_bucket[omap_obj_id] = keys_in_this_bucket;
      ceph::os::Transaction txn_write_omap_for_bucket;
      txn_write_omap_for_bucket.omap_setkeys(coll_id, hobj,
                                             omap_for_this_bucket);
      co_await submit_transaction(coll_ref, std::move(txn_write_omap_for_bucket));
    }

    std::cout << "[" << seastar::this_shard_id() << "] wrote rgw obj " << obj_id << " of " << get_obj_per_shard(false) << std::endl;
  }

  std::cout << "[" << seastar::this_shard_id() << "] finished populating" << std::endl;

  std::vector<int> size_per_bucket(get_obj_per_shard(false) / omap_object_per_rgw_objects + 1, target_keys_per_bucket);
  // min and max size is the range of allowable bucket size
  int min_size =
      std::floor(target_keys_per_bucket * (0.5));
  int max_size =
      std::ceil(target_keys_per_bucket * (1.5));

  static const int rbd_p1 = get_obj_per_shard(true) * 0.05;
  static const int rbd_p2 = get_obj_per_shard(true) * 0.20;
  static const int rgw_p1 = get_obj_per_shard(false) * 0.05;
  static const int rgw_p2 = get_obj_per_shard(false) * 0.20;

  while (true) {
    int p = std::experimental::randint<int>(0, 99);
    if (rbd_p1 > 0) {
      int rbd_obj_id;
      if (p < 50) {
        rbd_obj_id = std::experimental::randint<int>(0, rbd_p1-1);
      } else if (p < 80) {
        rbd_obj_id = std::experimental::randint<int>(rbd_p1, rbd_p2-1);
      } else {
        rbd_obj_id = std::experimental::randint<int>(rbd_p2, get_obj_per_shard(true)-1);
      }

      auto hobj = create_hobj(rbd_obj_id, true);
      auto coll_id = get_coll_id(rbd_obj_id, true);
      auto coll_ref = get_coll_ref(rbd_obj_id, true);

      uint64_t io_size = rbd_sizes[rbd_dist_table[std::experimental::randint<int>(0, 99)]];
      auto offset = std::experimental::randint<uint64_t>(
	0,
	(rbd_size_per_obj / io_size) - 1) * io_size;

      ceph::os::Transaction t;
      t.write(
	coll_id,
	hobj,
	offset,
	io_size,
	get_random_buffer(io_size));
      co_await submit_transaction(coll_ref, std::move(t));
    }

    if (rgw_p1 > 0) {
      int rgw_obj_id;
retry:
      if (p < 50) {
        rgw_obj_id = std::experimental::randint<int>(0, rgw_p1-1);
      } else if (p < 80) {
        rgw_obj_id = std::experimental::randint<int>(rgw_p1, rgw_p2-1);
      } else {
        rgw_obj_id = std::experimental::randint<int>(rgw_p2, get_obj_per_shard(false)-1);
      }

      auto omap_obj_id = rgw_obj_id / omap_object_per_rgw_objects;
      if (omap_obj_id == rgw_obj_id) {
	goto retry;
      }

      {
        auto hobj = create_hobj(rgw_obj_id, false);
        auto coll_id = get_coll_id(rgw_obj_id, false);
        auto coll_ref = get_coll_ref(rgw_obj_id, false);
	{
	  ceph::os::Transaction t;
	  t.remove(coll_id, hobj);
	  co_await submit_transaction(coll_ref, std::move(t));
	}
	{
	  ceph::os::Transaction t;
	  t.create(coll_id, hobj);
	  co_await submit_transaction(coll_ref, std::move(t));
	}
	{
          uint64_t io_size = rgw_sizes[rgw_dist_table[std::experimental::randint<int>(0, 99)]];
	  ceph::os::Transaction t;
	  t.write(coll_id, hobj, 0, io_size, get_random_buffer(io_size));
	  co_await submit_transaction(coll_ref, std::move(t));
	}
      }

      // 1 delete and 1 create object.
      for (int i = 0; i < 2; ++i) {
	auto hobj = create_hobj(omap_obj_id, false);
	auto coll_id = get_coll_id(omap_obj_id, false);
	auto coll_ref = get_coll_ref(omap_obj_id, false);

        int size_bucket_we_choose = size_per_bucket[omap_obj_id];
	auto &keys_in_that_bucket = keys_per_bucket[omap_obj_id];

	// this case happens when the size of the bucket is min size and we choose
	// to delete
	if (size_bucket_we_choose <= min_size) {
write_one_key:
	  std::string new_key = generate_random_string(key_size);
	  while (keys_in_that_bucket.count(new_key) > 0) {
	    new_key = generate_random_string(key_size);
	  }
	  keys_in_that_bucket.insert(new_key);

	  bufferlist value;
	  value.append_zero(value_size);

	  std::map<std::string, bufferlist> data_entry;
	  data_entry[new_key] = value;

	  ceph::os::Transaction one_write;
	  one_write.omap_setkeys(coll_id, hobj, data_entry);
	  co_await submit_transaction(coll_ref, std::move(one_write));

	  size_per_bucket[omap_obj_id] += 1;
	} else if (size_bucket_we_choose >= max_size) {
delete_one_key:
	  int index = std::rand() % keys_in_that_bucket.size();
	  auto it = keys_in_that_bucket.begin();
	  std::advance(it, index);
	  std::string key_to_delete = *it;
	  keys_in_that_bucket.erase(it);

	  ceph::os::Transaction one_delete;
	  one_delete.omap_rmkey(coll_id, hobj, key_to_delete);
	  co_await submit_transaction(coll_ref, std::move(one_delete));

	  size_per_bucket[omap_obj_id] -= 1;
	} else {
	  int choice = std::rand() % 2;
	  // choice 0 is write, choice 1 is delete
	  if (choice == 0) {
	    goto write_one_key;
	  } else {
	    goto delete_one_key;
	  }
	};
      }
    }
  }

  co_return;
}

int main(int argc, char **argv) {
  namespace po = boost::program_options;
  po::options_description desc{"Allowed options"};
  std::string workload;
  bool segmented;

  desc.add_options()
    ("help,h", "show help message")
    ("workload", po::value<std::string>(&workload)->default_value("rbd"),
      "workload type: rbd or rgw or mix")
    ("segmented-ssd", po::value<bool>(&segmented)->default_value(false),
     "use seastore random block ssd device type");
   po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }
    po::notify(vm);
  } catch (const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  seastar::app_template::seastar_options app_cfg;
  app_cfg.name = "crimson-store-waf";
  app_cfg.auto_handle_sigint_sigterm = false;
  app_cfg.reactor_opts.blocked_reactor_notify_ms.set_default_value(200);
  seastar::app_template app(std::move(app_cfg));

  std::vector<std::string> seastar_args;
  seastar_args.emplace_back("--smp");
  seastar_args.emplace_back("12");

  std::vector<char*> seastar_argv;
  seastar_argv.push_back(const_cast<char*>(argv[0]));
  for (auto& arg : seastar_args) {
    seastar_argv.push_back(const_cast<char*>(arg.c_str()));
  }

  std::error_code ec;
  std::filesystem::remove_all("store_waf_dir", ec);
  ::mkdir("store_waf_dir", 0755);
  int fd = ::open("store_waf_dir/block", O_CREAT|O_RDWR|O_TRUNC, 0644);
  ceph_assert(fd >= 0);
  ::ftruncate(fd, 59.52 * 1000 * 1000 * 1000);
  ::close(fd);

  return app.run(seastar_argv.size(), seastar_argv.data(),
    seastar::coroutine::lambda([&]() -> seastar::future<int> {
      co_await crimson::common::sharded_conf().start(
        EntityName{}, std::string_view{"ceph"});

      co_await crimson::common::local_conf().start();
      if (segmented) {
        // The default Seastore segment size (64M) exceeds the device line size (32M),
	// resulting in a device-level WAF of 0.
        // co_await crimson::common::local_conf().set_val("seastore_segment_size", "1_M");
      } else {
        co_await crimson::common::local_conf().set_val("seastore_main_device_type", "RANDOM_BLOCK_SSD");
        co_await crimson::common::local_conf().set_val("seastore_cbjournal_size", "25165824" /* 24MB */);
      }

      auto store = crimson::os::FuturizedStore::create(
        "seastore",
        "store_waf_dir",
        crimson::common::local_conf().get_config_values());

      co_await store->start();

      uuid_d uuid;
      uuid.generate_random();
      co_await store->mkfs(uuid).handle_error(
        crimson::stateful_ec::assert_failure("mkfs error"));
      co_await store->stop();

      co_await store->start();
      co_await store->mount().handle_error(
        crimson::stateful_ec::assert_failure("mount error"));

      std::vector<seastar::future<>> per_shard_futures;
      auto named_lambda = [&, &store_ref = *store]()
        -> seastar::future<> {
          co_return co_await cbw_workload(store_ref, workload);
      };
      for (unsigned i = 0; i < seastar::smp::count; ++i) {
	per_shard_futures.push_back(
	    seastar::smp::submit_to(i, std::move(named_lambda)));
      }

      for (unsigned i = 0; i < per_shard_futures.size(); ++i) {
	co_await std::move(per_shard_futures[i]);
      }

      co_await store->umount();
      co_await store->stop();

      co_await crimson::common::sharded_conf().stop();

      co_return 0;
    })
  );

  std::filesystem::remove_all("store_waf_dir", ec);
}
