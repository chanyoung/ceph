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

#include <seastar/core/app-template.hh>

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

// From tools/store_bench/store-bench.cc
ghobject_t create_hobj(unsigned id) {
  return ghobject_t(shard_id_t::NO_SHARD, seastar::this_shard_id(),
    id, "", "", 0, ghobject_t::NO_GEN);
};

// From tools/store_bench/store-bench.cc
coll_t make_cid(int obj_id, int num_objects_per_collection) {
  int pg_id = obj_id / num_objects_per_collection;
  return coll_t(spg_t(pg_t(pg_id, 0)));
}

// From tools/store_bench/store-bench.cc
seastar::future<> pg_log_workload(crimson::os::FuturizedStore &global_store) {
  auto &local_store = global_store.get_sharded_store();

  std::map<int, coll_t> collection_id;
  std::map<int, crimson::os::CollectionRef> coll_ref_map;

  const int fill_size = 50000;

  auto pre_fill_logs = [&]() -> seastar::future<> {
    for (int i = 0; i < fill_size; ++i) {
      auto obj_i = create_hobj(i);
      auto coll_id = make_cid(i, 1);
      collection_id[i] = coll_id;
      auto coll_ref = co_await local_store.create_new_collection(coll_id);
      coll_ref_map[i] = coll_ref;
      std::map<std::string, bufferlist> data;
      for (int j = 0; j < 4; ++j) {
        std::string key = std::to_string(j);
        bufferlist bl_value;
        bl_value.append_zero(1000);
        data[key] = bl_value;
      }
      ceph::os::Transaction txn;
      txn.create(coll_id, obj_i);
      txn.omap_setkeys(coll_id, obj_i, data);
      if (i % 1000 == 0) {
        std::cerr << "[pre_fill_logs] " << i * 100 / fill_size << " %" << std::endl;
      }
      co_await local_store.do_transaction(coll_ref, std::move(txn));
    }
    co_return;
  };

  auto random_updates = [&]() -> seastar::future<> {
    while (true) {
      int i = rand() % fill_size;
      auto cid      = collection_id[i];
      auto coll_ref = coll_ref_map[i];
      auto obj_i    = create_hobj(i);

      std::map<std::string, bufferlist> patch;
      for (int k = 0; k < 4; ++k) {
	std::string key = std::to_string(k);
	bufferlist bl_value;
	bl_value.append_zero(1000);
	patch.emplace(std::move(key), std::move(bl_value));
      }

      ceph::os::Transaction txn;
      txn.omap_setkeys(cid, obj_i, patch);
      co_await local_store.do_transaction(coll_ref, std::move(txn));
    }
    co_return;
  };

  co_await pre_fill_logs();
  co_await random_updates();
  co_return;
}

int main(int argc, char **argv) {
  namespace po = boost::program_options;
  po::options_description desc{"Allowed options"};
  bool segmented;

  desc.add_options()
    ("help,h", "show help message")
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
  seastar_args.emplace_back("1");
  seastar_args.emplace_back("--thread-affinity");
  seastar_args.emplace_back("0");

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
  ::ftruncate(fd, 3.84 * 1000 * 1000 * 1000);
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
        co_await crimson::common::local_conf().set_val("seastore_cbjournal_size", "134217728" /* 128MB */);
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

      co_await pg_log_workload(*store);

      co_await store->umount();
      co_await store->stop();

      co_await crimson::common::sharded_conf().stop();

      co_return 0;
    })
  );

  std::filesystem::remove_all("store_waf_dir", ec);
}
