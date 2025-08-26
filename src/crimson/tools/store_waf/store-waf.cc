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
ghobject_t create_hobj(unsigned id) {
  return ghobject_t(shard_id_t::NO_SHARD, seastar::this_shard_id(),
    id, "", "", 0, ghobject_t::NO_GEN);
};

// From tools/store_bench/store-bench.cc
coll_t make_cid(int obj_id, int num_objects_per_collection) {
  int pg_id = obj_id / num_objects_per_collection;
  return coll_t(spg_t(pg_t(pg_id, 0)));
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

// From tools/store_bench/store-bench.cc
seastar::future<> cbw_workload(crimson::os::FuturizedStore &global_store) {
  uint64_t prefill_size = 128<<10;
  // util 85%
  // uint64_t size_per_shard = 3500ULL<<20;
  // util 70%
  uint64_t size_per_shard = 2450ULL<<20;
  uint64_t size_per_obj = 4<<20;
  uint64_t colls_per_shard = 16;
  uint64_t io_concurrency_per_shard = 16;
  auto get_obj_per_shard = [&]() {
    return (size_per_shard + size_per_obj - 1) / size_per_obj;
  };
  auto get_obj_per_coll = [&]() {
    return (get_obj_per_shard() + colls_per_shard - 1) / colls_per_shard;
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
  static const int dist_table[100] = {
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
  static const size_t sizes[] = {
    512, 1024, 1536, 2048, 2560,
    3072, 3584, 4096, 8192,
    16384, 32768, 65536
  };

  auto &local_store = global_store.get_sharded_store();

  std::vector<std::pair<coll_t, crimson::os::CollectionRef>> coll_refs;
  for (uint64_t collidx = 0; collidx < colls_per_shard; ++collidx) {
    coll_t cid(
      spg_t(pg_t(0, (seastar::this_shard_id() * colls_per_shard) + collidx))
    );
    auto ref = co_await local_store.create_new_collection(cid);
    coll_refs.emplace_back(std::make_pair(cid, std::move(ref)));
  }
  auto get_coll_id = [&](uint64_t obj_id) {
    return coll_refs[obj_id / get_obj_per_coll()].first;
  };
  auto get_coll_ref = [&](uint64_t obj_id) {
    return coll_refs[obj_id / get_obj_per_coll()].second;
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

  for (uint64_t obj_id = 0; obj_id < get_obj_per_shard(); ++obj_id) {
    auto hobj = create_hobj(obj_id);
    auto coll_id = get_coll_id(obj_id);
    auto coll_ref = get_coll_ref(obj_id);

    {
      ceph::os::Transaction t;
      t.create(coll_id, hobj);
      co_await submit_transaction(coll_ref, std::move(t));
    }
    for (uint64_t off = 0; off < size_per_obj; off += prefill_size) {
      ceph::os::Transaction t;
      t.write(coll_id, hobj, off, prefill_size, get_random_buffer(prefill_size));
      co_await submit_transaction(coll_ref, std::move(t));
    }
    std::cout << "wrote obj " << obj_id << " of " << get_obj_per_shard() << std::endl;
  }

  std::cout << "finished populating" << std::endl;

  static const int p1 =  get_obj_per_shard() * 0.05;
  static const int p2 =  get_obj_per_shard() * 0.20;
  while (true) {
    int obj_id;
    int p = std::experimental::randint<int>(0, 99);
    if (p < 50) {
      obj_id = std::experimental::randint<int>(0, p1-1);
    } else if (p < 80) {
      obj_id = std::experimental::randint<int>(p1, p2-1);
    } else {
      obj_id = std::experimental::randint<int>(p2, get_obj_per_shard()-1);
    }
    auto hobj = create_hobj(obj_id);
    auto coll_id = get_coll_id(obj_id);
    auto coll_ref = get_coll_ref(obj_id);

    uint64_t io_size = sizes[dist_table[std::experimental::randint<int>(0, 99)]];
    auto offset = std::experimental::randint<uint64_t>(
	0,
	(size_per_obj / io_size) - 1) * io_size;

    ceph::os::Transaction t;
    t.write(
	coll_id,
	hobj,
	offset,
	io_size,
	get_random_buffer(io_size));
    co_await submit_transaction(coll_ref, std::move(t));
  }

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

      co_await cbw_workload(*store);

      co_await store->umount();
      co_await store->stop();

      co_await crimson::common::sharded_conf().stop();

      co_return 0;
    })
  );

  std::filesystem::remove_all("store_waf_dir", ec);
}
