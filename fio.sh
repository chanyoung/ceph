#!/bin/sh

../src/stop.sh --crimson

sleep 3
MDS=0 MON=1 OSD=1 MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --redirect-output --seastore --seastore-devs /dev/nvme0n1 --osd-args "--seastore_main_device_type=RANDOM_BLOCK_SSD" --crimson --crimson-smp 10 --no-restart

sleep 3
./bin/ceph -s

sleep 3
../src/stop.sh osd --crimson
python3 ../src/tools/contrib/assign_crimson_cores.py -o 1 -r 10 -a 0 -b osd
./bin/ceph -c ceph.conf config set osd.0 crimson_cpu_set 0-9

sleep 3
/bin/sh /root/fqa/fqa_test_script/chaney/ceph/src/ceph-run /root/fqa/fqa_test_script/chaney/ceph/build/bin/crimson-osd --seastore_main_device_type=RANDOM_BLOCK_SSD -i 0 -c /root/fqa/fqa_test_script/chaney/ceph/build/ceph.conf -f >> out/osd.0.stdout 2>&1 &

sleep 3
./bin/ceph osd pool create rbd 128 128

sleep 3
./bin/ceph osd pool set rbd size 1 --yes-i-really-mean-it

sleep 3
./bin/rbd create rbd/test --size 1400G --image-feature layering,deep-flatten

sleep 3
./bin/ceph osd pool set noautoscale
./bin/ceph balancer off
./bin/ceph osd set nodeep-scrub
./bin/ceph osd set noscrub
#./bin/ceph config set client rbd_io_scheduler none

sleep 3
fio --name=prefill --ioengine=rbd --pool=rbd --rbdname=test \
	--direct=1 --verify=0 \
	--numjobs=1 --iodepth=256 \
	--rw=write --bs=128k \
	--cpus_allowed=10 --cpus_allowed_policy=split \
	--group_reporting=1

sleep 3
fio --name=cbw --time_based --runtime=1h --ramp_time=5h \
	--ioengine=rbd --pool=rbd --rbdname=test --direct=1 --verify=0 \
	--numjob=8 --iodepth=8 \
	--rw=randrw --rwmixwrite=20 --rate=25600k,6400k \
	--random_distribution=zoned:50/5:30/15:20/80 \
	--bssplit=512/4:1024/1:1536/1:2048/1:2560/1:3072/1:3584/1:4k/67:8k/10:16k/7:32k/3:64k/3 \
	--percentile_list=95.0:95.33:95.66:96.0:96.33:96.66:97.0:97.33:97.66:98.0:98.33:98.66:99.0:99.33:99.66:99.99 \
	--cpus_allowed=10-17 --cpus_allowed_policy=split \
	--group_reporting=1

sleep 3
../src/stop.sh --crimson
