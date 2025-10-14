LBAF=0 # 4KB 
DEVICE=0

if [ -z "$1" ]; then
  echo "Usage: $0 <percentage>"
  echo "Example: $0 70"
  exit 1
fi

PERCENT=$1
TARGET_SIZE=$((468713472 * PERCENT / 100))

echo "Namespace size ($NS_SIZE) of ${PERCENT}% = $TARGET_SIZE"
# 2TB : 468713472
# 4TB : 937426944
# 8TB : 1875116032
# 16TB : 3749969920

# 1. discard all NVMe
nvme format /dev/ng${DEVICE}n1 --ses=1 --pi=0 --namespace-id=1 -l ${LBAF}
sleep 10

# 2. Create ns with FDP enable
nvme delete-ns /dev/nvme${DEVICE} -n 0xffffffff
sleep 1
nvme set-feature /dev/nvme${DEVICE} -f 0x1d --cdw12=1 -s --value=1 # set fdp enable
sleep 10
nvme get-feature /dev/nvme${DEVICE} -f 0x1d -s 2 --cdw11=1
sleep 1
# It is set to generate 8 RUHs.
# If you run with "-n 4 -p 0,1,2,3", it will be set to 4 RUHs.
#nvme create-ns /dev/nvme${DEVICE} -s ${TARGET_SIZE} -c ${TARGET_SIZE} -f ${LBAF} -n 8 -p 0,1,2,3,4,5,6,7
#nvme create-ns /dev/nvme${DEVICE} -s ${TARGET_SIZE} -c ${TARGET_SIZE} -f ${LBAF} -n 4 -p 0,1,2,3
#nvme create-ns /dev/nvme${DEVICE} -s ${TARGET_SIZE} -c ${TARGET_SIZE} -f ${LBAF} -n 5 -p 0,1,2,3,4
nvme create-ns /dev/nvme${DEVICE} -s ${TARGET_SIZE} -c ${TARGET_SIZE} -f ${LBAF} -n 6 -p 0,1,2,3,4,5
#nvme create-ns /dev/nvme${DEVICE} -s ${TARGET_SIZE} -c ${TARGET_SIZE} -f ${LBAF} -n 7 -p 0,1,2,3,4,5,6
sleep 1
nvme attach-ns /dev/nvme${DEVICE} -n 1 -c 1
sleep 3
nvme dir-send /dev/nvme${DEVICE} --namespace-id=1 --dir-type=0 --dir-spec=2 --dir-oper=1 --target-dir=2
sleep 1
nvme dir-receive /dev/nvme${DEVICE} --namespace-id=1 --dir-oper=1 --human-readable
sleep 1
nvme io-mgmt-recv /dev/nvme${DEVICE}n1 --data-len=300 --mo=1 # check fdp setting
sleep 10
