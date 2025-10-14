#!/bin/bash

PREV_NAND_WRITES=$(nvme ocp smart-add-log /dev/nvme0 | grep "media units written" | awk '{print $7}')
PREV_HOST_WRITES=$(nvme smart-log /dev/nvme0 | grep "Data Units Written" | awk '{print $5}' | sed 's/,//g')

while true; do
  sleep 300;
  NAND_WRITES=$(nvme ocp smart-add-log /dev/nvme0 | grep "media units written" | awk '{print $7}')
  HOST_WRITES=$(nvme smart-log /dev/nvme0 | grep "Data Units Written" | awk '{print $5}' | sed 's/,//g')

  CUR_NAND_WRITES=$((NAND_WRITES - PREV_NAND_WRITES))
  CUR_HOST_WRITES=$((HOST_WRITES - PREV_HOST_WRITES))

  if (( ${CUR_HOST_WRITES} > 0 )); then
    WAF="$(awk -v a="${CUR_NAND_WRITES}" -v b="$((${CUR_HOST_WRITES} * 512000))" 'BEGIN{printf "%.6f", a/b}')"
    echo ${WAF}
  fi

  PREV_NAND_WRITES=${NAND_WRITES}
  PREV_HOST_WRITES=${HOST_WRITES}
done
