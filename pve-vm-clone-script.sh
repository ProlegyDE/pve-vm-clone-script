#!/bin/bash

# Color definitions
RED='\e[91m'
GREEN='\e[92m'
YELLOW='\e[93m'
CYAN='\e[96m'
BLUE='\e[94m'
NC='\e[0m' # No Color

# Function: List all VMs
list_vms() {
  echo -e "${CYAN}Available VMs (VMID : Name):${NC}"
  for conf in /etc/pve/qemu-server/*.conf; do
    vmid=$(basename "$conf" .conf)
    name=$(grep -m1 '^name:' "$conf" | cut -d' ' -f2-)
    echo -e "  ${BLUE}$vmid${NC} : $name"
  done
}

# Function: List all snapshots of a dataset
list_snapshots() {
  local dataset="$1"
  zfs list -t snapshot -o name -s creation | grep "^${dataset}@"
}

echo -e "${CYAN}=== Proxmox VM Clone Script ===${NC}\n"

# 1. List VMs
list_vms
echo
read -p "Enter the source VMID: " SRC_VMID
SRC_CONF="/etc/pve/qemu-server/${SRC_VMID}.conf"

if [ ! -f "$SRC_CONF" ]; then
  echo -e "${RED}Error: Configuration file $SRC_CONF does not exist!${NC}"
  exit 1
fi

# 2. Enter new VMID
read -p "Enter the new VMID (e.g., 9100). If left blank, 9${SRC_VMID} will be used: " NEW_VMID
if [ -z "$NEW_VMID" ]; then
  NEW_VMID="9${SRC_VMID}"
  echo -e "${YELLOW}No input – new VMID will be: $NEW_VMID${NC}"
fi
NEW_CONF="/etc/pve/qemu-server/${NEW_VMID}.conf"
if [ -f "$NEW_CONF" ]; then
  echo -e "${RED}Error: Configuration file for VMID $NEW_VMID already exists!${NC}"
  exit 1
fi

# 3. Find disk datasets from source configuration  
declare -A DISK_DATASETS
echo -e "\n${CYAN}Finding ZFS datasets from $SRC_CONF ...${NC}"
while IFS= read -r line; do
  if [[ "$line" =~ ^(scsi|ide|sata)[0-9]+:.*local-zfs:([^,]+) ]]; then
    dataset_name="${BASH_REMATCH[2]}"
    dataset="rpool/data/${dataset_name}"
    key=$(echo "$line" | cut -d: -f1)
    DISK_DATASETS["$key"]="$dataset"
    echo -e "  ${BLUE}$key${NC} -> $dataset"
  fi
done < "$SRC_CONF"

if [ ${#DISK_DATASETS[@]} -eq 0 ]; then
  echo -e "${RED}No ZFS disk datasets found in the VM configuration.${NC}"
  exit 1
fi

# 4. Determine reference disk
min_disk=99999
ref_disk_key=""
for key in "${!DISK_DATASETS[@]}"; do
  dataset="${DISK_DATASETS[$key]}"
  if [[ "$dataset" =~ -disk-([0-9]+)$ ]]; then
    disknum="${BASH_REMATCH[1]}"
    if [ "$disknum" -lt "$min_disk" ]; then
      min_disk="$disknum"
      ref_disk_key="$key"
    fi
  else
    echo -e "${YELLOW}[WARN] Dataset $dataset does not match the expected pattern.${NC}"
  fi
done

if [ -z "$ref_disk_key" ]; then
  echo -e "${RED}No suitable reference disk found. Exiting.${NC}"
  exit 1
fi

ref_dataset="${DISK_DATASETS[$ref_disk_key]}"
echo -e "\n${CYAN}Reference disk: ${BLUE}$ref_disk_key${CYAN} ($ref_dataset)${NC}"
echo -e "${CYAN}Select the snapshot:${NC}"

# List snapshots
snaps=()
while IFS= read -r snap; do
  snaps+=("$snap")
done < <(list_snapshots "$ref_dataset")

if [ ${#snaps[@]} -eq 0 ]; then
  echo -e "${RED}No snapshots found for $ref_dataset. Exiting.${NC}"
  exit 1
fi

for i in "${!snaps[@]}"; do
  snap_suffix="${snaps[$i]#*@}"
  echo -e "  ${BLUE}[$i]${NC} $snap_suffix"
done

read -p "Enter the index of the snapshot to clone: " idx
if ! [[ "$idx" =~ ^[0-9]+$ ]] || [ "$idx" -ge "${#snaps[@]}" ]; then
  echo -e "${RED}Invalid input. Exiting.${NC}"
  exit 1
fi

selected_snapshot="${snaps[$idx]}"
snap_suffix="${selected_snapshot#*@}"
echo -e "${GREEN}Selected snapshot suffix: $snap_suffix${NC}"

# 5. Clone snapshots
declare -a CLONED_DISKS
echo -e "\n${CYAN}Creating ZFS clones ...${NC}"
for disk in "${!DISK_DATASETS[@]}"; do
  dataset="${DISK_DATASETS[$disk]}"
  target_snapshot="${dataset}@${snap_suffix}"
  
  if ! zfs list -t snapshot "$target_snapshot" &>/dev/null; then
    echo -e "${YELLOW}[WARN] Snapshot $target_snapshot does not exist for disk $disk. Skipping.${NC}"
    continue
  fi

  if [[ "$target_snapshot" =~ (rpool/data/)(vm-)([0-9]+)(-disk-.*)@.* ]]; then
    new_dataset="${BASH_REMATCH[1]}${BASH_REMATCH[2]}${NEW_VMID}${BASH_REMATCH[4]}"
  else
    echo -e "${YELLOW}[WARN] Invalid format for $target_snapshot. Skipping.${NC}"
    continue
  fi

  echo -e "  ${CYAN}Disk $disk:${NC}"
  echo -e "    Snapshot: ${BLUE}$target_snapshot${NC}"
  echo -e "    New path: ${GREEN}$new_dataset${NC}"

  if zfs list "$new_dataset" &>/dev/null; then
    echo -e "    ${YELLOW}Dataset already exists. Skipping.${NC}"
    CLONED_DISKS+=("$disk")
    continue
  fi

  source_dataset="${target_snapshot%@*}"
  ds_type=$(zfs get -H -o value type "$source_dataset")
  
  clone_opts=()
  [ "$ds_type" == "filesystem" ] && clone_opts=(-o mountpoint=none)

  if zfs clone "${clone_opts[@]}" "$target_snapshot" "$new_dataset"; then
    CLONED_DISKS+=("$disk")
  else
    echo -e "    ${RED}Error cloning! Exiting.${NC}"
    exit 1
  fi
done

# 6. Create new configuration
echo -e "\n${CYAN}Creating new VM configuration: ${BLUE}$NEW_CONF${NC}"
> "$NEW_CONF"
while IFS= read -r line; do
  if [[ "$line" =~ ^(scsi|ide|sata)[0-9]+: ]]; then
    key=$(echo "$line" | cut -d: -f1)
    for d in "${CLONED_DISKS[@]}"; do
      if [ "$d" == "$key" ]; then
        newline=$(echo "$line" | sed "s/vm-${SRC_VMID}-disk/vm-${NEW_VMID}-disk/g")
        echo "$newline" >> "$NEW_CONF"
        continue 2
      fi
    done
    echo -e "  ${YELLOW}[INFO] Skipping disk line '$key'.${NC}"
  else
    echo "$line" >> "$NEW_CONF"
  fi
done < "$SRC_CONF"

# Adjustments in the configuration
sed -i \
  -e 's/onboot: 1/onboot: 0/g' \
  -e 's/vmbr0/vmbr0,link_down=1/g' \
  -e 's/^name:\s*/name: clone-/g' \
  "$NEW_CONF"

echo -e "\n${GREEN}Done! New VM with VMID $NEW_VMID has been created.${NC}"