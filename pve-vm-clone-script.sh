#!/bin/bash

# Color definitions
RED='\e[91m'
GREEN='\e[92m'
YELLOW='\e[93m'
CYAN='\e[96m'
BLUE='\e[94m'
NC='\e[0m' # No Color

# Function: Format MB values to GB if >=1000
format_mb() {
    local mb=$1
    if [ "$mb" -ge 1000 ]; then
        awk -v mb="$mb" 'BEGIN {printf "%.2f GB", mb / 1000}'
    else
        echo "${mb} MB"
    fi
}

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

# RAM check
echo -e "\n${CYAN}Checking host RAM usage...${NC}"

total_ram_mb=$(free -m | awk '/^Mem:/ {print $2}')
src_vm_ram=$(qm config "$SRC_VMID" | awk '/^memory:/ {print $2}')
if [ -z "$src_vm_ram" ]; then
    src_vm_ram=512
    echo -e "${YELLOW}No memory setting found for VM $SRC_VMID, assuming ${src_vm_ram} MB.${NC}"
fi

sum_running_ram=0
# Get list of running VMs
while IFS= read -r vm; do
    [ -z "$vm" ] && continue
    ram=$(qm config "$vm" | awk '/^memory:/ {print $2}')
    sum_running_ram=$((sum_running_ram + ram))
done < <(qm list | awk '/running/ {print $1}')

threshold=$((total_ram_mb * 90 / 100))
prognostic_ram=$((sum_running_ram + src_vm_ram))

echo -e "  Total host RAM:      ${BLUE}$(format_mb ${total_ram_mb})${NC}"
echo -e "  Running VMs RAM:     ${BLUE}$(format_mb ${sum_running_ram})${NC}"
echo -e "  Source VM RAM:       ${BLUE}$(format_mb ${src_vm_ram})${NC}"
echo -e "  Projected RAM:       ${BLUE}$(format_mb ${prognostic_ram})${NC}"
echo -e "  90% Threshold:       ${BLUE}$(format_mb ${threshold})${NC}"

if [ "$prognostic_ram" -gt "$threshold" ]; then
    echo -e "${RED}WARNING: Starting the cloned VM would exceed 90% of total host RAM!${NC}"
    echo -e "${YELLOW}This may lead to performance issues or instability.${NC}"
    read -p "$(echo -e "${RED}Proceed with cloning? (y/N) ${NC}")" confirm
    confirm=${confirm:-N}
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo -e "${RED}Cloning aborted by user.${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}RAM check passed. Projected usage is below 90%.${NC}"
fi

# 3. Find disk datasets from source configuration  
declare -A DISK_DATASETS
echo -e "\n${CYAN}Finding ZFS datasets from $SRC_CONF ...${NC}"
while IFS= read -r line; do
  if [[ "$line" =~ ^(scsi|ide|sata|virtio|efidisk|tpmstate)[0-9]+:.*local-zfs:([^,]+) ]]; then
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
  if [[ "$dataset" =~ -disk[-]?([0-9]+)$ ]]; then
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
inside_snapshot=0
while IFS= read -r line; do
  # Check for the start of a snapshot section
  if [[ "$line" =~ ^\[.*\] ]]; then
    inside_snapshot=1
    continue
  fi

  # Skip lines within a snapshot section
  if (( inside_snapshot )); then
    continue
  fi

  # Process disk lines
  if [[ "$line" =~ ^(scsi|ide|sata|virtio|efidisk|tpmstate)[0-9]+: ]]; then
    key=$(echo "$line" | cut -d: -f1)
    found=0
    for d in "${CLONED_DISKS[@]}"; do
      if [[ "$d" == "$key" ]]; then
        newline=$(echo "$line" | sed "s/vm-${SRC_VMID}-/vm-${NEW_VMID}-/g")
        echo "$newline" >> "$NEW_CONF"
        found=1
        break
      fi
    done
    if (( found == 0 )); then
      echo -e "  ${YELLOW}[INFO] Skipping disk line '$key'.${NC}"
    fi
  else
    # Skip parent line
    if [[ "$line" =~ ^parent: ]]; then
      echo -e "  ${YELLOW}[INFO] Skipping Snapshot parent line in: $line${NC}"
      continue
    fi
    # Write other lines to the new config
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