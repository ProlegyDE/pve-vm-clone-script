#!/usr/bin/env python3

import sys
import os
import subprocess
import re
import glob
from pathlib import Path
import shutil
import tempfile
from datetime import datetime
import math

DEFAULT_ZFS_POOL_PATH = "rpool/data"
RAM_THRESHOLD_PERCENT = 90

COLORS = {
    "RED": '\033[91m',
    "GREEN": '\033[92m',
    "YELLOW": '\033[93m',
    "CYAN": '\033[96m',
    "BLUE": '\033[94m',
    "NC": '\033[0m'
}

def color_text(text, color_name):
    color = COLORS.get(color_name.upper(), COLORS["NC"])
    nc = COLORS["NC"]
    return f"{color}{text}{nc}"

def print_info(text):
    print(color_text(text, "CYAN"))

def print_success(text):
    print(color_text(text, "GREEN"))

def print_warning(text):
    print(color_text(text, "YELLOW"))

def print_error(text):
    print(color_text(text, "RED"), file=sys.stderr)

def run_command(cmd_list, check=True, capture_output=True, text=True, error_msg=None, suppress_stderr=False):
    stderr_setting = None
    stdout_setting = None

    if not capture_output:
        stderr_setting = subprocess.DEVNULL if suppress_stderr else None
        stdout_setting = None

    try:
        result = subprocess.run(
            cmd_list,
            check=check,
            capture_output=capture_output,
            text=text,
            stdout=stdout_setting,
            stderr=stderr_setting
        )
        return result.stdout.strip() if capture_output and result.stdout else ""
    except FileNotFoundError:
        msg = error_msg or f"Error: Command '{cmd_list[0]}' not found."
        print_error(msg)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        msg = error_msg or f"Error executing '{' '.join(cmd_list)}'"
        print_error(f"{msg}\nReturn Code: {e.returncode}")
        if capture_output:
            if e.stderr and not suppress_stderr:
                print_error(f"Stderr:\n{e.stderr.strip()}")
            elif e.stdout:
                 print_error(f"Stdout:\n{e.stdout.strip()}")
        sys.exit(1)
    except Exception as e:
        msg = error_msg or f"Unexpected error executing '{' '.join(cmd_list)}'"
        print_error(f"{msg}: {e}")
        sys.exit(1)

def format_mb(mb):
    try:
        mb_val = float(mb)
        if mb_val >= 1024:
            return f"{mb_val / 1024:.2f} GB"
        else:
            return f"{int(mb_val)} MB"
    except (ValueError, TypeError):
        return f"{mb} MB"

def parse_size_to_mb(size_str):
    size_str = str(size_str).strip().upper()
    if not size_str:
        return 0
    if size_str.endswith('G'):
        return int(float(size_str[:-1]) * 1024)
    elif size_str.endswith('M'):
        return int(float(size_str[:-1]))
    elif size_str.isdigit():
        return int(size_str)
    else:
        match = re.match(r'^(\d+)', size_str)
        if match:
            print_warning(f"Unknown/missing unit in '{size_str}', interpreting as MB.")
            return int(match.group(1))
        return 0

def get_instance_details(conf_path):
    instance_id = Path(conf_path).stem
    name = "<no name/hostname>"
    is_lxc = 'lxc' in conf_path

    try:
        with open(conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('name:'):
                    name = line.split(':', 1)[1].strip()
                    break
                elif is_lxc and line.startswith('hostname:'):
                     if name == "<no name/hostname>":
                         name = line.split(':', 1)[1].strip()
    except Exception as e:
        print_warning(f"Could not fully read configuration file {conf_path}: {e}")

    return instance_id, name

def list_instances():
    print_info("Available VMs and LXC containers:")
    vms = []
    lxcs = []

    vm_conf_files = sorted(glob.glob("/etc/pve/qemu-server/*.conf"))
    print(f" {color_text('VMs:', 'YELLOW')}")
    if vm_conf_files:
        for conf in vm_conf_files:
            vm_id, vm_name = get_instance_details(conf)
            vms.append({'id': vm_id, 'name': vm_name})
            print(f"   {color_text(vm_id, 'BLUE')} : {vm_name}")
    else:
        print(f"   {color_text('No VMs found.', 'YELLOW')}")

    lxc_conf_files = sorted(glob.glob("/etc/pve/lxc/*.conf"))
    print(f" {color_text('LXC Containers:', 'YELLOW')}")
    if lxc_conf_files:
        for conf in lxc_conf_files:
            lxc_id, lxc_name = get_instance_details(conf)
            lxcs.append({'id': lxc_id, 'name': lxc_name})
            print(f"   {color_text(lxc_id, 'BLUE')} : {lxc_name}")
    else:
        print(f"   {color_text('No LXC containers found.', 'YELLOW')}")

    if not vms and not lxcs:
        print(f"   {color_text('No VMs or LXC containers found on this system.', 'RED')}")
        return False

    return True

def list_snapshots(dataset):
    cmd = ['zfs', 'list', '-t', 'snapshot', '-o', 'name,creation', '-s', 'creation', '-H', '-p', dataset]
    output = run_command(cmd, check=False, suppress_stderr=True)
    snapshots = []
    if output:
        for line in output.strip().split('\n'):
            if line.startswith(f"{dataset}@"):
                try:
                    name, creation_ts = line.split('\t')
                    snapshots.append({'name': name, 'creation_timestamp': int(creation_ts)})
                except ValueError:
                    print_warning(f"Could not parse snapshot line: {line}")
                    if line.startswith(f"{dataset}@"):
                         snapshots.append({'name': line.strip(), 'creation_timestamp': 0})
    return snapshots

def get_zfs_property(target, property_name):
    cmd = ['zfs', 'get', '-H', '-o', 'value', property_name, target]
    return run_command(cmd, check=False, suppress_stderr=True).strip()

def main():
    print_info("=== Proxmox VM/LXC Clone Script (Python) ===")
    print()

    if not list_instances():
        print_error("Exiting script as no instances were found.")
        sys.exit(1)
    print()

    while True:
        src_id = input("Enter the source VM or LXC ID: ").strip()
        if src_id.isdigit():
            break
        else:
            print_error("Invalid ID format. Please enter a number.")

    vm_conf = Path(f"/etc/pve/qemu-server/{src_id}.conf")
    lxc_conf = Path(f"/etc/pve/lxc/{src_id}.conf")

    if vm_conf.is_file():
        clone_type = "vm"
        pve_cmd = "qm"
        conf_dir = vm_conf.parent
        dataset_prefix = "vm-"
        config_type = "VM"
        src_conf_path = vm_conf
    elif lxc_conf.is_file():
        clone_type = "lxc"
        pve_cmd = "pct"
        conf_dir = lxc_conf.parent
        dataset_prefix = "subvol-"
        config_type = "LXC Container"
        src_conf_path = lxc_conf
    else:
        print_error(f"Error: No VM or LXC with ID {src_id} found.")
        sys.exit(1)

    print_success(f"Selected source: ID {src_id} ({config_type})")

    default_new_id = f"9{src_id}"
    while True:
        new_id = input(f"Enter the new {config_type} ID (blank for default={default_new_id}): ").strip()
        if not new_id:
            new_id = default_new_id
            print_warning(f"No input - using default ID: {new_id}")

        if new_id.isdigit():
            new_conf_path = conf_dir / f"{new_id}.conf"
            other_conf_dir = Path("/etc/pve/lxc") if clone_type == "vm" else Path("/etc/pve/qemu-server")
            other_conf_path = other_conf_dir / f"{new_id}.conf"

            collision = False
            if new_conf_path.exists():
                print_error(f"Error: Configuration file for {config_type} ID {new_id} already exists! ({new_conf_path})")
                collision = True
            if other_conf_path.exists():
                other_type = "LXC Container" if clone_type == "vm" else "VM"
                print_error(f"Error: A {other_type} with ID {new_id} already exists! ({other_conf_path})")
                collision = True

            if not collision:
                break
        else:
            print_error("Invalid new ID format. Please enter a number.")

    new_conf_path = conf_dir / f"{new_id}.conf"

    if clone_type == "vm":
        print_info("\nChecking host RAM usage...")
        try:
            free_output = run_command(['free', '-m'])
            total_ram_mb = int(free_output.split('\n')[1].split()[1])

            qm_config_output = run_command([pve_cmd, 'config', src_id], suppress_stderr=True)
            src_vm_ram_mb = 512
            match = re.search(r'^memory:\s*(\S+)', qm_config_output, re.MULTILINE)
            if match:
                 src_vm_ram_mb = parse_size_to_mb(match.group(1))
                 if src_vm_ram_mb == 0:
                     print_warning(f"Could not parse 'memory' for VM {src_id}, using default 512 MB.")
                     src_vm_ram_mb = 512
            else:
                 print_warning(f"No 'memory' setting found for VM {src_id}, using default 512 MB.")

            qm_list_output = run_command([pve_cmd, 'list'], suppress_stderr=True)
            sum_running_ram_mb = 0
            running_vm_lines = [line for line in qm_list_output.split('\n')[1:] if 'running' in line]
            for line in running_vm_lines:
                 parts = line.split()
                 if len(parts) > 0 and parts[0].isdigit():
                     vmid = parts[0]
                     vm_config = run_command([pve_cmd, 'config', vmid], suppress_stderr=True)
                     ram_mb = 512
                     match_ram = re.search(r'^memory:\s*(\S+)', vm_config, re.MULTILINE)
                     if match_ram:
                         parsed_ram = parse_size_to_mb(match_ram.group(1))
                         if parsed_ram > 0: ram_mb = parsed_ram
                     sum_running_ram_mb += ram_mb

            threshold_mb = math.floor(total_ram_mb * RAM_THRESHOLD_PERCENT / 100)
            prognostic_ram_mb = sum_running_ram_mb + src_vm_ram_mb

            print(f"   Total host RAM:       {color_text(format_mb(total_ram_mb), 'BLUE')}")
            print(f"   Running VMs RAM:      {color_text(format_mb(sum_running_ram_mb), 'BLUE')}")
            print(f"   Source VM RAM:        {color_text(format_mb(src_vm_ram_mb), 'BLUE')}")
            print(f"   Projected RAM:        {color_text(format_mb(prognostic_ram_mb), 'BLUE')}")
            print(f"   {RAM_THRESHOLD_PERCENT}% Threshold:          {color_text(format_mb(threshold_mb), 'BLUE')}")

            if prognostic_ram_mb > threshold_mb:
                print_warning(f"\nWARNING: Starting the cloned VM would likely exceed {RAM_THRESHOLD_PERCENT}% of the host's total RAM!")
                print_warning("This may lead to performance issues or instability.")
                confirm = input(f"{color_text('Continue with cloning? (y/N) ', 'RED')}{COLORS['NC']}").strip().lower()
                if confirm not in ['y', 'yes']:
                    print_error("Cloning aborted by user.")
                    sys.exit(1)
            else:
                print_success("RAM check passed. Projected usage is below threshold.")

        except Exception as e:
            print_warning(f"\nCould not complete RAM check: {e}")
            print_warning("Skipping RAM check.")
    else:
        print_warning("\nSkipping RAM check for LXC containers.")

    print_info(f"\nSearching for ZFS datasets in {src_conf_path}...")
    storage_datasets = {}
    storage_regex_vm = re.compile(r'^(scsi|ide|sata|virtio|efidisk|tpmstate)\d+:\s*[^#]*local-zfs:([^,]+)')
    storage_regex_lxc = re.compile(r'^(rootfs|mp\d+):\s*[^#]*local-zfs:([^,]+)')

    try:
        with open(src_conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                match = None
                key = ""
                dataset_name_part = ""

                if clone_type == "vm":
                    match = storage_regex_vm.match(line)
                    if match:
                        key = match.group(1) + line[len(match.group(1)):line.find(':')]
                        dataset_name_part = match.group(2).strip()
                else:
                    match = storage_regex_lxc.match(line)
                    if match:
                        key = match.group(1)
                        dataset_name_part = match.group(2).split(',')[0].strip()

                if key and dataset_name_part:
                    if '/' in dataset_name_part and not dataset_name_part.startswith('/'):
                         dataset_name_part = dataset_name_part.split('/', 1)[1]

                    if '/' in dataset_name_part:
                        full_dataset_path = dataset_name_part
                    else:
                        full_dataset_path = f"{DEFAULT_ZFS_POOL_PATH}/{dataset_name_part}"

                    test_cmd = ['zfs', 'list', '-H', full_dataset_path]
                    if run_command(test_cmd, check=False, suppress_stderr=True):
                        storage_datasets[key] = full_dataset_path
                        print(f"   {color_text(key, 'BLUE')} -> {full_dataset_path}")
                    else:
                         print_warning(f"   Configured dataset for {color_text(key, 'BLUE')} ('{full_dataset_path}') not found in ZFS. Skipping.")

    except Exception as e:
        print_error(f"Error reading configuration file {src_conf_path}: {e}")
        sys.exit(1)

    if not storage_datasets:
        print_error(f"No existing ZFS disk/storage datasets linked with 'local-zfs' found in configuration {src_conf_path}.")
        print_warning("Verify that the source VM/LXC uses storage defined on the 'local-zfs' storage ID.")
        print_warning(f"Ensure the corresponding ZFS datasets exist (e.g., under {DEFAULT_ZFS_POOL_PATH}/).")
        sys.exit(1)

    ref_key = ""
    ref_dataset = ""
    min_disk_num = 99999
    efi_key = ""
    efi_dataset = ""

    if clone_type == 'lxc' and 'rootfs' in storage_datasets:
        ref_key = 'rootfs'
        ref_dataset = storage_datasets[ref_key]
        print_info(f"\nUsing LXC rootfs as snapshot reference: {color_text(ref_key, 'BLUE')} ({color_text(ref_dataset,'CYAN')})")
    else:
        disk_num_regex = re.compile(r'.*-disk-(\d+)$')
        for key, dataset in storage_datasets.items():
            match = disk_num_regex.search(dataset)
            if match:
                disk_num = int(match.group(1))
                if disk_num < min_disk_num:
                    min_disk_num = disk_num
                    ref_key = key
                    ref_dataset = dataset
            elif clone_type == 'vm' and key == 'efidisk0':
                 efi_key = key
                 efi_dataset = dataset
            elif clone_type == 'lxc' and key == 'rootfs' and len(storage_datasets) == 1:
                 ref_key = key
                 ref_dataset = dataset
                 break

        if not ref_key and efi_key:
            ref_key = efi_key
            ref_dataset = efi_dataset
            print_warning(f"\n[WARN] No '-disk-N' dataset found. Using EFI disk as reference: {color_text(ref_key, 'BLUE')} ({color_text(ref_dataset,'CYAN')})")
        elif not ref_key and storage_datasets:
            first_key = sorted(storage_datasets.keys())[0]
            ref_key = first_key
            ref_dataset = storage_datasets[ref_key]
            print_warning(f"\n[WARN] No suitable reference dataset found. Using first found dataset: {color_text(ref_key, 'BLUE')} ({color_text(ref_dataset,'CYAN')})")
        elif not ref_key:
             print_error("\nCould not determine reference dataset for snapshot listing. Aborting.")
             sys.exit(1)
        else:
             print_info(f"\nUsing reference storage: {color_text(ref_key, 'BLUE')} ({color_text(ref_dataset,'CYAN')})")

    print_info(f"Selecting snapshot based on reference storage ({ref_key}):")
    snapshots = list_snapshots(ref_dataset)

    if not snapshots:
        print_error(f"No snapshots found for reference dataset {ref_dataset}.")
        print_warning(f"Please create a snapshot for {config_type} {src_id} first.")
        sys.exit(1)

    for i, snap in enumerate(snapshots):
        snap_suffix = snap['name'].split('@', 1)[1]
        creation_dt = datetime.fromtimestamp(snap['creation_timestamp'])
        human_time = creation_dt.strftime('%Y-%m-%d %H:%M:%S')
        print(f"   {color_text(f'[{i}]', 'BLUE')} {snap_suffix} {color_text(f'({human_time})', 'YELLOW')}")

    while True:
        try:
            idx_input = input("Enter the index of the snapshot to clone: ").strip()
            idx = int(idx_input)
            if 0 <= idx < len(snapshots):
                selected_snapshot_info = snapshots[idx]
                selected_snapshot_name = selected_snapshot_info['name']
                snap_suffix = selected_snapshot_name.split('@', 1)[1]
                print_success(f"Selected snapshot suffix: {snap_suffix}")
                break
            else:
                print_error("Invalid index. Index out of range.")
        except ValueError:
            print_error("Invalid input. Please enter a number.")

    print_info("\nCreating ZFS clones...")
    cloned_keys = []
    clone_success = True

    dataset_name_pattern = re.compile(rf"^(.*\/)({dataset_prefix})({src_id})(-.*)$")

    for key, dataset in storage_datasets.items():
        target_snapshot = f"{dataset}@{snap_suffix}"
        print(f"  {color_text(f'Processing {key}:', 'CYAN')}")
        print(f"    Source dataset: {color_text(dataset, 'BLUE')}")

        if not get_zfs_property(target_snapshot, 'type'):
             print_warning(f"    [WARN] Snapshot '{target_snapshot}' does not exist for this dataset. Skipping.")
             continue

        match = dataset_name_pattern.match(dataset)
        if match:
            pool_base = match.group(1)
            suffix = match.group(4)
            new_dataset = f"{pool_base}{dataset_prefix}{new_id}{suffix}"
        else:
            print_error(f"    [ERROR] Could not parse dataset name '{dataset}' to create new name.")
            clone_success = False
            continue

        print(f"    Source snapshot: {color_text(target_snapshot, 'BLUE')}")
        print(f"    Target dataset:  {color_text(new_dataset, 'GREEN')}")

        if get_zfs_property(new_dataset, 'type'):
            print_warning(f"    Target dataset '{new_dataset}' already exists. Skipping cloning.")
            cloned_keys.append(key)
            continue

        clone_cmd = ['zfs', 'clone', target_snapshot, new_dataset]
        print(f"    Executing: {' '.join(clone_cmd)}")

        try:
            run_command(clone_cmd, check=True, capture_output=False)
            print_success("    Cloned successfully.")
            cloned_keys.append(key)
        except SystemExit:
            print_error("    Error during 'zfs clone'! Check permissions and storage space.")
            clone_success = False

    if not clone_success:
        print_error("\nOne or more ZFS clone operations failed. Please check the output above.")
        sys.exit(1)

    if not cloned_keys:
        print_error("\nError: No datasets were successfully processed or cloned.")
        sys.exit(1)

    print_info(f"\nCreating new {config_type} configuration: {color_text(str(new_conf_path), 'BLUE')}")

    new_config_lines = []
    try:
        with open(src_conf_path, 'r') as f_src:
            for line in f_src:
                line_strip = line.strip()

                if not line_strip or \
                   line_strip.startswith('#') or \
                   re.match(r'^\[.*\]$', line_strip) or \
                   line_strip.startswith(('parent:', 'snapdir:')):
                    continue

                current_key = ""
                is_storage_line = False
                original_dataset_basename = ""

                match_vm = storage_regex_vm.match(line_strip)
                match_lxc = storage_regex_lxc.match(line_strip)

                if clone_type == "vm" and match_vm:
                    key_part = match_vm.group(1) + line_strip[len(match_vm.group(1)):line_strip.find(':')]
                    if key_part in storage_datasets:
                        current_key = key_part
                        is_storage_line = True
                        original_dataset_basename = Path(storage_datasets[current_key]).name

                elif clone_type == "lxc" and match_lxc:
                     key_part = match_lxc.group(1)
                     if key_part in storage_datasets:
                         current_key = key_part
                         is_storage_line = True
                         original_dataset_basename = Path(storage_datasets[current_key]).name

                if is_storage_line and current_key in cloned_keys:
                    match_base = dataset_name_pattern.match(storage_datasets[current_key])
                    if match_base:
                         suffix = match_base.group(4)
                         new_dataset_basename = f"{dataset_prefix}{new_id}{suffix}"

                         old_part = f"local-zfs:{original_dataset_basename}"
                         new_part = f"local-zfs:{new_dataset_basename}"

                         newline = line.replace(old_part, new_part, 1)
                         if newline == line:
                              print_warning(f"  [WARN] Could not replace dataset '{original_dataset_basename}' in line: {line.strip()}")
                              new_config_lines.append(line)
                         else:
                              new_config_lines.append(newline)
                    else:
                        print_warning(f"  [WARN] Could not construct new basename for '{original_dataset_basename}'. Keeping original line: {line_strip}")
                        new_config_lines.append(line)
                else:
                    new_config_lines.append(line)

        with open(new_conf_path, 'w') as f_new:
             f_new.writelines(new_config_lines)

    except Exception as e:
        print_error(f"Error creating configuration file {new_conf_path}: {e}")
        sys.exit(1)

    print_info(f"\nApplying standard adjustments to {color_text(str(new_conf_path), 'BLUE')}")
    try:
        with open(new_conf_path, 'r') as f_orig:
            lines = f_orig.readlines()

        modified_lines = []
        changes_made = False

        for line in lines:
            original_line = line
            line_strip = line.strip()
            modified = False

            if re.match(r'^\s*onboot:\s*[0-9]+', line_strip):
                new_line_content = "onboot: 0"
                line = new_line_content + "\n"
                if line != original_line:
                    print("   Setting 'onboot: 0'")
                    modified = True

            elif line_strip.startswith('name:') and not line_strip.startswith('name: clone-'):
                new_line_content = re.sub(r'(^\s*name:\s*)(.*)', r'\1clone-\2', line.strip())
                line = new_line_content + "\n"
                if line != original_line:
                    print("   Adding 'clone-' to name")
                    modified = True

            elif line_strip.startswith('hostname:') and not line_strip.startswith('hostname: clone-'):
                new_line_content = re.sub(r'(^\s*hostname:\s*)(.*)', r'\1clone-\2', line.strip())
                line = new_line_content + "\n"
                if line != original_line:
                    print("   Adding 'clone-' to hostname")
                    modified = True

            elif re.match(r'^\s*net\d+:', line_strip):
                current_line_state = line
                if 'link_down=1' not in line_strip:
                    current_line_state = line_strip + ",link_down=1\n"
                    print(f"   Adding ',link_down=1' to network interface: {original_line.strip()}")
                    modified = True

                if modified:
                    line = current_line_state

            modified_lines.append(line)
            if modified:
                changes_made = True

        if changes_made:
            with open(new_conf_path, 'w') as f_new:
                f_new.writelines(modified_lines)
            print_success("   Configuration adjustments applied.")
        else:
             print_info("   No standard adjustments needed or applicable.")

    except Exception as e:
        print_error(f"\nError adjusting configuration file {new_conf_path}: {e}")
        print_warning(f"Configuration file {new_conf_path} may not have been properly adjusted.")

    print(f"\n{color_text('Done!', 'GREEN')} New {config_type} with ID {new_id} has been created from snapshot '{snap_suffix}'.")
    print(f"{color_text('Please review the configuration file', 'YELLOW')} {color_text(str(new_conf_path), 'BLUE')} {color_text('before starting the new instance.', 'YELLOW')}")
    print(color_text("Check network settings, hostname/name, and adjust or remove CD-ROM drives if necessary (VMs).", 'YELLOW'))

    sys.exit(0)

if __name__ == "__main__":
    if os.geteuid() != 0:
        print_warning("Warning: This script typically requires root privileges to execute Proxmox/ZFS commands.")

    main()