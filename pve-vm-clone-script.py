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

def is_tool(name):
    return shutil.which(name) is not None

def run_command(cmd_list, check=True, capture_output=True, text=True, error_msg=None, suppress_stderr=False, input_data=None):
    stdin_setting = subprocess.PIPE if input_data is not None else None
    stdout_setting = None if capture_output else None
    stderr_setting = subprocess.DEVNULL if suppress_stderr else None

    try:
        process = subprocess.run(
            cmd_list,
            check=check,
            capture_output=capture_output,
            text=text,
            stdout=stdout_setting if not capture_output else None,
            stderr=stderr_setting if not capture_output else None,
            input=input_data,
            stdin=stdin_setting
        )
        return process.stdout.strip() if capture_output and process.stdout else ""

    except FileNotFoundError:
        msg = error_msg or f"Error: Command '{cmd_list[0]}' not found."
        print_error(msg)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        msg = error_msg or f"Error executing '{' '.join(cmd_list)}'"
        print_error(f"{msg}\nReturn Code: {e.returncode}")
        stderr_content = e.stderr.strip() if hasattr(e, 'stderr') and e.stderr else ""
        if not suppress_stderr and stderr_content:
             print_error(f"Stderr:\n{stderr_content}")
        elif capture_output and hasattr(e, 'stdout') and e.stdout and (suppress_stderr or not stderr_content):
            stdout_content = e.stdout.strip()
            if stdout_content:
                print_error(f"Stdout:\n{stdout_content}")
        sys.exit(1)
    except Exception as e:
        msg = error_msg or f"Unexpected error executing '{' '.join(cmd_list)}'"
        print_error(f"{msg}: {e}")
        sys.exit(1)

def run_pipeline(commands, step_names=None):
    processes = []
    num_commands = len(commands)
    if step_names is None:
        step_names = [f"Step {i+1}" for i in range(num_commands)]

    process_info = []

    try:
        last_process_stdout = None

        for i, cmd in enumerate(commands):
            stdin_source = last_process_stdout
            stdout_dest = subprocess.PIPE if i < num_commands - 1 else None

            is_pv_command = (cmd[0] == 'pv')
            stderr_dest = subprocess.PIPE if not is_pv_command else None
            capture_stderr_flag = (stderr_dest == subprocess.PIPE)

            proc = subprocess.Popen(
                cmd,
                stdin=stdin_source,
                stdout=stdout_dest,
                stderr=stderr_dest,
                text=True,
                errors='ignore',
                bufsize=1
            )
            processes.append(proc)
            process_info.append({'proc': proc, 'capture_stderr': capture_stderr_flag, 'command': cmd})

            if stdin_source:
                stdin_source.close()

            last_process_stdout = proc.stdout if stdout_dest == subprocess.PIPE else None

        return_codes = []
        stderr_outputs = []
        success = True
        for idx, info in enumerate(process_info):
            proc = info['proc']
            capture_stderr_flag = info['capture_stderr']
            cmd = info['command']

            try:
                stdout_data, stderr_data = proc.communicate(timeout=3600)
            except subprocess.TimeoutExpired:
                print_error(f"Pipeline timed out at {step_names[idx]}: '{' '.join(cmd)}'")
                proc.kill()
                stdout_data, stderr_data = proc.communicate()
                success = False
                stderr_content = stderr_data.strip() if capture_stderr_flag and stderr_data else ""
                stderr_outputs.append(stderr_content)
                break
            except Exception as comm_err:
                print_error(f"Error during communicate() for {step_names[idx]} ('{' '.join(cmd)}'): {comm_err}")
                stderr_content = ""
                stderr_outputs.append(stderr_content)
                success = False
                rc = proc.returncode if proc.returncode is not None else 1

            if success:
                rc = proc.returncode
                return_codes.append(rc)

                stderr_content = stderr_data.strip() if capture_stderr_flag and stderr_data else ""
                stderr_outputs.append(stderr_content)

                if rc != 0:
                    success = False
                    print_error(f"Pipeline failed at {step_names[idx]}: '{' '.join(cmd)}' (rc={rc})")
                    if stderr_content:
                        print_error(f"Stderr:\n{stderr_content}")
            else:
                return_codes.append(rc)
                if len(stderr_outputs) == idx:
                    stderr_outputs.append("")

        for proc in processes:
            proc.poll()

        if success and (len(return_codes) != num_commands or any(rc != 0 for rc in return_codes)):
             print_warning("Pipeline completed but might have had unreported issues. Marking as failed.")
             success = False

        return success

    except FileNotFoundError as e:
        print_error(f"Error in pipeline: Command '{e.filename}' not found.")
        for info in process_info:
            try: info['proc'].terminate()
            except ProcessLookupError: pass
        return False
    except Exception as e:
        print_error(f"Unexpected error during pipeline setup or execution: {e}")
        for info in process_info:
            try: info['proc'].terminate()
            except ProcessLookupError: pass
        return False

def format_bytes(b):
    if b is None: return "N/A"
    try:
        b = float(b)
        if b < 1024:
            return f"{int(b)} B"
        elif b < 1024**2:
            return f"{b / 1024:.1f} KB"
        elif b < 1024**3:
            return f"{b / (1024**2):.2f} MB"
        else:
            return f"{b / (1024**3):.2f} GB"
    except (ValueError, TypeError):
        return "N/A"

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
        match = re.match(r'^(\d+(\.\d+)?)', size_str)
        if match:
            print_warning(f"Unknown/missing unit in '{size_str}', interpreting as MB.")
            return int(float(match.group(1)))
        print_warning(f"Could not parse size '{size_str}', returning 0 MB.")
        return 0

def get_instance_details(conf_path):
    instance_id = Path(conf_path).stem
    name = "<no name/hostname>"
    is_lxc = 'lxc' in conf_path.parts

    try:
        with open(conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('name:'):
                    name = line.split(':', 1)[1].strip()
                    if 'qemu-server' in conf_path.parts: break
                elif is_lxc and line.startswith('hostname:'):
                     if name == "<no name/hostname>": name = line.split(':', 1)[1].strip()
                     break
    except Exception as e:
        print_warning(f"Could not fully read configuration file {conf_path}: {e}")
    return instance_id, name

def list_instances():
    print_info("Available VMs and LXC containers:")
    vms = []
    lxcs = []
    vm_conf_files = sorted(glob.glob("/etc/pve/qemu-server/*.conf"))
    lxc_conf_files = sorted(glob.glob("/etc/pve/lxc/*.conf"))

    print(f" {color_text('VMs:', 'YELLOW')}")
    if vm_conf_files:
        for conf in vm_conf_files:
            vm_id, vm_name = get_instance_details(Path(conf))
            vms.append({'id': vm_id, 'name': vm_name})
            print(f"   {color_text(vm_id, 'BLUE')} : {vm_name}")
    else:
        print(f"   {color_text('No VMs found.', 'YELLOW')}")

    print(f"\n {color_text('LXC Containers:', 'YELLOW')}")
    if lxc_conf_files:
        for conf in lxc_conf_files:
            lxc_id, lxc_name = get_instance_details(Path(conf))
            lxcs.append({'id': lxc_id, 'name': lxc_name})
            print(f"   {color_text(lxc_id, 'BLUE')} : {lxc_name}")
    else:
        print(f"   {color_text('No LXC containers found.', 'YELLOW')}")

    if not vms and not lxcs:
        print(f"\n {color_text('No VMs or LXC containers found on this system.', 'RED')}")
        return False
    return True

def list_snapshots(dataset):
    cmd = ['zfs', 'list', '-t', 'snapshot', '-o', 'name,creation', '-s', 'creation', '-H', '-p', dataset]
    output = run_command(cmd, check=False, capture_output=True, suppress_stderr=True, error_msg=f"Failed to list snapshots for {dataset}")
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
    cmd = ['zfs', 'get', '-H', '-p', '-o', 'value', property_name, target]
    return run_command(cmd, check=False, capture_output=True, suppress_stderr=True).strip()

def get_snapshot_size_estimate(snapshot_name):
    cmd = ['zfs', 'send', '-nP', snapshot_name]
    output = run_command(cmd, check=False, capture_output=True, suppress_stderr=True)
    if output:
        match = re.search(r'^size\s+(\d+)$', output, re.MULTILINE)
        if match:
            return int(match.group(1))
    print_warning(f"Could not estimate size for snapshot {snapshot_name}.")
    return None

def adjust_config_file(conf_path, instance_type):
    print_info(f"\nApplying standard adjustments to {color_text(str(conf_path), 'BLUE')}")
    try:
        with open(conf_path, 'r') as f_orig:
            lines = f_orig.readlines()

        modified_lines = []
        changes_made = False
        name_prefix = "clone-"

        for line in lines:
            original_line = line
            line_strip = line.strip()
            modified = False

            if re.match(r'^\s*onboot:\s*[01]', line_strip):
                if line_strip != "onboot: 0":
                    new_line_content = "onboot: 0"
                    line = new_line_content + "\n"
                    print(f"   Setting '{color_text('onboot: 0', 'YELLOW')}'")
                    modified = True

            elif line_strip.startswith('name:') and not Path(line_strip.split(':', 1)[1].strip()).name.startswith(name_prefix):
                 new_line_content = re.sub(r'(^\s*name:\s*)(.*)', rf'\1{name_prefix}\2', line.strip())
                 line = new_line_content + "\n"
                 print(f"   Adding '{color_text(name_prefix, 'YELLOW')}' prefix to name")
                 modified = True

            elif line_strip.startswith('hostname:') and not Path(line_strip.split(':', 1)[1].strip()).name.startswith(name_prefix):
                 new_line_content = re.sub(r'(^\s*hostname:\s*)(.*)', rf'\1{name_prefix}\2', line.strip())
                 line = new_line_content + "\n"
                 print(f"   Adding '{color_text(name_prefix, 'YELLOW')}' prefix to hostname")
                 modified = True

            elif re.match(r'^\s*net\d+:', line_strip):
                current_line_state = line
                if 'link_down=1' not in line_strip:
                    parts = line_strip.split('#', 1)
                    main_part = parts[0].rstrip()
                    comment_part = f" #{parts[1]}" if len(parts) > 1 else ""
                    if main_part.endswith(','): main_part += "link_down=1"
                    else: main_part += ",link_down=1"
                    current_line_state = main_part + comment_part + "\n"
                    print(f"   Adding '{color_text('link_down=1', 'YELLOW')}' to network interface: {original_line.strip()}")
                    modified = True
                if modified: line = current_line_state

            modified_lines.append(line)
            if modified: changes_made = True

        if changes_made:
            with open(conf_path, 'w') as f_new:
                f_new.writelines(modified_lines)
            print_success("   Configuration adjustments applied.")
        else:
             print_info("   No standard adjustments needed or applicable.")

    except FileNotFoundError:
         print_error(f"\nError: Config file {conf_path} not found. Cannot apply adjustments.")
    except Exception as e:
        print_error(f"\nError adjusting config file {conf_path}: {e}")
        print_warning(f"Config file {conf_path} may not have been properly adjusted.")

def main():
    print_info("=== Proxmox VM/LXC Clone Script (ZFS Linked/Full from Snapshot) ===")
    print()

    pv_available = is_tool('pv')
    if not pv_available:
        print_warning("Tool 'pv' (Pipe Viewer) not found in PATH.")
        print_warning("Full clones will be created without a progress bar.")
    else:
        print_info("Tool 'pv' found, will be used for full clone progress.")

    if not list_instances():
        print_error("Exiting script as no instances were found.")
        sys.exit(1)
    print()

    while True:
        src_id = input("Enter the source VM or LXC ID: ").strip()
        if src_id.isdigit(): break
        else: print_error("Invalid ID format.")

    vm_conf = Path(f"/etc/pve/qemu-server/{src_id}.conf")
    lxc_conf = Path(f"/etc/pve/lxc/{src_id}.conf")

    if vm_conf.is_file():
        clone_type_src = "vm"; pve_cmd = "qm"; conf_dir = vm_conf.parent; dataset_prefix = "vm-"
        config_type = "VM"; src_conf_path = vm_conf
    elif lxc_conf.is_file():
        clone_type_src = "lxc"; pve_cmd = "pct"; conf_dir = lxc_conf.parent; dataset_prefix = "subvol-"
        config_type = "LXC Container"; src_conf_path = lxc_conf
    else:
        print_error(f"Error: No VM or LXC with ID {src_id} found."); sys.exit(1)

    src_instance_id, src_instance_name = get_instance_details(src_conf_path)
    print_success(f"Selected source: ID {src_instance_id} ({config_type}: {src_instance_name})")

    default_new_id = f"9{src_id}"
    while True:
        new_id = input(f"Enter the new {config_type} ID (blank for default={default_new_id}): ").strip()
        if not new_id: new_id = default_new_id; print_warning(f"Using default ID: {new_id}")
        if new_id.isdigit():
            new_conf_path_vm = Path("/etc/pve/qemu-server") / f"{new_id}.conf"
            new_conf_path_lxc = Path("/etc/pve/lxc") / f"{new_id}.conf"
            collision = False
            if new_conf_path_vm.exists(): print_error(f"Config file for VM ID {new_id} exists!"); collision = True
            if new_conf_path_lxc.exists(): print_error(f"Config file for LXC ID {new_id} exists!"); collision = True
            potential_new_ds_name = f"{dataset_prefix}{new_id}-disk-0"
            if get_zfs_property(f"{DEFAULT_ZFS_POOL_PATH}/{potential_new_ds_name}", 'type'):
                 print_error(f"Potential ZFS dataset for ID {new_id} seems to exist."); collision = True
            if not collision: break
        else: print_error("Invalid new ID format.")
    new_conf_path = conf_dir / f"{new_id}.conf"

    while True:
        clone_mode_input = input(f"Choose clone mode: [{color_text('linked', 'GREEN')}/full] (from snapshot, default: linked): ").strip().lower()
        if not clone_mode_input or clone_mode_input == 'linked': clone_mode = 'linked'; break
        elif clone_mode_input == 'full': clone_mode = 'full'; break
        else: print_error("Invalid choice.")
    print_info(f"Selected mode: {clone_mode.capitalize()} Clone")

    if clone_type_src == "vm":
        print_info("\nChecking host RAM usage...")
        try:
            free_output = run_command(['free', '-m'], capture_output=True)
            mem_line = free_output.split('\n')[1]; total_ram_mb = int(mem_line.split()[1])
            qm_config_output = run_command([pve_cmd, 'config', src_id], capture_output=True, suppress_stderr=True)
            src_vm_ram_mb = 512; match = re.search(r'^memory:\s*(\S+)', qm_config_output, re.MULTILINE)
            if match: parsed_ram = parse_size_to_mb(match.group(1)); src_vm_ram_mb = parsed_ram if parsed_ram > 0 else 512
            else: print_warning(f"No 'memory' setting for VM {src_id}, assuming {src_vm_ram_mb} MB.")

            qm_list_output = run_command([pve_cmd, 'list', '--full'], capture_output=True, suppress_stderr=True)
            sum_running_ram_mb = 0; running_vm_lines = [line for line in qm_list_output.split('\n')[1:] if 'running' in line.split()]
            header_line = qm_list_output.split('\n')[0]; headers = header_line.split()
            try: mem_index = headers.index('maxmem')
            except ValueError: mem_index = -1

            for line in running_vm_lines:
                parts = line.split(); vmid = parts[0]; ram_mb = 512
                if vmid.isdigit():
                    if mem_index != -1 and len(parts) > mem_index:
                        try: ram_bytes = int(parts[mem_index]); ram_mb = ram_bytes // (1024*1024) if ram_bytes > 0 else 512
                        except ValueError: pass
                    else:
                        vm_config = run_command([pve_cmd, 'config', vmid], capture_output=True, suppress_stderr=True)
                        match_ram = re.search(r'^memory:\s*(\S+)', vm_config, re.MULTILINE)
                        if match_ram: parsed = parse_size_to_mb(match_ram.group(1)); ram_mb = parsed if parsed > 0 else 512
                    sum_running_ram_mb += ram_mb

            threshold_mb = math.floor(total_ram_mb * RAM_THRESHOLD_PERCENT / 100)
            prognostic_ram_mb = sum_running_ram_mb + src_vm_ram_mb
            print(f"   Total host RAM:      {color_text(format_bytes(total_ram_mb*1024*1024), 'BLUE')}")
            print(f"   RAM running VMs:   {color_text(format_bytes(sum_running_ram_mb*1024*1024), 'BLUE')}")
            print(f"   Source VM RAM:       {color_text(format_bytes(src_vm_ram_mb*1024*1024), 'BLUE')}")
            print(f"   Projected RAM:     {color_text(format_bytes(prognostic_ram_mb*1024*1024), 'BLUE')} (if clone starts)")
            print(f"   {RAM_THRESHOLD_PERCENT}% Threshold:      {color_text(format_bytes(threshold_mb*1024*1024), 'BLUE')}")
            if prognostic_ram_mb > threshold_mb:
                print_warning(f"\nWARNING: Starting clone might exceed {RAM_THRESHOLD_PERCENT}% RAM usage!")
                confirm = input(f"{color_text('Continue cloning? (y/N) ', 'RED')}{COLORS['NC']}").strip().lower()
                if confirm not in ['y', 'yes']: print_error("Cloning aborted."); sys.exit(1)
            else: print_success("RAM check passed.")
        except Exception as e: print_warning(f"\nCould not complete RAM check: {e}. Proceeding cautiously.")
    else: print_info("\nSkipping RAM check for LXC containers.")

    print_info(f"\nSearching for ZFS datasets in {src_conf_path} linked to 'local-zfs'...")
    storage_datasets = {}
    storage_regex_vm = re.compile(r'^(scsi|ide|sata|virtio|efidisk|tpmstate)\d+:\s*[^#]*local-zfs:([^,]+)')
    storage_regex_lxc = re.compile(r'^(rootfs|mp\d+):\s*[^#]*local-zfs:([^,]+)')
    try:
        with open(src_conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                match = None; key = ""; dataset_name_part = ""
                if clone_type_src == "vm":
                    match = storage_regex_vm.match(line)
                    if match: key = match.group(1) + line[len(match.group(1)):line.find(':')]; dataset_name_part = match.group(2).strip()
                else:
                    match = storage_regex_lxc.match(line)
                    if match: key = match.group(1); dataset_name_part = match.group(2).split(',')[0].strip()

                if key and dataset_name_part:
                    if '/' in dataset_name_part: full_dataset_path = dataset_name_part
                    else: full_dataset_path = f"{DEFAULT_ZFS_POOL_PATH}/{dataset_name_part}"

                    if run_command(['zfs', 'list', '-H', full_dataset_path], check=False, capture_output=True, suppress_stderr=True):
                        storage_datasets[key] = full_dataset_path
                        print(f"   Found: {color_text(key, 'BLUE')} -> {full_dataset_path}")
                    else: print_warning(f"   Dataset for {color_text(key, 'BLUE')} ('{full_dataset_path}') not found. Skipping.")
    except Exception as e: print_error(f"Error reading {src_conf_path}: {e}"); sys.exit(1)

    if not storage_datasets: print_error(f"No ZFS datasets found for 'local-zfs' in {src_conf_path}."); sys.exit(1)

    ref_key = ""; ref_dataset = ""; min_disk_num = 99999; efi_key = ""; efi_dataset = ""
    if clone_type_src == 'lxc' and 'rootfs' in storage_datasets: ref_key = 'rootfs'; ref_dataset = storage_datasets[ref_key]
    else:
        disk_num_regex = re.compile(r'.*[/-](disk|subvol)-(\d+)$')
        for key, dataset in storage_datasets.items():
            match = disk_num_regex.search(Path(dataset).name)
            if match:
                disk_num = int(match.group(2))
                if disk_num < min_disk_num: min_disk_num = disk_num; ref_key = key; ref_dataset = dataset
            elif clone_type_src == 'vm' and key.startswith('efidisk') and not efi_key: efi_key = key; efi_dataset = dataset
            elif len(storage_datasets) == 1: ref_key = key; ref_dataset = dataset; break
        if not ref_key and efi_key: ref_key = efi_key; ref_dataset = efi_dataset; print_warning(f"\nUsing EFI disk as reference: {ref_key} ({ref_dataset})")
        elif not ref_key and storage_datasets: first_key = sorted(storage_datasets.keys())[0]; ref_key = first_key; ref_dataset = storage_datasets[ref_key]; print_warning(f"\nUsing first dataset as reference: {ref_key} ({ref_dataset})")
        elif not ref_key: print_error("\nCannot determine reference dataset."); sys.exit(1)

    print_info(f"\nUsing reference storage for snapshot listing: {color_text(ref_key, 'BLUE')} ({color_text(ref_dataset,'CYAN')})")
    snapshots = list_snapshots(ref_dataset)
    if not snapshots: print_error(f"No snapshots found for {ref_dataset}."); sys.exit(1)

    print("Available snapshots:")
    for i, snap in enumerate(snapshots):
        snap_suffix = snap['name'].split('@', 1)[1]
        creation_dt = datetime.fromtimestamp(snap['creation_timestamp']) if snap['creation_timestamp'] else None
        human_time = creation_dt.strftime('%Y-%m-%d %H:%M:%S') if creation_dt else "Unknown"
        print(f"   {color_text(f'[{i}]', 'BLUE')} {snap_suffix} {color_text(f'({human_time})', 'YELLOW')}")

    while True:
        try:
            idx = int(input("Enter the index of the snapshot to clone: ").strip())
            if 0 <= idx < len(snapshots):
                selected_snapshot_info = snapshots[idx]
                snap_suffix = selected_snapshot_info['name'].split('@', 1)[1]
                print_success(f"Selected snapshot suffix: {snap_suffix}"); break
            else: print_error("Index out of range.")
        except ValueError: print_error("Invalid input.")

    print_info(f"\n--- Starting ZFS {clone_mode.capitalize()} Clone Operations ---")
    cloned_keys = []
    all_ops_successful = True
    dataset_name_pattern = re.compile(rf"^(.*\/)({dataset_prefix})(\d+)(-.*)$")

    for key, dataset in storage_datasets.items():
        target_snapshot = f"{dataset}@{snap_suffix}"
        print(f"\n {color_text(f'Processing {key}:', 'CYAN')}")
        print(f"    Source dataset:  {color_text(dataset, 'BLUE')}")
        print(f"    Source snapshot: {color_text(target_snapshot, 'BLUE')}")

        if not get_zfs_property(target_snapshot, 'type'):
             print_warning(f"    [WARN] Snapshot '{target_snapshot}' does not exist for this dataset. Skipping.")
             continue

        match = dataset_name_pattern.match(dataset)
        if match:
            pool_base = match.group(1); suffix = match.group(4)
            new_dataset = f"{pool_base}{dataset_prefix}{new_id}{suffix}"
        else:
            old_base = Path(dataset).name; new_base = old_base.replace(f"{src_id}", f"{new_id}", 1)
            if new_base != old_base: new_dataset = str(Path(dataset).parent / new_base); print_warning("    Using fallback name generation.")
            else: print_error(f"    [ERROR] Cannot parse dataset name '{dataset}'. Skipping."); all_ops_successful = False; continue

        print(f"    Target dataset:  {color_text(new_dataset, 'GREEN')}")

        if get_zfs_property(new_dataset, 'type'):
            print_warning(f"    Target dataset '{new_dataset}' already exists. Skipping operation.")
            cloned_keys.append(key)
            continue

        op_success = False
        if clone_mode == 'linked':
            clone_cmd = ['zfs', 'clone', target_snapshot, new_dataset]
            print(f"    Executing linked clone: {' '.join(clone_cmd)}")
            try:
                run_command(clone_cmd, check=True, capture_output=False, error_msg="ZFS clone failed")
                print_success("    Linked clone successful.")
                op_success = True
            except SystemExit:
                print_error("    Error during 'zfs clone'.")
                all_ops_successful = False
        else:
            print(f"    Preparing full clone (send/receive)...")
            estimated_size_bytes = get_snapshot_size_estimate(target_snapshot)
            size_str = f"~{format_bytes(estimated_size_bytes)}" if estimated_size_bytes else "Unknown size"
            print(f"    Estimated size: {size_str}")

            send_cmd = ['zfs', 'send', target_snapshot]
            recv_cmd = ['zfs', 'receive', '-o', 'readonly=off', new_dataset]
            pipeline_cmds = []
            pipeline_names = []

            pipeline_cmds.append(send_cmd)
            pipeline_names.append("zfs send")

            if pv_available:
                pv_cmd = ['pv']
                if estimated_size_bytes:
                    pv_cmd.extend(['-s', str(estimated_size_bytes)])
                pv_cmd.extend(['-p', '-t', '-e', '-r', '-b', '-N', f'{Path(new_dataset).name}'])
                pipeline_cmds.append(pv_cmd)
                pipeline_names.append("pv")
            else:
                 print_warning("    Executing full clone without progress bar ('pv' not found).")

            pipeline_cmds.append(recv_cmd)
            pipeline_names.append("zfs receive")

            print(f"    Executing pipeline: {' | '.join([' '.join(c) for c in pipeline_cmds])}")
            if run_pipeline(pipeline_cmds, pipeline_names):
                 print_success("    Full clone (send/receive) successful.")
                 op_success = True
            else:
                 print_error("    Error during 'zfs send/receive' pipeline.")
                 all_ops_successful = False
                 if get_zfs_property(new_dataset, 'type'):
                     print_warning(f"    Attempting to destroy potentially incomplete target dataset: {new_dataset}")
                     run_command(['zfs', 'destroy', new_dataset], check=False, suppress_stderr=True)

        if op_success:
            cloned_keys.append(key)
        else:
             pass

    if not all_ops_successful:
         print_error(f"\nOne or more ZFS {clone_mode} clone operations failed. Please review errors above.")
         if not cloned_keys:
             print_error("No datasets were successfully cloned/created. Aborting.")
             sys.exit(1)
         else:
              print_warning("Attempting to create configuration for successfully cloned/created datasets...")

    if not cloned_keys:
        print_error("\nNo datasets were successfully processed or created. Cannot create config.")
        sys.exit(1)

    print_info(f"\nCreating new {config_type} configuration: {color_text(str(new_conf_path), 'BLUE')}")
    new_config_lines = []
    try:
        with open(src_conf_path, 'r') as f_src: config_content = f_src.readlines()
        for line in config_content:
            line_strip = line.strip()
            if not line_strip or line_strip.startswith('#') or re.match(r'^\[.*\]$', line_strip) or line_strip.startswith(('parent:', 'snapdir:')):
                new_config_lines.append(line); continue

            current_key = ""; is_storage_line = False; original_dataset_basename = ""
            match_vm = storage_regex_vm.match(line_strip)
            match_lxc = storage_regex_lxc.match(line_strip)

            if clone_type_src == "vm" and match_vm:
                key_part = match_vm.group(1) + line_strip[len(match_vm.group(1)):line_strip.find(':')]
                if key_part in storage_datasets: current_key = key_part; is_storage_line = True; original_dataset_basename = Path(storage_datasets[current_key]).name
            elif clone_type_src == "lxc" and match_lxc:
                key_part = match_lxc.group(1)
                if key_part in storage_datasets: current_key = key_part; is_storage_line = True; original_dataset_basename = Path(storage_datasets[current_key]).name

            if is_storage_line and current_key in cloned_keys:
                match_base = dataset_name_pattern.match(storage_datasets[current_key])
                if match_base:
                    pool_base = match_base.group(1); suffix = match_base.group(4)
                    new_dataset_basename = f"{dataset_prefix}{new_id}{suffix}"
                    old_part = f"local-zfs:{original_dataset_basename}"
                    new_part = f"local-zfs:{new_dataset_basename}"
                    newline = line.replace(old_part, new_part, 1)
                    if newline == line: print_warning(f"   [WARN] Failed replace in line: {line.strip()}"); new_config_lines.append(line)
                    else: print(f"   Updated storage line for {color_text(current_key, 'BLUE')}"); new_config_lines.append(newline)
                else: print_warning(f"   [WARN] Cannot parse base name for {current_key}. Keeping original."); new_config_lines.append(line)
            else:
                new_config_lines.append(line)

        with open(new_conf_path, 'w') as f_new: f_new.writelines(new_config_lines)
        print_success(f"Configuration file {new_conf_path} created.")
    except Exception as e: print_error(f"Error creating config {new_conf_path}: {e}"); sys.exit(1)

    adjust_config_file(new_conf_path, clone_type_src)

    print(f"\n{color_text('--- Clone Process Finished ---', 'GREEN')}")
    final_message = f"New {config_type} with ID {color_text(new_id, 'BLUE')} created as a {color_text(clone_mode + ' clone', 'YELLOW')} "
    final_message += f"from snapshot '{color_text(snap_suffix, 'CYAN')}' of {src_id}."
    print(final_message)
    if not all_ops_successful: print_warning("Note: Some ZFS operations may have failed. Review logs carefully.")
    print(f"{color_text('Review the configuration:', 'YELLOW')} {color_text(str(new_conf_path), 'BLUE')}")
    print(color_text("Important checks: Network settings (IP/MAC), Hostname/Name, Resources, CD-ROMs (VMs), link_down=1 on NICs.", 'YELLOW'))

    sys.exit(0)

if __name__ == "__main__":
    if os.geteuid() != 0:
        print_warning("Warning: Root privileges (sudo) likely required for ZFS/Proxmox commands.")

    try:
        main()
    except KeyboardInterrupt:
        print_error("\nOperation cancelled by user (Ctrl+C).")
        sys.exit(1)
    except Exception as e:
        print_error(f"\nAn unexpected critical error occurred: {e}")
        sys.exit(1)