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
import argparse
import json

# --- Default Configuration ---
DEFAULT_ZFS_POOL_PATH = "rpool/data"
DEFAULT_PVE_STORAGE = "local-zfs"
RAM_THRESHOLD_PERCENT = 90
DEFAULT_EXPORT_META_SUFFIX = ".meta.json"
DEFAULT_EXPORT_DATA_SUFFIX = ".zfs.stream"
DEFAULT_EXPORT_CONFIG_SUFFIX = ".conf"
# Suffixes for compressed streams
DEFAULT_EXPORT_DATA_SUFFIX_GZIP = ".zfs.stream.gz"
DEFAULT_EXPORT_DATA_SUFFIX_ZSTD = ".zfs.stream.zst"
DEFAULT_EXPORT_DATA_SUFFIX_PIGZ = ".zfs.stream.gz" # Same as gzip

# --- Compression Tools ---
# Define command names for easier checking and execution
COMPRESSION_TOOLS = {
    "gzip": {"compress": ["gzip", "-c"], "decompress": ["gunzip", "-c"], "suffix": DEFAULT_EXPORT_DATA_SUFFIX_GZIP},
    "pigz": {"compress": ["pigz", "-c"], "decompress": ["unpigz", "-c"], "suffix": DEFAULT_EXPORT_DATA_SUFFIX_PIGZ},
    "zstd": {"compress": ["zstd", "-T0", "-c"], "decompress": ["unzstd", "-c"], "suffix": DEFAULT_EXPORT_DATA_SUFFIX_ZSTD},
    "none": {"compress": None, "decompress": None, "suffix": DEFAULT_EXPORT_DATA_SUFFIX}
}


# --- Colors ---
COLORS = {
    'RED': '\033[91m',
    'GREEN': '\033[92m',
    'YELLOW': '\033[93m',
    'CYAN': '\033[96m',
    'BLUE': '\033[94m',
    'NC': '\033[0m'  # No Color
}

# --- Helper Functions ---

def color_text(text, color_name):
    """Färbt den Text für die Konsolenausgabe."""
    color = COLORS.get(color_name.upper(), COLORS['NC'])
    nc = COLORS['NC']
    return f"{color}{text}{nc}"

def print_info(text):
    """Gibt eine Informationsmeldung aus."""
    print(color_text(text, "CYAN"))

def print_success(text):
    """Gibt eine Erfolgsmeldung aus."""
    print(color_text(text, "GREEN"))

def print_warning(text):
    """Gibt eine Warnmeldung aus."""
    print(color_text(text, "YELLOW"))

def print_error(text, exit_code=None):
    """Gibt eine Fehlermeldung aus und beendet das Skript optional."""
    print(color_text(text, "RED"), file=sys.stderr)
    if exit_code is not None:
        sys.exit(exit_code)

def is_tool(name):
    """Prüft, ob ein Kommandozeilen-Tool im PATH verfügbar ist."""
    return shutil.which(name) is not None

def check_compression_tools(method):
    """Checks if the required compression/decompression tools for a method are available."""
    tool_info = COMPRESSION_TOOLS.get(method) # Get tool_info first

    if not tool_info: # Handle invalid method early
        print_error(f"Internal error: Unknown compression method '{method}' defined.")
        return False, False, None

    if method == "none":
        return True, True, tool_info

    compress_cmd_name = tool_info["compress"][0] if tool_info.get("compress") else None
    decompress_cmd_name = tool_info["decompress"][0] if tool_info.get("decompress") else None

    compress_ok = is_tool(compress_cmd_name) if compress_cmd_name else False
    decompress_ok = is_tool(decompress_cmd_name) if decompress_cmd_name else False

    if not compress_ok and compress_cmd_name:
        print_warning(f"Compression tool '{compress_cmd_name}' for method '{method}' not found.")
    if not decompress_ok and decompress_cmd_name:
        print_warning(f"Decompression tool '{decompress_cmd_name}' for method '{method}' not found.")

    return compress_ok, decompress_ok, tool_info

def run_command(cmd_list, check=True, capture_output=True, text=True, error_msg=None, suppress_stderr=False, input_data=None, allow_fail=False):
    """
    Führt einen Shell-Befehl aus und gibt die Ausgabe zurück oder prüft auf Erfolg.
    """
    stdin_setting = subprocess.PIPE if input_data is not None else None

    if capture_output:
        stdout_setting = subprocess.PIPE
        stderr_setting = subprocess.DEVNULL if suppress_stderr else subprocess.PIPE
    else:
        stdout_setting = None
        stderr_setting = subprocess.DEVNULL if suppress_stderr else None

    try:
        process = subprocess.run(
            cmd_list,
            check=check and not allow_fail,
            text=text,
            stdout=stdout_setting,
            stderr=stderr_setting,
            input=input_data,
            stdin=stdin_setting,
            errors='replace'
        )
        stdout_res = process.stdout.strip() if stdout_setting == subprocess.PIPE and process.stdout else ""
        stderr_res = process.stderr.strip() if stderr_setting == subprocess.PIPE and process.stderr else ""

        if allow_fail:
            return (process.returncode == 0, stdout_res, stderr_res)
        else:
            if process.returncode != 0:
                 pass
            return stdout_res if capture_output else None

    except FileNotFoundError:
        msg = error_msg or f"Error: Command '{cmd_list[0]}' not found."
        print_error(msg, exit_code=1)
    except subprocess.CalledProcessError as e:
        if allow_fail:
            stdout = e.stdout.strip() if hasattr(e, 'stdout') and e.stdout else ""
            stderr = e.stderr.strip() if hasattr(e, 'stderr') and e.stderr else ""
            return (False, stdout, stderr)
        else:
            msg = error_msg or f"Error executing '{' '.join(cmd_list)}'"
            print_error(f"{msg}\nReturn Code: {e.returncode}")
            stderr_content = e.stderr.strip() if hasattr(e, 'stderr') and e.stderr else ""
            if not suppress_stderr and stderr_content:
                 print_error(f"Stderr:\n{stderr_content}")
            elif hasattr(e, 'stdout') and e.stdout and (suppress_stderr or not stderr_content):
                stdout_content = e.stdout.strip()
                if stdout_content:
                    print_error(f"Stdout (relevant for error):\n{stdout_content}")
            sys.exit(1)
    except Exception as e:
        msg = error_msg or f"Unexpected error executing '{' '.join(cmd_list)}'"
        print_error(f"{msg}: {e}", exit_code=1)


def run_pipeline(commands, step_names=None, pv_options=None, output_file=None):
    """
    Führt eine Befehlspipeline aus (z.B. cmd1 | pv | compressor | cmd2 > file).
    """
    processes = []
    num_commands = len(commands)
    if step_names is None:
        step_names = [f"Step {i+1}" for i in range(num_commands)]
    elif len(step_names) != num_commands:
        print_warning("Length of step_names does not match number of commands in pipeline.")
        step_names = [f"Step {i+1}" for i in range(num_commands)] # Fallback

    process_info = []
    final_output_handle = None

    try:
        last_process_stdout = None

        if output_file:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            final_output_handle = open(output_file, 'wb')

        for i, cmd in enumerate(commands):
            stdin_source = last_process_stdout
            is_last_command = (i == num_commands - 1)
            stdout_dest = final_output_handle if is_last_command and final_output_handle else subprocess.PIPE

            is_pv_command = (cmd[0] == 'pv')
            stderr_dest = None if is_pv_command or cmd[0] in ['gzip', 'gunzip', 'pigz', 'unpigz', 'zstd', 'unzstd'] else subprocess.PIPE

            current_cmd = cmd[:]
            if is_pv_command and pv_options:
                current_cmd.extend(pv_options)

            proc = subprocess.Popen(
                current_cmd,
                stdin=stdin_source,
                stdout=stdout_dest,
                stderr=stderr_dest,
                bufsize=8192
            )
            processes.append(proc)
            process_info.append({'proc': proc, 'command': current_cmd})

            if stdin_source:
                try:
                   stdin_source.close()
                except BrokenPipeError:
                    print_warning(f"  Broken pipe closing stdin for {' '.join(current_cmd)}. Previous process likely exited.")
                except Exception as pipe_err:
                    print_warning(f"  Error closing stdin pipe for {' '.join(current_cmd)}: {pipe_err}")

            if not (is_last_command and final_output_handle):
                 last_process_stdout = proc.stdout

        return_codes = []
        stderr_outputs = []
        success = True
        timed_out = False

        for idx, info in enumerate(process_info):
            proc = info['proc']
            cmd_str = ' '.join(info['command'])
            capture_stderr = proc.stderr == subprocess.PIPE # Check if stderr was piped

            try:
                stdout_data, stderr_data = proc.communicate(timeout=7200) # 2 hours
                rc = proc.returncode
                return_codes.append(rc)

                stderr_content = ""
                if capture_stderr and stderr_data:
                    try:
                        stderr_content = stderr_data.decode('utf-8', errors='replace').strip()
                    except Exception:
                        stderr_content = "Could not decode stderr"
                stderr_outputs.append(stderr_content)

                if rc != 0:
                    if rc == -13: # SIGPIPE
                         print_warning(f"Pipeline step {step_names[idx]} ('{cmd_str}') exited with SIGPIPE (rc={rc}). Often okay if a later step failed.")
                    else:
                        success = False
                        print_error(f"Pipeline failed at {step_names[idx]} '{cmd_str}' (rc={rc})")
                        if stderr_content:
                            print_error(f"Stderr:\n{stderr_content}")
            except subprocess.TimeoutExpired:
                print_error(f"Pipeline timed out at {step_names[idx]} '{cmd_str}'")
                proc.kill()
                try:
                    stdout_data, stderr_data = proc.communicate(timeout=10)
                except Exception: pass
                success = False
                timed_out = True
                return_codes.append(proc.returncode if proc.returncode is not None else -1)
                stderr_content = "Timeout"
                if capture_stderr and stderr_data:
                    try: stderr_content = stderr_data.decode('utf-8', errors='replace').strip()
                    except Exception: pass
                stderr_outputs.append(stderr_content)
                break
            except Exception as comm_err:
                print_error(f"Error during communicate() for {step_names[idx]} ('{cmd_str}'): {comm_err}")
                success = False
                rc = proc.returncode if proc.returncode is not None else -99 # Arbitrary error code
                return_codes.append(rc)
                stderr_outputs.append(f"Communication Error: {comm_err}")


        while len(return_codes) < num_commands: return_codes.append(None)
        while len(stderr_outputs) < num_commands: stderr_outputs.append("Not executed or error")

        for p_info in process_info:
             try:
                 if p_info['proc'].poll() is None:
                    p_info['proc'].terminate()
                    try: p_info['proc'].wait(timeout=5)
                    except subprocess.TimeoutExpired:
                         print_warning(f"Process {' '.join(p_info['command'])} did not terminate gracefully, killing.")
                         p_info['proc'].kill()
                         p_info['proc'].wait(timeout=5)
             except ProcessLookupError: pass
             except Exception as kill_err:
                  print_warning(f"Error terminating/killing process {' '.join(p_info['command'])}: {kill_err}")
             finally:
                 if p_info['proc'].stdin:
                     try: p_info['proc'].stdin.close()
                     except Exception: pass
                 if p_info['proc'].stdout:
                     try: p_info['proc'].stdout.close()
                     except Exception: pass
                 if p_info['proc'].stderr:
                     try: p_info['proc'].stderr.close()
                     except Exception: pass

        if final_output_handle:
            try:
                final_output_handle.close()
            except Exception as close_err:
                 print_warning(f"Error closing output file handle: {close_err}")

        final_success = success and all(rc == 0 for rc in return_codes if rc is not None)
        if not timed_out and len([rc for rc in return_codes if rc == 0]) != len(return_codes):
             final_success = False

        if not final_success:
            print_warning("Pipeline completed but some steps failed, timed out, or did not finish correctly.")
        elif timed_out:
             print_error("Pipeline terminated due to timeout.")
             final_success = False

        if not final_success and output_file and output_file.exists():
             print_warning(f"Attempting to remove incomplete output file: {output_file}")
             try: output_file.unlink()
             except OSError as del_err: print_warning(f"Could not remove file: {del_err}")

        return final_success

    except FileNotFoundError as e:
        print_error(f"Error in pipeline: Command '{e.filename}' not found.")
        for info in process_info:
            try: info['proc'].kill()
            except Exception: pass
        if final_output_handle:
             try: final_output_handle.close()
             except: pass
        if output_file and output_file.exists():
            try: output_file.unlink()
            except OSError: pass
        return False
    except Exception as e:
        print_error(f"Unexpected error during pipeline setup or execution: {e}")
        for info in process_info:
            try: info['proc'].kill()
            except Exception: pass
        if final_output_handle:
            try: final_output_handle.close()
            except: pass
        if output_file and output_file.exists():
            try: output_file.unlink()
            except OSError: pass
        return False


def format_bytes(b):
    """Formatiert Bytes in eine lesbare Größe (B, KB, MB, GB, TB)."""
    if b is None: return "N/A"
    try:
        b = float(b)
        if b == 0: return "0 B"
        power = math.floor(math.log(abs(b), 1024))
        unit = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB'][max(0, min(power, 6))]
        val = b / (1024**power)
        if unit == 'B': return f"{int(val)} {unit}"
        elif unit in ['KB', 'MB']: return f"{val:.1f} {unit}"
        else: return f"{val:.2f} {unit}"
    except (ValueError, TypeError, OverflowError,):
        return "N/A"


def parse_size_to_mb(size_str):
    """Konvertiert Größenangaben (z.B. '8G', '8192M', '8192') in Megabytes."""
    size_str = str(size_str).strip().upper()
    if not size_str: return 0
    if size_str.endswith('G'): return int(float(size_str[:-1]) * 1024)
    elif size_str.endswith('M'): return int(float(size_str[:-1]))
    elif size_str.endswith('T'): return int(float(size_str[:-1]) * 1024 * 1024)
    elif size_str.isdigit(): return int(size_str)
    else:
        match = re.match(r'^(\d+(\.\d+)?)', size_str)
        if match:
            print_warning(f"Unknown/missing unit in '{size_str}', interpreting as MB.")
            return int(float(match.group(1)))
        print_warning(f"Could not parse size '{size_str}', returning 0 MB.")
        return 0

def get_instance_details(conf_path):
    """Liest ID und Name aus einer Proxmox Konfigurationsdatei."""
    instance_id = Path(conf_path).stem
    name = "no-name/hostname"
    is_lxc = 'lxc' in conf_path.parts
    config_type = "VM" if 'qemu-server' in conf_path.parts else "LXC"

    try:
        with open(conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('[') or line.startswith('#'): continue

                if line.startswith('name:'):
                    name = line.split(':', 1)[1].strip()
                elif is_lxc and line.startswith('hostname:'):
                     if name == "no-name/hostname": name = line.split(':', 1)[1].strip()
                     break
    except Exception as e:
        print_warning(f"Could not fully read configuration file {conf_path}: {e}")
    return instance_id, name, config_type

def list_instances():
    """Listet alle verfügbaren VMs und LXC-Container auf."""
    print_info("Available VMs and LXC containers:")
    vms = []
    lxcs = []
    vm_conf_files = sorted(glob.glob("/etc/pve/qemu-server/*.conf"))
    lxc_conf_files = sorted(glob.glob("/etc/pve/lxc/*.conf"))

    print(f"  {color_text('VMs', 'YELLOW')}:")
    if vm_conf_files:
        for conf in vm_conf_files:
            vm_id, vm_name, _ = get_instance_details(Path(conf))
            vms.append({'id': vm_id, 'name': vm_name})
            print(f"    {color_text(vm_id, 'BLUE')} - {vm_name}")
    else:
        print(f"    {color_text('No VMs found.', 'YELLOW')}")

    print(f"\n  {color_text('LXC Containers', 'YELLOW')}:")
    if lxc_conf_files:
        for conf in lxc_conf_files:
            lxc_id, lxc_name, _ = get_instance_details(Path(conf))
            lxcs.append({'id': lxc_id, 'name': lxc_name})
            print(f"    {color_text(lxc_id, 'BLUE')} - {lxc_name}")
    else:
        print(f"    {color_text('No LXC containers found.', 'YELLOW')}")

    if not vms and not lxcs:
        print(f"\n  {color_text('No VMs or LXC containers found on this system.', 'RED')}")
        return False
    return True


def find_instance_config(instance_id):
    """Findet den Konfigurationspfad für eine VM oder LXC ID."""
    vm_conf = Path(f"/etc/pve/qemu-server/{instance_id}.conf")
    lxc_conf = Path(f"/etc/pve/lxc/{instance_id}.conf")
    if vm_conf.is_file(): return vm_conf, "vm"
    if lxc_conf.is_file(): return lxc_conf, "lxc"
    return None, None

def list_snapshots(dataset):
    """Listet ZFS-Snapshots für ein gegebenes Dataset auf."""
    cmd = ['zfs', 'list', '-t', 'snapshot', '-o', 'name,creation', '-s', 'creation', '-H', '-p', dataset]
    success, output, stderr = run_command(cmd, check=False, capture_output=True, suppress_stderr=True, allow_fail=True, error_msg=f"Failed to list snapshots for {dataset}")
    snapshots = []
    if success and output:
        for line in output.strip().split('\n'):
            if line.startswith(f"{dataset}@"):
                try:
                    name, creation_ts = line.split('\t')
                    snapshots.append({'name': name, 'creation_timestamp': int(creation_ts)})
                except ValueError:
                    print_warning(f"Could not parse snapshot line: {line}")
                    if line.startswith(f"{dataset}@"):
                         snapshots.append({'name': line.strip(), 'creation_timestamp': 0})
    elif not success and "does not exist" not in stderr:
         print_warning(f"Could not list snapshots for {dataset}. Stderr: {stderr}")
    return snapshots


def get_zfs_property(target, property_name):
    """Ruft einen bestimmten ZFS-Property-Wert ab. Gibt None zurück, wenn nicht gefunden."""
    cmd = ['zfs', 'get', '-H', '-p', '-o', 'value', property_name, target]
    success, output, stderr = run_command(cmd, check=False, capture_output=True, suppress_stderr=True, allow_fail=True)
    if success:
        return output.strip()
    else:
        return None

def get_snapshot_size_estimate(snapshot_name):
    """Schätzt die Größe eines ZFS-Snapshots für 'zfs send'."""
    cmd = ['zfs', 'send', '-nP', snapshot_name]
    success, output, stderr = run_command(cmd, check=False, capture_output=True, suppress_stderr=True, allow_fail=True)
    if success and output:
        match = re.search(r'^size\s+(\d+)$', output, re.MULTILINE)
        if match: return int(match.group(1))
    return None

def adjust_config_file(conf_path, instance_type, new_id=None, target_pve_storage=None, dataset_map=None, name_prefix="clone-"):
    """
    Nimmt Anpassungen an einer Konfigurationsdatei für Klonen oder Wiederherstellen vor.
    """
    print_info(f"\nAdjusting configuration file {color_text(str(conf_path), 'BLUE')}...")
    if not conf_path.is_file():
        print_error(f"Config file {conf_path} not found for adjustments.", exit_code=1)

    try:
        with open(conf_path, 'r') as f_orig:
            lines = f_orig.readlines()

        modified_lines = []
        changes_made = False
        pve_storage_to_use = target_pve_storage if target_pve_storage else DEFAULT_PVE_STORAGE

        storage_regex_vm = re.compile(r'^(scsi|ide|sata|virtio|efidisk|tpmstate)(\d+):\s*([^#]+)')
        storage_regex_lxc = re.compile(r'^(rootfs|mp\d+):\s*([^#]+)')

        processing_active_config = True
        for line_num, line in enumerate(lines):
            original_line = line
            line_strip = line.strip()
            modified = False

            if line_strip.startswith('['): # Stop processing at first snapshot section
                print_warning(f"  Skipping snapshot section starting at line {line_num+1}")
                processing_active_config = False

            if not processing_active_config or not line_strip or line_strip.startswith('#'):
                modified_lines.append(line)
                continue

            # --- General Adjustments ---
            if re.match(r'^onboot:\s*[01]', line_strip) and line_strip != "onboot: 0":
                new_line_content = "onboot: 0"
                line = new_line_content + "\n"
                print(f"  Setting '{color_text('onboot: 0', 'YELLOW')}'")
                modified = True
            elif name_prefix and line_strip.startswith('name:') and not line_strip.split(':', 1)[1].strip().startswith(name_prefix):
                 new_line_content = re.sub(r'(^name:\s*)(.+)', rf'\1{name_prefix}\2', line.strip())
                 line = new_line_content + "\n"
                 print(f"  Adding '{color_text(name_prefix, 'YELLOW')}' prefix to name")
                 modified = True
            elif name_prefix and instance_type == 'lxc' and line_strip.startswith('hostname:') and not line_strip.split(':', 1)[1].strip().startswith(name_prefix):
                 new_line_content = re.sub(r'(^hostname:\s*)(.+)', rf'\1{name_prefix}\2', line.strip())
                 line = new_line_content + "\n"
                 print(f"  Adding '{color_text(name_prefix, 'YELLOW')}' prefix to hostname")
                 modified = True
            elif re.match(r'^net\d+:', line_strip):
                if 'link_down=1' not in line_strip:
                    parts = line_strip.split('#', 1)
                    main_part = parts[0].rstrip()
                    comment_part = f" #{parts[1]}" if len(parts) > 1 else ""
                    if main_part.split(':')[-1].strip() and not main_part.endswith(','):
                         main_part += ","
                    main_part += "link_down=1"
                    line = main_part + comment_part + "\n"
                    print(f"  Adding '{color_text('link_down=1', 'YELLOW')}' to network interface: {original_line.strip()}")
                    modified = True

            # --- Storage Adjustments (Dataset Mapping) ---
            match = None
            storage_key = None
            current_storage_name = None
            current_dataset_name = None
            line_options_part = ""

            if instance_type == "vm":
                match = storage_regex_vm.match(line_strip)
                if match:
                    key_base = match.group(1)
                    key_num = match.group(2)
                    storage_key = f"{key_base}{key_num}"
                    details_part = match.group(3).strip()
                    storage_match = re.match(r'([^:]+):([^,]+)(.*)', details_part) # e.g., local-zfs:vm-100-disk-0,size=32G
                    if storage_match:
                         current_storage_name = storage_match.group(1).strip()
                         current_dataset_name = storage_match.group(2).strip()
                         line_options_part = storage_match.group(3).strip() # Includes leading comma if present

            else: # LXC
                match = storage_regex_lxc.match(line_strip)
                if match:
                    storage_key = match.group(1)
                    details_part = match.group(2).strip()
                    storage_match = re.match(r'([^:]+):([^,]+)(.*)', details_part)
                    if storage_match:
                        current_storage_name = storage_match.group(1).strip()
                        current_dataset_name = storage_match.group(2).strip()
                        line_options_part = storage_match.group(3).strip()

            if storage_key and current_dataset_name and dataset_map and storage_key in dataset_map:
                new_dataset_basename = dataset_map[storage_key]
                new_storage_part = f"{pve_storage_to_use}:{new_dataset_basename}"

                newline = f"{storage_key}: {new_storage_part}{line_options_part}\n"

                if newline != line:
                    print(f"  Mapped storage {color_text(storage_key, 'BLUE')} -> {color_text(new_storage_part, 'GREEN')}")
                    line = newline
                    modified = True

            if not line.endswith('\n'): line += '\n'
            modified_lines.append(line)
            if modified: changes_made = True


        if changes_made:
            with open(conf_path, 'w') as f_new:
                f_new.writelines(modified_lines)
            print_success("  Configuration adjustments applied.")
        else:
             print_info("  No configuration adjustments needed or applied.")

    except FileNotFoundError:
         print_error(f"Config file {conf_path} disappeared before adjustments could be written.", exit_code=1)
    except Exception as e:
        print_error(f"\nError adjusting config file {conf_path}: {e}")
        print_warning(f"Config file {conf_path} may not have been properly adjusted.")


def find_zfs_datasets(conf_path, pve_storage_name, zfs_pool_path):
    """
    Findet ZFS-Datasets, die in einer Konfigurationsdatei für ein bestimmtes Storage referenziert werden.
    """
    storage_datasets = {} # {config_key: full_dataset_path}
    instance_type = "vm" if 'qemu-server' in conf_path.parts else "lxc"

    escaped_pve_storage_name = re.escape(pve_storage_name)
    # Regex for VM: scsi0: local-zfs:vm-100-disk-0,size=32G or efidisk0: local-zfs:vm-100-disk-1,efitype=4m,pre-enrolled-keys=1
    storage_regex_vm = re.compile(rf'^(scsi|ide|sata|virtio|efidisk|tpmstate)(\d+):\s*{escaped_pve_storage_name}:([^,\s]+)')
    # Regex for LXC: rootfs: local-zfs:subvol-101-disk-0,size=8G or mp0: local-zfs:subvol-101-disk-1,mp=/mnt/data,size=4G
    storage_regex_lxc = re.compile(rf'^(rootfs|mp\d+):\s*{escaped_pve_storage_name}:([^,\s]+)')


    print_info(f"Searching for ZFS datasets in {conf_path} linked to storage '{pve_storage_name}' (Pool: '{zfs_pool_path}')...")
    try:
        with open(conf_path, 'r') as f:
            processing_current_config = True
            for line_num, line in enumerate(f):
                line = line.strip()
                if line.startswith('['): # Stop at snapshot sections
                    processing_current_config = False
                if not processing_current_config: continue

                if not line or line.startswith('#') or line.startswith('parent='): continue

                match = None; key = ""; dataset_name_part = ""
                if instance_type == "vm":
                    match = storage_regex_vm.match(line)
                    if match:
                        key_base = match.group(1)
                        key_num = match.group(2)
                        key = f"{key_base}{key_num}"
                        dataset_name_part = match.group(3).strip()
                else: # LXC
                    match = storage_regex_lxc.match(line)
                    if match:
                        key = match.group(1)
                        dataset_name_part = match.group(2).strip()

                if key and dataset_name_part:
                    # Construct full dataset path, assuming dataset_name_part is relative to zfs_pool_path
                    # or is already a full path if it starts with the pool path
                    if dataset_name_part.startswith(zfs_pool_path + '/'):
                        full_dataset_path = dataset_name_part
                    elif '/' in dataset_name_part and not dataset_name_part.startswith('/'):
                        # Looks like a relative path but contains slashes - might be complex pool layout
                        full_dataset_path = f"{zfs_pool_path.rstrip('/')}/{dataset_name_part}"
                        print_warning(f"  (Line {line_num+1}) Interpreting relative path '{dataset_name_part}' as '{full_dataset_path}' under pool '{zfs_pool_path}'")
                    else: # Simple name, prepend pool path
                        full_dataset_path = f"{zfs_pool_path.rstrip('/')}/{dataset_name_part}"


                    if get_zfs_property(full_dataset_path, 'type'):
                         storage_datasets[key] = full_dataset_path
                         print(f"  Found {color_text(key, 'BLUE')} -> {full_dataset_path}")
                    else:
                        print_warning(f"  Dataset for {color_text(key, 'BLUE')} ('{full_dataset_path}') not found via 'zfs get type'. Skipping.")

    except FileNotFoundError:
        print_error(f"Configuration file {conf_path} not found.", exit_code=1)
    except Exception as e:
        print_error(f"Error reading {conf_path}: {e}", exit_code=1)

    if not storage_datasets:
        print_warning(f"No existing ZFS datasets found for storage '{pve_storage_name}' (Pool '{zfs_pool_path}') in {conf_path}.")

    return storage_datasets, instance_type

def select_reference_dataset(storage_datasets, instance_type):
    """Bestimmt das Referenz-Dataset für Snapshot-Listen basierend auf Konventionen."""
    if not storage_datasets:
        print_error("Cannot select reference dataset: No datasets provided.")
        return None, None

    ref_key = None; ref_dataset = None;

    if instance_type == 'lxc':
        if 'rootfs' in storage_datasets:
            ref_key = 'rootfs'
        else:
            mp_keys = sorted([k for k in storage_datasets if k.startswith('mp')], key=lambda x: int(x[2:]))
            if mp_keys: ref_key = mp_keys[0]
            else:
                sorted_keys = sorted(storage_datasets.keys())
                if sorted_keys: ref_key = sorted_keys[0]

            if ref_key: print_warning(f"LXC 'rootfs' not found or not on ZFS, using '{ref_key}' as reference.")
            else: print_error("LXC has no 'rootfs' or 'mpX' datasets on the specified ZFS storage."); return None, None
        ref_dataset = storage_datasets[ref_key]

    else: # VM
        disk_num_regex = re.compile(r'(scsi|ide|sata|virtio)(\d+)$')
        numbered_disks = {}
        efi_key = None; tpm_key = None;

        for key, dataset in storage_datasets.items():
             match = disk_num_regex.match(key)
             if match:
                 disk_num = int(match.group(2))
                 numbered_disks[disk_num] = {'key': key, 'dataset': dataset}
             elif key.startswith('efidisk') and not efi_key:
                 efi_key = key
             elif key.startswith('tpmstate') and not tpm_key:
                 tpm_key = key

        if numbered_disks:
            min_disk_num = min(numbered_disks.keys())
            ref_key = numbered_disks[min_disk_num]['key']
            ref_dataset = numbered_disks[min_disk_num]['dataset']
        elif efi_key:
            ref_key = efi_key; ref_dataset = storage_datasets[efi_key]
            print_warning(f"No standard numbered disk found, using EFI disk '{ref_key}' as reference.")
        elif tpm_key:
            ref_key = tpm_key; ref_dataset = storage_datasets[tpm_key]
            print_warning(f"No standard numbered disk or EFI disk found, using TPM state disk '{ref_key}' as reference.")
        else:
            sorted_keys = sorted(storage_datasets.keys())
            if sorted_keys:
                ref_key = sorted_keys[0]
                ref_dataset = storage_datasets[ref_key]
                print_warning(f"No standard disk found, using first dataset '{ref_key}' as reference.")
            else:
                print_error("No suitable reference dataset could be determined.")
                return None, None

    print_info(f"Using reference dataset for snapshot operations: {color_text(ref_key, 'BLUE')} ({color_text(ref_dataset,'CYAN')})")
    return ref_key, ref_dataset

def parse_snapshot_indices(index_str, max_index):
    """Hilfsfunktion zum Parsen von Snapshot-Index-Eingaben (z.B. "0,1,3-5")."""
    selected_indices = set()
    if not index_str.strip(): # Handle empty input
        return []
    parts = index_str.split(',')
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            try:
                start_str, end_str = part.split('-', 1)
                start = int(start_str)
                end = int(end_str)
                if not (0 <= start <= end <= max_index):
                    raise ValueError("Invalid range values or order.")
                selected_indices.update(range(start, end + 1))
            except ValueError as e:
                raise ValueError(f"Ungültiges Bereichsformat: '{part}'. {e}")
        else:
            try:
                idx = int(part)
                if not (0 <= idx <= max_index):
                    raise ValueError("Index außerhalb des Bereichs.")
                selected_indices.add(idx)
            except ValueError:
                raise ValueError(f"Ungültiges Indexformat: '{part}'.")
    return sorted(list(selected_indices))


def select_snapshots(ref_dataset):
    """Lässt den Benutzer einen oder mehrere Snapshots aus einer Liste auswählen."""
    snapshots = list_snapshots(ref_dataset)
    if not snapshots:
        print_error(f"Keine Snapshots für das Referenz-Dataset {ref_dataset} gefunden.", exit_code=1)

    print("\nVerfügbare Snapshots (neueste zuerst):")
    snapshots.sort(key=lambda x: x['creation_timestamp'], reverse=True)
    for i, snap in enumerate(snapshots):
        snap_suffix = snap['name'].split('@', 1)[1]
        creation_dt = datetime.fromtimestamp(snap['creation_timestamp']) if snap['creation_timestamp'] else None
        human_time = creation_dt.strftime('%Y-%m-%d %H:%M:%S') if creation_dt else "Unbekannte Zeit"
        print(f"  {color_text(f'[{i}]', 'BLUE')} {snap_suffix} {color_text(f'({human_time})', 'YELLOW')}")

    selected_snapshot_infos = []
    while True:
        try:
            idx_input = input(f"Geben Sie die Indizes der zu verwendenden Snapshots ein (z.B. 0,2,3 oder 1-3, leer für Abbruch): ").strip()
            if not idx_input:
                print_warning("Keine Auswahl getroffen. Vorgang abgebrochen.")
                return [] # Return empty list if user enters nothing

            raw_indices = parse_snapshot_indices(idx_input, len(snapshots) - 1)
            if not raw_indices: # If parsing resulted in empty list (e.g. invalid input fully filtered)
                print_error("Ungültige Eingabe oder keine gültigen Indizes ausgewählt.")
                continue

            selected_snapshot_infos = []
            for idx in raw_indices:
                selected_snapshot_full_name = snapshots[idx]['name']
                snap_suffix = selected_snapshot_full_name.split('@', 1)[1]
                selected_snapshot_infos.append({
                    'name': selected_snapshot_full_name,
                    'suffix': snap_suffix,
                    'display_name': snapshots[idx]['name'].split('@',1)[1] + f" ({datetime.fromtimestamp(snapshots[idx]['creation_timestamp']).strftime('%Y-%m-%d %H:%M:%S') if snapshots[idx]['creation_timestamp'] else 'Unbekannte Zeit'})"
                })
            
            if selected_snapshot_infos:
                print_success("\nAusgewählte Snapshot-Suffixe:")
                for info in selected_snapshot_infos:
                    print(f"  - {info['suffix']}")
                return selected_snapshot_infos
            else: # Should be caught by raw_indices check, but defensive
                print_error("Keine gültigen Snapshots nach der Auswahl gefunden.")
                
        except ValueError as e:
            print_error(f"Ungültige Eingabe: {e}. Bitte versuchen Sie es erneut.")
        except EOFError:
            print_error("\nOperation vom Benutzer abgebrochen (EOF).", exit_code=1)
            return [] # Should not be reached if EOFError exits


def generate_new_dataset_name(old_dataset_path, old_id, new_id, target_zfs_pool_path):
    """
    Generiert einen neuen Dataset-Namen für Klonen/Wiederherstellen.
    """
    old_dataset_name = Path(old_dataset_path).name
    new_dataset_name = old_dataset_name

    patterns_to_try = [
        (rf'-{old_id}-', f'-{new_id}-'),
        (rf'-{old_id}$', f'-{new_id}'),
        (rf'^{old_id}-', f'{new_id}-'),
        (rf'_{old_id}_', f'_{new_id}_'),
        (rf'{old_id}', f'{new_id}'), # Fallback
    ]
    replaced = False
    for pattern, replacement in patterns_to_try:
        temp_name, num_subs = re.subn(pattern, replacement, old_dataset_name, count=1)
        if num_subs > 0:
            new_dataset_name = temp_name
            replaced = True
            break

    if not replaced:
        new_dataset_name = f"{old_dataset_name}_newid_{new_id}"
        print_warning(f"Could not reliably replace ID '{old_id}' in '{old_dataset_name}'. Using fallback name: '{new_dataset_name}'")

    target_pool_base = target_zfs_pool_path.rstrip('/')
    return f"{target_pool_base}/{new_dataset_name}"


# --- Mode Functions ---

def do_clone(args):
    """Führt den Klonvorgang durch."""
    print_info("=== Running Clone Mode ===")
    src_id = args.source_id
    clone_mode = args.clone_mode
    target_zfs_pool_path = args.target_zfs_pool_path
    target_pve_storage = args.target_pve_storage

    src_conf_path, src_instance_type = find_instance_config(src_id)
    if not src_conf_path:
        print_error(f"Error: No VM or LXC with ID {src_id} found.", exit_code=1)

    src_instance_id_str, src_instance_name, config_type_str = get_instance_details(src_conf_path)
    print_success(f"Selected source: ID {src_instance_id_str} ({config_type_str} '{src_instance_name}')")
    pve_cmd = "qm" if src_instance_type == "vm" else "pct"

    # --- Determine and Validate Base Target ID ---
    base_new_id_str = args.new_id
    if not base_new_id_str:
        default_new_id = f"9{src_id}"
        try:
            new_id_input = input(f"Enter the BASE new {config_type_str} ID for the first clone (e.g., {default_new_id}, subsequent clones will increment this ID, leer für Standard): ").strip()
            base_new_id_str = new_id_input or default_new_id
            if not base_new_id_str.isdigit() or int(base_new_id_str) <= 0:
                 print_error(f"Invalid base new ID '{base_new_id_str}'. Must be a positive integer.", exit_code=1)
            if base_new_id_str == default_new_id and not new_id_input:
                 print_warning(f"Using default base ID: {base_new_id_str}")
        except ValueError:
             print_error("Invalid input for base new ID.", exit_code=1)
        except EOFError:
            print_error("\nNon-interactive mode: Base New ID must be provided as an argument (--new-id).", exit_code=1)
    
    try:
        base_new_id_int = int(base_new_id_str)
    except ValueError: # Should be caught by isdigit
        print_error(f"Base new ID '{base_new_id_str}' must be a number.", exit_code=1)


    print_info(f"Selected mode: {clone_mode.capitalize()} Clone")
    if src_instance_type == "vm":
        perform_ram_check(pve_cmd, src_id)
    else:
        print_info("\nSkipping RAM check for LXC containers.")

    storage_datasets, _ = find_zfs_datasets(src_conf_path, target_pve_storage, target_zfs_pool_path) # Use target storage/pool for finding relevant source datasets
    if not storage_datasets:
        print_error(f"No ZFS datasets found for storage '{target_pve_storage}' (Pool '{target_zfs_pool_path}') in {src_conf_path}. Cannot clone.", exit_code=1)

    ref_key, ref_dataset = select_reference_dataset(storage_datasets, src_instance_type)
    if not ref_key: sys.exit(1)

    selected_snapshots_info_list = select_snapshots(ref_dataset)
    if not selected_snapshots_info_list:
        print_error("No snapshots selected. Aborting clone.", exit_code=1)

    print_info(f"\n--- Starting ZFS {clone_mode.capitalize()} Clone Operations ---")
    print_info(f"Target ZFS Pool Path: {target_zfs_pool_path}")
    print_info(f"Target PVE Storage: {target_pve_storage}")

    pv_available = is_tool('pv')
    overall_clone_success = True
    successful_clones_summary = []

    for i, snap_info in enumerate(selected_snapshots_info_list):
        current_new_id_int = base_new_id_int + i
        current_new_id_str = str(current_new_id_int)
        snap_suffix = snap_info['suffix']
        selected_snapshot_full_ref_name = snap_info['name'] # Full name of the snapshot on the reference dataset

        print_info(f"\nProcessing Snapshot: {color_text(snap_suffix, 'YELLOW')} for new ID {color_text(current_new_id_str, 'BLUE')}")

        # Check for config file collision for current_new_id_str
        new_conf_path_vm = Path(f"/etc/pve/qemu-server/{current_new_id_str}.conf")
        new_conf_path_lxc = Path(f"/etc/pve/lxc/{current_new_id_str}.conf")
        collision = False
        if new_conf_path_vm.exists(): print_error(f"Config file for VM ID {current_new_id_str} ({new_conf_path_vm}) already exists!"); collision = True
        if new_conf_path_lxc.exists(): print_error(f"Config file for LXC ID {current_new_id_str} ({new_conf_path_lxc}) already exists!"); collision = True
        if collision:
            print_error(f"Aborting clone for snapshot '{snap_suffix}' due to config collision for ID {current_new_id_str}.")
            overall_clone_success = False
            break # Stop all further snapshot processing

        new_conf_path = new_conf_path_vm if src_instance_type == "vm" else new_conf_path_lxc

        # --- ZFS Clone Operations for this snapshot ---
        cloned_datasets_map_this_snap = {}
        all_ops_successful_this_snap = True
        cleanup_list_this_snap = []

        print_info(f"  Checking for potential target dataset collisions for ID {current_new_id_str}...")
        potential_targets_this_snap = {}
        dataset_collision_found_this_snap = False
        for key, dataset_path_in_source_config in storage_datasets.items():
            # Generate new dataset name based on original dataset path, original src_id, and current_new_id_str
            new_dataset_target_path = generate_new_dataset_name(dataset_path_in_source_config, src_id, current_new_id_str, target_zfs_pool_path)
            potential_targets_this_snap[key] = new_dataset_target_path
            if get_zfs_property(new_dataset_target_path, 'type'):
                print_error(f"  Target dataset '{new_dataset_target_path}' for key '{key}' (new ID {current_new_id_str}) already exists.")
                dataset_collision_found_this_snap = True
        
        if dataset_collision_found_this_snap:
            print_error(f"Aborting clone for snapshot '{snap_suffix}' due to target dataset collision(s) for ID {current_new_id_str}.")
            overall_clone_success = False
            break 

        print_success(f"  No target dataset collisions found for ID {current_new_id_str}.")

        for key, dataset_path_in_source_config in storage_datasets.items():
            # Construct the source snapshot name for this specific dataset using the chosen suffix
            source_snapshot_for_this_disk = f"{dataset_path_in_source_config}@{snap_suffix}"
            new_dataset_target_path = potential_targets_this_snap[key]

            print(f"\n  {color_text(f'Processing disk {key}', 'CYAN')} for snapshot '{snap_suffix}' -> new ID {current_new_id_str}")
            print(f"    Source dataset:  {color_text(dataset_path_in_source_config, 'BLUE')}")
            print(f"    Source snapshot: {color_text(source_snapshot_for_this_disk, 'BLUE')}")
            print(f"    Target dataset:  {color_text(new_dataset_target_path, 'GREEN')}")

            if not get_zfs_property(source_snapshot_for_this_disk, 'type'):
                 print_warning(f"    [WARN] Snapshot '{source_snapshot_for_this_disk}' does not exist for this specific dataset. Skipping this disk.")
                 continue

            op_success_this_disk = False
            if clone_mode == 'linked':
                clone_cmd = ['zfs', 'clone', source_snapshot_for_this_disk, new_dataset_target_path]
                print(f"    Executing linked clone: {' '.join(clone_cmd)}")
                try:
                    run_command(clone_cmd, check=True, capture_output=False, error_msg="ZFS clone failed")
                    print_success("    Linked clone successful.")
                    op_success_this_disk = True
                except SystemExit:
                    print_error("    Error during 'zfs clone'.")
                    all_ops_successful_this_snap = False
                    break # Break from disk loop for this snapshot
            else: # Full clone
                print("    Preparing full clone (send/receive)...")
                estimated_size_bytes = get_snapshot_size_estimate(source_snapshot_for_this_disk)
                size_str = f"~{format_bytes(estimated_size_bytes)}" if estimated_size_bytes is not None else "Unknown size"
                print(f"    Estimated size: {size_str}")

                send_cmd = ['zfs', 'send', source_snapshot_for_this_disk]
                recv_cmd = ['zfs', 'receive', '-o', 'readonly=off', new_dataset_target_path]
                pipeline_cmds = [send_cmd]
                pipeline_names = ["zfs send"]
                pv_opts = None

                if pv_available:
                    pv_cmd_base = ['pv']
                    pv_opts = ['-p', '-t', '-r', '-b', '-N', f'clone-{key}-{current_new_id_str}']
                    if estimated_size_bytes: pv_opts.extend(['-s', str(estimated_size_bytes)])
                    pipeline_cmds.append(pv_cmd_base)
                    pipeline_names.append("pv")
                else:
                    print_warning("    Executing full clone without progress bar ('pv' not found).")
                
                pipeline_cmds.append(recv_cmd)
                pipeline_names.append("zfs receive")

                print(f"    Executing pipeline: {' | '.join([' '.join(c) for c in pipeline_cmds])}")
                pipeline_successful = run_pipeline(pipeline_cmds, pipeline_names, pv_options=pv_opts)

                if pipeline_successful:
                     print_success("    Full clone (send/receive) successful.")
                     op_success_this_disk = True
                else:
                    print_error("    Error during 'zfs send/receive' pipeline.")
                    all_ops_successful_this_snap = False
                    break # Break from disk loop for this snapshot
            
            if op_success_this_disk:
                cloned_datasets_map_this_snap[key] = Path(new_dataset_target_path).name
                cleanup_list_this_snap.append(new_dataset_target_path)
            else: # Error already printed
                pass # Loop will break if all_ops_successful_this_snap became false

        # --- Post-ZFS Operations for this snapshot ---
        if not all_ops_successful_this_snap:
             print_error(f"\nOne or more ZFS {clone_mode} clone operations failed for snapshot '{snap_suffix}' (new ID {current_new_id_str}). Attempting cleanup...")
             for ds_path in reversed(cleanup_list_this_snap):
                 print_warning(f"    Destroying partially created dataset: {ds_path}")
                 run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
             overall_clone_success = False
             break # Stop processing subsequent snapshots

        if not cloned_datasets_map_this_snap:
            print_error(f"\nNo datasets were successfully processed for snapshot '{snap_suffix}' (new ID {current_new_id_str}). Cannot create config.",_suppress_stderr=True)
            overall_clone_success = False
            break

        # --- Create New Configuration File for this snapshot's clone ---
        print_info(f"\n  Creating new {config_type_str} configuration {color_text(str(new_conf_path), 'BLUE')} for ID {current_new_id_str}")
        config_created_successfully_this_snap = False
        try:
            shutil.copy2(src_conf_path, new_conf_path)
            print_success(f"  Copied base configuration from {src_conf_path} to {new_conf_path}.")

            adjust_config_file(
                conf_path=new_conf_path,
                instance_type=src_instance_type,
                new_id=current_new_id_str,
                target_pve_storage=target_pve_storage,
                dataset_map=cloned_datasets_map_this_snap,
                name_prefix=f"clone-{current_new_id_str}-" # More specific prefix
            )
            config_created_successfully_this_snap = True
            successful_clones_summary.append(
                f"Snapshot '{snap_suffix}' -> New {config_type_str} ID {current_new_id_str} (Mode: {clone_mode})"
            )

        except Exception as e:
            print_error(f"  Error processing config file {new_conf_path} for ID {current_new_id_str}: {e}")
            if new_conf_path.exists():
                print_warning(f"  Removing potentially incomplete config file: {new_conf_path}")
                try: new_conf_path.unlink()
                except OSError as del_err: print_warning(f"  Could not remove config file: {del_err}")
            
            print_warning(f"  Attempting to clean up cloned ZFS datasets for ID {current_new_id_str} due to config error...")
            for ds_path in reversed(cleanup_list_this_snap):
                 print_warning(f"    Destroying cloned dataset: {ds_path}")
                 run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
            overall_clone_success = False
            break # Stop processing subsequent snapshots

    # --- Final Clone Process Message ---
    print(f"\n{color_text('--- Clone Process Finished ---', 'GREEN' if overall_clone_success and successful_clones_summary else 'YELLOW')}")
    if successful_clones_summary:
        print_success("Successfully created clones:")
        for summary_msg in successful_clones_summary:
            print(f"  - {summary_msg}")
        print(f"\nTarget ZFS Pool Path: {color_text(target_zfs_pool_path, 'BLUE')}")
        print(f"Target PVE Storage: {color_text(target_pve_storage, 'BLUE')}")
        print(color_text("Important: Review configurations, network settings (IP/MAC), hostnames, resources, and link_down=1 on NICs for all new clones.", 'YELLOW'))
    else:
        if overall_clone_success: # No snapshots selected or other pre-loop issue
             print_warning("No clones were created. This might be due to no snapshots being selected or an early exit.")
        else:
             print_error("Clone process failed or was aborted. Some clones may not have been created or are incomplete.")


def do_export(args):
    """Führt den Exportvorgang durch."""
    print_info("=== Running Export Mode ===")
    src_id = args.source_id
    parent_export_dir_base = Path(args.export_dir)
    compress_method = args.compress
    source_zfs_pool_path = args.source_zfs_pool_path
    source_pve_storage = args.source_pve_storage

    compress_ok, _, compress_tool_info = check_compression_tools(compress_method)
    if compress_tool_info is None:
        print_error(f"Failed to get compression tool info for method '{compress_method}'. Aborting export.", exit_code=1)
    if compress_method != "none" and not compress_ok:
        print_error(f"Required compression tool for method '{compress_method}' not found. Aborting export.", exit_code=1)
    if compress_method != "none":
         print_info(f"Using compression method: {compress_method}")

    src_conf_path, src_instance_type = find_instance_config(src_id)
    if not src_conf_path:
        print_error(f"Error: No VM or LXC with ID {src_id} found.", exit_code=1)

    src_instance_id_str, src_instance_name, config_type_str = get_instance_details(src_conf_path)
    print_success(f"Selected source: ID {src_instance_id_str} ({config_type_str} '{src_instance_name}')")

    storage_datasets, _ = find_zfs_datasets(src_conf_path, source_pve_storage, source_zfs_pool_path)
    if not storage_datasets:
        print_error(f"No ZFS datasets found for source storage '{source_pve_storage}' (Pool '{source_zfs_pool_path}') in {src_conf_path}. Cannot export.", exit_code=1)

    ref_key, ref_dataset = select_reference_dataset(storage_datasets, src_instance_type)
    if not ref_key: sys.exit(1)

    selected_snapshots_info_list = select_snapshots(ref_dataset)
    if not selected_snapshots_info_list:
        print_error("No snapshots selected. Aborting export.", exit_code=1)

    pv_available = is_tool('pv')
    overall_export_success = True
    successful_exports_summary = []

    for snap_info in selected_snapshots_info_list:
        snap_suffix = snap_info['suffix']
        # Sanitize snapshot suffix for use in directory name
        snap_suffix_sanitized = re.sub(r'[^a-zA-Z0-9_\-.]', '_', snap_suffix)
        
        current_export_dir = parent_export_dir_base / f"{src_id}_{snap_suffix_sanitized}"
        ref_snapshot_name_this_iter = f"{ref_dataset}@{snap_suffix}" # Full name of ref snapshot for this iteration

        print_info(f"\nProcessing export for Snapshot: {color_text(snap_suffix, 'YELLOW')} to directory {color_text(str(current_export_dir), 'BLUE')}")

        try:
            current_export_dir.mkdir(parents=True, exist_ok=False)
            with tempfile.NamedTemporaryFile(prefix='write_test_', dir=current_export_dir, delete=True): pass
        except FileExistsError:
            print_error(f"Export directory '{current_export_dir}' already exists. Please remove it or choose a different parent directory. Skipping this snapshot.", exit_code=None) # Not exiting script
            overall_export_success = False
            continue # Skip to next snapshot
        except PermissionError:
             print_error(f"Permission denied: Cannot create or write to export directory '{current_export_dir}'. Skipping this snapshot.", exit_code=None)
             overall_export_success = False
             continue
        except Exception as e:
            print_error(f"Failed to create or access export directory '{current_export_dir}': {e}. Skipping this snapshot.", exit_code=None)
            overall_export_success = False
            continue
        print_success(f"  Using export directory: {current_export_dir}")

        # --- Export Config for this snapshot ---
        config_export_path = current_export_dir / f"{src_id}{DEFAULT_EXPORT_CONFIG_SUFFIX}"
        print_info(f"\n  Exporting configuration to {color_text(str(config_export_path), 'BLUE')}")
        try:
            shutil.copy2(src_conf_path, config_export_path)
            print_success("  Configuration file exported successfully.")
        except Exception as e:
            print_error(f"  Failed to export configuration file: {e}. Aborting export for this snapshot.")
            overall_export_success = False
            # Try to remove the partially created export directory for this snapshot
            if current_export_dir.exists():
                try: shutil.rmtree(current_export_dir)
                except Exception as rme: print_warning(f"Could not remove incomplete export dir {current_export_dir}: {rme}")
            continue # Skip to next snapshot

        # --- Export ZFS Data for this snapshot ---
        print_info(f"\n  --- Starting ZFS Data Export (Snapshot Suffix: {snap_suffix}) ---")
        exported_disks_metadata_this_snap = []
        all_data_ops_successful_this_snap = True

        for key, dataset_path in storage_datasets.items():
            target_snapshot_for_disk = f"{dataset_path}@{snap_suffix}"
            data_suffix = compress_tool_info["suffix"]
            stream_filename = f"{key}{data_suffix}"
            data_export_path = current_export_dir / stream_filename

            print(f"\n  {color_text(f'Exporting disk {key}', 'CYAN')}")
            print(f"    Source dataset:  {color_text(dataset_path, 'BLUE')}")
            print(f"    Source snapshot: {color_text(target_snapshot_for_disk, 'BLUE')}")
            print(f"    Output file:     {color_text(str(data_export_path), 'BLUE')}")

            if not get_zfs_property(target_snapshot_for_disk, 'type'):
                print_warning(f"    [WARN] Snapshot '{target_snapshot_for_disk}' does not exist for this dataset. Skipping export for {key}.")
                continue

            estimated_size_bytes = get_snapshot_size_estimate(target_snapshot_for_disk)
            size_str = f"~{format_bytes(estimated_size_bytes)}" if estimated_size_bytes is not None else "Unknown size"
            print(f"    Estimated raw size: {size_str}")

            send_cmd = ['zfs', 'send', target_snapshot_for_disk]
            pipeline_cmds = [send_cmd]
            pipeline_names = ["zfs send"]
            pv_opts = None

            if pv_available:
                pv_cmd_base = ['pv']
                pv_opts = ['-W', '-p', '-t', '-r', '-b', '-N', f'export-{key}-{snap_suffix_sanitized}']
                if estimated_size_bytes: pv_opts.extend(['-s', str(estimated_size_bytes)])
                pipeline_cmds.append(pv_cmd_base)
                pipeline_names.append("pv")
            else:
                print_warning("    Executing export without progress bar ('pv' not found).")

            if compress_method != "none":
                compress_cmd = compress_tool_info["compress"]
                pipeline_cmds.append(compress_cmd)
                pipeline_names.append(compress_method)
            
            print(f"    Executing pipeline: {' | '.join([' '.join(c) for c in pipeline_cmds])} > {data_export_path}")
            pipeline_successful = run_pipeline(pipeline_cmds, pipeline_names, pv_options=pv_opts, output_file=data_export_path)

            if pipeline_successful:
                 print_success(f"    ZFS data for {key} exported successfully.")
                 exported_disks_metadata_this_snap.append({
                     'key': key,
                     'original_dataset_basename': Path(dataset_path).name,
                     'original_dataset_path': dataset_path,
                     'stream_file': stream_filename,
                     'stream_suffix': data_suffix
                 })
            else:
                print_error(f"    Error during ZFS data export for {key}.")
                all_data_ops_successful_this_snap = False
                if data_export_path.exists():
                     print_warning(f"    Attempting to remove potentially incomplete file: {data_export_path}")
                     try: data_export_path.unlink()
                     except OSError as del_err: print_warning(f"    Could not remove file: {del_err}")
                break # Stop exporting other disks for this snapshot

        # --- Write Metadata File for this snapshot ---
        if not all_data_ops_successful_this_snap:
            print_error(f"\nExport of ZFS data failed for snapshot '{snap_suffix}'. Aborting metadata and cleaning up directory.")
            overall_export_success = False
            if current_export_dir.exists():
                try: shutil.rmtree(current_export_dir)
                except Exception as rme: print_warning(f"Could not remove incomplete export dir {current_export_dir}: {rme}")
            continue # Skip to next snapshot

        meta_export_path = current_export_dir / f"{src_id}{DEFAULT_EXPORT_META_SUFFIX}"
        print_info(f"\n  Writing metadata file {color_text(str(meta_export_path), 'BLUE')}")

        if not exported_disks_metadata_this_snap and all_data_ops_successful_this_snap :
             print_warning("  No disk data was exported for this snapshot. Metadata file will be minimal.")

        metadata = {
            "exported_at": datetime.now().isoformat(),
            "script_version": "pve-zfs-utility-v_multi_snapshot",
            "source_id": src_id,
            "source_instance_type": src_instance_type,
            "source_config_file": config_export_path.name,
            "source_pve_storage": source_pve_storage,
            "source_zfs_pool_path": source_zfs_pool_path,
            "snapshot_suffix": snap_suffix, # Suffix of THIS exported snapshot
            "reference_snapshot_name": ref_snapshot_name_this_iter, 
            "compression_method": compress_method,
            "exported_disks": exported_disks_metadata_this_snap
        }
        try:
            with open(meta_export_path, 'w') as f_meta:
                json.dump(metadata, f_meta, indent=4)
            print_success("  Metadata file written successfully.")
            successful_exports_summary.append(
                f"Snapshot '{snap_suffix}' -> Directory '{current_export_dir.name}' (Compression: {compress_method})"
            )
        except Exception as e:
            print_error(f"  Failed to write metadata file: {e}")
            overall_export_success = False
            # Attempt to cleanup this snapshot's export dir
            if current_export_dir.exists():
                try: shutil.rmtree(current_export_dir)
                except Exception as rme: print_warning(f"Could not remove incomplete export dir {current_export_dir}: {rme}")
            continue # Skip to next snapshot

    # --- Final Export Process Message ---
    print(f"\n{color_text('--- Export Process Finished ---', 'GREEN' if overall_export_success and successful_exports_summary else 'YELLOW')}")
    if successful_exports_summary:
        print_success(f"Successfully exported {config_type_str} ID {src_id}:")
        for summary_msg in successful_exports_summary:
            print(f"  - {summary_msg}")
        print(f"\nBase Export Directory: {parent_export_dir_base.resolve()}")
        print(color_text("\nStore the entire export directory (or relevant snapshot subdirectories) securely.", 'YELLOW'))
    else:
        if overall_export_success:
            print_warning("No snapshots were successfully exported. This might be due to no snapshots being selected or early exits for each.")
        else:
            print_error("Export process failed or was aborted for some snapshots. Review logs carefully.")
            print(color_text("Some exports might be incomplete or unusable.", "RED"))
            print(color_text(f"Consider reviewing or removing potentially incomplete export subdirectories in {parent_export_dir_base.resolve()}", "YELLOW"))


def do_restore(args):
    """Führt den Wiederherstellungsvorgang durch."""
    print_info("=== Running Restore Mode ===")
    import_dir = Path(args.import_dir).resolve()
    new_id_str = args.new_id # Can be None
    target_zfs_pool_path = args.target_zfs_pool_path
    target_pve_storage = args.target_pve_storage

    print_info(f"Checking import directory: {color_text(str(import_dir), 'BLUE')}")
    if not import_dir.is_dir():
        print_error(f"Import directory not found or not a directory: {import_dir}", exit_code=1)

    potential_meta_files = list(import_dir.glob(f"*{DEFAULT_EXPORT_META_SUFFIX}"))
    if not potential_meta_files:
        print_error(f"No metadata file (*{DEFAULT_EXPORT_META_SUFFIX}) found in {import_dir}", exit_code=1)
    if len(potential_meta_files) > 1:
        print_warning(f"Multiple metadata files found in {import_dir}. Using the first one: {potential_meta_files[0].name}")
    meta_import_path = potential_meta_files[0]

    print_info(f"Reading metadata from: {meta_import_path.name}")
    metadata = None
    compress_method = "none"
    try:
        with open(meta_import_path, 'r') as f_meta:
            metadata = json.load(f_meta)
        print_success("Metadata loaded successfully.")

        original_id = metadata.get("source_id")
        original_instance_type = metadata.get("source_instance_type")
        exported_disks = metadata.get("exported_disks")
        compress_method = metadata.get("compression_method", "none")

        if not original_id: raise ValueError("Missing 'source_id' in metadata.")
        if not original_instance_type or original_instance_type not in ['vm', 'lxc']:
             raise ValueError("Missing or invalid 'source_instance_type' (must be 'vm' or 'lxc') in metadata.")
        if exported_disks is None: raise ValueError("Missing 'exported_disks' list in metadata.")
        if not isinstance(exported_disks, list):
             raise ValueError("'exported_disks' in metadata is not a list.")
        if compress_method not in COMPRESSION_TOOLS:
             raise ValueError(f"Invalid 'compression_method' ('{compress_method}') found in metadata.")
        
        for i, disk_info in enumerate(exported_disks):
             if not disk_info.get("key"): raise ValueError(f"Disk entry {i} missing 'key'.")
             if not disk_info.get("original_dataset_basename"): raise ValueError(f"Disk entry {i} missing 'original_dataset_basename'.")
             if not disk_info.get("stream_file"): raise ValueError(f"Disk entry {i} missing 'stream_file'.")
             expected_suffix = COMPRESSION_TOOLS[compress_method]["suffix"]
             if disk_info.get("stream_suffix") != expected_suffix:
                 print_warning(f"Stream suffix '{disk_info.get('stream_suffix')}' for key '{disk_info['key']}' does not match expected suffix '{expected_suffix}' for compression '{compress_method}'.")


        print(f"  Original ID:       {original_id}")
        print(f"  Original Type:     {original_instance_type.upper()}")
        print(f"  Compression:       {compress_method}")
        print(f"  Disks in export:   {len(exported_disks)}")

    except json.JSONDecodeError:
        print_error(f"Failed to decode metadata file (invalid JSON): {meta_import_path}", exit_code=1)
    except ValueError as ve:
         print_error(f"Invalid or incomplete metadata in {meta_import_path}: {ve}", exit_code=1)
    except Exception as e:
        print_error(f"Failed to read or parse metadata file {meta_import_path}: {e}", exit_code=1)

    _, decompress_ok, decompress_tool_info = check_compression_tools(compress_method)
    if compress_method != "none" and not decompress_ok:
        print_error(f"Required decompression tool for method '{compress_method}' not found. Aborting restore.", exit_code=1)

    config_filename = metadata.get("source_config_file")
    if not config_filename:
        config_filename = f"{original_id}{DEFAULT_EXPORT_CONFIG_SUFFIX}"
        print_warning(f"Config filename not specified in metadata, assuming default: {config_filename}")

    config_import_path = import_dir / config_filename
    if not config_import_path.is_file():
         print_error(f"Required config file '{config_filename}' not found in {import_dir}", exit_code=1)
    print(f"  Config file found: {config_filename}")

    pve_cmd = "qm" if original_instance_type == "vm" else "pct"
    conf_dir_name = "qemu-server" if original_instance_type == "vm" else "lxc"
    target_conf_dir = Path("/etc/pve") / conf_dir_name

    if not new_id_str:
        default_new_id = f"8{original_id}"
        try:
            new_id_input = input(f"Enter the new {original_instance_type.upper()} ID for restore (leer für Standard={default_new_id}): ").strip()
            new_id_str = new_id_input or default_new_id
            if not new_id_str.isdigit() or int(new_id_str) <= 0:
                 print_error(f"Invalid new ID '{new_id_str}'. Must be a positive integer.", exit_code=1)
            if new_id_str == default_new_id and not new_id_input:
                print_warning(f"Using default ID: {new_id_str}")
        except ValueError:
             print_error("Invalid input for new ID.", exit_code=1)
        except EOFError:
            print_error("\nNon-interactive mode: New ID must be provided as an argument (--new-id).", exit_code=1)

    new_conf_path = target_conf_dir / f"{new_id_str}.conf"
    config_collision = False
    if new_conf_path.exists():
        print_error(f"Config file for target ID {new_id_str} ({new_conf_path}) already exists!");
        config_collision = True

    print_info("\nChecking for potential target dataset collisions...")
    potential_targets_map = {}
    dataset_collision_found = False
    if not exported_disks:
        print_warning("No disks listed in metadata to restore, proceeding to config only.")
    else:
        for disk_info in exported_disks:
            original_key = disk_info["key"]
            original_path_for_naming = disk_info.get("original_dataset_path")
            if not original_path_for_naming:
                 original_pool = metadata.get("source_zfs_pool_path", target_zfs_pool_path)
                 original_basename = disk_info["original_dataset_basename"]
                 original_path_for_naming = f"{original_pool.rstrip('/')}/{original_basename}"
                 print_warning(f"Original full path for key '{original_key}' not in metadata, reconstructed as '{original_path_for_naming}' for naming.")

            new_dataset_path = generate_new_dataset_name(original_path_for_naming, original_id, new_id_str, target_zfs_pool_path)
            potential_targets_map[original_key] = new_dataset_path
            if get_zfs_property(new_dataset_path, 'type'):
                print_error(f"Target ZFS dataset '{new_dataset_path}' for key '{original_key}' already exists.")
                dataset_collision_found = True

    if config_collision or dataset_collision_found:
        print_error("Aborting restore due to collision(s).", exit_code=1)
    print_success("No target configuration or dataset collisions found.")

    print_info(f"\n--- Starting ZFS Restore Operations ---")
    print_info(f"Target ZFS Pool Path: {target_zfs_pool_path}")
    print_info(f"Target PVE Storage: {target_pve_storage}")

    restored_datasets_map = {}
    all_data_ops_successful = True
    pv_available = is_tool('pv')
    cleanup_list = []

    if not exported_disks:
        print_info("No ZFS disks to restore based on metadata.")
    else:
        for disk_info in exported_disks:
            original_key = disk_info["key"]
            stream_filename = disk_info["stream_file"]
            data_import_path = import_dir / stream_filename
            new_dataset_path = potential_targets_map[original_key]

            print(f"\n  {color_text(f'Restoring {original_key}', 'CYAN')}")
            print(f"    Input stream:   {color_text(str(data_import_path.name), 'BLUE')}")
            print(f"    Target dataset: {color_text(new_dataset_path, 'GREEN')}")

            if not data_import_path.is_file():
                print_error(f"Data stream file '{data_import_path.name}' not found in {import_dir}. Aborting.")
                all_data_ops_successful = False
                break

            recv_cmd = ['zfs', 'receive', '-o', 'readonly=off', new_dataset_path]
            pipeline_cmds = []
            pipeline_names = []
            pv_opts = None

            cat_cmd = ['cat', str(data_import_path)]
            pipeline_cmds.append(cat_cmd)
            pipeline_names.append("cat")

            if compress_method != "none":
                decompress_cmd = decompress_tool_info["decompress"]
                pipeline_cmds.append(decompress_cmd)
                pipeline_names.append(f"decompress ({compress_method})")

            if pv_available:
                pv_cmd_base = ['pv']
                try:
                    file_size = data_import_path.stat().st_size
                    size_str = f"~{format_bytes(file_size)} (compressed)"
                except Exception:
                    file_size = None
                    size_str = "Unknown size"
                print(f"    Input file size: {size_str}")
                pv_opts = ['-W', '-p', '-t', '-r', '-b', '-N', f'restore-{original_key}']
                pipeline_cmds.append(pv_cmd_base)
                pipeline_names.append("pv")
            else:
                print_warning("    Executing restore without progress bar ('pv' not found).")

            pipeline_cmds.append(recv_cmd)
            pipeline_names.append("zfs receive")

            print(f"    Executing pipeline: {' | '.join([' '.join(c) for c in pipeline_cmds])}")
            pipeline_successful = run_pipeline(pipeline_cmds, pipeline_names, pv_options=pv_opts)

            if pipeline_successful:
                print_success(f"    ZFS data restore successful for {original_key}.")
                restored_datasets_map[original_key] = Path(new_dataset_path).name
                cleanup_list.append(new_dataset_path)
            else:
                print_error(f"Error during ZFS data restore pipeline for {original_key}. Aborting restore.")
                all_data_ops_successful = False
                break

    if not all_data_ops_successful:
        print_error("\n--- Restore Failed During ZFS Operations ---")
        if cleanup_list:
             print_warning("Attempting to clean up successfully restored datasets...")
             for ds_path in reversed(cleanup_list):
                 print_warning(f"    Destroying partially restored dataset: {ds_path}")
                 run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
        else:
             print_info("No datasets were created before failure occurred.")
        sys.exit(1)

    print_info(f"\nCreating and adjusting new configuration file: {color_text(str(new_conf_path), 'BLUE')}")
    config_created_successfully = False
    try:
        shutil.copy2(config_import_path, new_conf_path)
        print_success(f"Copied base configuration from {config_import_path.name} to {new_conf_path}.")

        adjust_config_file(
            conf_path=new_conf_path,
            instance_type=original_instance_type,
            new_id=new_id_str,
            target_pve_storage=target_pve_storage,
            dataset_map=restored_datasets_map,
            name_prefix=f"restored-{new_id_str}-"
        )
        config_created_successfully = True

    except Exception as e:
        print_error(f"Error processing config file {new_conf_path}: {e}")
        if new_conf_path.exists():
            print_warning(f"Removing potentially incomplete config file: {new_conf_path}")
            try: new_conf_path.unlink()
            except OSError: pass
        if cleanup_list:
             print_warning("Attempting to clean up restored ZFS datasets due to config error...")
             for ds_path in reversed(cleanup_list):
                  print_warning(f"    Destroying restored dataset: {ds_path}")
                  run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
        sys.exit(1)

    if all_data_ops_successful and config_created_successfully:
        print(f"\n{color_text('--- Restore Process Finished ---', 'GREEN')}")
        print(f"Restored {original_instance_type.upper()} from export directory '{import_dir}'")
        print(f"  New ID:              {color_text(new_id_str, 'BLUE')}")
        print(f"  Target PVE Storage:  {color_text(target_pve_storage, 'BLUE')}")
        if restored_datasets_map:
             print(f"  Restored Datasets ({len(restored_datasets_map)}):")
             for key, basename in restored_datasets_map.items():
                 full_path = f"{target_zfs_pool_path.rstrip('/')}/{basename}"
                 print(f"    - {key} -> {color_text(full_path, 'BLUE')}")
        else:
             print("  Restored Datasets: None")
        print(f"\n{color_text('Review the configuration:', 'YELLOW')} {color_text(str(new_conf_path), 'BLUE')}")
        print(color_text("Important: Check network settings (IP/MAC), hostname/name, resources, CD-ROMs (VMs), and link_down=1 on NICs.", 'YELLOW'))
    else:
         print(f"\n{color_text('--- Restore Process Failed ---', 'RED')}")


def perform_ram_check(pve_cmd, src_id):
    """Prüft die RAM-Nutzung des Hosts vor dem Klonen einer VM."""
    print_info("\nChecking host RAM usage...")
    try:
        free_output = run_command(['free', '-m'], capture_output=True, check=True)
        mem_line = free_output.split('\n')[1]
        total_ram_mb = int(mem_line.split()[1])

        src_vm_ram_mb = 512 # Default
        qm_config_output = run_command([pve_cmd, 'config', src_id], capture_output=True, suppress_stderr=True, check=True)
        match_mem = re.search(r'^memory:\s*(\S+)', qm_config_output, re.MULTILINE | re.IGNORECASE)
        if match_mem:
            parsed_ram = parse_size_to_mb(match_mem.group(1))
            src_vm_ram_mb = parsed_ram if parsed_ram > 0 else 0

            if src_vm_ram_mb == 0:
                 min_mem_match = re.search(r'^minimum:\s*(\S+)', qm_config_output, re.MULTILINE | re.IGNORECASE)
                 balloon_match = re.search(r'^balloon:\s*(\S+)', qm_config_output, re.MULTILINE | re.IGNORECASE)
                 min_ram_mb = parse_size_to_mb(min_mem_match.group(1)) if min_mem_match else 0
                 balloon_val_str = balloon_match.group(1) if balloon_match else "0"
                 balloon_mb = 0
                 if balloon_val_str.lower() == '0': balloon_mb = 0
                 else: balloon_mb = parse_size_to_mb(balloon_val_str) if balloon_val_str.isdigit() else 512


                 if min_ram_mb > 0:
                      src_vm_ram_mb = min_ram_mb
                      print_warning(f"VM {src_id} 'memory' is 0, using 'minimum' {src_vm_ram_mb} MB for check.")
                 elif balloon_mb > 0:
                      src_vm_ram_mb = balloon_mb
                      print_warning(f"VM {src_id} 'memory' is 0, using 'balloon' {src_vm_ram_mb} MB for check.")
                 else:
                      src_vm_ram_mb = 512
                      print_warning(f"VM {src_id} 'memory' is 0 and no valid 'minimum' or 'balloon' found, using default {src_vm_ram_mb} MB for check.")
        elif 'memory:' in qm_config_output.lower():
             print_warning(f"Could not parse 'memory' value for VM {src_id}, assuming {src_vm_ram_mb} MB for check.")
        else:
             print_warning(f"No 'memory' setting found for VM {src_id}, assuming {src_vm_ram_mb} MB for check.")

        qm_list_output = run_command([pve_cmd, 'list', '--full'], capture_output=True, suppress_stderr=True, check=True)
        sum_running_ram_mb = 0
        running_vm_lines = [line for line in qm_list_output.split('\n')[1:] if 'running' in line.split()] # Skip header
        header_line = qm_list_output.split('\n')[0].lower()
        headers = header_line.split()
        
        mem_index = -1
        for col_name in ['maxmem', 'mem']:
            try: mem_index = headers.index(col_name); break
            except ValueError: pass

        if mem_index == -1:
            print_warning("Could not determine memory column ('maxmem' or 'mem') in 'qm list' output. RAM check might be inaccurate.")
            sum_running_ram_mb = -1
        else:
             for line in running_vm_lines:
                 parts = line.split()
                 vmid = parts[0]
                 if vmid.isdigit() and len(parts) > mem_index:
                     try:
                         ram_bytes = int(parts[mem_index])
                         current_vm_ram_mb = ram_bytes // (1024*1024) if ram_bytes > 0 else 0
                         sum_running_ram_mb += current_vm_ram_mb
                     except (ValueError, IndexError):
                         print_warning(f"Could not parse memory for running VM {vmid} from 'qm list'.")
                         sum_running_ram_mb += 512

        threshold_mb = math.floor(total_ram_mb * RAM_THRESHOLD_PERCENT / 100)
        print(f"    Total host RAM:      {color_text(format_bytes(total_ram_mb*1024*1024), 'BLUE')}")
        if sum_running_ram_mb >= 0:
            print(f"    RAM running VMs (sum):{color_text(format_bytes(sum_running_ram_mb*1024*1024), 'BLUE')}")
            print(f"    Source VM RAM (Est.):{color_text(format_bytes(src_vm_ram_mb*1024*1024), 'BLUE')}")
            prognostic_ram_mb = sum_running_ram_mb + src_vm_ram_mb
            print(f"    Projected Total RAM: {color_text(format_bytes(prognostic_ram_mb*1024*1024), 'BLUE')} (if clone starts)")
            print(f"    {RAM_THRESHOLD_PERCENT}% Threshold:        {color_text(format_bytes(threshold_mb*1024*1024), 'BLUE')}")

            if prognostic_ram_mb > threshold_mb:
                print_warning(f"\nWARNING: Starting the clone might exceed the {RAM_THRESHOLD_PERCENT}% host RAM usage threshold!")
                try:
                    confirm = input(f"{color_text('Continue anyway (y/N)? ', 'RED')}{COLORS['NC']}").strip().lower()
                    if confirm not in ['y', 'yes']:
                        print_error("Operation aborted by user due to RAM concerns.", exit_code=1)
                    else:
                        print_info("Continuing despite RAM warning.")
                except EOFError:
                     print_error("Operation aborted due to RAM concerns (non-interactive).", exit_code=1)
            else: print_success("RAM check passed.")
        else:
             print_warning("Could not reliably sum RAM of running VMs. Skipping threshold check.")

    except subprocess.CalledProcessError as e:
         print_warning(f"\nCould not execute command for RAM check: {e}. Proceeding cautiously.")
    except Exception as e:
        print_warning(f"\nCould not complete RAM check due to unexpected error: {e}. Proceeding cautiously.")


# --- Main Execution ---

def main():
    compress_options = list(COMPRESSION_TOOLS.keys())
    examples = f"""
Examples:

  {color_text('List available VMs/LXCs:', 'YELLOW')}
    {sys.argv[0]} --list

  {color_text('Clone VM 100 to base ID 9100 (linked clone, prompts for snapshot(s), uses default storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} clone 100 9100

  {color_text('Clone LXC 105, prompt for base new ID (full clone, specify target storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} clone 105 --clone-mode full --target-pve-storage tankpve --target-zfs-pool-path tankpve/data

  {color_text('Export VM 101 to /mnt/backup/export (uncompressed, prompts for snapshot(s)):', 'YELLOW')}
    sudo {sys.argv[0]} export 101 /mnt/backup/export

  {color_text('Export LXC 105 to /mnt/backup/export (using zstd, specify source storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} export 105 /mnt/backup/export --compress zstd --source-pve-storage local-zfs --source-zfs-pool-path rpool/data

  {color_text('Restore from /mnt/backup/export/101_snapshotname to new ID 8101 (uses default target storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} restore /mnt/backup/export/101_snapshotname 8101

Configuration Note:
  - Target ZFS pool path for clone/restore defaults to: {color_text(DEFAULT_ZFS_POOL_PATH, 'BLUE')}
  - Target PVE storage name for clone/restore defaults to: {color_text(DEFAULT_PVE_STORAGE, 'BLUE')}
  (These can be overridden using --target-zfs-pool-path and --target-pve-storage options.)
"""
    parser = argparse.ArgumentParser(
        description="Proxmox VM/LXC Clone, Export, or Restore script using ZFS snapshots with multi-select and optional compression.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=examples
    )

    parser.add_argument('--list', action='store_true', help="List available VMs and LXC containers and exit.")
    parser.add_argument('--target-zfs-pool-path', default=DEFAULT_ZFS_POOL_PATH,
                        help=f"Base path for target ZFS datasets (clone/restore). Default: {DEFAULT_ZFS_POOL_PATH}")
    parser.add_argument('--target-pve-storage', default=DEFAULT_PVE_STORAGE,
                        help=f"PVE storage name for target datasets (clone/restore). Default: {DEFAULT_PVE_STORAGE}")

    subparsers = parser.add_subparsers(dest='mode', help='Operation mode (clone, export, restore)', required=False)

    parser_clone = subparsers.add_parser('clone', help='Clone a VM/LXC from one or more ZFS snapshots.', formatter_class=argparse.RawTextHelpFormatter)
    parser_clone.add_argument('source_id', help="ID of the source VM or LXC to clone.")
    parser_clone.add_argument('new_id', nargs='?', default=None,
                              help="Base ID for the new cloned instance(s). (Default: 9<source_id>, will prompt if omitted. Subsequent clones increment this ID).")
    parser_clone.add_argument('--clone-mode', choices=['linked', 'full'], default='linked',
                              help="Type of ZFS clone ('linked' uses 'zfs clone', 'full' uses send/receive). Default: linked")

    parser_export = subparsers.add_parser('export', help='Export a VM/LXC config and ZFS snapshot data for one or more snapshots (optionally compressed).', formatter_class=argparse.RawTextHelpFormatter)
    parser_export.add_argument('source_id', help="ID of the source VM or LXC to export.")
    parser_export.add_argument('export_dir',
                               help="Parent directory where export subdirectories (named after source_id_snapshot_suffix) will be created (e.g., /mnt/backups).")
    parser_export.add_argument('--compress', choices=compress_options, default='none',
                               help=f"Compression method for ZFS streams. Default: none. Options: {', '.join(compress_options)}")
    parser_export.add_argument('--source-zfs-pool-path', default=DEFAULT_ZFS_POOL_PATH,
                               help=f"Base path where source ZFS datasets reside. Default: {DEFAULT_ZFS_POOL_PATH}")
    parser_export.add_argument('--source-pve-storage', default=DEFAULT_PVE_STORAGE,
                               help=f"PVE storage name linked in the source config. Default: {DEFAULT_PVE_STORAGE}")

    parser_restore = subparsers.add_parser('restore', help='Restore a VM/LXC from a specific exported directory (auto-detects compression).', formatter_class=argparse.RawTextHelpFormatter)
    parser_restore.add_argument('import_dir',
                                help="Path to the specific export directory containing the .conf, .meta.json, and data stream files (e.g., /mnt/backups/101_snapshot_suffix).")
    parser_restore.add_argument('new_id', nargs='?', default=None,
                                help="ID for the new restored instance. (Default: 8<original_id>, will prompt if omitted).")

    args = parser.parse_args()

    if args.list:
        if os.geteuid() != 0:
            print_warning("Root privileges might be needed to read all config files for listing.")
        if list_instances():
            sys.exit(0)
        else:
            sys.exit(1)

    if not args.mode:
        parser.print_help()
        print_error("\nError: You must specify an operation mode (clone, export, restore) if not using --list.", exit_code=1)

    if os.geteuid() != 0:
        print_warning("Warning: Root privileges (sudo) are likely required for ZFS/Proxmox commands.")

    if not is_tool('pv'):
        print_warning("Tool 'pv' (Pipe Viewer) not found. Operations involving data streams will not show progress bars.")
    else:
        print_info("Tool 'pv' found, will be used for progress display.")

    if args.mode == 'clone':
        do_clone(args)
    elif args.mode == 'export':
        do_export(args)
    elif args.mode == 'restore':
        do_restore(args)
    else: # Should be caught by "if not args.mode"
        parser.print_help()
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_error("\nOperation cancelled by user (Ctrl+C).", exit_code=130)
    except EOFError:
        print_error("\nOperation aborted due to unexpected end of input.", exit_code=1)
    except Exception as e:
        print_error(f"\nAn unexpected critical error occurred: {e}")
        # import traceback # Uncomment for debugging
        # traceback.print_exc() # Uncomment for debugging
        sys.exit(2)
