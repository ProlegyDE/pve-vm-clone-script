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
    "RED": '\033[91m',
    "GREEN": '\033[92m',
    "YELLOW": '\033[93m',
    "CYAN": '\033[96m',
    "BLUE": '\033[94m',
    "NC": '\033[0m'  # No Color
}

# --- Helper Functions ---

def color_text(text, color_name):
    """Färbt den Text für die Konsolenausgabe."""
    color = COLORS.get(color_name.upper(), COLORS["NC"])
    nc = COLORS["NC"]
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
    # Check specifically for the command name (e.g., 'zstd', 'gunzip')
    return shutil.which(name) is not None

def check_compression_tools(method):
    """Checks if the required compression/decompression tools for a method are available."""
    tool_info = COMPRESSION_TOOLS.get(method) # Get tool_info first

    if not tool_info: # Handle invalid method early
        print_error(f"Internal error: Unknown compression method '{method}' defined.")
        # Return False for checks and None for info, let caller handle None
        return False, False, None

    if method == "none":
        # 'none' method is always 'available', no specific tools needed
        # Return the actual dict for 'none' which contains the correct suffix
        return True, True, tool_info

    # Check actual tool executables for other methods
    compress_cmd_name = tool_info["compress"][0] if tool_info.get("compress") else None
    decompress_cmd_name = tool_info["decompress"][0] if tool_info.get("decompress") else None

    compress_ok = is_tool(compress_cmd_name) if compress_cmd_name else False
    decompress_ok = is_tool(decompress_cmd_name) if decompress_cmd_name else False

    # Only warn if the command was expected but not found
    if not compress_ok and compress_cmd_name:
        print_warning(f"Compression tool '{compress_cmd_name}' for method '{method}' not found.")
    if not decompress_ok and decompress_cmd_name:
        print_warning(f"Decompression tool '{decompress_cmd_name}' for method '{method}' not found.")

    # Return tool availability and the info dict
    return compress_ok, decompress_ok, tool_info

def run_command(cmd_list, check=True, capture_output=True, text=True, error_msg=None, suppress_stderr=False, input_data=None, allow_fail=False):
    """
    Führt einen Shell-Befehl aus und gibt die Ausgabe zurück oder prüft auf Erfolg.
    Args:
        cmd_list (list): The command and its arguments.
        check (bool): If True, raise CalledProcessError on non-zero exit code (unless allow_fail=True).
        capture_output (bool): If True, capture stdout and stderr. If False, output goes to console.
        text (bool): If True, decode stdout/stderr as text.
        error_msg (str, optional): Custom error message on failure.
        suppress_stderr (bool): If True and capture_output=True, redirect stderr to DEVNULL.
        input_data (str, optional): Data to pass to the command's stdin.
        allow_fail (bool): If True, don't raise an exception on failure, instead return (success, stdout, stderr).
    Returns:
        str: Captured stdout if capture_output=True and allow_fail=False.
        tuple: (bool, str, str) representing (success, stdout, stderr) if allow_fail=True.
        None: If capture_output=False and allow_fail=False.
    Raises:
        SystemExit: On command not found or execution error (if check=True and allow_fail=False).
    """
    stdin_setting = subprocess.PIPE if input_data is not None else None

    # Determine stdout/stderr based on capture_output and suppress_stderr
    if capture_output:
        stdout_setting = subprocess.PIPE
        stderr_setting = subprocess.DEVNULL if suppress_stderr else subprocess.PIPE
    else:
        stdout_setting = None # Output goes to console/parent process stdout
        stderr_setting = subprocess.DEVNULL if suppress_stderr else None # Stderr to DEVNULL or console/parent

    try:
        process = subprocess.run(
            cmd_list,
            check=check and not allow_fail, # Don't check if allow_fail is True
            text=text,
            stdout=stdout_setting,
            stderr=stderr_setting,
            input=input_data,
            stdin=stdin_setting,
            errors='replace' # Replace decoding errors if text=True
        )
        # Read stdout/stderr from process object if PIPE was used
        stdout_res = process.stdout.strip() if stdout_setting == subprocess.PIPE and process.stdout else ""
        stderr_res = process.stderr.strip() if stderr_setting == subprocess.PIPE and process.stderr else ""

        if allow_fail:
            return (process.returncode == 0, stdout_res, stderr_res)
        else:
            # If check=False and it failed, stdout_res might still be useful
            if process.returncode != 0:
                 # Optionally warn if check=False but command failed
                 # print_warning(f"Command '{' '.join(cmd_list)}' failed with rc={process.returncode} but check=False.")
                 pass
            # Return stdout if captured, otherwise None
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
            # Only print stderr if it wasn't suppressed during the call
            if not suppress_stderr and stderr_content:
                 print_error(f"Stderr:\n{stderr_content}")
            # Maybe print stdout if stderr was empty/suppressed and stdout exists
            elif hasattr(e, 'stdout') and e.stdout and (suppress_stderr or not stderr_content):
                stdout_content = e.stdout.strip()
                if stdout_content:
                    print_error(f"Stdout (relevant for error?):\n{stdout_content}")
            sys.exit(1)
    except Exception as e:
        msg = error_msg or f"Unexpected error executing '{' '.join(cmd_list)}'"
        print_error(f"{msg}: {e}", exit_code=1)


def run_pipeline(commands, step_names=None, pv_options=None, output_file=None):
    """
    Führt eine Befehlspipeline aus (z.B. cmd1 | pv | compressor | cmd2 > file).
    Kann die Ausgabe des letzten Befehls in eine Datei umleiten.
    Args:
        commands (list[list[str]]): List of commands, where each command is a list of strings.
        step_names (list[str], optional): Names for each step for logging.
        pv_options (list[str], optional): Options to pass to the 'pv' command if present.
        output_file (Path, optional): Path object to redirect the final command's stdout to.
    Returns:
        bool: True if the entire pipeline completed successfully, False otherwise.
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

        # Open output file if specified (use binary mode for streams)
        if output_file:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            final_output_handle = open(output_file, 'wb') # Always use binary for streams

        for i, cmd in enumerate(commands):
            stdin_source = last_process_stdout
            # Determine stdout destination: pipe to next command or to file/stdout
            is_last_command = (i == num_commands - 1)
            stdout_dest = final_output_handle if is_last_command and final_output_handle else subprocess.PIPE

            is_pv_command = (cmd[0] == 'pv')
            # Let pv write to stderr for progress, capture others' stderr
            # Also let compression/decompression tools write to stderr (might show stats)
            stderr_dest = None if is_pv_command or cmd[0] in ['gzip', 'gunzip', 'pigz', 'unpigz', 'zstd', 'unzstd'] else subprocess.PIPE

            # Add pv options if this is the pv command
            current_cmd = cmd[:] # Copy
            if is_pv_command and pv_options:
                current_cmd.extend(pv_options)

            proc = subprocess.Popen(
                current_cmd,
                stdin=stdin_source,
                stdout=stdout_dest,
                stderr=stderr_dest,
                # No text mode here - handle binary streams like zfs send/recv correctly
                bufsize=8192 # Good buffer size for streams
            )
            processes.append(proc)
            process_info.append({'proc': proc, 'command': current_cmd})

            # Close the previous process's stdout pipe if it exists
            # This is important to prevent deadlocks and allow SIGPIPE propagation
            if stdin_source:
                # Need to handle potential BrokenPipeError if the reading process exited early
                try:
                   stdin_source.close()
                except BrokenPipeError:
                    print_warning(f"   Broken pipe closing stdin for: {' '.join(current_cmd)}. Previous process likely exited.")
                except Exception as pipe_err:
                    print_warning(f"   Error closing stdin pipe for {' '.join(current_cmd)}: {pipe_err}")


            # The stdout of the current process becomes the stdin for the next,
            # unless it's the last command writing to a file/stdout.
            if not (is_last_command and final_output_handle):
                 last_process_stdout = proc.stdout


        # --- Wait for completion and check results ---
        return_codes = []
        stderr_outputs = [] # Store captured stderr
        success = True
        timed_out = False

        for idx, info in enumerate(process_info):
            proc = info['proc']
            cmd = info['command']
            # Check if stderr was set to be captured for this command
            capture_stderr = proc.stderr == subprocess.PIPE

            try:
                # Communicate captures remaining stdout/stderr from pipes
                # Timeout is important for potentially long operations
                stdout_data, stderr_data = proc.communicate(timeout=7200) # 2 hours
                rc = proc.returncode
                return_codes.append(rc)

                stderr_content = ""
                if capture_stderr and stderr_data:
                    try:
                        # Try decoding stderr as utf-8, replace errors
                        stderr_content = stderr_data.decode('utf-8', errors='replace').strip()
                    except Exception: # Fallback if decoding fails entirely
                        stderr_content = "<Could not decode stderr>"
                stderr_outputs.append(stderr_content)

                if rc != 0:
                    # Check if the error is expected (e.g., SIGPIPE when reader exits early)
                    # ZFS send | zstd might get SIGPIPE if zstd fails, this is ok-ish
                    # Cat | unzstd | zfs recv might get SIGPIPE if zfs recv fails
                    if rc == -13: # SIGPIPE
                         print_warning(f"Pipeline step {step_names[idx]} ('{' '.join(cmd)}') exited with SIGPIPE (rc={rc}). Often okay if a later step failed.")
                         # Don't immediately mark as failed, let subsequent checks decide
                    else:
                        success = False
                        print_error(f"Pipeline failed at {step_names[idx]}: '{' '.join(cmd)}' (rc={rc})")
                        if stderr_content:
                            print_error(f"Stderr:\n{stderr_content}")
                        # Mark as failed, but let remaining communicate calls finish

            except subprocess.TimeoutExpired:
                print_error(f"Pipeline timed out at {step_names[idx]}: '{' '.join(cmd)}'")
                proc.kill()
                # Try to communicate again to get any remaining output
                try:
                    stdout_data, stderr_data = proc.communicate(timeout=10) # Short timeout for cleanup
                except Exception:
                    pass # Ignore errors during cleanup communicate
                success = False
                timed_out = True
                return_codes.append(proc.returncode if proc.returncode is not None else -1)
                stderr_content = "<Timeout>"
                if capture_stderr and stderr_data:
                    try: stderr_content = stderr_data.decode('utf-8', errors='replace').strip()
                    except Exception: pass
                stderr_outputs.append(stderr_content)
                break # Exit loop on timeout
            except Exception as comm_err:
                print_error(f"Error during communicate() for {step_names[idx]} ('{' '.join(cmd)}'): {comm_err}")
                success = False
                rc = proc.returncode if proc.returncode is not None else 1
                return_codes.append(rc)
                stderr_outputs.append(f"<Communication Error: {comm_err}>")


        # Fill lists if loop broke early
        while len(return_codes) < num_commands: return_codes.append(None)
        while len(stderr_outputs) < num_commands: stderr_outputs.append("<Not executed or error>")

        # Ensure all processes are cleaned up
        for p_info in process_info:
             try:
                 if p_info['proc'].poll() is None: # Still running?
                    # Try terminate first, then kill
                    p_info['proc'].terminate()
                    try: p_info['proc'].wait(timeout=5)
                    except subprocess.TimeoutExpired:
                         print_warning(f"Process {' '.join(p_info['command'])} did not terminate gracefully, killing.")
                         p_info['proc'].kill()
                         p_info['proc'].wait(timeout=5)
             except ProcessLookupError: pass
             except Exception as kill_err:
                  print_warning(f"Error terminating/killing process {' '.join(p_info['command'])}: {kill_err}")
             finally: # Ensure pipes are closed even if termination failed
                 if p_info['proc'].stdin:
                     try: p_info['proc'].stdin.close()
                     except Exception: pass
                 if p_info['proc'].stdout:
                     try: p_info['proc'].stdout.close()
                     except Exception: pass
                 if p_info['proc'].stderr:
                     try: p_info['proc'].stderr.close()
                     except Exception: pass

        # Close the output file handle if it was opened
        if final_output_handle:
            try:
                final_output_handle.close()
            except Exception as close_err:
                 print_warning(f"Error closing output file handle: {close_err}")


        # Final success check: success is True only if *all* non-None return codes are 0
        final_success = success and all(rc == 0 for rc in return_codes if rc is not None)
        # Also check if the number of successful steps matches expected (unless timeout)
        if not timed_out and len([rc for rc in return_codes if rc == 0]) != len(return_codes):
             final_success = False

        if not final_success:
            print_warning("Pipeline completed but some steps failed, timed out, or did not finish correctly.")
        elif timed_out:
             print_error("Pipeline terminated due to timeout.")
             final_success = False


        # If pipeline failed and an output file was specified, try to remove it
        if not final_success and output_file and output_file.exists():
             print_warning(f"Attempting to remove incomplete output file: {output_file}")
             try: output_file.unlink()
             except OSError as del_err: print_warning(f"Could not remove file: {del_err}")


        return final_success

    except FileNotFoundError as e:
        print_error(f"Error in pipeline: Command '{e.filename}' not found.")
        # Cleanup already attempted processes
        for info in process_info:
            try: info['proc'].kill() # Kill directly if setup failed
            except Exception: pass
        if final_output_handle:
             try: final_output_handle.close()
             except: pass
        # Attempt to remove potentially created file
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
        if b == 0: return "0 B" # Handle 0 explicitly
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
    elif size_str.endswith('T'): return int(float(size_str[:-1]) * 1024 * 1024) # Terabytes
    elif size_str.isdigit(): return int(size_str) # Assume MB
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
    name = "<no name/hostname>"
    is_lxc = 'lxc' in conf_path.parts
    config_type = "VM" if 'qemu-server' in conf_path.parts else "LXC"

    try:
        with open(conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Stop at snapshot sections or comments
                if line.startswith('[') or line.startswith('#'): continue

                if line.startswith('name:'):
                    name = line.split(':', 1)[1].strip()
                    # For VMs, name is usually enough, but keep looking for hostname in LXC
                elif is_lxc and line.startswith('hostname:'):
                     # Use hostname for LXC if name wasn't explicitly set
                     if name == "<no name/hostname>": name = line.split(':', 1)[1].strip()
                     break # Hostname is definitive for LXC if name isn't set
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

    print(f" {color_text('VMs:', 'YELLOW')}")
    if vm_conf_files:
        for conf in vm_conf_files:
            vm_id, vm_name, _ = get_instance_details(Path(conf))
            vms.append({'id': vm_id, 'name': vm_name})
            print(f"   {color_text(vm_id, 'BLUE')} : {vm_name}")
    else:
        print(f"   {color_text('No VMs found.', 'YELLOW')}")

    print(f"\n {color_text('LXC Containers:', 'YELLOW')}")
    if lxc_conf_files:
        for conf in lxc_conf_files:
            lxc_id, lxc_name, _ = get_instance_details(Path(conf))
            lxcs.append({'id': lxc_id, 'name': lxc_name})
            print(f"   {color_text(lxc_id, 'BLUE')} : {lxc_name}")
    else:
        print(f"   {color_text('No LXC containers found.', 'YELLOW')}")

    if not vms and not lxcs:
        print(f"\n {color_text('No VMs or LXC containers found on this system.', 'RED')}")
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
                    # Fallback: add with timestamp 0 if parsing fails
                    if line.startswith(f"{dataset}@"):
                         snapshots.append({'name': line.strip(), 'creation_timestamp': 0})
    elif not success and "dataset does not exist" not in stderr:
         print_warning(f"Could not list snapshots for {dataset}. Stderr: {stderr}")
    return snapshots


def get_zfs_property(target, property_name):
    """Ruft einen bestimmten ZFS-Property-Wert ab. Gibt None zurück, wenn nicht gefunden."""
    cmd = ['zfs', 'get', '-H', '-p', '-o', 'value', property_name, target]
    # Use allow_fail to handle non-existent datasets/snapshots gracefully
    success, output, stderr = run_command(cmd, check=False, capture_output=True, suppress_stderr=True, allow_fail=True)
    if success:
        return output.strip()
    else:
        # Check stderr for specific 'does not exist' messages if needed for debugging
        # if "does not exist" in stderr:
        #     pass # Expected for non-existent targets
        return None

def get_snapshot_size_estimate(snapshot_name):
    """Schätzt die Größe eines ZFS-Snapshots für 'zfs send'."""
    cmd = ['zfs', 'send', '-nP', snapshot_name]
    success, output, stderr = run_command(cmd, check=False, capture_output=True, suppress_stderr=True, allow_fail=True)
    if success and output:
        match = re.search(r'^size\s+(\d+)$', output, re.MULTILINE)
        if match: return int(match.group(1))
    # Don't warn here, it's called per disk, could be noisy
    # print_warning(f"Could not estimate size for snapshot {snapshot_name}. Stderr: {stderr}")
    return None

def adjust_config_file(conf_path, instance_type, new_id=None, target_pve_storage=None, dataset_map=None, name_prefix="clone-"):
    """
    Nimmt Anpassungen an einer Konfigurationsdatei für Klonen oder Wiederherstellen vor.
    Args:
        conf_path (Path): Path to the configuration file to adjust.
        instance_type (str): 'vm' or 'lxc'.
        new_id (str, optional): The new ID (not used directly for adjustments, but conceptually relevant).
        target_pve_storage (str): The PVE storage name to use for ZFS volumes in the adjusted config.
        dataset_map (dict): Maps original config keys (e.g., 'scsi0', 'rootfs') to new ZFS dataset basenames.
        name_prefix (str): Prefix to add to the 'name' or 'hostname' property.
    """
    print_info(f"\nAdjusting configuration file: {color_text(str(conf_path), 'BLUE')}")
    if not conf_path.is_file():
        print_error(f"Config file {conf_path} not found for adjustments.", exit_code=1)

    try:
        with open(conf_path, 'r') as f_orig:
            lines = f_orig.readlines()

        modified_lines = []
        changes_made = False
        # Use target_pve_storage if provided, otherwise fallback to the script default
        pve_storage_to_use = target_pve_storage if target_pve_storage else DEFAULT_PVE_STORAGE

        # Regex to find storage lines (match any storage initially, then verify)
        storage_regex_vm = re.compile(r'^(scsi|ide|sata|virtio|efidisk|tpmstate)(\d+):\s*([^#]+)')
        storage_regex_lxc = re.compile(r'^(rootfs|mp\d+):\s*([^#]+)')

        processing_active_config = True
        for line_num, line in enumerate(lines):
            original_line = line
            line_strip = line.strip()
            modified = False

            # Stop processing at first snapshot section
            if line_strip.startswith('['):
                print_warning(f"   Skipping snapshot section starting at line {line_num+1}")
                processing_active_config = False

            if not processing_active_config or not line_strip or line_strip.startswith('#'):
                modified_lines.append(line)
                continue

            # --- General Adjustments ---
            if re.match(r'^\s*onboot:\s*[01]', line_strip) and line_strip != "onboot: 0":
                new_line_content = "onboot: 0"
                line = new_line_content + "\n"
                print(f"   Setting '{color_text('onboot: 0', 'YELLOW')}'")
                modified = True
            elif name_prefix and line_strip.startswith('name:') and not line_strip.split(':', 1)[1].strip().startswith(name_prefix):
                 # Use regex to ensure only the value part is prefixed
                 new_line_content = re.sub(r'(^\s*name:\s*)(.*)', rf'\1{name_prefix}\2', line.strip())
                 line = new_line_content + "\n"
                 print(f"   Adding '{color_text(name_prefix, 'YELLOW')}' prefix to name")
                 modified = True
            elif name_prefix and instance_type == 'lxc' and line_strip.startswith('hostname:') and not line_strip.split(':', 1)[1].strip().startswith(name_prefix):
                 new_line_content = re.sub(r'(^\s*hostname:\s*)(.*)', rf'\1{name_prefix}\2', line.strip())
                 line = new_line_content + "\n"
                 print(f"   Adding '{color_text(name_prefix, 'YELLOW')}' prefix to hostname")
                 modified = True
            elif re.match(r'^\s*net\d+:', line_strip):
                if 'link_down=1' not in line_strip:
                    parts = line_strip.split('#', 1)
                    main_part = parts[0].rstrip()
                    comment_part = f" #{parts[1]}" if len(parts) > 1 else ""
                    # Add comma if needed before adding link_down
                    if main_part.split(':')[-1].strip() and not main_part.endswith(','):
                         main_part += ","
                    main_part += "link_down=1"
                    line = main_part + comment_part + "\n"
                    print(f"   Adding '{color_text('link_down=1', 'YELLOW')}' to network interface: {original_line.strip()}")
                    modified = True

            # --- Storage Adjustments (Dataset Mapping) ---
            match = None
            storage_key = None
            current_storage_name = None
            current_dataset_name = None
            line_options_part = "" # Part after the dataset name (e.g., ,size=XX)

            if instance_type == "vm":
                match = storage_regex_vm.match(line_strip)
                if match:
                    key_base = match.group(1) # scsi, ide, etc.
                    key_num = match.group(2)  # 0, 1, etc.
                    storage_key = f"{key_base}{key_num}" # e.g., scsi0, efidisk0
                    details_part = match.group(3).strip() # e.g., local-zfs:vm-100-disk-0,size=32G or file=/path/to/iso
                    # Check if it uses PVE storage format (storage:volume)
                    storage_match = re.match(r'([^:]+):([^,]+)(.*)', details_part)
                    if storage_match:
                         current_storage_name = storage_match.group(1).strip()
                         current_dataset_name = storage_match.group(2).strip()
                         line_options_part = storage_match.group(3).strip() # Includes leading comma if present

            else: # LXC
                match = storage_regex_lxc.match(line_strip)
                if match:
                    storage_key = match.group(1) # e.g., rootfs, mp0
                    details_part = match.group(2).strip() # e.g., local-zfs:subvol-101-disk-0,size=8G,acl=1
                    storage_match = re.match(r'([^:]+):([^,]+)(.*)', details_part)
                    if storage_match:
                        current_storage_name = storage_match.group(1).strip()
                        current_dataset_name = storage_match.group(2).strip()
                        line_options_part = storage_match.group(3).strip()

            # If this is a storage line that needs mapping AND we have a map entry for it:
            if storage_key and current_dataset_name and dataset_map and storage_key in dataset_map:
                new_dataset_basename = dataset_map[storage_key]
                old_storage_part = f"{current_storage_name}:{current_dataset_name}"
                new_storage_part = f"{pve_storage_to_use}:{new_dataset_basename}"

                # Reconstruct the line: key: new_storage_part + options
                # Ensure options start with comma if they exist
                if line_options_part and not line_options_part.startswith(','):
                    line_options_part = ',' + line_options_part

                newline = f"{storage_key}: {new_storage_part}{line_options_part}\n"

                if newline != line:
                    print(f"   Mapped storage {color_text(storage_key, 'BLUE')} -> {color_text(new_storage_part, 'GREEN')}")
                    line = newline
                    modified = True
                else:
                    # This might happen if the original line was already correct (e.g., during clone)
                    # print_warning(f"   [DEBUG] Line seems unchanged after mapping attempt: {line.strip()}")
                    pass


            # Append the (potentially modified) line
            if not line.endswith('\n'): line += '\n'
            modified_lines.append(line)
            if modified: changes_made = True


        if changes_made:
            # Write back the modified content
            with open(conf_path, 'w') as f_new:
                f_new.writelines(modified_lines)
            print_success("   Configuration adjustments applied.")
        else:
             print_info("   No configuration adjustments needed or applied.")

    except FileNotFoundError:
         # Error should be handled before calling this function
         print_error(f"Config file {conf_path} disappeared before adjustments could be written.", exit_code=1)
    except Exception as e:
        print_error(f"\nError adjusting config file {conf_path}: {e}")
        print_warning(f"Config file {conf_path} may not have been properly adjusted.")


def find_zfs_datasets(conf_path, pve_storage_name, zfs_pool_path):
    """
    Findet ZFS-Datasets, die in einer Konfigurationsdatei für ein bestimmtes Storage referenziert werden.
    Args:
        conf_path (Path): Path to the Proxmox config file.
        pve_storage_name (str): The name of the PVE storage backend (e.g., 'local-zfs').
        zfs_pool_path (str): The base path of the ZFS pool (e.g., 'rpool/data').
    Returns:
        tuple: (dict, str) where dict maps config keys to full dataset paths,
               and str is 'vm' or 'lxc'.
    """
    storage_datasets = {} # {config_key: full_dataset_path}
    instance_type = "vm" if 'qemu-server' in conf_path.parts else "lxc"

    # Regex needs to match the specific PVE storage name provided
    # It looks for lines like: key: storage:volume,... or key: volume,... (assuming default storage)
    # Make sure pve_storage_name is properly escaped in case it contains special regex characters
    escaped_pve_storage_name = re.escape(pve_storage_name)
    storage_regex_vm = re.compile(rf'^(scsi|ide|sata|virtio|efidisk|tpmstate)(\d+):\s*(?:{escaped_pve_storage_name}:)?([^,\s]+)')
    storage_regex_lxc = re.compile(rf'^(rootfs|mp\d+):\s*(?:{escaped_pve_storage_name}:)?([^,\s]+)')

    print_info(f"Searching for ZFS datasets in {conf_path} linked to storage '{pve_storage_name}' (Pool: {zfs_pool_path})...")
    try:
        with open(conf_path, 'r') as f:
            processing_current_config = True
            for line_num, line in enumerate(f):
                line = line.strip()
                # Stop at snapshot sections
                if line.startswith('['):
                    processing_current_config = False
                if not processing_current_config: continue

                if not line or line.startswith('#') or line.startswith('parent:'): continue

                match = None; key = ""; dataset_name_part = ""
                if instance_type == "vm":
                    match = storage_regex_vm.match(line)
                    if match:
                        key_base = match.group(1) # scsi, ide, etc.
                        key_num = match.group(2)  # 0, 1, etc.
                        key = f"{key_base}{key_num}" # e.g., scsi0, efidisk0
                        # Group 3 captures the volume name, possibly including the storage prefix if it wasn't the one we searched for
                        potential_volume = match.group(3).split(',')[0].strip()
                        # If it contains ':' OR if the line explicitly matched the target storage name, use it
                        # This logic needs care: if the line was `scsi0: other-storage:vm-100-disk-0`, group 3 is `other-storage:vm-100-disk-0`
                        # If the line was `scsi0: target-storage:vm-100-disk-0`, group 3 is `vm-100-disk-0` (because the prefix was optionally matched)
                        # If the line was `scsi0: vm-100-disk-0`, group 3 is `vm-100-disk-0`
                        line_matches_target_storage = f"{escaped_pve_storage_name}:" in match.group(0)
                        if ':' not in potential_volume or line_matches_target_storage:
                            dataset_name_part = potential_volume
                else: # LXC
                    match = storage_regex_lxc.match(line)
                    if match:
                        key = match.group(1) # 'rootfs' or 'mpX'
                        potential_volume = match.group(2).split(',')[0].strip()
                        line_matches_target_storage = f"{escaped_pve_storage_name}:" in match.group(0)
                        if ':' not in potential_volume or line_matches_target_storage:
                             dataset_name_part = potential_volume

                if key and dataset_name_part:
                    # Build the full dataset path
                    # Assume dataset_name_part is relative to zfs_pool_path
                    # Handle cases where it might already include the pool path (less likely for standard PVE configs)
                    if dataset_name_part.startswith(zfs_pool_path + '/'):
                        full_dataset_path = dataset_name_part
                    elif '/' in dataset_name_part and not dataset_name_part.startswith('/'):
                        # Looks like a relative path but contains slashes - might be complex pool layout
                        full_dataset_path = f"{zfs_pool_path.rstrip('/')}/{dataset_name_part}"
                        print_warning(f"   (Line {line_num+1}) Interpreting relative path '{dataset_name_part}' as '{full_dataset_path}' under pool '{zfs_pool_path}'")
                    else: # Simple name, prepend pool path
                        full_dataset_path = f"{zfs_pool_path.rstrip('/')}/{dataset_name_part}"

                    # Check if the dataset actually exists using zfs get
                    if get_zfs_property(full_dataset_path, 'type'):
                         storage_datasets[key] = full_dataset_path
                         print(f"   Found: {color_text(key, 'BLUE')} -> {full_dataset_path}")
                    else:
                        print_warning(f"   Dataset for {color_text(key, 'BLUE')} ('{full_dataset_path}') not found via 'zfs get type'. Skipping.")

    except FileNotFoundError:
        print_error(f"Configuration file {conf_path} not found.", exit_code=1)
    except Exception as e:
        print_error(f"Error reading {conf_path}: {e}", exit_code=1)

    if not storage_datasets:
        print_warning(f"No existing ZFS datasets found for storage '{pve_storage_name}' (Pool: '{zfs_pool_path}') in {conf_path}.")

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
            # Find the mountpoint with the lowest index (mp0, mp1, ...)
            mp_keys = sorted([k for k in storage_datasets if k.startswith('mp')], key=lambda x: int(x[2:]))
            if mp_keys: ref_key = mp_keys[0]
            else: # Fallback to the first key alphabetically if no rootfs or mpX found
                sorted_keys = sorted(storage_datasets.keys())
                if sorted_keys: ref_key = sorted_keys[0]

            if ref_key: print_warning(f"LXC 'rootfs' not found or not on ZFS, using '{ref_key}' as reference.")
            else: print_error("LXC has no 'rootfs' or 'mpX' datasets on the specified ZFS storage."); return None, None
        ref_dataset = storage_datasets[ref_key]

    else: # VM
        # Prioritize disks with numbers (scsi0, virtio1, etc.), lowest number first
        disk_num_regex = re.compile(r'(scsi|ide|sata|virtio)(\d+)$')
        numbered_disks = {} # {disk_num: {'key': key, 'dataset': dataset}}
        efi_key = None; efi_dataset = None
        tpm_key = None; tpm_dataset = None

        for key, dataset in storage_datasets.items():
             match = disk_num_regex.match(key)
             if match:
                 disk_num = int(match.group(2))
                 numbered_disks[disk_num] = {'key': key, 'dataset': dataset}
             elif key.startswith('efidisk') and not efi_key: # Track first EFI disk
                 efi_key = key; efi_dataset = dataset
             elif key.startswith('tpmstate') and not tpm_key: # Track first TPM state
                 tpm_key = key; tpm_dataset = dataset

        if numbered_disks:
            min_disk_num = min(numbered_disks.keys())
            ref_key = numbered_disks[min_disk_num]['key']
            ref_dataset = numbered_disks[min_disk_num]['dataset']
        elif efi_key: # Fallback to EFI disk if no numbered disks found
            ref_key = efi_key; ref_dataset = efi_dataset
            print_warning(f"No standard numbered disk found, using EFI disk '{ref_key}' as reference.")
        elif tpm_key: # Fallback to TPM state disk if no EFI disk found
            ref_key = tpm_key; ref_dataset = tpm_dataset
            print_warning(f"No standard numbered disk or EFI disk found, using TPM state disk '{ref_key}' as reference.")
        else: # Final fallback to the first dataset found alphabetically
            sorted_keys = sorted(storage_datasets.keys())
            if sorted_keys:
                ref_key = sorted_keys[0]
                ref_dataset = storage_datasets[ref_key]
                print_warning(f"No standard numbered disk, EFI disk, or TPM state disk found, using first dataset '{ref_key}' as reference.")
            else: # Should be caught earlier, but defensive check
                print_error("No suitable reference dataset could be determined.")
                return None, None

    print_info(f"Using reference dataset for snapshot operations: {color_text(ref_key, 'BLUE')} ({color_text(ref_dataset,'CYAN')})")
    return ref_key, ref_dataset


def select_snapshot(ref_dataset):
    """Lässt den Benutzer einen Snapshot aus einer Liste auswählen."""
    snapshots = list_snapshots(ref_dataset)
    if not snapshots:
        print_error(f"No snapshots found for reference dataset {ref_dataset}.", exit_code=1)

    print("\nAvailable snapshots (newest first):")
    # Sort by timestamp descending (newest first) for display
    snapshots.sort(key=lambda x: x['creation_timestamp'], reverse=True)
    for i, snap in enumerate(snapshots):
        snap_suffix = snap['name'].split('@', 1)[1]
        creation_dt = datetime.fromtimestamp(snap['creation_timestamp']) if snap['creation_timestamp'] else None
        human_time = creation_dt.strftime('%Y-%m-%d %H:%M:%S') if creation_dt else "Unknown time"
        print(f"   {color_text(f'[{i}]', 'BLUE')} {snap_suffix} {color_text(f'({human_time})', 'YELLOW')}")

    while True:
        try:
            idx_input = input("Enter the index of the snapshot to use: ").strip()
            if not idx_input:
                print_error("No index entered. Please select a snapshot.")
                continue
            idx = int(idx_input)
            if 0 <= idx < len(snapshots):
                selected_snapshot_info = snapshots[idx]
                selected_snapshot_full_name = selected_snapshot_info['name']
                # Extract the suffix part (after '@')
                snap_suffix = selected_snapshot_full_name.split('@', 1)[1]
                print_success(f"Selected snapshot suffix: {snap_suffix}")
                # Return the full name of the selected snapshot AND its suffix
                return selected_snapshot_full_name, snap_suffix
            else: print_error(f"Index out of range (must be 0 to {len(snapshots)-1}).")
        except ValueError: print_error("Invalid input. Please enter a number.")
        except EOFError: print_error("\nOperation aborted by user (EOF).", exit_code=1)


def generate_new_dataset_name(old_dataset_path, old_id, new_id, target_zfs_pool_path):
    """
    Generiert einen neuen Dataset-Namen für Klonen/Wiederherstellen.
    Versucht, die alte ID im Namen durch die neue ID zu ersetzen.
    """
    old_dataset_name = Path(old_dataset_path).name
    new_dataset_name = old_dataset_name

    # Try to replace the old ID with the new ID using common delimiters
    # Prioritize replacing `-old_id-` or `_old_id_` etc.
    patterns_to_try = [
        (rf"(-{old_id}-)", f"-{new_id}-"),  # -100- -> -9100-
        (rf"(-{old_id}$)", f"-{new_id}"),   # -100  -> -9100 (at end)
        (rf"(^{old_id}-)", f"{new_id}-"),   # 100-  -> 9100- (at start)
        (rf"(_-_{old_id}_-_)", f"_-_{new_id}_-_"), # _-_100_-_ -> _-_9100_-_ (more specific?)
        (rf"({old_id})", f"{new_id}"),      # 100 -> 9100 (fallback, less specific)
    ]
    replaced = False
    for pattern, replacement in patterns_to_try:
        temp_name, num_subs = re.subn(pattern, replacement, old_dataset_name, count=1)
        if num_subs > 0:
            new_dataset_name = temp_name
            replaced = True
            break # Stop after first successful replacement

    if not replaced:
        # Final fallback if no pattern matched: prepend new ID? Or append? Append seems safer.
        new_dataset_name = f"{old_dataset_name}_newid_{new_id}"
        print_warning(f"Could not reliably replace ID '{old_id}' in '{old_dataset_name}'. Using fallback name: '{new_dataset_name}'")

    # Ensure it's joined correctly with the target pool path
    target_pool_base = target_zfs_pool_path.rstrip('/')
    return f"{target_pool_base}/{new_dataset_name}"


# --- Mode Functions ---

def do_clone(args):
    """Führt den Klonvorgang durch."""
    print_info("=== Running Clone Mode ===")
    src_id = args.source_id
    new_id = args.new_id
    clone_mode = args.clone_mode
    # Use arguments passed, falling back to defaults if not provided
    target_zfs_pool_path = args.target_zfs_pool_path
    target_pve_storage = args.target_pve_storage

    # --- Find Source Instance ---
    src_conf_path, src_instance_type = find_instance_config(src_id)
    if not src_conf_path:
        print_error(f"Error: No VM or LXC with ID {src_id} found.", exit_code=1)

    src_instance_id_str, src_instance_name, config_type_str = get_instance_details(src_conf_path)
    print_success(f"Selected source: ID {src_instance_id_str} ({config_type_str}: {src_instance_name})")
    pve_cmd = "qm" if src_instance_type == "vm" else "pct"
    conf_dir = src_conf_path.parent

    # --- Determine and Validate Target ID ---
    if not new_id:
        default_new_id = f"9{src_id}" # Simple default prefix
        try:
            new_id_input = input(f"Enter the new {config_type_str} ID (blank for default={default_new_id}): ").strip()
            new_id = new_id_input or default_new_id
            if not new_id.isdigit() or int(new_id) <= 0:
                 print_error(f"Invalid new ID '{new_id}'. Must be a positive integer.", exit_code=1)
            if new_id == default_new_id and not new_id_input:
                 print_warning(f"Using default ID: {new_id}")
        except ValueError:
             print_error(f"Invalid input for new ID.", exit_code=1)
        except EOFError:
            print_error("\nNon-interactive mode: New ID must be provided as an argument.", exit_code=1)


    # Check for config file collision
    new_conf_path_vm = Path("/etc/pve/qemu-server") / f"{new_id}.conf"
    new_conf_path_lxc = Path("/etc/pve/lxc") / f"{new_id}.conf"
    collision = False
    if new_conf_path_vm.exists(): print_error(f"Config file for VM ID {new_id} ({new_conf_path_vm}) already exists!"); collision = True
    if new_conf_path_lxc.exists(): print_error(f"Config file for LXC ID {new_id} ({new_conf_path_lxc}) already exists!"); collision = True

    if collision: sys.exit(1)
    # Determine the correct target path based on source type
    new_conf_path = new_conf_path_vm if src_instance_type == "vm" else new_conf_path_lxc

    # --- Clone Mode Info ---
    print_info(f"Selected mode: {clone_mode.capitalize()} Clone")

    # --- RAM Check (VMs only) ---
    if src_instance_type == "vm":
        perform_ram_check(pve_cmd, src_id)
    else: print_info("\nSkipping RAM check for LXC containers.")

    # --- Find ZFS Datasets ---
    # Use the *target* PVE storage and *target* ZFS pool path to find relevant source datasets
    # This assumes the source datasets are structured similarly or under the same storage/pool as the target
    storage_datasets, _ = find_zfs_datasets(src_conf_path, target_pve_storage, target_zfs_pool_path)
    if not storage_datasets:
        print_error(f"No ZFS datasets found for storage '{target_pve_storage}' (Pool: '{target_zfs_pool_path}') in {src_conf_path}. Cannot clone.", exit_code=1)

    # --- Select Reference Dataset & Snapshot ---
    ref_key, ref_dataset = select_reference_dataset(storage_datasets, src_instance_type)
    if not ref_key: sys.exit(1) # Error already printed by select_reference_dataset

    selected_snapshot_full_name, snap_suffix = select_snapshot(ref_dataset)

    # --- ZFS Clone Operations ---
    print_info(f"\n--- Starting ZFS {clone_mode.capitalize()} Clone Operations ---")
    print_info(f"Target ZFS Pool Path: {target_zfs_pool_path}")
    print_info(f"Target PVE Storage: {target_pve_storage}")

    cloned_datasets_map = {} # {key: new_dataset_basename}
    all_ops_successful = True
    pv_available = is_tool('pv')
    cleanup_list = [] # Keep track of created datasets for potential rollback

    # Check for potential target dataset collisions *before* starting operations
    print_info("Checking for potential target dataset collisions...")
    potential_targets = {}
    collision_found = False
    for key, dataset in storage_datasets.items():
        # Use the target ZFS pool path provided by argument/default
        new_dataset = generate_new_dataset_name(dataset, src_id, new_id, target_zfs_pool_path)
        potential_targets[key] = new_dataset
        if get_zfs_property(new_dataset, 'type'):
            print_error(f"Target dataset '{new_dataset}' for key '{key}' already exists.")
            collision_found = True
    if collision_found:
        print_error("Aborting due to target dataset collision(s).", exit_code=1)
    print_success("No target dataset collisions found.")


    # --- Execute Clones ---
    for key, dataset in storage_datasets.items():
        # Construct the source snapshot name for this specific dataset using the chosen suffix
        target_snapshot = f"{dataset}@{snap_suffix}"
        new_dataset = potential_targets[key] # Use pre-calculated name

        print(f"\n {color_text(f'Processing {key}:', 'CYAN')}")
        print(f"    Source dataset:  {color_text(dataset, 'BLUE')}")
        print(f"    Source snapshot: {color_text(target_snapshot, 'BLUE')}")
        print(f"    Target dataset:  {color_text(new_dataset, 'GREEN')}")

        # Check snapshot existence for THIS dataset
        if not get_zfs_property(target_snapshot, 'type'):
             print_warning(f"    [WARN] Snapshot '{target_snapshot}' does not exist for this specific dataset. Skipping.")
             continue # Skip this disk

        op_success = False
        if clone_mode == 'linked':
            clone_cmd = ['zfs', 'clone', target_snapshot, new_dataset]
            print(f"    Executing linked clone: {' '.join(clone_cmd)}")
            try:
                # Run without capturing output, let zfs clone show messages/errors
                run_command(clone_cmd, check=True, capture_output=False, error_msg="ZFS clone failed")
                print_success("    Linked clone successful.")
                op_success = True
            except SystemExit:
                # Error message printed by run_command
                print_error("    Error during 'zfs clone'.")
                all_ops_successful = False
                # Do not proceed further if a clone fails
                break

        else: # Full clone (send/receive)
            print(f"    Preparing full clone (send/receive)...")
            estimated_size_bytes = get_snapshot_size_estimate(target_snapshot)
            size_str = f"~{format_bytes(estimated_size_bytes)}" if estimated_size_bytes is not None else "Unknown size"
            print(f"    Estimated size: {size_str}")

            send_cmd = ['zfs', 'send', target_snapshot]
            recv_cmd = ['zfs', 'receive', '-o', 'readonly=off', new_dataset] # Ensure cloned dataset is writable
            pipeline_cmds = [send_cmd]
            pipeline_names = ["zfs send"]
            pv_opts = None

            if pv_available:
                pv_cmd_base = ['pv']
                # Keep -p (percent), -t (time), -r (rate), -b (bytes)
                # -N gives a name to the progress bar
                pv_opts = ['-p', '-t', '-r', '-b', '-N', f'clone-{key}']
                if estimated_size_bytes: pv_opts.extend(['-s', str(estimated_size_bytes)])
                pipeline_cmds.append(pv_cmd_base)
                pipeline_names.append("pv")
            else:
                print_warning("    Executing full clone without progress bar ('pv' not found).")

            pipeline_cmds.append(recv_cmd)
            pipeline_names.append("zfs receive")

            print(f"    Executing pipeline: {' | '.join([' '.join(c) for c in pipeline_cmds])}")
            pipeline_successful = run_pipeline(pipeline_cmds, pipeline_names, pv_options=pv_opts, output_file=None)

            if pipeline_successful:
                 print_success("    Full clone (send/receive) successful.")
                 op_success = True
            else:
                # Error message printed by run_pipeline
                 print_error("    Error during 'zfs send/receive' pipeline.")
                 all_ops_successful = False
                 # Do not proceed further if a clone fails
                 break

        if op_success:
            cloned_datasets_map[key] = Path(new_dataset).name # Store basename for config adjustment
            cleanup_list.append(new_dataset) # Add to cleanup list in case config fails later
        else:
             # Error already printed, loop broken if necessary
             pass

    # --- Post-ZFS Operations ---
    if not all_ops_successful:
         print_error(f"\nOne or more ZFS {clone_mode} clone operations failed. Attempting cleanup...", exit_code=1)
         # Attempt to destroy datasets created so far
         for ds_path in reversed(cleanup_list): # Destroy in reverse order of creation
             print_warning(f"    Destroying partially created dataset: {ds_path}")
             run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
         sys.exit(1)

    if not cloned_datasets_map:
        print_error("\nNo datasets were successfully processed or created. Cannot create config.", exit_code=1)

    # --- Create New Configuration File ---
    print_info(f"\nCreating new {config_type_str} configuration: {color_text(str(new_conf_path), 'BLUE')}")
    config_created_successfully = False
    try:
        shutil.copy2(src_conf_path, new_conf_path)
        print_success(f"Copied base configuration from {src_conf_path} to {new_conf_path}.")

        adjust_config_file(
            conf_path=new_conf_path,
            instance_type=src_instance_type,
            new_id=new_id, # Pass new_id for potential future use in adjustments
            target_pve_storage=target_pve_storage, # Pass the target storage name
            dataset_map=cloned_datasets_map,
            name_prefix="clone-"
        )
        config_created_successfully = True

    except Exception as e:
        print_error(f"Error processing config file {new_conf_path}: {e}")
        # Attempt cleanup of config file AND cloned datasets
        if new_conf_path.exists():
            print_warning(f"Removing potentially incomplete config file: {new_conf_path}")
            try: new_conf_path.unlink()
            except OSError as del_err: print_warning(f"Could not remove config file: {del_err}")

        print_warning("Attempting to clean up cloned ZFS datasets due to config error...")
        for ds_path in reversed(cleanup_list):
             print_warning(f"    Destroying cloned dataset: {ds_path}")
             run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
        sys.exit(1)


    # --- Final Message ---
    print(f"\n{color_text('--- Clone Process Finished ---', 'GREEN')}")
    final_message = f"New {config_type_str} with ID {color_text(new_id, 'BLUE')} created as a {color_text(clone_mode + ' clone', 'YELLOW')} "
    final_message += f"from snapshot '{color_text(snap_suffix, 'CYAN')}' of {src_id}."
    print(final_message)
    print(f"Target ZFS Pool Path: {color_text(target_zfs_pool_path, 'BLUE')}")
    print(f"Target PVE Storage: {color_text(target_pve_storage, 'BLUE')}")
    print(f"{color_text('Review the configuration:', 'YELLOW')} {color_text(str(new_conf_path), 'BLUE')}")
    print(color_text("Important checks: Network settings (IP/MAC), Hostname/Name, Resources, CD-ROMs (VMs), link_down=1 on NICs.", 'YELLOW'))


def do_export(args):
    """Führt den Exportvorgang durch."""
    print_info("=== Running Export Mode ===")
    src_id = args.source_id
    parent_export_dir = Path(args.export_dir)
    compress_method = args.compress
    # Use specific source paths for export
    source_zfs_pool_path = args.source_zfs_pool_path
    source_pve_storage = args.source_pve_storage

    # --- Check Compression Tool Availability ---
    # _ ignores decompress_ok as it's not needed for export itself
    compress_ok, _, compress_tool_info = check_compression_tools(compress_method)

    # This check ensures compress_tool_info is a dictionary before proceeding.
    # It should catch internal errors from check_compression_tools if any.
    if compress_tool_info is None:
        print_error(f"Failed to get compression tool info for method '{compress_method}'. Aborting export.", exit_code=1)

    # Check if the *required* tool for the *selected* non-none method is available
    if compress_method != "none" and not compress_ok:
        # Tool specific warning was already printed by check_compression_tools
        print_error(f"Required compression tool for method '{compress_method}' not found. Aborting export.", exit_code=1)

    if compress_method != "none":
         print_info(f"Using compression method: {compress_method}")

    # --- Define and Create Export Directory ---
    # Create a subdirectory named after the source ID
    export_dir = parent_export_dir / src_id
    print_info(f"Target export directory: {color_text(str(export_dir), 'BLUE')}")
    try:
        export_dir.mkdir(parents=True, exist_ok=False) # exist_ok=False to prevent overwriting previous exports
        # Test writability
        with tempfile.NamedTemporaryFile(prefix='write_test_', dir=export_dir, delete=True): pass
    except FileExistsError:
        print_error(f"Export directory '{export_dir}' already exists. Please remove it or choose a different parent directory.", exit_code=1)
    except PermissionError:
         print_error(f"Permission denied: Cannot create or write to export directory '{export_dir}'. Check permissions.", exit_code=1)
    except Exception as e:
        print_error(f"Failed to create or access export directory '{export_dir}': {e}", exit_code=1)
    print_success(f"Using export directory: {export_dir}")

    # --- Find Source Instance ---
    src_conf_path, src_instance_type = find_instance_config(src_id)
    if not src_conf_path:
        print_error(f"Error: No VM or LXC with ID {src_id} found.", exit_code=1)

    src_instance_id_str, src_instance_name, config_type_str = get_instance_details(src_conf_path)
    print_success(f"Selected source: ID {src_instance_id_str} ({config_type_str}: {src_instance_name})")

    # --- Find ZFS Datasets ---
    # Use the *source* arguments here
    storage_datasets, _ = find_zfs_datasets(src_conf_path, source_pve_storage, source_zfs_pool_path)
    if not storage_datasets:
        print_error(f"No ZFS datasets found for source storage '{source_pve_storage}' (Pool: '{source_zfs_pool_path}') in {src_conf_path}. Cannot export.", exit_code=1)

    # --- Select Reference Dataset & Snapshot ---
    ref_key, ref_dataset = select_reference_dataset(storage_datasets, src_instance_type)
    if not ref_key: sys.exit(1) # Error already printed

    selected_snapshot_full_name, snap_suffix = select_snapshot(ref_dataset)
    ref_snapshot_name = f"{ref_dataset}@{snap_suffix}" # Full name of the reference snapshot

    # --- Export Config ---
    config_export_path = export_dir / f"{src_id}{DEFAULT_EXPORT_CONFIG_SUFFIX}"
    print_info(f"\nExporting configuration to: {color_text(str(config_export_path), 'BLUE')}")
    try:
        shutil.copy2(src_conf_path, config_export_path)
        print_success(f"Configuration file exported successfully.")
    except Exception as e:
        print_error(f"Failed to export configuration file: {e}", exit_code=1)

    # --- Export ZFS Data (All Disks using the selected snapshot suffix) ---
    print_info(f"\n--- Starting ZFS Data Export (Snapshot Suffix: {snap_suffix}) ---")
    exported_disks_metadata = []
    all_data_ops_successful = True
    pv_available = is_tool('pv')

    for key, dataset_path in storage_datasets.items():
        # Construct snapshot name for the current dataset
        target_snapshot = f"{dataset_path}@{snap_suffix}"

        # Determine output filename based on compression
        data_suffix = compress_tool_info["suffix"]
        stream_filename = f"{key}{data_suffix}"
        data_export_path = export_dir / stream_filename

        print(f"\n {color_text(f'Exporting {key}:', 'CYAN')}")
        print(f"    Source dataset:  {color_text(dataset_path, 'BLUE')}")
        print(f"    Source snapshot: {color_text(target_snapshot, 'BLUE')}")
        print(f"    Output file:     {color_text(str(data_export_path), 'BLUE')}")

        # Check snapshot exists for this specific dataset
        if not get_zfs_property(target_snapshot, 'type'):
            print_warning(f"    [WARN] Snapshot '{target_snapshot}' does not exist for this dataset. Skipping export for {key}.")
            continue # Skip this disk

        estimated_size_bytes = get_snapshot_size_estimate(target_snapshot)
        size_str = f"~{format_bytes(estimated_size_bytes)}" if estimated_size_bytes is not None else "Unknown size"
        print(f"    Estimated raw size: {size_str}")

        # Prepare commands for pipeline: zfs send | [pv] | [compressor] > file
        send_cmd = ['zfs', 'send', target_snapshot]
        pipeline_cmds = [send_cmd]
        pipeline_names = ["zfs send"]
        pv_opts = None

        if pv_available:
            pv_cmd_base = ['pv']
            # Add -W (wait) for export might help ensure file is flushed before next step?
            pv_opts = ['-W', '-p', '-t', '-r', '-b', '-N', f'export-{key}']
            if estimated_size_bytes: pv_opts.extend(['-s', str(estimated_size_bytes)])
            pipeline_cmds.append(pv_cmd_base)
            pipeline_names.append("pv")
        else:
            print_warning("    Executing export without progress bar ('pv' not found).")

        # Add compression command if needed
        if compress_method != "none":
            compress_cmd = compress_tool_info["compress"]
            pipeline_cmds.append(compress_cmd)
            pipeline_names.append(compress_method) # Use method name for step name

        # Execute pipeline with output redirection to file
        print(f"    Executing pipeline: {' | '.join([' '.join(c) for c in pipeline_cmds])} > {data_export_path}")
        pipeline_successful = run_pipeline(pipeline_cmds, pipeline_names, pv_options=pv_opts, output_file=data_export_path)

        if pipeline_successful:
             print_success(f"    ZFS data for {key} exported successfully.")
             exported_disks_metadata.append({
                 'key': key,
                 'original_dataset_basename': Path(dataset_path).name, # Store original basename
                 'original_dataset_path': dataset_path, # Store full original path too
                 'stream_file': stream_filename,
                 'stream_suffix': data_suffix # Store the suffix used
             })
        else:
            print_error(f"    Error during ZFS data export for {key}.")
            all_data_ops_successful = False
            # If one disk fails, should we abort the whole export? Or continue?
            # Let's continue but report failure at the end.
            # Optionally try to remove the failed partial file
            if data_export_path.exists():
                 print_warning(f"Attempting to remove potentially incomplete file: {data_export_path}")
                 try: data_export_path.unlink()
                 except OSError as del_err: print_warning(f"Could not remove file: {del_err}")


    # --- Write Metadata File ---
    meta_export_path = export_dir / f"{src_id}{DEFAULT_EXPORT_META_SUFFIX}"
    print_info(f"\nWriting metadata file: {color_text(str(meta_export_path), 'BLUE')}")

    if not exported_disks_metadata and all_data_ops_successful:
         # This case means snapshots existed but maybe for none of the actual disks?
         print_warning("No disk data was exported (maybe snapshots only existed for reference disk?). Metadata file will be minimal.")
         # We should still write a metadata file indicating the attempt.

    metadata = {
        "exported_at": datetime.now().isoformat(),
        "script_version": "pve-zfs-utility-v_compressed", # Add a version marker
        "source_id": src_id,
        "source_instance_type": src_instance_type,
        "source_config_file": config_export_path.name,
        "source_pve_storage": source_pve_storage, # Record the source storage used
        "source_zfs_pool_path": source_zfs_pool_path, # Record the source pool used
        "snapshot_suffix": snap_suffix,
        "reference_snapshot_name": ref_snapshot_name, # Full name of the ref snapshot used for selection
        "compression_method": compress_method, # Store the compression method used
        "exported_disks": exported_disks_metadata # List of dicts for successfully exported disks
    }
    try:
        with open(meta_export_path, 'w') as f_meta:
            json.dump(metadata, f_meta, indent=4)
        print_success(f"Metadata file written successfully.")
    except Exception as e:
        print_error(f"Failed to write metadata file: {e}")
        all_data_ops_successful = False # Mark export as failed if metadata fails

    # --- Final Message ---
    print(f"\n{color_text('--- Export Process Finished ---', 'GREEN' if all_data_ops_successful else 'YELLOW')}")
    print(f"Exported {config_type_str} ID {src_id} (using snapshot suffix '{snap_suffix}')")
    if compress_method != "none":
        print(f"  Compression:      {compress_method}")
    print(f"  Target Directory: {export_dir}")
    print(f"  Config File:      {config_export_path.name}")
    print(f"  Metadata File:    {meta_export_path.name}")
    if exported_disks_metadata:
        print(f"  Data Files ({len(exported_disks_metadata)} exported):")
        for disk_info in exported_disks_metadata:
            print(f"    - {disk_info['stream_file']} (for key: {disk_info['key']}, original: {disk_info['original_dataset_basename']})")
    else:
         print(f"  Data Files:       {color_text('None exported.', 'YELLOW')}")


    if not all_data_ops_successful:
        print_warning("\nNote: Some ZFS data export operations or metadata writing may have failed. Review logs carefully.")
        print(color_text("The export might be incomplete or unusable.", "RED"))
        # Suggest removing the directory?
        print(color_text(f"Consider removing the potentially incomplete export directory: {export_dir}", "YELLOW"))
    else:
        print(color_text("\nStore the entire export directory securely.", "YELLOW"))


def do_restore(args):
    """Führt den Wiederherstellungsvorgang durch."""
    print_info("=== Running Restore Mode ===")
    import_dir = Path(args.import_dir).resolve() # Use resolved path
    new_id = args.new_id
    # Use arguments passed, falling back to defaults if not provided
    target_zfs_pool_path = args.target_zfs_pool_path
    target_pve_storage = args.target_pve_storage

    # --- Validate Import Directory and Files ---
    print_info(f"Checking import directory: {color_text(str(import_dir), 'BLUE')}")
    if not import_dir.is_dir():
        print_error(f"Import directory not found or not a directory: {import_dir}", exit_code=1)

    # Find the metadata file (expect exactly one)
    potential_meta_files = list(import_dir.glob(f"*{DEFAULT_EXPORT_META_SUFFIX}"))
    if not potential_meta_files:
        print_error(f"No metadata file (*{DEFAULT_EXPORT_META_SUFFIX}) found in {import_dir}", exit_code=1)
    if len(potential_meta_files) > 1:
        print_warning(f"Multiple metadata files found in {import_dir}. Using the first one: {potential_meta_files[0].name}")
    meta_import_path = potential_meta_files[0]

    # --- Read Metadata ---
    print_info(f"Reading metadata from: {meta_import_path.name}")
    metadata = None
    compress_method = "none" # Default if not in metadata
    decompress_tool_info = None
    try:
        with open(meta_import_path, 'r') as f_meta:
            metadata = json.load(f_meta)
        print_success("Metadata loaded successfully.")

        # Validate essential keys
        original_id = metadata.get("source_id")
        original_instance_type = metadata.get("source_instance_type")
        exported_disks = metadata.get("exported_disks") # This is a list of dicts
        compress_method = metadata.get("compression_method", "none") # Get compression method

        if not original_id: raise ValueError("Missing 'source_id' in metadata.")
        if not original_instance_type or original_instance_type not in ['vm', 'lxc']:
             raise ValueError("Missing or invalid 'source_instance_type' (must be 'vm' or 'lxc') in metadata.")
        if exported_disks is None: raise ValueError("Missing 'exported_disks' list in metadata.")
        if not isinstance(exported_disks, list):
             raise ValueError("'exported_disks' in metadata is not a list.")
        if compress_method not in COMPRESSION_TOOLS:
             raise ValueError(f"Invalid 'compression_method' ('{compress_method}') found in metadata.")

        # Check keys within exported_disks list
        for i, disk_info in enumerate(exported_disks):
             if not disk_info.get("key"): raise ValueError(f"Disk entry {i} missing 'key'.")
             if not disk_info.get("original_dataset_basename"): raise ValueError(f"Disk entry {i} missing 'original_dataset_basename'.")
             if not disk_info.get("stream_file"): raise ValueError(f"Disk entry {i} missing 'stream_file'.")
             # Optional: check if stream_suffix matches compression method
             expected_suffix = COMPRESSION_TOOLS[compress_method]["suffix"]
             if disk_info.get("stream_suffix") != expected_suffix:
                 print_warning(f"Stream suffix '{disk_info.get('stream_suffix')}' for key '{disk_info['key']}' does not match expected suffix '{expected_suffix}' for compression '{compress_method}'.")


        print(f"   Original ID:       {original_id}")
        print(f"   Original Type:     {original_instance_type.upper()}")
        print(f"   Compression:       {compress_method}")
        print(f"   Disks in export:   {len(exported_disks)}")

    except json.JSONDecodeError:
        print_error(f"Failed to decode metadata file (invalid JSON): {meta_import_path}", exit_code=1)
    except ValueError as ve:
         print_error(f"Invalid or incomplete metadata in {meta_import_path}: {ve}", exit_code=1)
    except Exception as e:
        print_error(f"Failed to read or parse metadata file {meta_import_path}: {e}", exit_code=1)

    # --- Check Decompression Tool Availability ---
    _, decompress_ok, decompress_tool_info = check_compression_tools(compress_method)
    if compress_method != "none" and not decompress_ok:
        print_error(f"Required decompression tool for method '{compress_method}' not found. Aborting restore.", exit_code=1)


    # --- Find Config File ---
    # Use filename from metadata if present, otherwise construct default
    config_filename = metadata.get("source_config_file")
    if not config_filename:
        config_filename = f"{original_id}{DEFAULT_EXPORT_CONFIG_SUFFIX}"
        print_warning(f"Config filename not specified in metadata, assuming default: {config_filename}")

    config_import_path = import_dir / config_filename
    if not config_import_path.is_file():
         print_error(f"Required config file '{config_filename}' not found in {import_dir}", exit_code=1)
    print(f"   Config file found: {config_filename}")

    # --- Determine and Validate Target ID ---
    pve_cmd = "qm" if original_instance_type == "vm" else "pct"
    conf_dir_name = "qemu-server" if original_instance_type == "vm" else "lxc"
    target_conf_dir = Path("/etc/pve") / conf_dir_name

    if not new_id:
        default_new_id = f"8{original_id}" # Different prefix for restore default
        try:
            new_id_input = input(f"Enter the new {original_instance_type.upper()} ID for restore (blank for default={default_new_id}): ").strip()
            new_id = new_id_input or default_new_id
            if not new_id.isdigit() or int(new_id) <= 0:
                 print_error(f"Invalid new ID '{new_id}'. Must be a positive integer.", exit_code=1)
            if new_id == default_new_id and not new_id_input:
                print_warning(f"Using default ID: {new_id}")
        except ValueError:
             print_error("Invalid input for new ID.", exit_code=1)
        except EOFError:
            print_error("\nNon-interactive mode: New ID must be provided as an argument.", exit_code=1)


    # Check for config file collision
    new_conf_path = target_conf_dir / f"{new_id}.conf"
    config_collision = False
    if new_conf_path.exists():
        print_error(f"Config file for target ID {new_id} ({new_conf_path}) already exists!");
        config_collision = True

    # --- Check for Target Dataset Collisions ---
    print_info("\nChecking for potential target dataset collisions...")
    potential_targets = {} # {original_key: new_dataset_full_path}
    dataset_collision_found = False
    if not exported_disks:
        # This should have been caught by metadata validation, but double-check
        print_warning("No disks listed in metadata to restore, proceeding to config only.")
    else:
        for disk_info in exported_disks:
            original_key = disk_info["key"]
            # Use original_dataset_path if available, otherwise reconstruct from basename and pool
            original_path_for_naming = disk_info.get("original_dataset_path")
            if not original_path_for_naming:
                 # Reconstruct using original pool path from metadata if available, else the *target* pool path as a fallback
                 original_pool = metadata.get("source_zfs_pool_path", target_zfs_pool_path)
                 original_basename = disk_info["original_dataset_basename"]
                 original_path_for_naming = f"{original_pool.rstrip('/')}/{original_basename}"
                 print_warning(f"Original full path for key '{original_key}' not in metadata, reconstructed as '{original_path_for_naming}' for naming.")

            # Use the target ZFS pool path provided by argument/default
            new_dataset_path = generate_new_dataset_name(original_path_for_naming, original_id, new_id, target_zfs_pool_path)
            potential_targets[original_key] = new_dataset_path
            if get_zfs_property(new_dataset_path, 'type'):
                print_error(f"Target ZFS dataset '{new_dataset_path}' for key '{original_key}' already exists.")
                dataset_collision_found = True

    if config_collision or dataset_collision_found:
        print_error("Aborting restore due to collision(s).", exit_code=1)
    print_success("No target configuration or dataset collisions found.")


    # --- Restore ZFS Data (All Disks) ---
    print_info(f"\n--- Starting ZFS Restore Operations ---")
    print_info(f"Target ZFS Pool Path: {target_zfs_pool_path}")
    print_info(f"Target PVE Storage: {target_pve_storage}")

    restored_datasets_map = {} # {original_key: new_dataset_basename}
    all_data_ops_successful = True
    pv_available = is_tool('pv')
    cleanup_list = [] # Keep track of created datasets for potential rollback

    if not exported_disks:
        print_info("No ZFS disks to restore based on metadata.")
    else:
        for disk_info in exported_disks:
            original_key = disk_info["key"]
            stream_filename = disk_info["stream_file"]
            data_import_path = import_dir / stream_filename
            new_dataset_path = potential_targets[original_key] # Get pre-calculated target path

            print(f"\n {color_text(f'Restoring {original_key}:', 'CYAN')}")
            print(f"    Input stream:   {color_text(str(data_import_path.name), 'BLUE')}")
            print(f"    Target dataset: {color_text(new_dataset_path, 'GREEN')}")

            if not data_import_path.is_file():
                print_error(f"Data stream file '{data_import_path.name}' not found in {import_dir}. Aborting.")
                all_data_ops_successful = False
                break # Stop restore if a required data file is missing

            # Prepare pipeline: cat file | [decompressor] | [pv] | zfs receive
            recv_cmd = ['zfs', 'receive', '-o', 'readonly=off', new_dataset_path] # Ensure dataset is writable
            pipeline_cmds = []
            pipeline_names = []
            pv_opts = None

            # Use 'cat' to feed the file into the pipeline
            # Important: Ensure cat reads the specific file path!
            cat_cmd = ['cat', str(data_import_path)] # Use str() to ensure it's a string path
            pipeline_cmds.append(cat_cmd)
            pipeline_names.append("cat")

            # Add decompression command if needed
            if compress_method != "none":
                decompress_cmd = decompress_tool_info["decompress"]
                pipeline_cmds.append(decompress_cmd)
                pipeline_names.append(f"decompress ({compress_method})")

            if pv_available:
                pv_cmd_base = ['pv']
                # Size estimation for PV is tricky with compression.
                # We can use the compressed file size, but PV will track uncompressed data.
                # Or we can omit size for PV during decompression. Let's omit it.
                try:
                    file_size = data_import_path.stat().st_size
                    size_str = f"~{format_bytes(file_size)} (compressed)"
                except Exception:
                    file_size = None
                    size_str = "Unknown size"
                print(f"    Input file size: {size_str}")

                # Add -W (wait) might be good practice
                pv_opts = ['-W', '-p', '-t', '-r', '-b', '-N', f'restore-{original_key}']
                # Do NOT add -s size for decompression, as PV tracks uncompressed bytes
                # if file_size: pv_opts.extend(['-s', str(file_size)])
                pipeline_cmds.append(pv_cmd_base)
                pipeline_names.append("pv")
            else:
                print_warning("    Executing restore without progress bar ('pv' not found).")

            pipeline_cmds.append(recv_cmd)
            pipeline_names.append("zfs receive")

            print(f"    Executing pipeline: {' | '.join([' '.join(c) for c in pipeline_cmds])}")
            pipeline_successful = run_pipeline(pipeline_cmds, pipeline_names, pv_options=pv_opts, output_file=None)

            if pipeline_successful:
                print_success(f"    ZFS data restore successful for {original_key}.")
                restored_datasets_map[original_key] = Path(new_dataset_path).name # Store basename for config
                cleanup_list.append(new_dataset_path) # Add to cleanup list
            else:
                print_error(f"Error during ZFS data restore pipeline for {original_key}. Aborting restore.")
                all_data_ops_successful = False
                # Let's break and cleanup all successfully created ones so far.
                break

    # --- Cleanup if ZFS Restore Failed ---
    if not all_data_ops_successful:
        print_error("\n--- Restore Failed During ZFS Operations ---")
        if cleanup_list:
             print_warning("Attempting to clean up successfully restored datasets...")
             for ds_path in reversed(cleanup_list): # Destroy in reverse order of creation
                 print_warning(f"    Destroying partially restored dataset: {ds_path}")
                 run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
        else:
             print_info("No datasets were created before failure occurred.")
        sys.exit(1)


    # --- Create and Adjust Configuration File ---
    print_info(f"\nCreating and adjusting new configuration file: {color_text(str(new_conf_path), 'BLUE')}")
    config_created_successfully = False
    try:
        shutil.copy2(config_import_path, new_conf_path)
        print_success(f"Copied base configuration from {config_import_path.name} to {new_conf_path}.")

        adjust_config_file(
            conf_path=new_conf_path,
            instance_type=original_instance_type,
            new_id=new_id, # Pass new_id
            target_pve_storage=target_pve_storage, # Pass the target storage name
            dataset_map=restored_datasets_map, # Map original keys to new basenames
            name_prefix="restored-"
        )
        config_created_successfully = True

    except Exception as e:
        print_error(f"Error processing config file {new_conf_path}: {e}")
        # Attempt cleanup of config file AND restored datasets
        if new_conf_path.exists():
            print_warning(f"Removing potentially incomplete config file: {new_conf_path}")
            try: new_conf_path.unlink()
            except OSError: pass
        # Cleanup ZFS datasets if config failed
        if cleanup_list:
             print_warning("Attempting to clean up restored ZFS datasets due to config error...")
             for ds_path in reversed(cleanup_list):
                  print_warning(f"    Destroying restored dataset: {ds_path}")
                  run_command(['zfs', 'destroy', '-r', ds_path], check=False, capture_output=False, suppress_stderr=True)
        sys.exit(1)

    # --- Final Message ---
    if all_data_ops_successful and config_created_successfully:
        print(f"\n{color_text('--- Restore Process Finished ---', 'GREEN')}")
        print(f"Restored {original_instance_type.upper()} from export directory '{import_dir}'")
        print(f"  New ID:              {color_text(new_id, 'BLUE')}")
        print(f"  Target PVE Storage: {color_text(target_pve_storage, 'BLUE')}")
        if restored_datasets_map:
             print(f"  Restored Datasets ({len(restored_datasets_map)}):")
             for key, basename in restored_datasets_map.items():
                 # Use the target ZFS pool path provided by argument/default
                 full_path = f"{target_zfs_pool_path.rstrip('/')}/{basename}"
                 print(f"    - {key} -> {color_text(full_path, 'BLUE')}")
        else:
             print("  Restored Datasets: None")
        print(f"{color_text('Review the configuration:', 'YELLOW')} {color_text(str(new_conf_path), 'BLUE')}")
        print(color_text("Important checks: Network settings (IP/MAC), Hostname/Name, Resources, CD-ROMs (VMs), link_down=1 on NICs.", 'YELLOW'))
    else:
         # Should have exited earlier if something failed, but as a fallback:
         print(f"\n{color_text('--- Restore Process Failed ---', 'RED')}")


def perform_ram_check(pve_cmd, src_id):
    """Prüft die RAM-Nutzung des Hosts vor dem Klonen einer VM."""
    print_info("\nChecking host RAM usage...")
    try:
        # Get total host RAM using 'free -m'
        free_output = run_command(['free', '-m'], capture_output=True, check=True)
        mem_line = free_output.split('\n')[1]
        total_ram_mb = int(mem_line.split()[1])

        # Get source VM RAM from its config
        src_vm_ram_mb = 512 # Default fallback
        # Use 'qm config' which is generally available
        qm_config_output = run_command([pve_cmd, 'config', src_id], capture_output=True, suppress_stderr=True, check=True)
        match_mem = re.search(r'^memory:\s*(\S+)', qm_config_output, re.MULTILINE | re.IGNORECASE)
        if match_mem:
            parsed_ram = parse_size_to_mb(match_mem.group(1))
            src_vm_ram_mb = parsed_ram if parsed_ram > 0 else 0 # Allow 0 initially

            if src_vm_ram_mb == 0: # Handle 'memory: 0' -> check minimum or ballooning
                 min_mem_match = re.search(r'^minimum:\s*(\S+)', qm_config_output, re.MULTILINE | re.IGNORECASE)
                 balloon_match = re.search(r'^balloon:\s*(\S+)', qm_config_output, re.MULTILINE | re.IGNORECASE)

                 min_ram_mb = 0
                 if min_mem_match:
                      min_ram_mb = parse_size_to_mb(min_mem_match.group(1))

                 balloon_mb = 0
                 if balloon_match:
                      balloon_val = balloon_match.group(1)
                      if balloon_val.lower() == '0': # Ballooning disabled
                          balloon_mb = 0
                      else: # Ballooning enabled, use value if numeric, else default
                          balloon_mb = parse_size_to_mb(balloon_val) if balloon_val.isdigit() else 512

                 # Use minimum if set, else balloon if set and > 0, else default
                 if min_ram_mb > 0:
                      src_vm_ram_mb = min_ram_mb
                      print_warning(f"VM {src_id} 'memory' is 0, using 'minimum': {src_vm_ram_mb} MB for check.")
                 elif balloon_mb > 0:
                      src_vm_ram_mb = balloon_mb
                      print_warning(f"VM {src_id} 'memory' is 0, using 'balloon': {src_vm_ram_mb} MB for check.")
                 else:
                      src_vm_ram_mb = 512 # Fallback if memory=0 and no min/balloon
                      print_warning(f"VM {src_id} 'memory' is 0 and no valid 'minimum' or 'balloon' found, using default: {src_vm_ram_mb} MB for check.")
        elif 'memory' in qm_config_output.lower(): # Line exists but couldn't parse value
             print_warning(f"Could not parse 'memory' value for VM {src_id}, assuming {src_vm_ram_mb} MB for check.")
        else: # 'memory' line not found at all
             print_warning(f"No 'memory' setting found for VM {src_id}, assuming {src_vm_ram_mb} MB for check.")


        # Sum RAM of all *running* VMs
        # Use 'qm list' which provides memory usage directly
        qm_list_output = run_command([pve_cmd, 'list', '--full'], capture_output=True, suppress_stderr=True, check=True)
        sum_running_ram_mb = 0
        running_vm_lines = [line for line in qm_list_output.split('\n')[1:] if 'running' in line.split()]
        header_line = qm_list_output.split('\n')[0].lower() # Lowercase for easier matching
        headers = header_line.split()

        # Find column index for memory (prefer maxmem, fallback to mem)
        mem_index = -1
        for col_name in ['maxmem', 'mem']: # Order of preference
            try: mem_index = headers.index(col_name); break
            except ValueError: pass

        if mem_index == -1:
            print_warning("Could not determine memory column ('maxmem' or 'mem') in 'qm list' output. RAM check might be inaccurate.")
            # Fallback: Try to guess based on typical position? Risky.
            # Let's proceed without summing running VMs if column not found.
            sum_running_ram_mb = -1 # Indicate failure to sum
        else:
             for line in running_vm_lines:
                 parts = line.split()
                 vmid = parts[0]
                 if vmid.isdigit() and len(parts) > mem_index:
                     try:
                         # Value is in bytes in 'qm list' output
                         ram_bytes = int(parts[mem_index])
                         current_vm_ram_mb = ram_bytes // (1024*1024) if ram_bytes > 0 else 0
                         sum_running_ram_mb += current_vm_ram_mb
                     except (ValueError, IndexError):
                         print_warning(f"Could not parse memory for running VM {vmid} from 'qm list'.")
                         # Could try qm config as a fallback here, but might slow down significantly
                         sum_running_ram_mb += 512 # Add default as a guess


        # Perform the check and warn user
        threshold_mb = math.floor(total_ram_mb * RAM_THRESHOLD_PERCENT / 100)
        print(f"    Total host RAM:      {color_text(format_bytes(total_ram_mb*1024*1024), 'BLUE')}")
        if sum_running_ram_mb >= 0:
            print(f"   RAM running VMs (sum):{color_text(format_bytes(sum_running_ram_mb*1024*1024), 'BLUE')}")
            print(f"   Source VM RAM (Est.):{color_text(format_bytes(src_vm_ram_mb*1024*1024), 'BLUE')}")
            prognostic_ram_mb = sum_running_ram_mb + src_vm_ram_mb
            print(f"   Projected Total RAM: {color_text(format_bytes(prognostic_ram_mb*1024*1024), 'BLUE')} (if clone starts)")
            print(f"   {RAM_THRESHOLD_PERCENT}% Threshold:        {color_text(format_bytes(threshold_mb*1024*1024), 'BLUE')}")

            if prognostic_ram_mb > threshold_mb:
                print_warning(f"\nWARNING: Starting the clone might exceed the {RAM_THRESHOLD_PERCENT}% host RAM usage threshold!")
                try:
                    confirm = input(f"{color_text('Continue anyway? (y/N) ', 'RED')}{COLORS['NC']}").strip().lower()
                    if confirm not in ['y', 'yes']:
                        print_error("Operation aborted by user due to RAM concerns.", exit_code=1)
                    else:
                        print_info("Continuing despite RAM warning.")
                except EOFError: # Handle non-interactive session
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
    # --- Argument Parser Setup ---
    # Add epilog for usage examples
    compress_options = list(COMPRESSION_TOOLS.keys())
    examples = f"""
Examples:

  {color_text('List available VMs/LXCs:', 'YELLOW')}
    {sys.argv[0]} --list

  {color_text('Clone VM 100 to 9100 (linked clone, prompts for snapshot, uses default storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} clone 100 9100

  {color_text('Clone LXC 105 to 9105 (full clone, prompts for snapshot, specify target storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} clone 105 9105 --clone-mode full --target-pve-storage tank/pve --target-zfs-pool-path tank/pve/data

  {color_text('Clone VM 102, prompt for new ID and snapshot (linked, uses default storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} clone 102

  {color_text('Export VM 101 to /mnt/backup/export/101 (uncompressed, prompts for snapshot):', 'YELLOW')}
    sudo {sys.argv[0]} export 101 /mnt/backup/export

  {color_text('Export LXC 105 to /mnt/backup/export/105 (using zstd compression):', 'YELLOW')}
    sudo {sys.argv[0]} export 105 /mnt/backup/export --compress zstd

  {color_text('Export VM 101, specify source storage/pool and use pigz compression:', 'YELLOW')}
    sudo {sys.argv[0]} export 101 /mnt/backup/export --source-pve-storage local-zfs --source-zfs-pool-path rpool/data --compress pigz

  {color_text('Restore from /mnt/backup/export/101 to new ID 8101 (auto-detects compression, uses default target storage/pool):', 'YELLOW')}
    sudo {sys.argv[0]} restore /mnt/backup/export/101 8101

  {color_text('Restore from /mnt/backup/export/105, prompt for new ID, specify target storage/pool:', 'YELLOW')}
    sudo {sys.argv[0]} restore /mnt/backup/export/105 --target-pve-storage tank/pve --target-zfs-pool-path tank/pve/data

Configuration Note:
  - Target ZFS pool path for clone/restore defaults to: {color_text(DEFAULT_ZFS_POOL_PATH, 'BLUE')}
  - Target PVE storage name for clone/restore defaults to: {color_text(DEFAULT_PVE_STORAGE, 'BLUE')}
  (These can be overridden using the --target-zfs-pool-path and --target-pve-storage options.)
"""
    parser = argparse.ArgumentParser(
        description="Proxmox VM/LXC Clone, Export, or Restore script using ZFS snapshots with optional compression.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=examples
    )

    # --- Global Options ---
    parser.add_argument('--list', action='store_true', help="List available VMs and LXC containers and exit.")
    # Add target options back as global options affecting clone/restore
    parser.add_argument('--target-zfs-pool-path', default=DEFAULT_ZFS_POOL_PATH,
                        help=f"Base path for target ZFS datasets (clone/restore). Default: {DEFAULT_ZFS_POOL_PATH}")
    parser.add_argument('--target-pve-storage', default=DEFAULT_PVE_STORAGE,
                        help=f"PVE storage name for target datasets (clone/restore). Default: {DEFAULT_PVE_STORAGE}")


    # --- Subparsers for Modes ---
    subparsers = parser.add_subparsers(dest='mode', help='Operation mode (clone, export, restore)', required=True) # Make mode required

    # --- Clone Mode ---
    parser_clone = subparsers.add_parser('clone', help='Clone a VM/LXC from a ZFS snapshot.', formatter_class=argparse.RawTextHelpFormatter)
    parser_clone.add_argument('source_id', help="ID of the source VM or LXC to clone.")
    parser_clone.add_argument('new_id', nargs='?', default=None,
                              help="ID for the new cloned instance. (Default: 9<source_id>, will prompt if omitted)")
    parser_clone.add_argument('--clone-mode', choices=['linked', 'full'], default='linked',
                              help="Type of ZFS clone ('linked' uses 'zfs clone', 'full' uses send/receive). Default: linked")

    # --- Export Mode ---
    parser_export = subparsers.add_parser('export', help='Export a VM/LXC config and ZFS snapshot data (optionally compressed).', formatter_class=argparse.RawTextHelpFormatter)
    parser_export.add_argument('source_id', help="ID of the source VM or LXC to export.")
    parser_export.add_argument('export_dir',
                               help="Parent directory where the export subdirectory (named after source_id) will be created (e.g., /mnt/backups).")
    parser_export.add_argument('--compress', choices=compress_options, default='none',
                               help=f"Compression method for ZFS streams. Default: none")
    # Keep source arguments specific to export
    parser_export.add_argument('--source-zfs-pool-path', default=DEFAULT_ZFS_POOL_PATH,
                               help=f"Base path where source ZFS datasets reside. Default: {DEFAULT_ZFS_POOL_PATH}")
    parser_export.add_argument('--source-pve-storage', default=DEFAULT_PVE_STORAGE,
                               help=f"PVE storage name linked in the source config. Default: {DEFAULT_PVE_STORAGE}")


    # --- Restore Mode ---
    parser_restore = subparsers.add_parser('restore', help='Restore a VM/LXC from an exported directory (auto-detects compression).', formatter_class=argparse.RawTextHelpFormatter)
    parser_restore.add_argument('import_dir',
                                help="Path to the specific export directory containing the .conf, .meta.json, and data stream files (e.g., /mnt/backups/101).")
    parser_restore.add_argument('new_id', nargs='?', default=None,
                                help="ID for the new restored instance. (Default: 8<original_id>, will prompt if omitted)")


    args = parser.parse_args()

    # --- Handle --list option ---
    if args.list:
        if os.geteuid() != 0:
            print_warning("Root privileges might be needed to read all config files for listing.")
        if list_instances():
             sys.exit(0)
        else:
             sys.exit(1) # Exit if listing failed somehow

    # --- Mode selection needed if --list wasn't used ---
    # (Now handled by subparsers(required=True))
    # if not args.mode:
    #     parser.print_help()
    #     print_error("\nError: You must specify an operation mode (clone, export, restore) or use --list.", exit_code=1)


    # --- Initial Checks (only if a mode is selected) ---
    if os.geteuid() != 0:
        print_warning("Warning: Root privileges (sudo) are likely required for ZFS/Proxmox commands.")

    # Check for 'pv' tool
    pv_available = is_tool('pv')
    if not pv_available:
        print_warning("Tool 'pv' (Pipe Viewer) not found. Operations involving data streams will not show progress bars.")
    else:
        print_info("Tool 'pv' found, will be used for progress display.")


    # --- Execute Selected Mode ---
    if args.mode == 'clone':
        do_clone(args)
    elif args.mode == 'export':
        do_export(args)
    elif args.mode == 'restore':
        do_restore(args)
    else:
        # Should have been caught earlier, but as a fallback
        parser.print_help()
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_error("\nOperation cancelled by user (Ctrl+C).", exit_code=130) # Standard exit code for Ctrl+C
    except EOFError: # Catch unexpected EOF during input prompts
        print_error("\nOperation aborted due to unexpected end of input.", exit_code=1)
    except Exception as e:
        print_error(f"\nAn unexpected critical error occurred: {e}")
        # Uncomment the next two lines for detailed debugging traceback
        # import traceback
        # traceback.print_exc()
        sys.exit(2) # General error exit code