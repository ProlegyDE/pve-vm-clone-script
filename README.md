# Proxmox VM/LXC ZFS Utility Script 💾 🔄 📤

![Python Version 3.7+](https://img.shields.io/badge/Python-3.7%2B-blue) ![License GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-green)

This Python script provides command-line utilities for managing Proxmox Virtual Machines (VMs) and LXC containers using ZFS snapshots. It supports cloning (linked and full), exporting configurations and data streams, and restoring from exports with **multi-snapshot support**.

## ⚠️ Critical Warning

This script requires root privileges (`sudo`) and modifies system configurations (creates/destroys ZFS datasets, creates PVE configuration files)!

**Use only if you:**

*   Are familiar with Proxmox VE and ZFS administration.
*   Understand ZFS snapshot, clone, send/receive mechanics.
*   Maintain regular backups of your Proxmox environment.
*   Have tested the script thoroughly in a non-production environment.

## ⚙️ Installation

```bash
git clone https://github.com/ProlegyDE/pve-zfs-utility.git
cd pve-zfs-utility
chmod +x pve-zfs-utility.py
```

## ✔️ Requirements

*   Proxmox VE environment
*   ZFS storage configured for VMs/LXCs (default used by script: `local-zfs` on pool `rpool/data`, configurable via arguments)
*   Python 3.7+
*   Required system tools: `zfs`, `qm` (for VMs), `pct` (for LXC)
*   Optional for compression:
    *   gzip / gunzip (usually available)
    *   pigz / unpigz (for parallel gzip)
    *   zstd / unzstd (for Zstandard compression)
*   Recommended: `pv` (Pipe Viewer) for progress display during full clones, exports, and restores.

## 💻 Features

*   Supports both **VMs and LXC containers**.
*   Modes:
    *   **`clone`**: Create linked (default) or full clones from multiple selected snapshots.
    *   **`export`**: Export VM/LXC configuration and ZFS data stream(s) from multiple snapshots to separate directories.
    *   **`restore`**: Restore a VM/LXC from a specific exported snapshot directory to a new ID.
    *   **`--list`**: List available VMs and LXCs.
*   Interactive CLI with multi-snapshot selection and color-coded output.
*   Command-line argument parsing (`argparse`) for non-interactive use.
*   Automatic configuration adjustments for clones/restores:
    *   Adds "clone-" or "restored-" prefix to names/hostnames.
    *   Sets `onboot: 0`.
    *   Adds `link_down=1` to network interfaces.
    *   Maps ZFS dataset paths in the new configuration.
*   Handles complex storage configurations (multiple disks, EFI, etc.) on the specified ZFS storage.
*   RAM usage verification before VM cloning (configurable threshold).
*   Collision detection for target VM/LXC IDs and ZFS datasets.
*   Progress display for data operations (full clone, export, restore) when `pv` is available.
*   Export creates structured directories per snapshot with `.conf`, `.meta.json`, and compressed `.zfs.stream` files.

## 🚀 Usage

The script uses command-line arguments to define the operation mode and parameters.

### General Syntax:

```bash
sudo ./pve-zfs-utility.py [global_options] <mode> [mode_options]
```

### Modes:

*   `clone <source_id> [new_id]`
*   `export <source_id> <export_dir_parent>`
*   `restore <import_dir> [new_id]`
*   `--list` (no mode needed)

### Key Options:

*   `--clone-mode {linked|full}`: (Clone only) Type of ZFS clone. Default: `linked`.
*   `--compress {none|gzip|pigz|zstd}`: (Export only) Compression method for ZFS streams. Default: `none`.
*   `--target-zfs-pool-path <path>`: (Clone/Restore) ZFS pool path for the _target_. Default: `rpool/data`.
*   `--target-pve-storage <name>`: (Clone/Restore) PVE storage name for the _target_. Default: `local-zfs`.
*   `--source-zfs-pool-path <path>`: (Export only) ZFS pool path for the _source_. Default: `rpool/data`.
*   `--source-pve-storage <name>`: (Export only) PVE storage name for the _source_. Default: `local-zfs`.

### Examples:

1.  **List available instances:**
    
    ```bash
    ./pve-zfs-utility.py --list
    ```
    
2.  **Clone VM 100 with multiple snapshots (prompts for selection):**
    
    ```bash
    sudo ./pve-zfs-utility.py clone 100 9100
    # Creates clones for each selected snapshot (e.g., 9100, 9101, etc.)
    ```
    
3.  **Export LXC 105 with zstd compression (creates subdir per snapshot):**
    
    ```bash
    sudo ./pve-zfs-utility.py export 105 /mnt/backup --compress zstd
    # Exports each selected snapshot to /mnt/backup/105_snapshotname
    ```
    
4.  **Restore from specific snapshot directory:**
    
    ```bash
    sudo ./pve-zfs-utility.py restore /mnt/backup/105_autosnap_20231026 8101
    ```
    

## 🔧 Configuration Defaults

*   `DEFAULT_ZFS_POOL_PATH = "rpool/data"` (Target for clone/restore, Source for export)
*   `DEFAULT_PVE_STORAGE = "local-zfs"` (Target for clone/restore, Source for export)
*   `RAM_THRESHOLD_PERCENT = 90` (Warning threshold for VM cloning RAM check)
*   `DEFAULT_EXPORT_META_SUFFIX = ".meta.json"`
*   `DEFAULT_EXPORT_DATA_SUFFIX = ".zfs.stream"`
*   `DEFAULT_EXPORT_CONFIG_SUFFIX = ".conf"`
*   Compression suffixes: `.zfs.stream.gz`, `.zfs.stream.zst`

## ⚖️ License

This project is licensed under the GPL-3.0 License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Key Limitations & Considerations:

*   Requires pre-existing ZFS snapshots for the source instance.
*   Only operates on disks associated with the specified ZFS storage/pool in the config. Other storage types are ignored.
*   RAM check before cloning is only performed for VMs.
*   EFI disk handling relies on standard PVE configuration; manual verification after clone/restore is recommended.

## 📄 Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

**Use this script at your own risk.** The author shall not be liable for:

*   Data loss or corruption.
*   System failures or instability.
*   Any direct or indirect damages resulting from the use of this script.
*   Incompatibilities with specific or non-standard system configurations.

**Always test thoroughly on non-critical systems before using in production.**

## 🤝 Contributing

Contributions, bug reports, and feature requests are welcome! Please:

1.  Use GitHub Issues for bug reports and feature suggestions.
2.  Submit Pull Requests with clear descriptions of changes.
3.  Try to maintain compatibility and avoid breaking changes without prior discussion.