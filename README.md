
# Proxmox VM Snapshot Clone :floppy_disk:

![License](https://img.shields.io/badge/License-GPL-green)

This Bash script facilitates cloning a Proxmox virtual machine (VM) from a specific ZFS snapshot. It lists all available VMs, allows selection of a source VM, and identifies all associated ZFS datasets before creating a new cloned VM.

## :warning: Critical Warning
**This script requires root privileges and can cause data changes!**  
Use only if you:
- Are familiar with Proxmox VE/ZFS system administration
- Understand the consequences of ZFS commands
- Maintain regular data backups

## :gear: Installation
```bash
git clone https://github.com/ProlegyDE/pve-snapshot-clone-script.git
cd pve-snapshot-clone-script
chmod +x pve-snapshot-clone-script.sh
```

## :white_check_mark: Requirements
-   Proxmox Virtual Environment (PVE)
-   ZFS storage backend (local-zfs)
-   Bash shell

## :computer: Features
- Lists all available VMs and their configurations
- Detects ZFS disk datasets linked to the selected VM
- Displays available snapshots for the primary disk dataset
- Clones selected snapshots and assigns them to a new VM
- Generates a new VM configuration file with necessary adjustments
- Provides color-coded output for better readability

## :rocket: Usage
```bash
sudo ./pve-snapshot-clone-script.sh
```

## :balance_scale: License
GPL License - See LICENSE for details.

### Key Limitations:
- No data integrity guarantees
- No liability for damages
- Not suitable for production systems without testing

## :page_facing_up: Disclaimer
THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND. THE AUTHOR SHALL NOT BE HELD LIABLE FOR:
- Data loss/corruption
- System failures
- Direct/indirect damages from usage
- Incompatibilities with specific system configurations

Use only on test systems or after thorough validation.

## :handshake: Contributing
Contributions are welcome! Please:
- Use Issues for bug reports
- Submit Pull Requests with change descriptions
- Avoid breaking changes without discussion
