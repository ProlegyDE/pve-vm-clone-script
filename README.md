# Proxmox VM Clone Script :floppy_disk:

![Python Version](https://img.shields.io/badge/Python-3.7%2B-blue)
![License](https://img.shields.io/badge/License-GPL-green)

This Python script facilitates cloning Proxmox virtual machines (VMs) and LXC containers from ZFS snapshots. It lists all available instances, performs safety checks, and creates optimized clones with adjusted configurations.

## :warning: Critical Warning

This script requires root privileges and modifies system configurations!

Use only if you:

*   Are familiar with Proxmox VE/ZFS administration
*   Understand ZFS cloning mechanics
*   Maintain regular backups

## :gear: Installation

```
git clone https://github.com/ProlegyDE/pve-vm-clone-script.git
cd pve-vm-clone-script
chmod +x pve-vm-clone-script.py
```

## :white_check_mark: Requirements

*   Proxmox Virtual Environment (PVE 7+)
*   ZFS storage backend (local-zfs)
*   Python 3.7+
*   Root access

## :computer: Features

*   Supports both **VMs and LXC containers**
*   Interactive CLI with color-coded output
*   Automatic RAM usage analysis for VMs
*   ZFS dataset detection and validation
*   Configuration auto-adjustments for safe cloning
*   Support for zfs-auto-snapshot patterns

## :rocket: Usage

```
sudo ./pve-vm-clone-script.py
```

## :balance_scale: License

GPL-3.0 License - See [LICENSE](LICENSE) for details.

### Key Limitations:

*   Requires pre-existing ZFS snapshots
*   First-run RAM check only for VMs
*   EFI disk handling requires manual verification

## :page_facing_up: Disclaimer

THE SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY. THE AUTHOR SHALL NOT BE LIABLE FOR:

*   Data loss/corruption
*   System failures
*   Direct/indirect damages from usage
*   Incompatibilities with specific system configurations

Use only on test systems or after thorough validation.

## :handshake: Contributing

Contributions welcome! Please:

1.  Use Issues for bug reports
2.  Submit Pull Requests with change descriptions
3.  Avoid breaking changes without discussion