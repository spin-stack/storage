# The device set this project's QEMU is built with, passed as `--with-devices-x86_64=spin`.
#
# A file and not a `sed` over the source tree, because the source lives in a BuildKit cache
# mount keyed by version alone: an edit made there survives every later build, including
# ones meant to undo it, and nothing would show that the tree being compiled is not the
# tree upstream shipped. This is read on every build and reviewed like anything else.
#
# **One device per function, and the function's most compatible device.** For a Linux guest
# that is virtio, without qualification: virtio-blk, virtio-net and the rest have been in
# the mainline kernel since 2.6.25 (2008) and are in every distribution's kernel. e1000 and
# rtl8139 are "more compatible" only for a guest with no virtio drivers — a Windows install
# without them, or something pre-2008 — which is not what this fleet boots. Every extra
# model is a device a guest could be given by accident, a driver its kernel has to carry,
# and in the case of a NIC an option ROM QEMU refuses to start without.
#
# Only symbols upstream marks optional are touched, which at this level means the ones
# carrying `default y if PCI_DEVICES` (or ISA/PCIE/USB) in hw/*/Kconfig — nothing `select`s
# them, so each can go on its own without touching PCI_DEVICES itself. Turning off a symbol
# upstream does not offer produces a link failure twenty minutes in, which is what
# `CONFIG_CXL=n` did on 11.0.2 (ACPI and the PCI expander bridge still reference it).
#
# Every change here is checked by running scripts/minikconf.py against this file before a
# build is started; it names the symbol and says whether it is undefined or contradicted.
include ../i386-softmmu/default.mak

# --- boards ---------------------------------------------------------------------------
# Q35 is what every lane boots and MICROVM is kept for the day one wants it (it needs FDT,
# which is why fdt is not disabled either). The rest are a 1996 PC and a machine type for
# AWS enclaves.
CONFIG_ISAPC=n
CONFIG_I440FX=n
CONFIG_NITRO_ENCLAVE=n

# --- network: one card ------------------------------------------------------------------
# virtio-net, and nothing else. This repository gives its guests no network at all, but the
# binary is the one a host runs for real (ADR-0021 — spin's runner launches QEMU) and a VM
# there gets a NIC. An emulated e1000 in a microVM is a performance bug with a driver, and
# each of these carries an option ROM: 1.6 MB of pc-bios for cards nobody would attach on
# purpose, in an image where firmware was already the largest thing.
CONFIG_E1000_PCI=n
CONFIG_E1000E_PCI_EXPRESS=n
CONFIG_IGB_PCI_EXPRESS=n
CONFIG_EEPRO100_PCI=n
CONFIG_NE2000_PCI=n
CONFIG_NE2000_ISA=n
CONFIG_PCNET_PCI=n
CONFIG_RTL8139_PCI=n
CONFIG_TULIP=n
CONFIG_VMXNET3_PCI=n
CONFIG_ROCKER=n
CONFIG_USB_NETWORK=n
CONFIG_CAN_SJA1000=n
CONFIG_CAN_PCI=n
CONFIG_CAN_CTUCANFD=n
CONFIG_CAN_CTUCANFD_PCI=n

# --- storage: one disk --------------------------------------------------------------------
# virtio-blk is how a volume reaches a guest here; virtio-scsi stays with it for a caller
# that wants more disks than PCI slots. The SCSI HBAs below emulate 1990s hardware, and NVMe
# is a storage device this system exists to replace. AHCI is not in the list because Q35
# selects it (hw/i386/Kconfig) — the ICH9 southbridge has SATA whether or not anything
# plugs into it.
CONFIG_NVME_PCI=n
CONFIG_LSI_SCSI_PCI=n
CONFIG_MPTSAS_SCSI_PCI=n
CONFIG_MEGASAS_SCSI_PCI=n
CONFIG_VMW_PVSCSI_SCSI_PCI=n
CONFIG_ESP_PCI=n
CONFIG_SDHCI_PCI=n
CONFIG_UFS_PCI=n

# --- display: one adapter -----------------------------------------------------------------
# The standard VGA, whose vgabios-stdvga.bin is shipped with the firmware, so a guest with
# no serial console still has somewhere to print. Cirrus, VMware's SVGA, Bochs, ATI and
# Apple's paravirtual adapter are five more answers to a question a headless VM does not ask.
CONFIG_VGA_CIRRUS=n
CONFIG_VMWARE_VGA=n
CONFIG_BOCHS_DISPLAY=n
CONFIG_ATI_VGA=n
CONFIG_MAC_PVG_PCI=n

# --- no sound, no USB, no serial over PCI -------------------------------------------------
# The console is the ISA 16550 the kernel prints to on ttyS0, which is what -serial gives
# it. Audio has no backend compiled in at all (the configure flags disable every driver),
# so these are device models with nowhere to play; USB is a bus this fleet's guests have no
# devices on, and `usb=off` is already on every machine line here.
CONFIG_ES1370=n
CONFIG_AC97=n
CONFIG_HDA=n
CONFIG_SERIAL_PCI=n
CONFIG_SERIAL_PCI_MULTI=n
CONFIG_USB_UHCI=n
CONFIG_USB_OHCI_PCI=n
CONFIG_USB_EHCI_PCI=n
CONFIG_USB_XHCI_PCI=n
CONFIG_USB_XHCI_NEC=n

# --- legacy PC and the rest ---------------------------------------------------------------
# A Mac's SMC, the qtest devices, Spice's QXL, IPMI over four buses, the Hyper-V
# enlightenments a Linux guest does not use, a watchdog, an industrial I/O carrier and the
# inter-VM shared memory device.
CONFIG_APPLESMC=n
CONFIG_TEST_DEVICES=n
CONFIG_QXL=n
CONFIG_ISA_DEBUG=n
CONFIG_ISA_IPMI_BT=n
CONFIG_ISA_IPMI_KCS=n
CONFIG_PCI_IPMI_BT=n
CONFIG_PCI_IPMI_KCS=n
CONFIG_IPMI_SSIF=n
CONFIG_HYPERV=n
CONFIG_WDT_IB6300ESB=n
CONFIG_TPCI200=n
CONFIG_IVSHMEM_DEVICE=n

# Two that upstream's default.mak offers and 11.1.1 refuses, each checked by running
# scripts/minikconf.py against this file rather than by reading the list:
#   CONFIG_SGA=n  — "undefined symbol SGA". The serial graphics adapter is gone from the
#                   Kconfig tree; the line survives in upstream's default.mak alone.
#   CONFIG_FDC=n  — "contradiction between clauses when setting FDC". Q35's ICH9 brings an
#                   ISA_SUPERIO, which selects FDC_ISA, which selects FDC. A floppy
#                   controller is not optional while the board that needs it is kept.
#
# Deliberately left on, each for a reason:
#   PCI_DEVICES  — q35 still references controllers this would take with it, and every
#                  model above is reachable one at a time without it.
#   PCI_BRIDGE, PCIE_PORT, XIO3130, IOH3420, I82801B11 — a q35's root ports; without them
#                  a device cannot be plugged into anything.
#   VIRTIO_PCI, VIRTIO_NET, VIRTIO_BLK, VIRTIO_SCSI, VIRTIO_BALLOON, VIRTIO_RNG — the set
#                  a guest here is actually given.
#   VTD, AMD_IOMMU — VFIO on q35 needs them; there is no passthrough here yet.
#   HPET, PVPANIC — cheap, and pvpanic is how a guest reports a panic.
#   SEV, TDX, SGX — confidential computing is a product decision, not debloat.
