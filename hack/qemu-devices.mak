# The device set this project's QEMU is built with, passed as `--with-devices-x86_64=spin`.
#
# A file and not a `sed` over the source tree, because the source lives in a BuildKit cache
# mount keyed by version alone: an edit made there survives every later build, including
# ones meant to undo it, and nothing would show that the tree being compiled is not the
# tree upstream shipped. This is read on every build and reviewed like anything else.
#
# Only symbols upstream lists as optional in configs/devices/i386-softmmu/default.mak are
# touched. Everything else stays: turning off a symbol upstream does not offer produces a
# link failure twenty minutes in, which is what `CONFIG_CXL=n` did on 11.0.2 (ACPI and the
# PCI expander bridge still reference it).
include ../i386-softmmu/default.mak

# Boards. Q35 is what every lane boots and MICROVM is kept for the day one wants it (it
# needs FDT, which is why fdt is not disabled either). The rest are a 1996 PC and a machine
# type for AWS enclaves.
CONFIG_ISAPC=n
CONFIG_I440FX=n
CONFIG_NITRO_ENCLAVE=n

# Devices no guest of ours has: a floppy controller, a Mac's SMC, the qtest devices, Spice's
# QXL, the serial-graphics adapter, IPMI over four buses, and the Hyper-V enlightenments a
# Linux guest does not use.
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

# Two that upstream's default.mak offers and 11.1.1 refuses, each checked by running
# scripts/minikconf.py against this file rather than by reading the list:
#   CONFIG_SGA=n  — "undefined symbol SGA". The serial graphics adapter is gone from the
#                   Kconfig tree; the line survives in upstream's default.mak alone.
#   CONFIG_FDC=n  — "contradiction between clauses when setting FDC". Q35's ICH9 brings an
#                   ISA_SUPERIO, which selects FDC_ISA, which selects FDC. A floppy
#                   controller is not optional while the board that needs it is kept.
#
# Deliberately left on, each for a reason:
#   PCI_DEVICES  — q35 still references controllers this would take with it.
#   VTD, AMD_IOMMU — VFIO on q35 needs them; there is no passthrough here yet.
#   HPET, PVPANIC — cheap, and pvpanic is how a guest reports a panic.
#   SEV, TDX, SGX — confidential computing is a product decision, not debloat.
