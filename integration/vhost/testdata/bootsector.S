/*
 * A 512-byte MBR that exercises the vhost-user-blk device from inside a real
 * guest, with no kernel and no initrd.
 *
 * SeaBIOS finds the 0x55aa signature at the end of this sector, which means it
 * has already issued a virtio-blk READ of LBA 0 through our virtqueue just to
 * get here. This code then does one more of each:
 *
 *   READ  (INT 13h AH=42h) LBA 1 -> 0000:8000
 *   WRITE (INT 13h AH=43h) 0000:8000 -> LBA 64
 *
 * so the test seeds a pattern at LBA 1 and asserts it reappears at LBA 64.
 * Success and failure are reported through QEMU's isa-debug-exit device rather
 * than a serial console: the exit status needs no parsing. QEMU exits with
 * (value << 1) | 1, so this writes 0x20 for success (exit 65) and 0x21 for
 * failure (exit 67). Deliberately not 0 and 1: those encode to 1 and 3, and 1 is
 * also what QEMU exits with when *it* fails — which makes "the guest succeeded"
 * indistinguishable from "the guest never ran". The negative control depends on
 * being able to tell those apart.
 *
 * There is no FLUSH here, and there cannot be: SeaBIOS's INT 13h has no verb
 * for it. FLUSH is covered by the unit tests against the simulated front-end
 * and by hostio's RawFile tests; proving it from a real guest needs a Linux
 * kernel. See docs/plan/RISKS.md (RISK-10).
 *
 * Rebuild with: task vhost:bootsector   (needs binutils: as + ld)
 */
	.code16
	.section .text
	.globl _start
_start:
	cli
	xorw	%ax, %ax
	movw	%ax, %ds
	movw	%ax, %es
	movw	%ax, %ss
	movw	$0x7c00, %sp
	sti

	/* %dl still holds the BIOS boot drive; INT 13h needs it untouched. */

	movb	$0x42, %ah
	movw	$dap_read, %si
	int	$0x13
	jc	fail

	movb	$0x43, %ah
	movb	$0x00, %al
	movw	$dap_write, %si
	int	$0x13
	jc	fail

	movb	$0x20, %al
	outb	%al, $0xf4
hang:
	hlt
	jmp	hang

fail:
	movb	$0x21, %al
	outb	%al, $0xf4
fhang:
	hlt
	jmp	fhang

/* struct disk_address_packet: size, reserved, blocks, offset, segment, lba. */
dap_read:
	.byte	0x10, 0x00
	.word	1
	.word	0x8000
	.word	0x0000
	.quad	1

dap_write:
	.byte	0x10, 0x00
	.word	1
	.word	0x8000
	.word	0x0000
	.quad	64

	. = _start + 510
	.byte	0x55, 0xaa
