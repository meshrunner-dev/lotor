//go:build linux

// Package i2c is the one place in this daemon that knows what an
// i2c-dev ioctl is. Two drivers need the same four things — open a
// bus, bind one address, bound how long the adapter waits, move bytes
// to and from a register — and getting the binding or the bound wrong
// in two places is a way to have it wrong in one of them.
package i2c

import (
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// The i2c-dev ioctls. Stable kernel ABI, which x/sys/unix does not name.
const (
	slave   = 0x0703 // bind this descriptor to one address
	timeout = 0x0702 // adapter timeout, in units of 10ms
)

// busTimeout is what the adapter is told to wait before giving up on a
// transfer. It is the only thing between a slave holding the line low
// and a read that never returns: no context reaches a thread already
// blocked in the kernel, so the bound has to be set on the bus itself.
const busTimeout = 2 * time.Second

// Device is one part on one bus. Its methods are serialised because a
// register pointer is state on the chip: two interleaved read pairs
// would each answer with the other's register.
type Device struct {
	mu sync.Mutex
	f  *os.File
}

// Open binds a descriptor to one address on one bus.
func Open(bus string, addr int) (*Device, error) {
	f, err := os.OpenFile(bus, os.O_RDWR, 0) //nolint:gosec // the operator's own i2c node, from their configuration
	if err != nil {
		return nil, err
	}
	if err := ioctl(f, slave, uintptr(addr)); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("addressing 0x%02x: %w", addr, err)
	}
	// The kernel counts this one in tens of milliseconds.
	if err := ioctl(f, timeout, uintptr(busTimeout/(10*time.Millisecond))); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("bounding the bus: %w", err)
	}
	return &Device{f: f}, nil
}

// ReadRegister points the part at one register and reads n bytes,
// most significant first where the part says so. Two transfers rather
// than one combined one: this daemon is the only master on the buses
// it is given, and a second would need I2C_RDWR here.
func (d *Device) ReadRegister(reg byte, into []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.f.Write([]byte{reg}); err != nil {
		return err
	}
	n, err := d.f.Read(into)
	if err != nil {
		return err
	}
	if n != len(into) {
		return fmt.Errorf("short read: %d of %d bytes", n, len(into))
	}
	return nil
}

// WriteRegister sets one register to one byte.
func (d *Device) WriteRegister(reg, v byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.f.Write([]byte{reg, v})
	return err
}

// WriteRegister16 sets one register to a big-endian pair.
func (d *Device) WriteRegister16(reg byte, v uint16) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.f.Write([]byte{reg, byte(v >> 8), byte(v)})
	return err
}

// Close gives the descriptor back.
func (d *Device) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.f.Close()
}

func ioctl(f *os.File, req, arg uintptr) error {
	sc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var errno unix.Errno
	if err := sc.Control(func(fd uintptr) {
		_, _, errno = unix.Syscall(unix.SYS_IOCTL, fd, req, arg)
	}); err != nil {
		return err
	}
	if errno != 0 {
		// Returned whole: a caller telling a permission refusal from
		// bus trouble needs errors.Is, not a string to match.
		return errno
	}
	return nil
}
