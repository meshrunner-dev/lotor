//go:build linux

package ina219

// The bus half. Everything that knows what an ioctl is lives here, and
// nothing above internal/sensor sees it — the seam internal/radio draws
// for SPI and GPIO, drawn again for I2C.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"meshrunner.dev/lotor/internal/sensor"
)

func init() {
	sensor.Register("ina219", sensor.Driver{
		Open: open, Inspect: inspect, Schema: Schema(),
	})
}

// The i2c-dev ioctls this driver uses. They are stable kernel ABI and
// x/sys/unix does not name them.
const (
	i2cSlave   = 0x0703 // bind this descriptor to one address
	i2cTimeout = 0x0702 // adapter timeout, in units of 10ms
)

// busTimeout is what the adapter is told to wait before giving up on a
// transfer. It is the only thing between a slave holding the line low
// and a read that never returns: no context reaches a thread already
// blocked in the kernel, so the bound has to be set on the bus itself.
const busTimeout = 2 * time.Second

// The register map. A datasheet's numbers, kept as its numbers.
const (
	regConfig = 0x00
	regShunt  = 0x01
	regBus    = 0x02
)

// configContinuous is the part's own reset value: 32V bus range, the
// ±320mV shunt range, 12-bit conversions on both, sampling without
// being asked. Written rather than assumed, because whatever ran
// before us on this bus may have left the part somewhere else.
const configContinuous = 0x399F

// device is one INA219 on one bus. Reads are serialised because the
// register pointer is state on the chip: two interleaved read pairs
// would each answer with the other's register.
type device struct {
	mu    sync.Mutex
	f     *os.File
	shunt float64
}

func open(cfg map[string]any, _ *zap.Logger) (sensor.Device, error) {
	s, err := decode(cfg)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.Bus, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("ina219: %w", err)
	}
	d := &device{f: f, shunt: s.ShuntOhms}
	if err := d.bind(s.Address); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := d.writeReg(regConfig, configContinuous); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("ina219: configuring: %w", err)
	}
	return d, nil
}

// bind ties the descriptor to one address and bounds how long the
// adapter will wait on it.
func (d *device) bind(addr int) error {
	if err := ioctl(d.f, i2cSlave, uintptr(addr)); err != nil {
		return fmt.Errorf("ina219: addressing 0x%02x: %w", addr, err)
	}
	// The kernel counts this one in tens of milliseconds.
	if err := ioctl(d.f, i2cTimeout, uintptr(busTimeout/(10*time.Millisecond))); err != nil {
		return fmt.Errorf("ina219: bounding the bus: %w", err)
	}
	return nil
}

// Read takes one sample: the bus voltage and the shunt voltage, from
// which the current and the power follow. The calibration register is
// left alone — the part's own current register needs it, and computing
// from the shunt voltage instead costs one division and no state.
func (d *device) Read(ctx context.Context) ([]sensor.Reading, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	rawBus, err := d.readReg(regBus)
	if err != nil {
		return nil, fmt.Errorf("ina219: bus voltage: %w", err)
	}
	rawShunt, err := d.readReg(regShunt)
	if err != nil {
		return nil, fmt.Errorf("ina219: shunt voltage: %w", err)
	}

	return readings(rawBus, rawShunt, d.shunt, time.Now()), nil
}

func (d *device) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.f.Close()
}

// readReg points the chip at one register and reads its two bytes,
// most significant first. Two transfers rather than one combined
// one: this daemon is the only master on the buses it is given, and
// a second one would need I2C_RDWR here.
func (d *device) readReg(reg byte) (uint16, error) {
	if _, err := d.f.Write([]byte{reg}); err != nil {
		return 0, err
	}
	var buf [2]byte
	n, err := d.f.Read(buf[:])
	if err != nil {
		return 0, err
	}
	if n != len(buf) {
		return 0, fmt.Errorf("short read: %d of %d bytes", n, len(buf))
	}
	return uint16(buf[0])<<8 | uint16(buf[1]), nil
}

func (d *device) writeReg(reg byte, v uint16) error {
	_, err := d.f.Write([]byte{reg, byte(v >> 8), byte(v)})
	return err
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
