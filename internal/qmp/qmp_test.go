package qmp_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/qmp"
)

// greeting is what a real QEMU sends before anything is asked of it.
const greeting = `{"QMP": {"version": {"qemu": {"micro": 2, "minor": 0, "major": 11}}, "capabilities": []}}`

// ok is the answer to qmp_capabilities.
const ok = `{"return": {}}`

// scriptConn is a QMP endpoint whose answers were decided in advance. It is a canned
// reader rather than a goroutine talking over net.Pipe because the exchange is strictly
// request-then-answer: the client reads lines in the order they were written, so a
// script is the whole server, and there is no scheduling for a test to get wrong.
type scriptConn struct {
	r      io.Reader
	sent   bytes.Buffer
	closed int
}

func newConn(lines ...string) *scriptConn {
	return &scriptConn{r: strings.NewReader(strings.Join(lines, "\n") + "\n")}
}

func (c *scriptConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *scriptConn) Write(p []byte) (int, error) { return c.sent.Write(p) }
func (c *scriptConn) Close() error                { c.closed++; return nil }

// dialer hands out one prepared connection, or an error.
type dialer struct {
	conn *scriptConn
	err  error
	path string
}

func (d *dialer) Dial(_ context.Context, path string) (io.ReadWriteCloser, error) {
	d.path = path
	if d.err != nil {
		return nil, d.err
	}
	return d.conn, nil
}

// TestDialNegotiatesCapabilities asserts on what left the process — the bytes on the
// wire — rather than on a field the client set. A client that skipped qmp_capabilities
// would satisfy any assertion about "connected" and then fail every command with
// CommandNotFound.
func TestDialNegotiatesCapabilities(t *testing.T) {
	t.Parallel()
	conn := newConn(greeting, ok)
	d := &dialer{conn: conn}

	c, err := qmp.Dial(t.Context(), d, "/run/vol/qmp.sock")
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer func() { _ = c.Close() }()

	if d.path != "/run/vol/qmp.sock" {
		t.Errorf("dialled %q, want the socket it was given", d.path)
	}
	if got := conn.sent.String(); !strings.Contains(got, `"execute":"qmp_capabilities"`) {
		t.Errorf("nothing negotiated capabilities; the wire carried: %q", got)
	}
}

func TestDialRejectsAnEndpointThatIsNotQMP(t *testing.T) {
	t.Parallel()
	conn := newConn(`{"hello": "i am not qemu"}`)
	if _, err := qmp.Dial(t.Context(), &dialer{conn: conn}, "/run/vol/qmp.sock"); err == nil {
		t.Fatal("a socket that answers without a QMP greeting was accepted")
	}
	if conn.closed == 0 {
		t.Error("the connection was left open after a failed handshake")
	}
}

func TestDialReportsNoEndpoint(t *testing.T) {
	t.Parallel()
	_, err := qmp.Dial(t.Context(), &dialer{err: errors.New("connect: no such file or directory")}, "/run/vol/qmp.sock")
	if !errors.Is(err, qmp.ErrNoEndpoint) {
		// The caller branches on this: a volume whose chain is ready and whose VM has
		// not been launched is the ordinary state, not a failure.
		t.Fatalf("want ErrNoEndpoint, got %v", err)
	}
}

func TestBlockDevices(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		reply string
		want  []qmp.BlockDevice
	}{
		{
			name: "a disk and its file",
			reply: `{"return": [{"device": "", "qdev": "/machine/peripheral-anon/device[0]/virtio-backend",
			          "inserted": {"file": "/var/lib/va/volumes/v1/active/current.qcow2", "drv": "qcow2"}}]}`,
			want: []qmp.BlockDevice{{
				QDev:   "/machine/peripheral-anon/device[0]/virtio-backend",
				File:   "/var/lib/va/volumes/v1/active/current.qcow2",
				Format: "qcow2",
			}},
		},
		{
			// The corrupt flag as a *running* QEMU reports it, in the same ImageInfo
			// `qemu-img info` prints. Measured against the pinned 11.1.1: this is the
			// only reader of that bit while a guest holds the image.
			name: "an image the guest has corrupted",
			reply: `{"return": [{"device": "virtio0", "inserted": {"file": "/var/lib/va/tip.qcow2", "drv": "qcow2",
			          "image": {"format-specific": {"type": "qcow2", "data": {"corrupt": true}}}}}]}`,
			want: []qmp.BlockDevice{{
				Device:  "virtio0",
				File:    "/var/lib/va/tip.qcow2",
				Format:  "qcow2",
				Corrupt: true,
			}},
		},
		{
			// An empty CD-ROM tray. Reporting it as a device with an empty path would
			// make it indistinguishable from a device whose file could not be read.
			name:  "a backend with no medium is skipped",
			reply: `{"return": [{"device": "ide1-cd0", "qdev": "/machine/x"}]}`,
			want:  []qmp.BlockDevice{},
		},
		{
			name:  "no block devices at all",
			reply: `{"return": []}`,
			want:  []qmp.BlockDevice{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := qmp.Dial(t.Context(), &dialer{conn: newConn(greeting, ok, tt.reply)}, "sock")
			if err != nil {
				t.Fatalf("dialling: %v", err)
			}
			got, err := c.BlockDevices()
			if err != nil {
				t.Fatalf("query-block: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d devices, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("device %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestAsynchronousEventsDoNotBecomeAnswers is the one piece of protocol subtlety in the
// package. QEMU interleaves events with command replies on the same stream, so a client
// that took the next line as its answer would read an event as a result — and would do
// it only on the days an event happened to land in the gap.
func TestAsynchronousEventsDoNotBecomeAnswers(t *testing.T) {
	t.Parallel()
	conn := newConn(
		greeting,
		`{"event": "JOB_STATUS_CHANGE", "data": {"status": "created"}, "timestamp": {"seconds": 1, "microseconds": 2}}`,
		ok,
		`{"event": "RTC_CHANGE", "data": {"offset": 1}, "timestamp": {"seconds": 3, "microseconds": 4}}`,
		`{"return": [{"device": "d", "inserted": {"file": "/img.qcow2", "drv": "qcow2"}}]}`,
	)
	c, err := qmp.Dial(t.Context(), &dialer{conn: conn}, "sock")
	if err != nil {
		t.Fatalf("dialling through an event: %v", err)
	}
	got, err := c.BlockDevices()
	if err != nil {
		t.Fatalf("query-block through an event: %v", err)
	}
	if len(got) != 1 || got[0].File != "/img.qcow2" {
		t.Fatalf("an event was read as the answer: %+v", got)
	}
}

func TestCommandErrorIsReported(t *testing.T) {
	t.Parallel()
	c, err := qmp.Dial(t.Context(), &dialer{conn: newConn(greeting, ok,
		`{"error": {"class": "CommandNotFound", "desc": "The command query-block has not been found"}}`)}, "sock")
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	_, err = c.BlockDevices()
	if err == nil {
		t.Fatal("a QMP error answer was reported as success")
	}
	if !strings.Contains(err.Error(), "CommandNotFound") {
		t.Errorf("the error does not carry QEMU's own words: %v", err)
	}
}

func TestATruncatedStreamIsAnError(t *testing.T) {
	t.Parallel()
	// The greeting arrives and the connection then dies — a QEMU that exited between
	// accept and the first answer.
	if _, err := qmp.Dial(t.Context(), &dialer{conn: newConn(greeting)}, "sock"); err == nil {
		t.Fatal("a stream that ended mid-handshake was accepted")
	}
}
