package qcow

import (
	"context"

	"github.com/spin-stack/storage/internal/qmp"
)

// DeviceForTest exposes the drive lookup a rotation performs.
//
// Exported for a test rather than tested through Apply, because what it decides is one
// string handed to QMP and every path that reaches it in anger also seals a layer: a test
// driving Apply would be asserting on the drive id through four other decisions. This is
// the seam.
func (m *Manager) DeviceForTest(ctx context.Context, volumeID, image string) (string, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	client, err := qmp.Dial(ctx, m.dialer, QMPSocket(m.cfg.Root, volumeID))
	if err != nil {
		return "", err
	}
	defer func() { _ = client.Close() }()
	return deviceFor(client, image)
}
