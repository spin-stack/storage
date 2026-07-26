package cpserver_test

import (
	"net/http/httptest"
	"testing"

	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/cpserver"
)

// serveForTest mounts the handler on a throwaway HTTP server and returns a client
// pointed at it. It exists so at least one test exercises the real wire path
// (Connect over HTTP/1.1) rather than calling the methods in process.
func serveForTest(t *testing.T, srv *cpserver.Server) (storagev1connect.ControlPlaneServiceClient, func()) {
	t.Helper()
	mux := httptest.NewServer(cpserver.Handler(srv))
	client := storagev1connect.NewControlPlaneServiceClient(mux.Client(), mux.URL)
	return client, mux.Close
}
