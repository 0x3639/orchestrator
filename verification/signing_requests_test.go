//go:build bend

package verification_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Use the same test on revisions with and without certificate support. The
// overlay swaps only the external go-tss transport import. Pool selection,
// commitment generation, certificate verification and both calls to KeySign
// still come from the checkout under test.
func TestSplitTipBridgeSignatureRequestsAgreeWithBend(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	manager := filepath.Join(root, "tss", "manager.go")
	source, err := os.ReadFile(manager)
	if err != nil {
		t.Fatal(err)
	}
	upstream := []byte(`"github.com/HyperCore-Team/go-tss/tss"`)
	if bytes.Count(source, upstream) != 1 {
		t.Fatal("could not locate the go-tss transport import")
	}
	dir := t.TempDir()
	fixture := filepath.Join(dir, "manager.go")
	if err := os.WriteFile(fixture, bytes.Replace(source, upstream, []byte(`tss "orchestrator/verification/testdata/signtransport"`), 1), 0600); err != nil {
		t.Fatal(err)
	}
	overlay, err := json.Marshal(struct{ Replace map[string]string }{map[string]string{
		manager: fixture,
		filepath.Join(root, "node", "split_tip_transport_test.go"): filepath.Join(root, "verification", "testdata", "split_tip_transport_test.go"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	overlayPath := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	// Go 1.22 vet reopens the on-disk files and cannot see virtual test files.
	cmd := exec.Command("go", "test", "-vet=off", "-race", "-count=1", "-tags=bend", "-ldflags=-extldflags=-Wl,--allow-multiple-definition", "-overlay", overlayPath, "-run", "^TestRequestsAtSigningTransport$", "-v", "./node")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("signing transport comparison: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("--- PASS: TestRequestsAtSigningTransport ")) {
		t.Fatalf("signing test did not run:\n%s", out)
	}
	for _, line := range bytes.Split(out, []byte("\n")) {
		if bytes.Contains(line, []byte("bridge-signature requests")) {
			t.Logf("%s", bytes.TrimSpace(line))
		}
	}
}
