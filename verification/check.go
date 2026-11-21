//go:build bend

// Package verification compares observations from the Go implementation with
// the Bend model used by LAWS.bend.
package verification

import (
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed bridge.bend
var model []byte

type Comparisons []string

func (c *Comparisons) Equal(expression, observed, typ string) {
	*c = append(*c, fmt.Sprintf("def case_%d() -> {%s == %s : %s}:\n  {==}\n", len(*c), expression, observed, typ))
}

func Bool(value bool) string {
	if value {
		return "True{}"
	}
	return "False{}"
}

func (c Comparisons) Check(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if len(c) == 0 {
		t.Fatal("no Go observations to check")
	}
	if err := os.WriteFile(filepath.Join(dir, "bridge.bend"), model, 0600); err != nil {
		t.Fatal(err)
	}
	source := "import Base\nimport ./bridge.bend as B\n\n" + strings.Join(c, "\n")
	if err := os.WriteFile(filepath.Join(dir, "cases.bend"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bend", "cases.bend", "--verdict")
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(output), "ALL PROOFS CHECK") {
		t.Fatalf("Bend comparison failed: %v\n%s", err, output)
	}
	t.Logf("%d Go outcomes checked by Bend", len(c))
}
