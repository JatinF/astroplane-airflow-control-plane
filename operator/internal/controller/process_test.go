package controller

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func moduleRoot(t *testing.T) string {
	t.Helper()
	return filepath.Clean(filepath.Join("..", ".."))
}

func exeName() string {
	if runtime.GOOS == "windows" {
		return "astroplane-controller.exe"
	}
	return "astroplane-controller"
}

func buildAt(t *testing.T, path string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-ldflags=-buildid=", "-o", path, "./cmd/astroplane-controller")
	cmd.Dir = moduleRoot(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
}

func runController(t *testing.T, bin string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command(bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return out
}

func TestControllerProcessRestarts(t *testing.T) {
	bin := filepath.Join(t.TempDir(), exeName())
	buildAt(t, bin)
	journal := filepath.Join(t.TempDir(), "state.json")
	file := filepath.Join(moduleRoot(t), "testdata", "syn-cluster-01.json")
	runController(t, bin, "-journal", journal, "-action", "provision", "-file", file)
	first := dumpCluster(t, bin, journal)
	if first.Status.Phase != "Planned" || len(first.Status.Deployments) != 6 || first.Status.Applied {
		t.Fatalf("provision %#v", first.Status.Phase)
	}
	if len(first.Metadata.Finalizers) != 1 {
		t.Fatal(first.Metadata.Finalizers)
	}
	second := dumpCluster(t, bin, journal)
	if second.Status.Phase != "Planned" || len(second.Status.Deployments) != 6 {
		t.Fatal("restart lost the journal")
	}
	runController(t, bin, "-journal", journal, "-action", "upgrade", "-name", "syn-cluster-01", "-release", "r2")
	upgraded := dumpCluster(t, bin, journal)
	if upgraded.Status.ObservedRelease != "r2" || upgraded.Status.Applied {
		t.Fatalf("upgrade %#v", upgraded.Status)
	}
	image := upgraded.Status.Deployments[0].Spec.Template.Spec.Containers[0].Image
	if image != "registry.local/astroplane/airflow:2.11.0-r2" {
		t.Fatal(image)
	}
	runController(t, bin, "-journal", journal, "-action", "mark-deleting", "-name", "syn-cluster-01")
	cmd := exec.Command(bin, "-journal", journal, "-action", "dump", "-name", "syn-cluster-01")
	if err := cmd.Run(); err == nil {
		t.Fatal("the next process kept a cluster that was marked deleting")
	}
}

func dumpCluster(t *testing.T, bin, journal string) Cluster {
	t.Helper()
	out := runController(t, bin, "-journal", journal, "-action", "dump", "-name", "syn-cluster-01")
	var cluster Cluster
	if err := json.Unmarshal(out, &cluster); err != nil {
		t.Fatalf("dump: %v\n%s", err, out)
	}
	return cluster
}

func TestTwoTrimpathBuildsMatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), exeName())
	buildAt(t, path)
	first, err := hashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buildAt(t, path)
	second, err := hashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("trimpath builds differ %s %s", first, second)
	}
}

func hashFile(path string) (string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return fmtHex(sum[:]), nil
}

func fmtHex(value []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(value)*2)
	for i, b := range value {
		out[i*2] = digits[b>>4]
		out[i*2+1] = digits[b&0x0f]
	}
	return string(out)
}
