package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func fixture(t *testing.T, name string) Cluster {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var cluster Cluster
	if err := json.Unmarshal(body, &cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func openTemp(t *testing.T) *Controller {
	t.Helper()
	runtime, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestProvisionPlansSixPendingDeployments(t *testing.T) {
	runtime := openTemp(t)
	got, err := runtime.Provision(fixture(t, "syn-cluster-01.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Planned" || got.Status.Applied {
		t.Fatalf("phase %s applied %v", got.Status.Phase, got.Status.Applied)
	}
	if len(got.Metadata.Finalizers) != 1 || got.Metadata.Finalizers[0] != Finalizer {
		t.Fatalf("finalizers %#v", got.Metadata.Finalizers)
	}
	if len(got.Status.Deployments) != 6 {
		t.Fatalf("deployments %d", len(got.Status.Deployments))
	}
	want := []string{
		"syn-cluster-01-scheduler",
		"syn-cluster-01-webserver",
		"syn-cluster-01-dag-processor",
		"syn-cluster-01-triggerer",
		"syn-cluster-01-metadata",
		"syn-cluster-01-statsd",
	}
	for i, name := range want {
		item := got.Status.Deployments[i]
		if item.Metadata.Name != name {
			t.Fatalf("name %s", item.Metadata.Name)
		}
		if item.Status.Phase != "Pending" || item.Status.Applied || item.Status.ReadyReplicas != 0 {
			t.Fatalf("status %#v", item.Status)
		}
		if item.Status.Reason != "kubelet not-run" || item.Status.ImageDigest != "not-pulled" {
			t.Fatalf("status %#v", item.Status)
		}
		if item.Spec.Template.Spec.Containers[0].ImagePullPolicy != "Never" {
			t.Fatal(item.Spec.Template.Spec.Containers[0].ImagePullPolicy)
		}
	}
	again, err := runtime.Provision(fixture(t, "syn-cluster-01.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Status.Deployments) != 6 || len(again.Metadata.Finalizers) != 1 {
		t.Fatalf("second reconcile duplicated objects")
	}
}

func TestPublicImageIsDeniedWithoutDeployments(t *testing.T) {
	runtime := openTemp(t)
	got, err := runtime.Provision(fixture(t, "syn-cluster-egress.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Denied" || len(got.Status.Deployments) != 0 || len(got.Metadata.Finalizers) != 0 {
		t.Fatalf("%#v", got.Status)
	}
	if len(got.Status.AdmissionReasons) != 1 || got.Status.AdmissionReasons[0] != "image-registry:airflow" {
		t.Fatalf("reasons %#v", got.Status.AdmissionReasons)
	}
}

func TestUpgradeKeepsObjectsUnapplied(t *testing.T) {
	runtime := openTemp(t)
	if _, err := runtime.Provision(fixture(t, "syn-cluster-01.json")); err != nil {
		t.Fatal(err)
	}
	got, err := runtime.Upgrade("syn-cluster-01", "r2")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Planned" || got.Status.Applied || got.Status.ObservedRelease != "r2" {
		t.Fatalf("%#v", got.Status)
	}
	if got.Metadata.Generation != 2 {
		t.Fatal(got.Metadata.Generation)
	}
	scheduler := got.Status.Deployments[0].Spec.Template.Spec.Containers[0].Image
	metadata := got.Status.Deployments[4].Spec.Template.Spec.Containers[0].Image
	if scheduler != "registry.local/astroplane/airflow:2.11.0-r2" {
		t.Fatal(scheduler)
	}
	if metadata != "registry.local/astroplane/postgres:16" {
		t.Fatal(metadata)
	}
	if _, err := runtime.Upgrade("syn-cluster-egress", "r2"); err == nil {
		t.Fatal("missing cluster upgraded")
	}
}

func TestDeleteRunsTheFinalizer(t *testing.T) {
	runtime := openTemp(t)
	if _, err := runtime.Provision(fixture(t, "syn-cluster-01.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Delete("syn-cluster-01"); err != nil {
		t.Fatal(err)
	}
	if _, ok := runtime.Get("syn-cluster-01"); ok {
		t.Fatal("deleted cluster is still stored")
	}
}

func TestRestartFinishesASavedDeletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Provision(fixture(t, "syn-cluster-01.json")); err != nil {
		t.Fatal(err)
	}
	if err := first.MarkDeleting("syn-cluster-01"); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := second.Get("syn-cluster-01"); ok {
		t.Fatal("recovery left the deleting cluster in the journal")
	}
	third, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := third.Get("syn-cluster-01"); ok {
		t.Fatal("cluster returned")
	}
}

func TestParallelProvision(t *testing.T) {
	runtime := openTemp(t)
	base := fixture(t, "syn-cluster-01.json")
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			doc := base
			doc.Metadata.Name = fmt.Sprintf("syn-par-%d", i)
			if _, err := runtime.Provision(doc); err != nil {
				errors <- err
			}
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	for i := 0; i < 8; i++ {
		if _, ok := runtime.Get(fmt.Sprintf("syn-par-%d", i)); !ok {
			t.Fatalf("missing syn-par-%d", i)
		}
	}
}

func TestTempJournalIsRestored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	body := []byte("{\"clusters\":{\"syn-cluster-01\":{\"metadata\":{\"name\":\"syn-cluster-01\"}}}}\n")
	if err := os.WriteFile(path+".tmp", body, 0o644); err != nil {
		t.Fatal(err)
	}
	journal, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if journal.Clusters["syn-cluster-01"].Metadata.Name != "syn-cluster-01" {
		t.Fatalf("%#v", journal.Clusters)
	}
}
