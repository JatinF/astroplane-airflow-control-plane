package controller

import (
	"fmt"
	"regexp"
	"sync"
	"time"
)

var releasePattern = regexp.MustCompile(`^r[1-9][0-9]{0,3}$`)

var componentOrder = []struct {
	name    string
	yamlKey string
	image   string
}{
	{name: "scheduler", yamlKey: "scheduler", image: "airflow"},
	{name: "webserver", yamlKey: "webserver", image: "airflow"},
	{name: "dag-processor", yamlKey: "dagProcessor", image: "airflow"},
	{name: "triggerer", yamlKey: "triggerer", image: "airflow"},
	{name: "metadata", yamlKey: "metadata", image: "postgres"},
	{name: "statsd", yamlKey: "statsd", image: "statsd"},
}

// Controller is a reference controller. It reconciles AirflowCluster
// objects in a local JSON journal using the standard library.
// The mutex is the stand-in for a single-threaded work queue. Parallel
// callers are serialized so `go test -race` can exercise the lock.

type Controller struct {
	mu      sync.Mutex
	journal *Journal
}

func Open(path string) (*Controller, error) {
	journal, err := Load(path)
	if err != nil {
		return nil, err
	}
	controller := &Controller{journal: journal}
	if _, err := controller.Recover(); err != nil {
		return nil, err
	}
	return controller, nil
}

func (c *Controller) Get(name string) (Cluster, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.journal.Clusters[name]
	return item, ok
}

func (c *Controller) Provision(desired Cluster) (Cluster, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if desired.Metadata.DeletionTimestamp != "" {
		return c.finishLocked(desired.Metadata.Name)
	}
	admitted, reasons := admit(desired.Spec)
	release := desired.Spec.Release
	if release == "" {
		release = DefaultRelease
	}
	if !releasePattern.MatchString(release) {
		admitted = false
		reasons = append(reasons, "release")
	}
	next := desired
	next.Spec.Release = release
	next.Metadata.Generation = 1
	next.Status = Status{Applied: false, AdmissionReasons: reasons}
	if admitted {
		next.Metadata.Finalizers = []string{Finalizer}
		next.Status.Phase = "Planned"
		next.Status.ObservedRelease = release
		next.Status.Deployments = plan(next, release)
		next.Status.Conditions = []Condition{
			{Type: "Egress", Status: "True", Reason: "deny-all"},
			{Type: "Ready", Status: "False", Reason: "kube-apiserver not-run"},
		}
	} else {
		next.Metadata.Finalizers = nil
		next.Status.Phase = "Denied"
		next.Status.Deployments = nil
		next.Status.Conditions = []Condition{
			{Type: "Egress", Status: "False", Reason: join(reasons)},
			{Type: "Ready", Status: "False", Reason: "egress-denied"},
		}
	}
	c.journal.Clusters[next.Metadata.Name] = next
	if err := c.journal.Save(); err != nil {
		return Cluster{}, err
	}
	return next, nil
}

func (c *Controller) Upgrade(name, release string) (Cluster, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !releasePattern.MatchString(release) {
		return Cluster{}, fmt.Errorf("release must look like r2")
	}
	current, ok := c.journal.Clusters[name]
	if !ok {
		return Cluster{}, fmt.Errorf("cluster not found")
	}
	if current.Metadata.DeletionTimestamp != "" {
		return Cluster{}, fmt.Errorf("cluster is deleting")
	}
	if current.Status.Phase != "Planned" {
		return Cluster{}, fmt.Errorf("upgrade requires phase Planned")
	}
	current.Spec.Release = release
	current.Metadata.Generation++
	current.Status.ObservedRelease = release
	current.Status.Applied = false
	current.Status.Deployments = plan(current, release)
	for i := range current.Status.Conditions {
		if current.Status.Conditions[i].Type == "Ready" {
			current.Status.Conditions[i].Status = "False"
			current.Status.Conditions[i].Reason = "kube-apiserver not-run"
		}
	}
	c.journal.Clusters[name] = current
	if err := c.journal.Save(); err != nil {
		return Cluster{}, err
	}
	return current, nil
}

func (c *Controller) Delete(name string) (Cluster, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.journal.Clusters[name]
	if !ok {
		return Cluster{}, fmt.Errorf("cluster not found")
	}
	current.Metadata.DeletionTimestamp = time.Now().UTC().Format(time.RFC3339)
	c.journal.Clusters[name] = current
	if err := c.journal.Save(); err != nil {
		return Cluster{}, err
	}
	return c.finishLocked(name)
}

// MarkDeleting saves a deletion timestamp and returns before cleanup.
// The next Open call finishes that cleanup. It is the crash window the
// restart test uses.
func (c *Controller) MarkDeleting(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.journal.Clusters[name]
	if !ok {
		return fmt.Errorf("cluster not found")
	}
	current.Metadata.DeletionTimestamp = time.Now().UTC().Format(time.RFC3339)
	c.journal.Clusters[name] = current
	return c.journal.Save()
}

// Recover finishes deletions whose timestamp was saved before the process stopped.
func (c *Controller) Recover() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var finished []string
	for name, item := range c.journal.Clusters {
		if item.Metadata.DeletionTimestamp == "" {
			continue
		}
		if _, err := c.finishLocked(name); err != nil {
			return finished, err
		}
		finished = append(finished, name)
	}
	return finished, nil
}

func (c *Controller) finishLocked(name string) (Cluster, error) {
	current, ok := c.journal.Clusters[name]
	if !ok {
		return Cluster{}, fmt.Errorf("cluster not found")
	}
	current.Status.Deployments = nil
	current.Status.Phase = "Deleted"
	current.Status.Applied = false
	current.Metadata.Finalizers = nil
	delete(c.journal.Clusters, name)
	if err := c.journal.Save(); err != nil {
		return Cluster{}, err
	}
	return current, nil
}

func admit(spec Spec) (bool, []string) {
	var reasons []string
	if spec.Egress.Mode != "deny-all" {
		reasons = append(reasons, "egress-mode")
	}
	for _, host := range spec.Egress.Allow {
		if containsScheme(host) || !hasSuffix(host, ".svc") {
			reasons = append(reasons, "allow-host")
			break
		}
	}
	for _, name := range []string{"airflow", "postgres", "statsd"} {
		image := spec.Images[name]
		if image == "" || !hasPrefix(image, LocalPrefix) {
			reasons = append(reasons, "image-registry:"+name)
		}
	}
	if spec.OfflineDagPack != PackPath {
		reasons = append(reasons, "pack-path")
	}
	if spec.AirflowVersion != CompatVersion {
		reasons = append(reasons, "airflow-version")
	}
	return len(reasons) == 0, reasons
}

func plan(cluster Cluster, release string) []Deployment {
	namespace := cluster.Metadata.Namespace
	if namespace == "" {
		namespace = "astroplane"
	}
	base := cluster.Spec.Images["airflow"]
	objects := make([]Deployment, 0, len(componentOrder))
	for _, component := range componentOrder {
		image := cluster.Spec.Images[component.image]
		if component.image == "airflow" && release != DefaultRelease {
			image = base + "-" + release
		}
		replicas := cluster.Spec.Components[component.yamlKey].Replicas
		labels := map[string]string{
			"app.kubernetes.io/name":     component.name,
			"astroplane.example/cluster": cluster.Metadata.Name,
		}
		item := Deployment{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Metadata: DeploymentMeta{
				Name:      cluster.Metadata.Name + "-" + component.name,
				Namespace: namespace,
				Labels:    labels,
			},
			Spec: DeploymentSpec{
				Replicas: replicas,
				Selector: map[string]any{"matchLabels": labels},
			},
			Status: DeploymentStatus{
				Applied:       false,
				Phase:         "Pending",
				Reason:        "kubelet not-run",
				ReadyReplicas: 0,
				ImageDigest:   "not-pulled",
			},
		}
		item.Spec.Template.Metadata.Labels = labels
		item.Spec.Template.Spec.Containers = []Container{{
			Name:            component.name,
			Image:           image,
			ImagePullPolicy: "Never",
		}}
		objects = append(objects, item)
	}
	return objects
}

func join(reasons []string) string {
	out := ""
	for i, reason := range reasons {
		if i > 0 {
			out += ","
		}
		out += reason
	}
	return out
}

func containsScheme(value string) bool {
	return len(value) >= 3 && (indexOf(value, "://") >= 0)
}

func indexOf(value, part string) int {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return i
		}
	}
	return -1
}

func hasPrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}

func hasSuffix(value, suffix string) bool {
	return len(value) >= len(suffix) && value[len(value)-len(suffix):] == suffix
}
