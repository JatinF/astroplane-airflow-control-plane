package controller

// AirflowCluster is the in-process object the controller reconciles.
// Objects are not posted to a Kubernetes API server.

const (
	Finalizer      = "astroplane.example/finalizer"
	CompatVersion  = "2.11.0"
	LocalPrefix    = "registry.local/astroplane/"
	PackPath       = "datasets/offline/dags"
	DefaultRelease = "r1"
)

type Cluster struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       Spec     `json:"spec"`
	Status     Status   `json:"status"`
}

type Metadata struct {
	Name              string   `json:"name"`
	Namespace         string   `json:"namespace"`
	Finalizers        []string `json:"finalizers"`
	Generation        int      `json:"generation"`
	DeletionTimestamp string   `json:"deletionTimestamp,omitempty"`
}

type Spec struct {
	Tenant         string               `json:"tenant"`
	AirflowVersion string               `json:"airflowVersion"`
	Executor       string               `json:"executor"`
	OfflineDagPack string               `json:"offlineDagPack"`
	Release        string               `json:"release,omitempty"`
	Egress         Egress               `json:"egress"`
	Images         map[string]string    `json:"images"`
	Components     map[string]Component `json:"components"`
}

type Egress struct {
	Mode  string   `json:"mode"`
	Allow []string `json:"allow"`
}

type Component struct {
	Replicas int `json:"replicas"`
}

type Status struct {
	Phase            string       `json:"phase"`
	Applied          bool         `json:"applied"`
	ObservedRelease  string       `json:"observedRelease,omitempty"`
	Conditions       []Condition  `json:"conditions"`
	Deployments      []Deployment `json:"deployments"`
	AdmissionReasons []string     `json:"admissionReasons"`
}

type Condition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type Deployment struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   DeploymentMeta   `json:"metadata"`
	Spec       DeploymentSpec   `json:"spec"`
	Status     DeploymentStatus `json:"status"`
}

type DeploymentMeta struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels"`
}

type DeploymentSpec struct {
	Replicas int                `json:"replicas"`
	Selector map[string]any     `json:"selector"`
	Template DeploymentTemplate `json:"template"`
}

type DeploymentTemplate struct {
	Metadata struct {
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Containers []Container `json:"containers"`
	} `json:"spec"`
}

type Container struct {
	Name            string `json:"name"`
	Image           string `json:"image"`
	ImagePullPolicy string `json:"imagePullPolicy"`
}

type DeploymentStatus struct {
	Applied       bool   `json:"applied"`
	Phase         string `json:"phase"`
	Reason        string `json:"reason"`
	ReadyReplicas int    `json:"readyReplicas"`
	ImageDigest   string `json:"image_digest"`
}
