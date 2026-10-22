"""Build Deployment-shaped objects. Nothing is sent to an API server."""

from __future__ import annotations

COMPONENT_ORDER = (
    "scheduler",
    "webserver",
    "dag-processor",
    "triggerer",
    "metadata",
    "statsd",
)

YAML_KEY = {
    "scheduler": "scheduler",
    "webserver": "webserver",
    "dag-processor": "dagProcessor",
    "triggerer": "triggerer",
    "metadata": "metadata",
    "statsd": "statsd",
}

AIRFLOW_COMPONENTS = {"scheduler", "webserver", "dag-processor", "triggerer"}


def image_for(component: str, images: dict) -> str:
    if component in AIRFLOW_COMPONENTS:
        return str(images["airflow"])
    if component == "metadata":
        return str(images["postgres"])
    if component == "statsd":
        return str(images["statsd"])
    raise KeyError(component)


def plan_deployments(document: dict) -> list[dict]:
    metadata = document["metadata"]
    spec = document["spec"]
    cluster_id = metadata["name"]
    namespace = metadata.get("namespace", "astroplane")
    components = spec["components"]
    images = spec["images"]
    objects: list[dict] = []
    for component in COMPONENT_ORDER:
        replicas = int(components[YAML_KEY[component]]["replicas"])
        image = image_for(component, images)
        objects.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": f"{cluster_id}-{component}",
                    "namespace": namespace,
                    "labels": {
                        "app.kubernetes.io/name": component,
                        "astroplane.example/cluster": cluster_id,
                    },
                },
                "spec": {
                    "replicas": replicas,
                    "selector": {
                        "matchLabels": {
                            "app.kubernetes.io/name": component,
                            "astroplane.example/cluster": cluster_id,
                        }
                    },
                    "template": {
                        "metadata": {
                            "labels": {
                                "app.kubernetes.io/name": component,
                                "astroplane.example/cluster": cluster_id,
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": component,
                                    "image": image,
                                    "imagePullPolicy": "Never",
                                }
                            ]
                        },
                    },
                },
                "status": {
                    "applied": False,
                    "phase": "Pending",
                    "reason": "kubelet not-run",
                    "readyReplicas": 0,
                    "image_digest": "not-pulled",
                },
            }
        )
    return objects
