# Threat model

AstroPlane keeps installation decisions inside one process and refuses anything that would need a public registry, an external pack URL, or a scheduler process that is not running.

## What the stand-in enforces

- Images must use the `registry.local/astroplane/` prefix. A public reference such as `docker.io/apache/airflow:2.11.0` denies the install and stores no Deployment objects.
- Egress mode must be `deny-all`. Allow entries must be cluster-local DNS names ending in `.svc`, with no URL scheme.
- The DAG pack path is fixed to `datasets/offline/dags`. A custom resource cannot point the scanner at another directory.
- Pack files are read as text. A URL in the caller's own tenant becomes an import error. Another tenant's file is dropped before that scan.
- Creating a DAG run returns 503. The stand-in does not invent a run id, a logical date, or a heartbeat.
- Deployment objects stay unapplied. `imagePullPolicy` is `Never`. Digests stay `not-pulled`.
- Fixture bearer tokens are the only credentials. They are not cloud credentials. The metadata password `local-demo-only` in Compose is a fixture for a database that was not created.

## What a green local run does not prove

- The egress function does not configure a network namespace, a firewall, or an air-gapped node.
- The content hash does not prove a wheel was vendored or that an image digest was verified.
- Process-wide metrics include every cluster in the process. They are not a per-tenant authorization boundary. Cluster, DAG, and audit reads are tenant-scoped. Metrics are not.
- `version` `2.11.0` is a compatibility label. The Apache Airflow package is not installed, so the label is not `airflow.__version__`.
- Unhealthy health checks are the truthful state. They must not be flipped to healthy to make a dashboard look ready.

## Deployment decision

NO-GO. The kube-apiserver, kind, Helm, the Airflow scheduler process, image pull, Postgres, a zero-egress network namespace, and wheelhouse vendoring were not executed.
