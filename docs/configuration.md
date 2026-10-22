# Configuration

## AirflowCluster

The sample documents live in `datasets/clusters/`. The Go controller reads the same shape as JSON in `operator/testdata/`.

| Field | Required value in this reference |
| --- | --- |
| `spec.tenant` | `tenant-a` or `tenant-b` |
| `spec.airflowVersion` | `2.11.0` |
| `spec.executor` | `LocalExecutor` |
| `spec.offlineDagPack` | `datasets/offline/dags` |
| `spec.egress.mode` | `deny-all` |
| `spec.egress.allow` | empty, or names that end in `.svc` and contain no `://` |
| `spec.images.*` | prefix `registry.local/astroplane/` |
| `spec.release` | `r1` when omitted; an upgrade sets `r2` and so on |

An upgrade does not accept a raw image reference. The controller builds `registry.local/astroplane/airflow:2.11.0-<release>` from the base image in the document. `r1` keeps the base tag.

## API access

Fixture bearer tokens authorize nothing outside the process.

| Token | Tenant | Role |
| --- | --- | --- |
| `dev-operator-a` | tenant-a | operator |
| `dev-viewer-a` | tenant-a | viewer |
| `dev-operator-b` | tenant-b | operator |

Operators install, upgrade, and delete. Viewers read. A missing token is 401. A viewer mutation is 403. Another tenant's cluster is 404.

## State

| Process | File | Environment |
| --- | --- | --- |
| Python API | SQLite | `ASTROPLANE_STATE` |
| Go controller | JSON journal | `-journal` |

The in-memory Python store is the benchmark path. It does not write a file.

## Finalizer

`astroplane.example/finalizer` is added to a Planned cluster. Delete clears Deployments, removes the finalizer, and drops the object. A Denied cluster has no finalizer because it stored no Deployments.
