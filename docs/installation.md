# Installation

These steps build and test AstroPlane on a workstation. They do not install Airflow into a cluster.

## Python control plane

```text
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\python -m pytest
```

On Linux, use `.venv/bin/python`. The install downloads pinned wheels from PyPI the first time. Later test runs use the local environment.

`ASTROPLANE_STATE` may point at a SQLite file. When it is unset, the API keeps state in memory.

## Go controller

The module has no third-party requirements.

```text
cd operator
go test -race ./...
```

CI sets `GOPROXY=off` and `GOSUMDB=off` for that test. The Go toolchain itself is still downloaded by `actions/setup-go` when the runner does not have it. That is not a network-namespace measurement.

The controller binary reads and writes a JSON journal:

```text
go build -trimpath -buildvcs=false -ldflags=-buildid= -o astroplane-controller ./cmd/astroplane-controller
astroplane-controller -journal state.json -action provision -file testdata/syn-cluster-01.json
astroplane-controller -journal state.json -action upgrade -name syn-cluster-01 -release r2
astroplane-controller -journal state.json -action delete -name syn-cluster-01
```

`TestTwoTrimpathBuildsMatch` builds that binary twice and requires the hashes to match.

## What is not installed

kind, Helm, a Kubernetes API server, Postgres, and the Apache Airflow distribution are not installed by these commands. The CRD file is not applied. The Dockerfile is not built.
