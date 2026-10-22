# AstroPlane control-plane benchmark

Evidence class: MEASURED. Decision: NO-GO.

40 in-process installs of syn-cluster-01. One denied install of syn-cluster-egress is excluded from the percentile sample.

P50 5.8009 ms, P95 12.9726 ms.
Denied install latency 13.0978 ms is outside the percentile sample.
Machine: Windows-11-10.0.26200-SP0. Processor: Intel64 Family 6 Model 140 Stepping 1, GenuineIntel. cpu_count 8. Python 3.12.10.

After the planned sample, phase Planned, deployments 6, components_running 0, accepted DAGs ['SYN-DAG-ETL'].
After the denied install, phase Denied, deployments 0, reasons ['image-registry:airflow'].

Cost: no price applied.
Airflow distribution: not-installed. Images pulled: false. Wheelhouse: not-vendored.

Gates:
- kube-apiserver: not-run
- kind-cluster: not-run
- helm: not-run
- airflow-scheduler-process: not-run
- image-pull: not-run
- postgres: not-run
- zero-egress-network-namespace: not-run
- wheelhouse-vendoring: not-run
