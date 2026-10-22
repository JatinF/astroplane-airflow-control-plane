# Benchmark methodology

The measured operation is `install()` for `syn-cluster-01` in this process. The sample size is 40. Latency is wall time around that function, in milliseconds, from `time.perf_counter`. The percentile is nearest-rank: sort the samples, take rank `ceil(p/100 * n)`, and report that value. P50 and P95 in the report are that value rounded to 4 decimal places.

One install of `syn-cluster-egress` is recorded separately. It is not a member of the 40 samples, and it is not included in P50 or P95.

The script writes `benchmarks/results/benchmark_report.json` and `benchmarks/results/benchmark_report.md`. Tests recompute P50 and P95 from the stored samples. A green pytest run does not replace those samples and does not mark a gate as run.

The machine block uses `platform.platform()`, `platform.processor()`, `os.cpu_count()`, and `platform.python_version()`. It does not record a hostname.

Cost is `no price applied`. This report does not reuse prices from other repositories.

These gates stay `not-run` because this machine did not execute them:

- kube-apiserver
- kind-cluster
- helm
- airflow-scheduler-process
- image-pull
- postgres
- zero-egress-network-namespace
- wheelhouse-vendoring

The egress check is an in-process spec check. It is not a measurement of a network namespace. The offline manifest is a hash of the committed pack. It is not a vendored wheelhouse.
