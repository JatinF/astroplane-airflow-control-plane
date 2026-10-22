# Recovery

## Procedure

1. Stop the controller or the API process.
2. Keep the journal file. Python uses the SQLite path in `ASTROPLANE_STATE`. Go uses the `-journal` path.
3. Start the process again on that same file.
4. On open, a cluster whose `deletionTimestamp` is set is finished: its Deployments are cleared, `astroplane.example/finalizer` is removed, and the object is dropped.
5. A cluster that was Planned and had no deletion timestamp is still Planned. Its Deployments stay Pending and unapplied.

The Go command `astroplane-controller -action mark-deleting` saves the timestamp and exits before cleanup. The next process performs the cleanup. `TestControllerProcessRestarts` and `TestRestartFinishesASavedDeletion` run that sequence. The Python equivalent is `tests/test_lifecycle.py`.

If `state.json` is missing and `state.json.tmp` is present, the Go loader renames the temp file back. Windows cannot replace an existing file with `rename`, so a crash between the delete and the rename can leave the temp file. The loader covers that case. `TestTempJournalIsRestored` checks it.

## What recovery does not do

It does not restart kubelet, a Kubernetes controller manager, SchedulerJob, or Postgres. Those processes were not started. A recovered plan is still unapplied. `components_running` stays 0.
