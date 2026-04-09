/*
Package k8s contains the SDK's controller-runtime client helpers for StoryRun,
StepRun, and Impulse operations.

Consumers using these helpers need Kubernetes authentication plus RBAC that
matches the methods they call. The common SDK paths require at least:

  - StoryRun launch and lookup: `storytriggers` `create`/`get` and `storyruns` `get`
  - StoryRun stop/status updates: `storyruns` `get` and `storyruns/status` `patch`
  - StepRun status updates: `stepruns` `get` and `stepruns/status` `patch`
  - Impulse trigger counters: `impulses` `get` and `impulses/status` `patch`

Additional controller or application features may require broader read access,
but the list above covers the baseline SDK operations exposed by this package.
*/
package k8s
