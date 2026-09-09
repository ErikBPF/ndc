# Proposal: resource-configurable Kubernetes execution

Status: deployment design for review. The cluster commands below are proposed,
not implemented. Implementation follows #7; SF1000 has not been qualified.

## Existing foundation and remaining scope

PR #7 consolidates bounded DuckDB and Spark preparation, the canonical lossless
nested data model, file-collection validation, and root devenv tooling. Its base
shell provides Git and just; `duckdb`, `spark`, and `spark-k8s` add engine tooling.
Reuse these definitions and their lockfile. Do not add a second environment or
retain old workspace layouts and command aliases for this pre-launch project.

Spark preparation currently supports client deploy mode with filesystem paths
visible to the driver and every executor. The benchmark runner still orchestrates
local execution. This proposal adds reproducible Kubernetes submission for
preparation and benchmark jobs, named targets, resource configuration, shared
storage, and target-specific qualification. Installing Kubernetes tools alone
does not implement those workflows.

Generate one canonical dataset and share its exact artifacts across targets.
Preserve complete order and line-item values, child ordering, empty parents,
keys, relationships, and exact round-trip checks. Preparation and loading remain
outside query measurement. Comet settings belong to candidate execution.

## Workflow responsibilities

- Root devenv profiles supply pinned tooling; `spark-k8s` is the deployment shell.
- just exposes the cluster workflow and selects named targets.
- DevSpace deploys the minimum workload resources with an explicit context and
  namespace on every invocation.
- Versioned target configuration declares topology, resource budgets, storage,
  and immutable image identity. Credentials remain outside that file.

Use native Spark submission on Kubernetes. Reuse an existing Spark Operator when
available rather than requiring a new operator installation. Do not import
unrelated services from the reference development stack. Propagate deployment
and job failures to the command exit status.

## Profiles and target resources

Here, multi-cluster means independent Kubernetes clusters. Multiple workers in
one cluster use one target with an executor count. A Spark application runs
within one Kubernetes cluster.

| Profile | Behavior |
|---|---|
| `single-cluster` | One explicit context for preparation and benchmark campaigns |
| `multi-cluster` | One preparation target; independent campaigns on named targets |

Invoke DevSpace separately for each target context. Give each target its own
namespace, deployment identity and local state directory to avoid concurrent
configuration/cache collisions. Never switch the user's global kubeconfig context.
The profile selects targets; each target owns its resource configuration.

Each target declares:

- Kubernetes context and namespace, image digest and service account.
- Driver cores, memory and memory overhead.
- Executor count, cores, memory and memory overhead.
- Shuffle partitions, scratch storage size/class and node placement.
- Namespace resource budget and workload concurrency.
- Canonical dataset location and a distinct campaign output location.
  Start with shared filesystem paths supported by the current preparation CLI;
  object-store URIs require additional backend support.
- For managed local Minikube targets only: node count, CPU, memory and disk budget.

Preparation and benchmark execution have separate resource settings. Executor
count is fixed for controlled comparisons unless dynamic allocation is explicitly
part of the experiment and disclosed. Record resolved settings, actual allocation,
engine/source identity and failures with each campaign.

Resource requests do not provision cloud capacity. SF1000 profiles target existing
clusters with sufficient nodes, storage and registry access. Independent clusters
need access to the shared dataset, or verified byte-identical copies. Cluster
provisioning remains outside DevSpace. Local Minikube is for smoke qualification;
no SF1000 capacity claim is made for it.

## Proposed command surface

```sh
devenv --profile spark-k8s shell
just up single-cluster
just prepare single-cluster 1000
just verify single-cluster
just run single-cluster
just status single-cluster
just down single-cluster
```

Use the same sequence with `multi-cluster` to deploy and qualify every selected
target. Its `prepare` step runs once on the designated preparation target; all
other targets consume that dataset.

`up` deploys the required workload infrastructure; it does not generate data or
start measurement. `prepare` creates a fresh dataset and refuses an existing
output. `verify` checks artifact identity and performs semantic qualification for every
selected engine/target. `run` requires those target verdicts for the exact dataset,
engine and source revision and creates fresh campaign outputs. `status` reports each target
and job separately. `down` removes only NDC-owned workload resources and retains
datasets/results; deletion of local clusters or persistent data is separate.

## Implementation and acceptance

1. Define target configuration and render driver/executor resources, storage
   mounts, image identity and scoped service accounts. Reuse the canonical
   generator and existing devenv profiles.
2. Implement DevSpace submission and the just commands above. Give preparation
   and benchmark execution separate resource settings; adapt the benchmark
   runner's local-only orchestration explicitly.
3. Verify resolved profiles, resource overrides, context routing, target
   isolation, job failure propagation and cleanup without a live cluster using
   controlled CLI substitutes.
4. Qualify a tiny dataset on one cluster, then two independent targets consuming
   identical artifacts. Require exact semantic checks and per-target verdicts
   before timing. Exercise failed jobs and retries into fresh output locations.
5. Qualify SF0.5 and SF10, recording requested/limited CPU and memory, executor
   count, observed memory and scratch use, and preparation duration. SF100 and
   SF1000 are later gates; no production-readiness claim before SF1000 completes.

Describe profiles in resource terms, for example a 1 CPU / 2 GiB driver and two
4 CPU / 16 GiB executors. These are illustrative container budgets, not SF1000
sizing recommendations. JVM heap and memory overhead must fit the pod budget;
record CPU requests and limits separately when they differ.

Before implementation, resolve target contexts/storage and whether native
submission or an existing Spark Operator is the chosen deployment interface.

## Sources

- [DuckDB larger-than-memory limitations](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads#limitations)
- [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [DevSpace profiles](https://www.devspace.sh/docs/configuration/profiles/)
- [DevSpace explicit contexts and variable overrides](https://www.devspace.sh/docs/cli/devspace_deploy)
- [Shared NDC data contract](data-contract.md)
- [Current runner and engine boundaries](engines.md)
