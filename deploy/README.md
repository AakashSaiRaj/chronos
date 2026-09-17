# Deploying Chronos

Terraform provisions the AWS infrastructure; Kustomize deploys the workloads onto
EKS; GitHub Actions runs both.

```
deploy/
  terraform/          VPC, EKS, RDS, IAM/IRSA, ECR, optional Redis + SQS
    envs/             per-environment tfvars and backend config
  k8s/
    base/             namespace, service accounts, config, workloads, policies
    overlays/dev/     1 replica each, debug logs, tight HPA ceiling
    overlays/prod/    3 API + 2 scheduler, real HPA headroom, ingress
```

## What runs where

| Workload | Replicas | Engine | Reaper | Scales on |
|---|---|---|---|---|
| `chronos-api` | fixed (3 in prod) | off | off | request volume, manually |
| `chronos-scheduler` | fixed (2 in prod) | on | on | nothing — see below |
| `chronos-worker` | HPA 3–40 | — | — | CPU / memory |

The tiers are split because their load is unrelated. API load follows request
volume; scheduling load follows the number of live executions. Coupling them would
mean an API traffic spike spawning schedulers with no extra work to do.

**The scheduler runs two replicas, and that is deliberate.** The engine locks each
execution with `SELECT ... FOR UPDATE SKIP LOCKED` and the reaper's writes are
guarded compare-and-swaps, so a second replica either works on a different
execution or moves on. It is not leader-elected and does not need to be. A single
replica would mean one node failure stops scheduling for every workflow in the
system.

**Only workers autoscale.** They hold no state and reach the control plane only
over HTTP, so killing one just means its lease lapses and the reaper hands its task
to another. That property is what makes aggressive scaling safe here and not for
the control plane.

## Bootstrap

Terraform cannot store its own state, so the state bucket is created once per
account, outside the module:

```bash
aws s3api create-bucket --bucket chronos-terraform-state-<account> --region us-east-1
aws s3api put-bucket-versioning --bucket chronos-terraform-state-<account> \
  --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket chronos-terraform-state-<account> \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

Then edit the bucket name into `envs/*.backend.hcl` and:

```bash
cd deploy/terraform
terraform init -backend-config=envs/dev.backend.hcl
terraform plan  -var-file=envs/dev.tfvars
terraform apply -var-file=envs/dev.tfvars
```

### A note on `.terraform.lock.hcl`

It is committed on purpose — it pins provider versions and checksums, so CI cannot
silently resolve a different version than the one reviewed.

It also carries hashes for `linux_amd64`, `darwin_arm64`, and `darwin_amd64`. A
plain `terraform init` records hashes only for the platform it ran on, so a lock
file generated on a Mac makes `terraform init` fail on Linux CI with a checksum
mismatch. After changing a provider version, regenerate it for every platform
rather than letting `init` do it:

```bash
terraform providers lock \
  -platform=linux_amd64 -platform=darwin_arm64 -platform=darwin_amd64
```

`.terraform/` itself is gitignored: it is a cache of platform-specific provider
binaries, hundreds of megabytes, and rebuilt by `terraform init`.

## Cluster add-ons Terraform does not manage

Three things are installed with Helm rather than Terraform, because they are
cluster components with their own release cadence and Terraform is a poor fit for
managing Helm releases it did not create.

```bash
aws eks update-kubeconfig --name "$(terraform output -raw cluster_name)"

# The HPA cannot function without this. There is no EKS add-on for it.
helm upgrade --install metrics-server metrics-server \
  --repo https://kubernetes-sigs.github.io/metrics-server/ \
  --namespace kube-system

# Serves the prod Ingress. Uses IRSA; see the controller's own docs for its role.
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
  --namespace kube-system \
  --set clusterName="$(terraform output -raw cluster_name)"

# Provides the AWS provider for the Secrets Store CSI driver. The driver itself is
# an EKS add-on installed by Terraform; this is the provider plugin it calls.
helm upgrade --install secrets-provider-aws \
  aws-secrets-manager/secrets-store-csi-driver-provider-aws \
  --namespace kube-system
```

NetworkPolicy enforcement also has to be switched on explicitly — the VPC CNI
ignores policies by default, which means the manifests would apply cleanly and
enforce nothing:

```bash
aws eks update-addon --cluster-name "$(terraform output -raw cluster_name)" \
  --addon-name vpc-cni \
  --configuration-values '{"enableNetworkPolicy":"true"}'
```

## Deploying

CI/CD does this; the manual equivalent is:

```bash
cd deploy/terraform
export CHRONOS_SERVER_ROLE_ARN=$(terraform output -raw server_role_arn)
export CHRONOS_WORKER_ROLE_ARN=$(terraform output -raw worker_role_arn)
export CHRONOS_DATABASE_SECRET_NAME=$(terraform output -raw database_secret_name)
cd ../..

kubectl kustomize deploy/k8s/overlays/dev | envsubst > /tmp/chronos.yaml

# Migrations first, as a Job, so a bad migration fails the deploy rather than
# crash-looping every replica.
kubectl apply -f /tmp/chronos.yaml
kubectl -n chronos wait --for=condition=complete job/chronos-migrate --timeout=10m

kubectl -n chronos rollout status deployment/chronos-api
kubectl -n chronos rollout status deployment/chronos-scheduler
kubectl -n chronos rollout status deployment/chronos-worker
```

Every tier sets `maxUnavailable: 0`, so a rollout that cannot produce a ready pod
stalls instead of replacing healthy pods with broken ones. `rollout status`
converts that stall into a failed pipeline step, and the CD workflow rolls back.

## Secrets

The database DSN lives in Secrets Manager and is mounted as a file by the Secrets
Store CSI driver, using each pod's own IRSA identity. `CHRONOS_DATABASE_URL_FILE`
points the process at it.

There is no Kubernetes Secret. That is the point:

- nothing to `kubectl get secret`, so cluster-wide secret read access — which is
  broader than intended in most clusters — does not expose the database password;
- the value never enters an environment variable, so it is absent from
  `kubectl describe`, from crash dumps, and from `/proc/<pid>/environ`;
- authorization is the pod's own, so no controller holds standing read access to
  every secret in the account;
- rotating the credential in Secrets Manager does not require recreating a Secret.

Verify it is actually being used rather than a fallback:

```bash
kubectl -n chronos exec deploy/chronos-api -- \
  ls -l /etc/chronos/secrets/database-url
kubectl -n chronos get deploy chronos-api -o yaml | grep -c 'postgres://'   # want 0
```

## IAM

Three roles, each scoped to one thing:

| Role | Grants |
|---|---|
| `chronos-*-server` | read one named secret; `kms:Decrypt` only via Secrets Manager |
| `chronos-*-worker` | nothing |
| `chronos-*-cicd` | push to one ECR repo; `eks:DescribeCluster` on one cluster |

The worker role having no permissions is intentional, not an omission. Workers need
no AWS access, so a compromised worker pod cannot read the credential the API pods
can. The role exists so the service account has a stable audit identity and so
granting it something later is an explicit, reviewable change.

The CI/CD role is assumed through GitHub OIDC, so no access key exists to leak, and
its trust policy pins the repository and ref. Its in-cluster authorization is a
separate EKS access entry scoped to the `chronos` namespace, so it cannot grant
itself Kubernetes permissions by editing IAM.

## Redis and SQS are off

`enable_redis` and `enable_sqs` default to `false`.

Chronos's queue is the `tasks` table, claimed with `SKIP LOCKED`. That is a design
decision, not a gap: scheduling a task is a state change the engine is already
writing, so a table-backed queue commits the enqueue in the same transaction and
needs no outbox to stay consistent. Nothing in the codebase reads from a cache
either — workflow definitions are immutable and cheap to hold in process.

Provisioning them anyway would buy a bill, two more things that can be down, and a
more impressive architecture diagram. The modules are written and flag-gated, so
turning them on the day something actually uses them is one variable. Phase 4 load
testing is what should decide whether PostgreSQL throughput is the binding
constraint — not preference.

## What has and has not been verified

Verified locally:

- `terraform fmt -check` and `terraform validate` pass
- both Kustomize overlays render (20 and 21 objects), with no credential in the
  output, and with probes, resource requests, and hardened security contexts
  present on every workload
- all workflow and manifest YAML parses
- `chronos-server -migrate` applies migrations and exits, reading its DSN from a
  file with no `CHRONOS_DATABASE_URL` in the environment
- the worker's probe endpoints return 200/503 correctly, including during a cold
  start when the control plane is unreachable

**Not verified: an actual deployment.** No AWS account was used, so `terraform
apply` has never run, and nothing here has been exercised against a real EKS
cluster. `terraform validate` checks syntax and provider schemas; it does not catch
a missing IAM permission, an IRSA trust policy that does not match, or an add-on
version conflict. Treat the first `apply` as an exercise expected to surface
problems.
