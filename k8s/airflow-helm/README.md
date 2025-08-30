# How to generate Airflow configs in Kubernetes

# Prerequisites
Make sure you have installed (and know how they work):
- kubectl
- kustomize
- [yq](https://github.com/mikefarah/yq) (or [go-yq](https://archlinux.org/packages/extra/x86_64/go-yq/) in Arch)
- helm
- Airflow Helm chart version 1.16.0 (since we're currently using Airflow 2.10.5)

Access to a Kubernetes cluster (Kind/minikube for local dev, and AWS EKS/GCP GKE for dev/prod env) is also required.

## 1. Generate the rendered Helm-generated configs
By this step you should have fulfilled the prerequisites. If yes, then run this in the terminal:
```
kustomize build overlays/dev --enable-helm > values-rendered.yaml
```

*NOTE: you can remove the folder `chart` in `overlays/dev` once the `rendered.yaml` has been generated.*

## 2. Separate the configs
Run these commands to separate the configs into a dedicated YAML file using `yq`.

Customize the scripts and filters based on your needs.

```
# Service Accounts
yq eval 'select(.kind == "ServiceAccount")' values-rendered.yaml > helm/service-accounts-rendered.yaml

# Roles
yq eval 'select(.kind == "Role")' values-rendered.yaml > helm/roles-rendered.yaml

# RoleBindings
yq eval 'select(.kind == "RoleBinding")' values-rendered.yaml > helm/rolebindings-rendered.yaml

# Secrets
yq eval 'select(.kind == "Secret")' values-rendered.yaml > helm/secrets-rendered.yaml

# PostgreSQL resources (excluding secrets)
yq eval 'select(.metadata.name | contains("postgresql") and .kind != "Secret")' values-rendered.yaml > helm/postgres-rendered.yaml

# StatsD resources
yq eval 'select(.metadata.name | contains("statsd"))' values-rendered.yaml > helm/statsd-rendered.yaml

# CronJob for cleanup
yq eval 'select(.kind == "CronJob")' values-rendered.yaml > helm/cleanup-cronjob-rendered.yaml

# Core Airflow resources (everything else)
yq eval 'select(.kind != "ServiceAccount" and .kind != "Role" and .kind != "RoleBinding" and .kind != "Secret" and .kind != "CronJob" and (.metadata.name | contains("postgresql") | not) and (.metadata.name | contains("statsd") | not))' values-rendered.yaml > helm/airflow-core-rendered.yaml
```

*NOTE: move the `secrets-rendered.yaml` to dev or prod overlay*

## 3. Create kustomization.yaml files

In `base` folder:
```
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - helm/airflow-core-rendered.yaml
  - helm/cleanup-cronjob-rendered.yaml
  - helm/postgres-rendered.yaml
  - helm/rolebindings-rendered.yaml
  - helm/roles-rendered.yaml
  - helm/service-accounts-rendered.yaml
  - helm/statsd-rendered.yaml
```

In `overlays/dev` folder:
```
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: airflow-dev

resources:
  - ../../base
  - secrets-rendered.yaml

# You can add patches down here
```

In the end you're folder structure in the end should look like this:
```
├── base
│   ├── helm
│   │   ├── airflow-core-rendered.yaml
│   │   ├── cleanup-cronjob-rendered.yaml
│   │   ├── postgres-rendered.yaml
│   │   ├── rolebindings-rendered.yaml
│   │   ├── roles-rendered.yaml
│   │   ├── service-accounts-rendered.yaml
│   │   └── statsd-rendered.yaml
│   ├── kustomization.yaml
│   └── values-rendered.yaml
├── overlays
│   └── dev
│       ├── env.yaml
│       ├── kustomization.yaml
│       └── secrets-rendered.yaml
└─── README.md

```