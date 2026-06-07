#!/usr/bin/env bash

set -euo pipefail

KSERVE_VERSION="${KSERVE_VERSION:-v0.17.0}"

echo "=== 1. Installing KServe serverless dependencies: Knative, Istio, cert-manager ==="
curl -fsSL "https://github.com/kserve/kserve/releases/download/${KSERVE_VERSION}/kserve-knative-mode-dependency-install.sh" | bash

echo "=== 2. Installing KServe CRDs and controller ==="
kubectl apply --server-side -f "https://github.com/kserve/kserve/releases/download/${KSERVE_VERSION}/kserve.yaml"

echo "=== 3. Installing KServe cluster serving runtimes ==="
kubectl apply --server-side -f "https://github.com/kserve/kserve/releases/download/${KSERVE_VERSION}/kserve-cluster-resources.yaml"

echo "=== 4. Waiting for core control planes ==="
kubectl wait --for=condition=Available deployment -n knative-serving --all --timeout=10m
kubectl wait --for=condition=Available deployment -n istio-system --all --timeout=10m
kubectl wait --for=condition=Available deployment -n kserve --all --timeout=10m

echo "KServe serverless stack is installed."
echo "Verify with:"
echo "  kubectl get pods -n knative-serving"
echo "  kubectl get pods -n istio-system"
echo "  kubectl get pods -n kserve"
