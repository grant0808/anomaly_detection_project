#!/usr/bin/env bash

# ==============================================================================
# GitOps & Kafka Operator Installation Script
# ==============================================================================

set -euo pipefail

echo "=== 1. Creating namespaces ==="
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace deeplog --dry-run=client -o yaml | kubectl apply -f -

echo "=== 2. Installing ArgoCD ==="
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

echo "=== 3. Exposing ArgoCD Server via LoadBalancer (Optional) ==="
# In production, use Ingress. For dev, LoadBalancer or port-forwarding works:
# kubectl patch svc argocd-server -n argocd -p '{"spec": {"type": "LoadBalancer"}}'

echo "=== 4. Installing Strimzi Kafka Operator ==="
# Strimzi installation manifest
kubectl create -f 'https://strimzi.io/install/latest?namespace=deeplog' -n deeplog

echo "=== 5. Deploying Declarative ArgoCD Application ==="
# Apply the GitOps application tracking this repository
kubectl apply -f k8s/argocd/argocd-app.yaml

echo "======================================================================"
echo "ArgoCD and Strimzi Operators installed successfully!"
echo "======================================================================"
echo "To get ArgoCD admin password:"
echo "  kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d; echo"
echo ""
echo "To port-forward ArgoCD API Server:"
echo "  kubectl port-forward svc/argocd-server -n argocd 8080:443"
echo "  Open https://localhost:8080 in your browser (admin / <password>)"
echo "======================================================================"
