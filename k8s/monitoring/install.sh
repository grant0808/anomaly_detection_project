#!/usr/bin/env bash

# ==============================================================================
# Prometheus & Grafana Monitoring Stack Installation Script
# ==============================================================================

set -euo pipefail

echo "=== 1. Adding Prometheus Community Helm Repository ==="
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

echo "=== 2. Creating monitoring namespace ==="
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

echo "=== 3. Installing Kube-Prometheus-Stack ==="
# Installs Prometheus Operator, Grafana, Alertmanager, Node Exporters
helm upgrade --install prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --wait \
  --timeout 10m \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false

echo "=== 4. Applying ServiceMonitor for Preprocessing Service ==="
# Note: In a pure GitOps workflow, this file will be synced automatically by ArgoCD.
# We apply it manually here as part of setup validation.
kubectl apply -f k8s/monitoring/servicemonitor.yaml

echo "======================================================================"
echo "Kube-Prometheus-Stack installed successfully!"
echo "======================================================================"
echo "To access Grafana Dashboard:"
echo "  kubectl port-forward svc/prometheus-stack-grafana -n monitoring 3000:80"
echo "  Open http://localhost:3000 in your browser"
echo "  Default Login: admin / prom-operator"
echo "======================================================================"
