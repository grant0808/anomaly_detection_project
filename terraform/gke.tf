# ==============================================================================
# GKE Cluster & Networking Provisioning
# ==============================================================================

# 1. VPC Network
resource "google_compute_network" "gke_vpc" {
  name                    = "deeplog-gke-vpc"
  auto_create_subnetworks = false
}

# 2. Subnet for GKE Nodes
resource "google_compute_subnetwork" "gke_subnet" {
  name          = "deeplog-gke-subnet"
  ip_cidr_range = "10.0.0.0/16"
  region        = var.gcp_region
  network       = google_compute_network.gke_vpc.id

  # Enable secondary IP ranges for pods and services (Alias IPs)
  secondary_ip_range {
    range_name    = "gke-pods-range"
    ip_cidr_range = "10.1.0.0/16"
  }

  secondary_ip_range {
    range_name    = "gke-services-range"
    ip_cidr_range = "10.2.0.0/20"
  }
}

# 3. GKE Cluster Definition (Control Plane)
resource "google_container_cluster" "primary" {
  name     = "deeplog-k8s-cluster"
  location = var.gcp_region
  network    = google_compute_network.gke_vpc.name
  subnetwork = google_compute_subnetwork.gke_subnet.name

  # We use a separated node pool resource, so we delete the default node pool upon creation
  remove_default_node_pool = true
  initial_node_count       = 1

  ip_allocation_policy {
    cluster_secondary_range_name  = "gke-pods-range"
    services_secondary_range_name = "gke-services-range"
  }

  # Enable Workload Identity for secure GCP access from pods
  workload_identity_config {
    workload_pool = "${var.gcp_project_id}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }
}

# 4. GKE Custom Node Pool (Auto-scaling & GPU-ready config)
resource "google_container_node_pool" "primary_nodes" {
  name       = "deeplog-node-pool"
  location   = var.gcp_region
  cluster    = google_container_cluster.primary.name
  node_count = 2

  autoscaling {
    min_node_count = 2
    max_node_count = 5
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    preemptible  = false
    machine_type = "e2-standard-4" # 4 vCPUs, 16GB RAM (ideal for Kafka + Servings + Observability)

    labels = {
      role = "general"
    }

    # Enable Workload Identity metadata access
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}

# Output cluster endpoint for kubeconfig setup
output "gke_cluster_name" {
  value       = google_container_cluster.primary.name
  description = "GKE Cluster Name"
}

output "gke_endpoint" {
  value       = google_container_cluster.primary.endpoint
  description = "GKE Cluster Control Plane Endpoint"
}
