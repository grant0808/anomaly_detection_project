provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

variable "gcp_project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "gcp_region" {
  type        = string
  default     = "asia-northeast3" # Seoul region
  description = "GCP Region"
}

variable "bucket_name" {
  type        = string
  default     = "deeplog-mlflow-model-registry"
  description = "Name of the GCS bucket for MLflow models"
}

resource "google_storage_bucket" "mlflow_registry" {
  name          = var.bucket_name
  location      = var.gcp_region
  storage_class = "STANDARD"

  # Force destroy allows deleting bucket with artifacts (recommended for dev/test)
  force_destroy = true

  # Enforce Uniform Bucket-Level Access for security
  uniform_bucket_level_access = true

  # Encryption at rest (using Google-managed encryption keys by default)
  encryption {
    default_kms_key_name = null
  }

  # Lifecycle policies to clean up old experiments/runs (optional, example configuration)
  lifecycle_rule {
    condition {
      age = 30 # Auto-delete artifacts older than 90 days if needed (turned off by default here)
      matches_storage_class = ["STANDARD"]
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "dev"
    project     = "deeplog-anomaly-detection"
  }
}

output "gcs_bucket_url" {
  value       = google_storage_bucket.mlflow_registry.url
  description = "GCS bucket URL to configure MLflow tracking URI"
}
