terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "trade_lake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

resource "google_bigquery_dataset" "trade" {
  dataset_id                 = var.bq_dataset
  location                   = var.region
  delete_contents_on_destroy = true
}

output "gcs_bucket" {
  value = google_storage_bucket.trade_lake.name
}

output "bq_dataset" {
  value = google_bigquery_dataset.trade.dataset_id
}
