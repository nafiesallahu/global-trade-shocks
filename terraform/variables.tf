variable "project_id" {
  type        = string
  description = "GCP project id"
}

variable "region" {
  type        = string
  default     = "US"
  description = "BigQuery dataset location / bucket region"
}

variable "gcs_bucket_name" {
  type        = string
  description = "Globally unique GCS bucket for the trade data lake"
}

variable "bq_dataset" {
  type        = string
  default     = "global_trade"
  description = "BigQuery dataset id"
}

variable "credentials" {
  type        = string
  description = "Path to service account JSON (used by Terraform provider only)"
}
