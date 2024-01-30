
variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "project" {
  description = "Project_ID"
  default     = "praxis-wall-411617"
}

variable "region" {
  description = "Region"
  default     = "europe-west2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "praxis-wall-411617"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}