# Credential file path
variable "credentials" {
  description = "My Credentials"
  default     = "cred.json"
}

# BigQuery Dataset Name
variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "sp500_analytics"
}

# GCP Region
variable "region" {
  description = "project Region"
  default     = "asia-southeast3"
}

# Project ID
variable "project" {
  description = "Project"
  default     = "de-zoomcamp-project-491217"
}

# GCS Bucket Name
variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-zoomcamp-project-491217-terra-bucket"
}

# GCS Storage Class
variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

# GCS Location
variable "location" {
  description = "Project Location"
  default     = "ASIA-SOUTHEAST3"
}