terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  #  credentials = 
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}



resource "google_storage_bucket" "data-lake-bucket" {
  name     = var.gcs_bucket_name
  location = var.location

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 1 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location

  delete_contents_on_destroy = true
}