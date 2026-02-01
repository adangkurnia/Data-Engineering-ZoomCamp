terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = "project-1a58d016-57b1-4ae2-b06"
  region  = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "project-1a58d016-57b1-4ae2-b06-terra-bucket"
  location      = "US"
  force_destroy = true

  uniform_bucket_level_access = true
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}