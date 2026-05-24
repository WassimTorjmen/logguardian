resource "google_storage_bucket" "datalake" {
  name          = "logguardian-datalake-${var.project_id}"
  location      = "EU"
  force_destroy = false

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

resource "google_storage_bucket" "models" {
  name          = "logguardian-models-${var.project_id}"
  location      = "EU"
  force_destroy = false
}