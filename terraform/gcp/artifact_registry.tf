resource "google_artifact_registry_repository" "logguardian" {
  location      = var.region
  repository_id = "logguardian"
  format        = "DOCKER"
}