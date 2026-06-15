output "cluster_name" {
  value = google_container_cluster.logguardian.name
}

output "cluster_endpoint" {
  value = google_container_cluster.logguardian.endpoint
}

output "datalake_bucket" {
  value = google_storage_bucket.datalake.name
}

output "registry_url" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/logguardian"
}