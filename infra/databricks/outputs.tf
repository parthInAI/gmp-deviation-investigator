output "job_id" {
  value = databricks_job.pipeline.id
}

output "job_url" {
  value = databricks_job.pipeline.url
}

output "landing_volume_path" {
  value = "/Volumes/${var.catalog}/${databricks_schema.gmpdi.name}/${databricks_volume.landing.name}"
}

output "deployed_code_path" {
  value = databricks_repo.deploy.path
}
