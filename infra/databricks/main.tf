# Databricks side of the GMPDI lakehouse, as code:
#   schema + landing volume  (where the NDJSON replay files are uploaded)
#   a Git folder pinned to a branch (the code the job runs, separate from your dev folder)
#   a job: bronze (Auto Loader) -> silver + gold (contracts, quarantine, alerts)
#
# Notebook tasks with no cluster run on serverless compute, the only compute on Free Edition.

data "databricks_current_user" "me" {}

locals {
  deploy_path = "${data.databricks_current_user.me.home}/gmpdi-deploy"
  notebooks   = "${local.deploy_path}/lakehouse/databricks"
  params = {
    catalog = var.catalog
    schema  = var.schema
    volume  = databricks_volume.landing.name
  }
}

resource "databricks_schema" "gmpdi" {
  catalog_name = var.catalog
  name         = var.schema
  comment      = "GMP Deviation Investigator: IndPenSim process data and openFDA recalls. Managed by Terraform (infra/databricks)."
}

resource "databricks_volume" "landing" {
  catalog_name = var.catalog
  schema_name  = databricks_schema.gmpdi.name
  name         = "landing"
  volume_type  = "MANAGED"
  comment      = "NDJSON from the replayer (--sink ndjson) and the openFDA producer; Auto Loader reads from here."
}

resource "databricks_repo" "deploy" {
  url    = var.git_url
  branch = var.git_branch
  path   = local.deploy_path
}

resource "databricks_job" "pipeline" {
  name        = "gmpdi-lakehouse"
  description = "Bronze (Auto Loader) -> silver (data contracts, quarantine, alerts) -> gold. Same transforms as the local Spark jobs."

  max_concurrent_runs = 1 # streaming checkpoints and MERGE: one writer at a time

  task {
    task_key = "bronze"
    notebook_task {
      notebook_path   = "${local.notebooks}/bronze_autoloader"
      base_parameters = local.params
    }
  }

  task {
    task_key = "silver_gold"
    depends_on {
      task_key = "bronze"
    }
    notebook_task {
      notebook_path   = "${local.notebooks}/silver_gold"
      base_parameters = local.params
    }
  }

  schedule {
    quartz_cron_expression = "0 7 * * * ?" # hourly, at minute 7
    timezone_id            = "UTC"
    pause_status           = var.job_schedule_paused ? "PAUSED" : "UNPAUSED"
  }

  tags = {
    project = "gmpdi"
    phase   = "2"
  }

  depends_on = [databricks_repo.deploy]
}
