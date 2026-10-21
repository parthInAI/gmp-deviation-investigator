# Databricks objects as code

Creates the Databricks side of the lakehouse in your workspace (Free Edition works):

| Resource | What it is |
|---|---|
| `databricks_schema.gmpdi` | `workspace.gmpdi`, holds every table |
| `databricks_volume.landing` | `/Volumes/workspace/gmpdi/landing`, where the NDJSON files are uploaded |
| `databricks_repo.deploy` | a Git folder `~/gmpdi-deploy` pinned to `main`: the code the job runs, kept apart from your dev folder |
| `databricks_job.pipeline` | `gmpdi-lakehouse`: `bronze_autoloader` → `silver_gold`, on serverless; hourly schedule, created **paused** |

## Run it

Needs Terraform 1.6+ and a personal access token (Databricks: your user icon → **Settings → Developer → Access tokens**).

```powershell
cd infra/databricks
$env:DATABRICKS_HOST  = "https://<your-workspace>.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "<token>"        # never commit this; it stays in your shell
terraform init
```

Phase 1 already created the schema and the volume from the notebook. Adopt them instead of creating duplicates:

```powershell
terraform import databricks_schema.gmpdi workspace.gmpdi
terraform import databricks_volume.landing workspace.gmpdi.landing
terraform plan      # read it: expect the Git folder and the job to be created, nothing destroyed
terraform apply
```

Then run the job once from the URL in the `job_url` output (or **Jobs & Pipelines → gmpdi-lakehouse → Run now**).

## Notes

- `terraform validate` passes against provider 1.138.0. `.terraform.lock.hcl` pins the provider with hashes for Windows, Linux and macOS.
- State is local (`terraform.tfstate`, git-ignored). For a team you would use a remote backend; for one person on Free Edition, local is the honest choice.
- `terraform destroy` removes the job and the Git folder. The schema is only removed if it is empty, so your tables are safe from an accidental destroy.
