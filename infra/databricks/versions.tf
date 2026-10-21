terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.138"
    }
  }
}

# Auth comes from the environment, never from this repo:
#   DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
#   DATABRICKS_TOKEN=<personal access token>     (or run `databricks auth login` and set DATABRICKS_CONFIG_PROFILE)
provider "databricks" {}
