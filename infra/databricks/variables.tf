variable "catalog" {
  description = "Unity Catalog catalog. Free Edition gives you `workspace`."
  type        = string
  default     = "workspace"
}

variable "schema" {
  description = "Schema for all GMPDI tables (bronze_*, silver_*, quarantine_*, gold_*)."
  type        = string
  default     = "gmpdi"
}

variable "git_url" {
  description = "Public Git repo the job runs from."
  type        = string
  default     = "https://github.com/parthInAI/gmp-deviation-investigator.git"
}

variable "git_branch" {
  description = "Branch the deployed Git folder tracks."
  type        = string
  default     = "main"
}

variable "job_schedule_paused" {
  description = "Create the hourly schedule paused (Free Edition has a daily compute quota)."
  type        = bool
  default     = true
}
