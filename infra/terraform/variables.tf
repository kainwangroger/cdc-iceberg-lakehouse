variable "location" {
  description = "Azure region"
  type        = string
  default     = "France Central"
}

variable "postgres_admin_user" {
  description = "PostgreSQL admin username"
  type        = string
  default     = "pgadmin"
}

variable "postgres_admin_password" {
  description = "PostgreSQL admin password"
  type        = string
  sensitive   = true
}
