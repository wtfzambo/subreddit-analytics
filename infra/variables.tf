variable "project" {
  description = "Your GCP project ID"
}

variable "region" {
  description = "Region for GCP resources."
  default = "europe-west6"
  type = string
}

variable "raw_dataset" {
  description = "BigQuery dataset that raw data will be written to."
  type = string
  default = "subreddit_data_raw"
}

variable "submissions_table" {
  description = "Table name for the submissions' data."
  type = string
  default = "submissions"
}

variable "comments_table" {
  description = "Table name for the comments' data."
  type = string
  default = "comments"
}

variable "dbt_dataset" {
  description = "BigQuery dataset for dbt models' outputs."
  type = string
  default = "subreddit_analytics_dbt"
}