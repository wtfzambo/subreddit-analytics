terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
        source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

resource "google_bigquery_dataset" "subreddit_data_raw" {
  dataset_id = var.raw_dataset
  project = var.project
  location = var.region

  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "submissions" {
  table_id = var.submissions_table
  dataset_id = google_bigquery_dataset.subreddit_data_raw.dataset_id
  project = var.project

  deletion_protection = false
}

resource "google_bigquery_table" "comments" {
  table_id = var.comments_table
  dataset_id = google_bigquery_dataset.subreddit_data_raw.dataset_id
  project = var.project

  deletion_protection = false
}

resource "google_bigquery_dataset" "dbt_dataset" {
  dataset_id = var.dbt_dataset
  project = var.project
  location = var.region

  delete_contents_on_destroy = true
}