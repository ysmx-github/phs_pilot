# Databricks notebook source
# DBTITLE 1,imports
import json, requests

# COMMAND ----------

# DBTITLE 1,substitutions
notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
user = notebook_context.userName().get()

# COMMAND ----------

# DBTITLE 1,dlt body
body_dlt = {
    "name": "wf3_dlt",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": True,
    "continuous": False,
    "channel": "CURRENT",
    "photon": False,
    "libraries": [
        {
            "notebook": {
                "path": f"/Users/{user}/phs_pilot/src/wf3/wf3_dlt"
            }
        }
    ],
    "name": "wf3_dlt",
    "edition": "ADVANCED",
    "catalog": "dbr_ssa_clinical",
    "configuration": {
        "catalog": "dbr_ssa_clinical",
        "schema": "clinical_bronze"
    },
    "target": "clinical_silver",
    "data_sampling": False
}

# COMMAND ----------

# DBTITLE 1,dlt
databricks_url = notebook_context.apiUrl().get()
api_token = notebook_context.apiToken().get()
api_headers = {'Authorization': 'Bearer {}'.format(api_token), "Content-Type": "application/json"}


response = requests.post(f"{databricks_url}/api/2.0/pipelines"
                        ,headers=api_headers
                        ,json=body_dlt)

response.json()

# COMMAND ----------

# DBTITLE 1,pipeline id
pipeline_id = response.json()['pipeline_id']

# COMMAND ----------

# DBTITLE 1,job body
body_job = {
  "name": "phs_wf3",
  "email_notifications": {
    "on_failure": [
      f"{user}"
    ],
    "no_alert_for_skipped_runs": True
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "schedule": {
    "quartz_cron_expression": "20 0 10 * * ?",
    "timezone_id": "America/Los_Angeles",
    "pause_status": "UNPAUSED"
  },
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "dlt_task",
      "run_if": "ALL_SUCCESS",
      "pipeline_task": {
        "pipeline_id": f"{pipeline_id}"
      },
      "timeout_seconds": 0,
      "email_notifications": {}
    }
  ],
  "run_as": {
    "user_name": f"{user}"
  }
}

# COMMAND ----------

# DBTITLE 1,job
response = requests.post(f"{databricks_url}/api/2.1/jobs/create"
                        ,headers=api_headers
                        ,json=body_job)

response.json()
