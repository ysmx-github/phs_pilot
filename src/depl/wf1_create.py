# Databricks notebook source
import json, requests

notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

user = notebook_context.userName().get()

body = {
  "name": "phs_wf1",
  "email_notifications": {
    "no_alert_for_skipped_runs": False
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "parse_ddl",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf1/wf1_ddl_parser",
        "base_parameters": {
          "cleanup": "True",
          "clustering": "True"
        },
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf1_cluster",
      "libraries": [
        {
          "pypi": {
            "package": "pyyaml"
          }
        },
        {
          "pypi": {
            "package": "colorama"
          }
        }
      ],
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": False,
        "no_alert_for_canceled_runs": False,
        "alert_on_last_attempt": False
      },
      "webhook_notifications": {}
    },
    {
      "task_key": "load_patient_data",
      "depends_on": [
        {
          "task_key": "parse_ddl"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf_common/load_patient",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf1_cluster",
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": False,
        "no_alert_for_canceled_runs": False,
        "alert_on_last_attempt": False
      },
      "webhook_notifications": {}
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "wf1_cluster",
      "new_cluster": {
        "cluster_name": "",
        "spark_version": "15.4.x-scala2.12",
        "azure_attributes": {
          "first_on_demand": 1,
          "availability": "ON_DEMAND_AZURE",
          "spot_bid_max_price": -1
        },
        "node_type_id": "Standard_D4ds_v5",
        "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": True,
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD",
        "num_workers": 4
      }
    }
  ],
  "queue": {
    "enabled": True
  },
  "parameters": [
    {
      "name": "yaml_file",
      "default": "./../wf_common/config.yaml"
    }
  ],
  "run_as": {
    "user_name": "yuriy.margulis@databricks.com"
  }
}

databricks_url = notebook_context.apiUrl().get()
api_token = notebook_context.apiToken().get()
api_headers = {'Authorization': 'Bearer {}'.format(api_token), "Content-Type": "application/json"}

response = requests.post(f"{databricks_url}/api/2.1/jobs/create"
                        ,headers=api_headers
                        ,json=body)

response.json()
