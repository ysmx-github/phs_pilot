# Databricks notebook source
# DBTITLE 1,imports
import json, requests

# COMMAND ----------

# DBTITLE 1,substitutions
notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
user = notebook_context.userName().get()


# COMMAND ----------

# DBTITLE 1,body

body = {
  "name": "phs_wf2",
  "email_notifications": {
    "no_alert_for_skipped_runs": False
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "cleanup_cond",
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.run_cleanup}}",
        "right": "true"
      },
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
      "task_key": "cleanup",
      "depends_on": [
        {
          "task_key": "cleanup_cond",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf2/wf2_ddl_cleanup",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf2_cluster",
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
      "task_key": "table_cond",
      "depends_on": [
        {
          "task_key": "cleanup"
        },
        {
          "task_key": "cleanup_cond",
          "outcome": "false"
        }
      ],
      "run_if": "AT_LEAST_ONE_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.run_table}}",
        "right": "true"
      },
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
      "task_key": "table",
      "depends_on": [
        {
          "task_key": "table_cond",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf2/wf2_ddl_table",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf2_cluster",
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
      "task_key": "alter_cond",
      "depends_on": [
        {
          "task_key": "table"
        }
      ],
      "run_if": "NONE_FAILED",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.run_alter}}",
        "right": "true"
      },
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
      "task_key": "alter",
      "depends_on": [
        {
          "task_key": "alter_cond",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf2/wf2_ddl_alter",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf2_cluster",
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
      "task_key": "insert_cond",
      "depends_on": [
        {
          "task_key": "table"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.run_insert}}",
        "right": "true"
      },
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
      "task_key": "insert",
      "depends_on": [
        {
          "task_key": "insert_cond",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf2/wf2_ddl_insert",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf2_cluster",
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
      "task_key": "liquid_clustering_cond",
      "depends_on": [
        {
          "task_key": "cleanup_cond",
          "outcome": "false"
        },
        {
          "task_key": "table"
        }
      ],
      "run_if": "AT_LEAST_ONE_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.run_liquid_clistering}}",
        "right": "true"
      },
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
      "task_key": "liquid_clustering",
      "depends_on": [
        {
          "task_key": "liquid_clustering_cond",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf2/wf2_ddl_cluster",
        "base_parameters": {
          "clustering": "False"
        },
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf2_cluster",
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
      "task_key": "load_patient_cond",
      "depends_on": [
        {
          "task_key": "table"
        },
        {
          "task_key": "cleanup_cond",
          "outcome": "false"
        }
      ],
      "run_if": "AT_LEAST_ONE_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.run_patient_load}}",
        "right": "true"
      },
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
      "task_key": "load_patient",
      "depends_on": [
        {
          "task_key": "load_patient_cond",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf_common/load_patient",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf2_cluster",
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
      "task_key": "view_cond",
      "depends_on": [
        {
          "task_key": "table"
        },
        {
          "task_key": "cleanup_cond",
          "outcome": "false"
        }
      ],
      "run_if": "AT_LEAST_ONE_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.run_view}}",
        "right": "true"
      },
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
      "task_key": "view",
      "depends_on": [
        {
          "task_key": "view_cond",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"/Workspace/Users/{user}/phs_pilot/src/wf2/wf2_ddl_view",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "wf2_cluster",
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
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "wf2_cluster",
      "new_cluster": {
        "cluster_name": "",
        "spark_version": "15.4.x-scala2.12",
        "azure_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK_AZURE",
            "spot_bid_max_price": -1
          },       
        "node_type_id": "Standard_D4ds_v5",
        "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": False,
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD",
        "num_workers": 3
      }
    }
  ],
  "queue": {
    "enabled": True
  },
  "parameters": [
    {
      "name": "run_alter",
      "default": "true"
    },
    {
      "name": "run_cleanup",
      "default": "true"
    },
    {
      "name": "run_insert",
      "default": "true"
    },
    {
      "name": "run_liquid_clistering",
      "default": "true"
    },
    {
      "name": "run_patient_load",
      "default": "true"
    },
    {
      "name": "run_table",
      "default": "true"
    },
    {
      "name": "run_view",
      "default": "true"
    },
    {
      "name": "yaml_file",
      "default": "./../wf_common/config.yaml"
    }
  ],
  "run_as": {
    "user_name": "yuriy.margulis@databricks.com"
  }
}

# COMMAND ----------

# DBTITLE 1,api call

databricks_url = notebook_context.apiUrl().get()
api_token = notebook_context.apiToken().get()
api_headers = {'Authorization': 'Bearer {}'.format(api_token), "Content-Type": "application/json"}

response = requests.post(f"{databricks_url}/api/2.1/jobs/create"
                        ,headers=api_headers
                        ,json=body)

response.json()
