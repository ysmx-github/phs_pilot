# PHS Pilot Project


### Deployment instructions


1. Create and start (or use existing one) standard interactive cluster, no Photon (Optional). Install `pyyaml` and `colorama` libraries from PyPi
2. Create (or use existing one) 2X-Small Serverless warehouse, 1 Min 1 Max, Preview channel (Optional)
3. Workspace -> Home -> Create -> Git folder
4. Git repository URL: https://github.com/ysmx-github/phs_pilot.git -> Create Git Folder
5. Open SQL notebook `/Workspace/Users/firstname.lastname@databricks.com/phs_pilot/src/depl/schema_sql`
  1. Connect to Serverless
  2. Run Cell 1
  3. Fill the widgets with the catalog name and target folder name (`dbr_ssa_clinical` and `dbr_ddl_clinical` are used in this example)
  4. Run all
6. Open the volume: Catalog explorer -> dbr_ssa_clinical -> clinical_raw -> clinical_data_volume
7. Download from the shared folder and unzip `data.zip` and `emr_ddl_clinical.zip`. Manually upload folders `data` and `emr_ddl_clinical` to the volume
8. Open notebook `/Workspace/Users/firstname.lastname@databricks.com/phs_pilot/src/depl/wf1_create`, connect to cluster or Serverless, Run all
9. Open notebook `/Workspace/Users/firstname.lastname@databricks.com/phs_pilot/src/depl/wf2_create`, connect to cluster or Serverless, Run all
10. Open notebook `/Workspace/Users/firstname.lastname@databricks.com/phs_pilot/src/depl/wf3_create`, connect to cluster or Serverless, Run all
11. Open YAML file `/Workspace/Users/firstname.lastname@databricks.com/phs_pilot/src/wf_common/config.yaml`, edit db_catalog and volume parameters as needed
12. Open Workflows
  1. Run `phs_wf1` workflow, review workflow and results
  2. Run `phs_wf2` workflow, review workflow and results
  3. Run `phs_wf2` workflow, review workflow and results
13. Open `/Workspace/Users/firstname.lastname@databricks.com/phs_pilot/src/wf3/wf3_dlt_test.sql`
   1. Select all
   2. Copy
   3. Open SQL Editor -> New query
   4. Paste
   5. Select catalog `dbr_ssa_clinical` and schema `clinical_bronze`
   6. Run CDC tests on the `wf3_dlt` pipeline