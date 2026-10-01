# NEW YORK TAXI MEDALLION ARCHITECTURE

A Databricks pipeline implementing the **Medallion architecture (Bronze – Silver – Gold)** for public **NYC Taxi & Limousine Commission** data.

---

## Table of contents

1. [Project overview](#project-overview)
2. [Project structure](#project-structure)
3. [Requirements and installation](#requirements-and-installation)
4. [Environment setup (warehouse)](#environment-setup-warehouse)
5. [Job installation](#job-installation)
6. [Input parameters](#input-parameters)
7. [Running the pipeline from the Databricks UI](#running-the-pipeline-from-the-databricks-ui)
8. [Email notifications](#email-notifications)

---

## Project overview

The project downloads monthly Parquet files from the official NYC TLC source:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/{yellow|green}_tripdata_YYYY-MM.parquet
```

It then processes the data through three layers:

- **Bronze** (`load_bronze.ipynb`) — downloads raw `yellow` and `green` taxi files and stores them in the Unity Catalog volume: `/Volumes/bronze/dbo/ny_taxi/`.
- **Silver** (`load_silver.ipynb`) — cleans, standardizes and unifies the data schema (e.g. column names, payment types, rate codes, missing values) and writes the result to the `silver.dbo.ny_taxi` table.
- **Gold** (`load_gold.ipynb`) — builds analytical aggregations in the `gold` layer:
  - `gold.dbo.ny_taxi_daily` — daily aggregations by taxi type,
  - `gold.dbo.ny_taxi_zone_daily` — daily aggregations by pickup/dropoff zone.

The whole flow is orchestrated as a single Databricks Job with three tasks connected by dependencies:

```
load_bronze → load_silver → load_gold
```

---

## Project structure

```
NY_Taxi_Databricks_medallion_architecture/
├── load_bronze.ipynb              # Task 1: ingest raw data into Bronze
├── load_silver.ipynb              # Task 2: clean and standardize into Silver
├── load_gold.ipynb                # Task 3: build analytical aggregations in Gold
├── NEY_YORK_TAXI_Job.yml          # Databricks Job definition (three-task DAG)
├── warehouse_script.sql           # Script creating catalogs, schemas and volume
└── README.md                      # This file
```

---

## Requirements and installation

To run this project you need:

- A Databricks Workspace with **Unity Catalog** enabled,
- Permissions to create catalogs, schemas, volumes and tables in Unity Catalog,
- Access to a cluster or warehouse (Job compute) capable of running Python notebooks,
- Internet access from the cluster (to download files from the NYC TLC cloud front),
- Git integration configured in Databricks (Repos / Git folders) for this repository.

### Step 1: Clone the repository into Databricks

In the Databricks UI:

1. Go to **Catalog → Git folders** or **Workspace → Repos**.
2. Click **Add repo** / **Create folder from Git**.
3. Provide the repository URL:

   ```
   https://github.com/KacprusJeden/NY_Taxi_Databricks_medallion_architecture.git
   ```

4. Select the `main` branch.
5. Save the repo — the notebooks and job file will become available in the workspace.

---

## Environment setup (warehouse)

Before running the job for the first time, execute `warehouse_script.sql` in the SQL editor or a SQL notebook to create the Unity Catalog objects:

```sql
-- warehouse_script.sql

CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS gold;

CREATE SCHEMA IF NOT EXISTS bronze.dbo;
CREATE SCHEMA IF NOT EXISTS silver.dbo;
CREATE SCHEMA IF NOT EXISTS gold.dbo;

CREATE VOLUME IF NOT EXISTS bronze.dbo.ny_taxi;
```

The script creates:

- catalogs: `bronze`, `silver`, `gold`,
- schemas: `dbo` in each catalog,
- volume: `bronze.dbo.ny_taxi`, where raw Parquet files are stored.

---

## Job installation

The job is defined in `NEY_YORK_TAXI_Job.yml` as a **Databricks Asset Bundle (DAB)**. To install/create it:

### Option A: The simplest way in UI

If you are not using DAB/CLI, you can create the job manually:

1. Go to **Workflows → Jobs** and click **Create job**.
2. Edit YAML Configuration - copy and paste content from NEY_YORK_TAXI_Job.yml
3. Save the job.

### Option B: Databricks CLI (recommended)

1. Install and authenticate with [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html).
2. From the project directory run:

   ```bash
   databricks bundle deploy
   ```

   or, to validate the definition only:

   ```bash
   databricks bundle validate
   ```

3. After deployment, the `NEW_YORK_TAXI_Job` will appear in the Databricks UI under **Workflows → Jobs**.

### Option C: Manual creation in the UI

If you are not using DAB/CLI, you can create the job manually:

1. Go to **Workflows → Jobs** and click **Create job**.
2. Add three **Notebook** tasks:

   | Task key       | Notebook path   | Depends on   | Source |
   |----------------|-----------------|--------------|--------|
   | `load_bronze`  | `/load_bronze`  | —            | Git    |
   | `load_silver`  | `/load_silver`  | `load_bronze`| Git    |
   | `load_gold`    | `/load_gold`    | `load_silver`| Git    |

3. In the **Parameters** section at job level, add the parameters described below.
4. Save the job.


---

## Input parameters

The job accepts the following parameters (defined in `NEY_YORK_TAXI_Job.yml`):

| Parameter         | Type    | Required | Default value             | Description |
|-------------------|---------|----------|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `date_of_data`    | string  | yes      | `""`                      | Date in `YYYY-MM-DD` format. Based on this date, the pipeline downloads and processes data for the whole month (e.g. `2026-06-30` → data for June 2026). |
| `run_id`          | string  | no       | `"{{job.run_id}}"`        | Identifier of the current job run. By default it is filled automatically by Databricks. It is stored in the Silver table as `task_id`.                   |
| `reset_checkpoint`| boolean | yes      | `False`                   | If `True`, deletes the checkpoint directory before starting the stream (useful for a full reload of a month). |

This parameter is currently not passed from the job level — when triggered from the UI it uses the default value `False`.

---

## Running the pipeline from the Databricks UI

After the job (`NEW_YORK_TAXI_Job`) is installed, follow the steps below to run the pipeline from the Databricks UI.

### Step 1: Open the job

1. In the Databricks UI, go to **Workflows → Jobs**.
2. Find and click the job named **NEW_YORK_TAXI_Job**.

### Step 2: Fill in the input parameters

1. In the upper-right corner of the job screen, click the arrow next to the **Run now** button.
2. Select **Run now with parameters**.
3. In the parameters dialog, fill in the field:

   - **Key**: `date_of_data`
   - **Value**: a date in `YYYY-MM-DD` format, e.g. `2026-06-30`

   Leave the `run_id` parameter with its default value `{{job.run_id}}` — Databricks will automatically insert the current run identifier.

4. Click **Run now**.

### Step 3: Monitor the pipeline run

1. After clicking **Run now**, you will be redirected to the run view.
2. You will see a graph of three tasks:

   ```
   load_bronze → load_silver → load_gold
   ```

3. Each task changes color:
   - grey — pending,
   - blue — running,
   - green — success,
   - red — failure.
4. By clicking on a task, you can inspect the logs and outputs of each notebook.

### Step 4: Verify the results

After all tasks finish, you can inspect the created objects:

- raw files in the volume:

  ```
  /Volumes/bronze/dbo/ny_taxi/{yellow|green}/{YYYY}/{MM}/
  ```

- Silver table:

  ```sql
  SELECT * FROM silver.dbo.ny_taxi WHERE date_of_data = '2026-06-30';
  ```

- Gold tables:

  ```sql
  SELECT * FROM gold.dbo.ny_taxi_daily WHERE date_of_data = '2026-06-30';
  SELECT * FROM gold.dbo.ny_taxi_zone_daily WHERE date_of_data = '2026-06-30';
  ```

---

## Email notifications

You can configure email notifications about the job run status either in the job definition or directly in the Databricks UI.

### Setting notifications in the UI

1. Open the **NEW_YORK_TAXI_Job** job in **Workflows → Jobs**.
2. Click **Edit** (if the job is read-only, click **Clone** first).
3. In the left panel, find the **Job details → Notifications** section.
4. Click **Add notification**.
5. Fill in the fields:

   - **Email addresses** — recipient addresses separated by commas,
   - **Event** — select one or more events:
     - `On success` — notification when the job completes successfully,
     - `On failure` — notification when the job fails,
     - `On start` — notification when the job starts.

6. Click **Save** or **Update** to persist the changes.

From now on, the selected recipients will receive emails about the status of each pipeline run.

---

## Summary

The project enables automatic downloading, cleaning and aggregation of NYC Taxi data using the Medallion architecture. After the environment and job are configured once, simply run the job from the Databricks UI and provide the `date_of_data` parameter to process data for the selected month.
