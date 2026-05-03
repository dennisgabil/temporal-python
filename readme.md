# Garnishi Workflow — Temporal PoC

---

## What is Temporal?

[Temporal](https://temporal.io/) is an open-source, durable workflow orchestration platform. It lets developers write long-running, fault-tolerant business processes as plain code (Python, Go, Java, TypeScript, etc.) without managing queues, retries, or state machines manually.

### Key capabilities

| Feature | Description |
|---|---|
| **Durable Execution** | Workflow state is persisted automatically. If a worker crashes mid-run, execution resumes exactly where it left off. |
| **Automatic Retries** | Activities can be configured to retry on failure with back-off strategies — no custom retry logic needed. |
| **Long-running Workflows** | A workflow can run for seconds, days, or years while staying fully consistent. |
| **Visibility & Observability** | The built-in Temporal Web UI and CLI give real-time visibility into workflow state, history, and errors. |
| **Scalability** | Workers are stateless; horizontal scaling is achieved simply by running more worker processes. |

---

## Problem Statement

### Background

In Australia, when a citizen commits a traffic violation, a civil infraction, or any other offence covered under federal or state revenue law, the relevant government authority issues an official **penalty notice** (fine) to the offender.

### The Challenge

1. A **fine** is raised against a citizen and a payment deadline is set.
2. The system sends **multiple automated reminders** (email, SMS, postal notice) over the notice period.
3. If the citizen **still does not clear the outstanding amount** after all reminders are exhausted, the case is escalated to the **Revenue Department**.
4. The Revenue Department team is then authorised to place a **hold on the customer's bank account** for the penalty amount, preventing the customer from using those funds until the debt is settled.

Coordinating this multi-step, time-sensitive, stateful process across CSV uploads, S3 storage, database lookups, CIF code generation, and core-banking hold requests — while guaranteeing no step is skipped even under infrastructure failures — is exactly the kind of problem Temporal is built to solve.

---

## Why Temporal?

| Requirement | How Temporal Addresses It |
|---|---|
| Multi-step pipeline (upload → enrich → validate → hold) | Each step is an **Activity**; the **Workflow** orchestrates them with guaranteed ordering. |
| Resilience to partial failures | Failed activities are automatically retried; workflow state survives worker restarts. |
| Auditability | Full execution history is stored in the Temporal server — every step, input, output, and retry is logged. |
| Decoupled workers | CSV enrichment, S3 upload, and bank-hold activities run in separate, independently scalable workers. |
| Long-running processes | Reminder sequences spanning days/weeks are trivially expressed as `await asyncio.sleep()` inside a workflow. |

---

## Clone the repo.
Run below commnad to clone the repo code.

`git clone https://github.com/Google-Cloud-CSAU213534-1/Temporal_PoC`

``
## Checkout the branch
`git checkout <branch_name>`

## Create python virtual environment.
Create the vitual environemnt using below command.


`python -m venv venv`

## Activate the virtual environment in the git bash.
Activate the virutal environment by using below commands.

`source venv/Scripts/activate`

## Download and install the packages.
Whenever we are facing issue to download the package via pip, then please follow below options to download the package.

Run below command to downloads and install package.

`pip install -r requirements.txt --trusted-host=pypi.org --trusted-host=files.pythonhosted.org`


## Generate the CIF_ENCRYPTION_KEY
Please run the below command in the terminal.

`python - <<EOF
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())`

## For Sqlite3 based DB operation only.
`python others/db_ops_sqlite.py`

## Run the temporal server
`temporal server start-dev`

## Run the worker
`python <workfer_file.py>`

Example:

`python run_worker.py`

## Run the workflow

Run workflow for extracting_users. Create three different terminal and activate the venv
a. temporal server start-dev
b. python ru

`python run_workflow_event.py extracting_users sample_user_data.csv`

Run workflow for putting hold on amount.

`python run_workflow_event.py hold_account_with_penality_amount hold_amount_file.csv`

## Run workflow using RestAPI
`uvicorn main:app --reload`


Goto Swagger document on brower at localhost:8000/docs and upload file on the endpoint: http://localhost:8000/load-revenue-file for generating cif codes for the given file containing user base.

This will create S3 object where the file with cif codes will get store.

Goto Swagger document on brower at localhost:8000/docs and upload file on the endpoint: http://localhost:8000/put-amount-on-hold for putting amount hold on eligible user accounts for the given file containing user base.

---

## Sample API Responses

### `POST /load-revenue-file`

Upload a CSV file containing user details (`first_name`, `last_name`, `dob`, `address`). The API validates the file, uploads it to S3, and triggers the `RevenueFileWorkflow` to enrich each record with an encrypted CIF code.

**Request** (multipart/form-data)
```
file: users_info.csv
```

**Response — 200 OK**
```json
{
  "message": "File validated and uploaded successfully.",
  "bucket": "garnishi-revenue-bucket",
  "key": "2026-04-20-14/telemetry/users_info.csv"
}
```

**Response — 400 Bad Request** (missing or wrong columns)
```json
{
  "detail": "Missing required columns: ['dob', 'address']"
}
```

**Response — 500 Internal Server Error** (S3 or Temporal unreachable)
```json
{
  "detail": "Failed to upload to S3: An error occurred (NoCredentialsError) when calling the PutObject operation: Unable to locate credentials"
}
```

---

### `POST /put-amount-on-hold`

Upload a CSV file containing enriched user data with CIF codes and `hold_amount`. The API validates the file, uploads it to S3, and triggers the `HoldAccountWithPenaltyWorkflow` to place a hold on each customer's bank account.

**Request** (multipart/form-data)
```
file: hold_amout_with_cif_codes.csv
```

**Response — 200 OK**
```json
{
  "message": "File validated and uploaded successfully.",
  "bucket": "garnishi-revenue-bucket",
  "key": "2026-04-20-14/telemetry-amount-hold/hold_amout_with_cif_codes.csv"
}
```

**Response — 400 Bad Request** (non-CSV file uploaded)
```json
{
  "detail": "Only .csv files are allowed."
}
```

**Response — 500 Internal Server Error** (workflow execution failure)
```json
{
  "detail": "Error while processing: Workflow with ID 'extracting_users-hold_amout_with_cif_codes.csv' is already running"
}
```

---

### Temporal Web UI — Workflow Execution

After triggering either endpoint, open the Temporal Web UI at `http://localhost:8233` to monitor workflow progress.

**Example workflow run (RevenueFileWorkflow):**
```
Workflow ID  : extracting_users-users_info.csv
Run ID       : a3f2c1d0-7e84-4b56-9f12-0d3e8a1c5b97
Status       : Completed
Start Time   : 2026-04-20 14:05:32 IST
Close Time   : 2026-04-20 14:05:47 IST
Task Queue   : revenue-file-queue

Activities Executed:
  1. fetch_file_from_s3          → SUCCESS (duration: 1.2s)
  2. csv_read_activity           → SUCCESS (duration: 0.4s)
  3. file_validation_activity    → SUCCESS (duration: 0.3s)
  4. postgres_lookup_activity    → SUCCESS (50 records matched)
  5. csv_enrich_activity         → SUCCESS (CIF codes generated)
  6. file_upload_activity        → SUCCESS (uploaded to S3)
```

**Example workflow run (HoldAccountWithPenaltyWorkflow):**
```
Workflow ID  : extracting_users-hold_amout_with_cif_codes.csv
Run ID       : b7d4e2f1-9c03-4a71-8e25-1f5a7b2d6c08
Status       : Completed
Start Time   : 2026-04-20 14:10:11 IST
Close Time   : 2026-04-20 14:10:29 IST
Task Queue   : revenue-file-queue

Activities Executed:
  1. fetch_file_from_s3          → SUCCESS (duration: 1.1s)
  2. read_masked_cif_csv         → SUCCESS (50 records read)
  3. file_validation_activity    → SUCCESS
  4. hold_amount_activity        → SUCCESS (50 accounts placed on hold)
  5. write_amount_hold_csv       → SUCCESS (output CSV written)
  6. file_upload_activity        → SUCCESS (result uploaded to S3)
```
