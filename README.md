 ## labelling_task (Data Labelling Backend)
 
 Backend service for a **multi-tenant data labelling system** (tasks, allocation, review, and metrics).
 
 ### Key design points implemented in this scaffold
 - **Multi-tenant**: every task has `tenant_id`; access enforced via JWT `tenantId`.
 - **Auditing**: `created_by`, `updated_by`, `created_at`, `updated_at`, `deleted_at` on core documents.
 - **MongoDB**: single `tasks` collection; indexes created on frequently queried fields.
 - **Redis**:
   - **Cache**: static-ish task metadata (labels/instructions) can be cached per `(tenant_id, external_id)`.
   - **Queues**: allocation/review events use **Redis Streams**.
 - **S3**: placeholder hooks for storing uploaded files (future: ZIP->unzip->upload-per-file).
 - **API envelope**:
   - Success: `{ "status": "success", "message": "...", "data": {...}, "timestamp": <ms> }`
   - Failure: `{ "status": "failure", "message": "..." , "timestamp": <ms> }`
 
 ### Running locally
 1. Create a virtualenv (Python 3.9) and install:
 
 ```bash
 cd labelling_task
 python3.9 -m venv .venv
 source .venv/bin/activate
 pip install -U pip
 pip install -e ".[dev]"
 ```
 
 2. Export env vars (examples):
 
 ```bash
 export LT_MONGO_URI="mongodb://localhost:27017"
 export LT_MONGO_DB="labelling_task"
 export LT_REDIS_URL="redis://localhost:6379/0"
 export LT_JWT_JWKS_URL=""            # optional; if empty uses LT_JWT_SECRET
 export LT_JWT_SECRET="dev-secret"
 export LT_JWT_ALG="HS256"
 ```
 
 3. Start server:
 
 ```bash
 uvicorn labelling_task.main:app --reload --port 8088
 ```
 
 ### Endpoints included
 - `POST /task/create`
 - `POST /task/list`
 - `POST /task/detail`
 - `GET /health`
 
 ### JWT expectations
 JWT must include:
 - `tenantId` (string)
 - `role` (SuperAdmin|Admin|Labeller|Reviewer)
 - `permissions` (array of strings)
 - `sub` (user id)
 
 ### Notes / next steps
 - ZIP upload + S3 ingestion + per-file task creation is scaffolded but not yet implemented.
 - Retention/archival jobs are stubbed (Mongo TTL for `deleted_at` is supported; yearly archival is a job).
