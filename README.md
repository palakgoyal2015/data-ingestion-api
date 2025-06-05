# 
# 🚀 Data Ingestion API System (Hosted on Vercel)
# 
# A RESTful API system built with FastAPI for simulating asynchronous data ingestion with priority-based batch processing and rate limiting.
# 
# =======================
# 📌 Features
# =======================
# - Accepts a list of IDs with priority levels: HIGH, MEDIUM, LOW.
# - Processes IDs in batches of 3.
# - Batches are processed every 5 seconds (rate limit).
# - Prioritized by priority and submission time.
# - Status of ingestion and each batch can be checked via an API.
# 
# =======================
# 🛠 Tech Stack
# =======================
# - Python 3.10+
# - FastAPI
# - SQLite
# - asyncio
# - httpx + pytest (for testing)
# 
# =======================
# ▶️ Endpoints
# =======================
# 
# 1. POST /ingest
# -----------------
# Submits a data ingestion request.
# 
# 🧾 Sample Curl Request:
# curl -X 'POST' \
#   'http://127.0.0.1:8000/docs' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "ids": [1, 2, 3, 4, 5],
#   "priority": "MEDIUM"
# }'
# 
# ✅ Sample Response:
# {
#   "ingestion_id": "72112dfe-1ac8-42d4-979c-99c784500fa8"
# }
# 
# 2. GET /status/{ingestion_id}
# -----------------------------
# Fetches status of ingestion.
# 
# 🧾 Sample Curl Request:
# curl -X 'GET' \
#   'http://127.0.0.1:8000/status/72112dfe-1ac8-42d4-979c-99c784500fa8' \
#   -H 'accept: application/json'
# 
# ✅ Sample Response:
# {
#   "ingestion_id": "72112dfe-1ac8-42d4-979c-99c784500fa8",
#   "status": "triggered",
#   "batches": [
#     {
#       "batch_id": "eaf49177-95ac-42cb-9a18-ae7bc2750557",
#       "ids": [1, 2, 3],
#       "status": "yet_to_start"
#     },
#     {
#       "batch_id": "016ec676-631d-451b-abfa-eea748f34961",
#       "ids": [4, 5],
#       "status": "yet_to_start"
#     }
#   ]
# }
# 
# =======================
# 📦 Schemas
# =======================
# 
# 🔹 IngestionRequest
# {
#   "ids": [int],
#   "priority": "HIGH" | "MEDIUM" | "LOW"
# }
# 
# 🔹 IngestionStatus
# {
#   "ingestion_id": str,
#   "status": "yet_to_start" | "triggered" | "completed",
#   "batches": [BatchStatus]
# }
# 
# 🔹 BatchStatus
# {
#   "batch_id": str,
#   "ids": [int],
#   "status": "yet_to_start" | "triggered" | "completed"
# }
# 
# =======================
# ⚙️ Setup Instructions (Local)
# =======================
# 
# 1. Clone and Setup
# -------------------
# git clone <your-repo-url>
# cd data-ingestion-api
# python -m venv venv
# source venv/bin/activate     # On Windows: venv\Scripts\activate
# pip install -r requirements.txt
# 
# 2. Run Server
# -------------
# uvicorn main:app --reload
# 
# Visit Swagger UI:
# http://localhost:8000/docs
# 
# Visit Redoc:
# http://localhost:8000/redoc
# 
# =======================
# 🧪 Running Tests
# =======================
# 
# Run test suite with:
# pytest test_api.py -v
# 
# Sample Output:
# ----------------
# test_api.py::test_ingest_endpoint PASSED
# test_api.py::test_invalid_ids PASSED
# test_api.py::test_empty_ids PASSED
# test_api.py::test_status_endpoint PASSED
# test_api.py::test_priority_and_rate_limit PASSED
# test_api.py::test_invalid_ingestion_id PASSED
# 
# =======================
# 📂 File Summary
# =======================
# - main.py: FastAPI logic
# - test_api.py: Pytest test suite
# - requirements.txt:
#     - fastapi==0.103.2
#     - uvicorn==0.23.2
#     - httpx==0.25.0
#     - pytest==7.4.2
# 
# =======================
# 🌐 Hosted URL (Vercel)
# =======================
# Base URL: https://<your-vercel-project-name>.vercel.app
# 
# =======================
# ✅ Submission Checklist
# =======================
# - [x] Hosted base URL (Vercel)
# - [x] GitHub repo link
# - [x] Functional /ingest and /status endpoints
# - [x] Tests included and passing
# - [x] README in .txt format
# - [x] Sample test run output
# 
# =======================
# 📤 Submit Here
# =======================
# https://forms.gle/6HWbbfHywwqeN8Hz5
