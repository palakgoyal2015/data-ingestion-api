import asyncio
import uuid
from enum import Enum
from typing import List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import sqlite3
from collections import deque
import logging
import os  # For reading PORT environment variable
import uvicorn  # Required for programmatic run

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class IngestionRequest(BaseModel):
    ids: List[int]
    priority: Priority = Priority.MEDIUM


class BatchStatus(BaseModel):
    batch_id: str
    ids: List[int]
    status: str


class IngestionStatus(BaseModel):
    ingestion_id: str
    status: str
    batches: List[BatchStatus]


job_queue = deque()
processing_lock = asyncio.Lock()


def init_db():
    conn = sqlite3.connect("ingestion.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ingestions (
            ingestion_id TEXT PRIMARY KEY,
            status TEXT,
            created_time TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            batch_id TEXT PRIMARY KEY,
            ingestion_id TEXT,
            ids TEXT,
            status TEXT,
            FOREIGN KEY (ingestion_id) REFERENCES ingestions (ingestion_id)
        )
    """)
    conn.commit()
    conn.close()


init_db()


async def fetch_data_from_external_api(id: int) -> Dict:
    await asyncio.sleep(1)
    return {"id": id, "data": "processed"}


async def process_batch(batch_id: str, ingestion_id: str, ids: List[int]):
    try:
        logger.info(f"Processing batch {batch_id} with IDs {ids}")
        for id in ids:
            await fetch_data_from_external_api(id)

        conn = sqlite3.connect("ingestion.db")
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE batches SET status = ? WHERE batch_id = ?",
            ("completed", batch_id)
        )
        conn.commit()

        cursor.execute(
            "SELECT status FROM batches WHERE ingestion_id = ?",
            (ingestion_id,)
        )
        batch_statuses = [row[0] for row in cursor.fetchall()]
        overall_status = "yet_to_start"
        if any(s == "triggered" for s in batch_statuses):
            overall_status = "triggered"
        elif all(s == "completed" for s in batch_statuses):
            overall_status = "completed"
        cursor.execute(
            "UPDATE ingestions SET status = ? WHERE ingestion_id = ?",
            (overall_status, ingestion_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")


async def process_jobs():
    while True:
        async with processing_lock:
            if not job_queue:
                await asyncio.sleep(1)
                continue

            high_priority_job = None
            high_priority_index = None
            for i, job in enumerate(job_queue):
                if high_priority_job is None or job["priority"].value < high_priority_job["priority"].value or \
                        (job["priority"] == high_priority_job["priority"] and job["created_time"] < high_priority_job["created_time"]):
                    high_priority_job = job
                    high_priority_index = i

            if high_priority_job:
                job_queue.pop(high_priority_index)
                batch_id = high_priority_job["batch_id"]
                ingestion_id = high_priority_job["ingestion_id"]
                ids = high_priority_job["ids"]

                conn = sqlite3.connect("ingestion.db")
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE batches SET status = ? WHERE batch_id = ?",
                    ("triggered", batch_id)
                )
                conn.commit()
                conn.close()

                await process_batch(batch_id, ingestion_id, ids)
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(1)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_jobs())


@app.post("/ingest")
async def ingest_data(request: IngestionRequest):
    if not request.ids:
        raise HTTPException(status_code=400, detail="IDs list cannot be empty")
    if any(id < 1 or id > 10 ** 9 + 7 for id in request.ids):
        raise HTTPException(status_code=400, detail="IDs must be between 1 and 10^9+7")

    ingestion_id = str(uuid.uuid4())
    created_time = datetime.utcnow()

    conn = sqlite3.connect("ingestion.db")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ingestions (ingestion_id, status, created_time) VALUES (?, ?, ?)",
        (ingestion_id, "yet_to_start", created_time)
    )

    batches = [request.ids[i:i + 3] for i in range(0, len(request.ids), 3)]
    for batch_ids in batches:
        batch_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO batches (batch_id, ingestion_id, ids, status) VALUES (?, ?, ?, ?)",
            (batch_id, ingestion_id, str(batch_ids), "yet_to_start")
        )
        job_queue.append({
            "batch_id": batch_id,
            "ingestion_id": ingestion_id,
            "ids": batch_ids,
            "priority": request.priority,
            "created_time": created_time
        })

    cursor.execute(
        "UPDATE ingestions SET status = ? WHERE ingestion_id = ?",
        ("triggered", ingestion_id)
    )
    conn.commit()
    conn.close()

    return {"ingestion_id": ingestion_id}


@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    conn = sqlite3.connect("ingestion.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status, created_time FROM ingestions WHERE ingestion_id = ?",
        (ingestion_id,)
    )
    ingestion = cursor.fetchone()
    if not ingestion:
        conn.close()
        raise HTTPException(status_code=404, detail="Ingestion ID not found")

    cursor.execute(
        "SELECT batch_id, ids, status FROM batches WHERE ingestion_id = ?",
        (ingestion_id,)
    )
    batches = [{"batch_id": row[0], "ids": eval(row[1]), "status": row[2]} for row in cursor.fetchall()]
    conn.close()

    return {
        "ingestion_id": ingestion_id,
        "status": ingestion[0],
        "batches": batches
    }


# ✅ Required for Render: Bind to $PORT
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))  # Render sets $PORT
    uvicorn.run("main:app", host="0.0.0.0", port=port)
