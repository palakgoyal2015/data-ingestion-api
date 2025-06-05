from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator
from typing import List
from enum import Enum
import sqlite3
import uuid
import asyncio
from collections import deque
from datetime import datetime
import os
import uvicorn

app = FastAPI()

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class IngestionRequest(BaseModel):
    ids: List[int]
    priority: Priority

    @validator('ids')
    def validate_ids(cls, ids):
        if not ids:
            raise ValueError("IDs list cannot be empty")
        for id in ids:
            if not (1 <= id <= 1000000007):
                raise ValueError("IDs must be between 1 and 10^9+7")
        return ids

job_queue = deque()
queue_lock = asyncio.Lock()

def init_db():
    conn = sqlite3.connect('ingestion.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS ingestions
                 (ingestion_id TEXT PRIMARY KEY, status TEXT, created_time INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS batches
                 (batch_id TEXT PRIMARY KEY, ingestion_id TEXT, ids TEXT, status TEXT,
                  FOREIGN KEY (ingestion_id) REFERENCES ingestions(ingestion_id))''')
    conn.commit()
    conn.close()

init_db()

async def fetch_data_from_external_api(id: int):
    await asyncio.sleep(1)  # Simulate 1-second delay
    return {"id": id, "data": "processed"}

async def process_batch(batch_id: str, ingestion_id: str, ids: List[int]):
    try:
        for id in ids:
            await fetch_data_from_external_api(id)
        conn = sqlite3.connect('ingestion.db')
        c = conn.cursor()
        c.execute("UPDATE batches SET status = ? WHERE batch_id = ?", ('completed', batch_id))
        c.execute("SELECT status FROM batches WHERE ingestion_id = ?", (ingestion_id,))
        statuses = [row[0] for row in c.fetchall()]
        overall_status = 'yet_to_start'
        if 'triggered' in statuses:
            overall_status = 'triggered'
        elif all(s == 'completed' for s in statuses):
            overall_status = 'completed'
        c.execute("UPDATE ingestions SET status = ? WHERE ingestion_id = ?", (overall_status, ingestion_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")

async def process_jobs():
    while True:
        if not job_queue:
            await asyncio.sleep(1)
            continue
        async with queue_lock:
            high_priority_job = min(job_queue, key=lambda x: (x['priority'], x['created_time']), default=None)
            if high_priority_job:
                job_queue.remove(high_priority_job)
        if high_priority_job:
            batch_id = high_priority_job['batch_id']
            ingestion_id = high_priority_job['ingestion_id']
            ids = high_priority_job['ids']
            conn = sqlite3.connect('ingestion.db')
            c = conn.cursor()
            c.execute("UPDATE batches SET status = ? WHERE batch_id = ?", ('triggered', batch_id))
            conn.commit()
            conn.close()
            await process_batch(batch_id, ingestion_id, ids)
            await asyncio.sleep(5)  # 5-second rate limit
        else:
            await asyncio.sleep(1)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_jobs())

@app.get("/")
async def root():
    return {"message": "Data Ingestion API is running"}

@app.post("/ingest")
async def ingest(request: IngestionRequest):
    ingestion_id = str(uuid.uuid4())
    created_time = int(datetime.now().timestamp())
    conn = sqlite3.connect('ingestion.db')
    c = conn.cursor()
    c.execute("INSERT INTO ingestions (ingestion_id, status, created_time) VALUES (?, ?, ?)",
              (ingestion_id, 'yet_to_start', created_time))
    batches = [request.ids[i:i+3] for i in range(0, len(request.ids), 3)]
    for batch_ids in batches:
        batch_id = str(uuid.uuid4())
        c.execute("INSERT INTO batches (batch_id, ingestion_id, ids, status) VALUES (?, ?, ?, ?)",
                  (batch_id, ingestion_id, str(batch_ids), 'yet_to_start'))
        async with queue_lock:
            job_queue.append({
                'batch_id': batch_id,
                'ingestion_id': ingestion_id,
                'ids': batch_ids,
                'priority': request.priority,
                'created_time': created_time
            })
    c.execute("UPDATE ingestions SET status = ? WHERE ingestion_id = ?", ('triggered', ingestion_id))
    conn.commit()
    conn.close()
    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    conn = sqlite3.connect('ingestion.db')
    c = conn.cursor()
    c.execute("SELECT status, created_time FROM ingestions WHERE ingestion_id = ?", (ingestion_id,))
    ingestion = c.fetchone()
    if not ingestion:
        conn.close()
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    c.execute("SELECT batch_id, ids, status FROM batches WHERE ingestion_id = ?", (ingestion_id,))
    batches = [{"batch_id": row[0], "ids": eval(row[1]), "status": row[2]} for row in c.fetchall()]
    conn.close()
    return {
        "ingestion_id": ingestion_id,
        "status": ingestion[0],
        "batches": batches
    }

if __name__ == "__main__":
    # Use environment variable PORT, default to 8000 if not set
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)