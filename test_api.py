import pytest
import asyncio
import time
from httpx import AsyncClient
from main import app, Priority
from main import init_db
import sqlite3

@pytest.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client

@pytest.fixture
def setup_db():
    init_db()  # Ensure database is initialized
    conn = sqlite3.connect("ingestion.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ingestions")
    cursor.execute("DELETE FROM batches")
    conn.commit()
    conn.close()

@pytest.mark.asyncio
async def test_ingest_endpoint(client, setup_db):
    response = await client.post("/ingest", json={"ids": [1, 2, 3, 4, 5], "priority": "HIGH"})
    assert response.status_code == 200
    data = response.json()
    assert "ingestion_id" in data
    assert len(data["ingestion_id"]) > 0

@pytest.mark.asyncio
async def test_invalid_ids(client, setup_db):
    response = await client.post("/ingest", json={"ids": [0], "priority": "HIGH"})
    assert response.status_code == 400
    assert "IDs must be between 1 and 10^9+7" in response.json()["detail"]
    
    response = await client.post("/ingest", json={"ids": [10**9 + 8], "priority": "HIGH"})
    assert response.status_code == 400
    assert "IDs must be between 1 and 10^9+7" in response.json()["detail"]

@pytest.mark.asyncio
async def test_empty_ids(client, setup_db):
    response = await client.post("/ingest", json={"ids": [], "priority": "HIGH"})
    assert response.status_code == 400
    assert "IDs list cannot be empty" in response.json()["detail"]

@pytest.mark.asyncio
async def test_status_endpoint(client, setup_db):
    response = await client.post("/ingest", json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"})
    ingestion_id = response.json()["ingestion_id"]
    
    await asyncio.sleep(2)
    response = await client.get(f"/status/{ingestion_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["ingestion_id"] == ingestion_id
    assert data["status"] in ["yet_to_start", "triggered", "completed"]
    assert len(data["batches"]) == 2
    assert data["batches"][0]["ids"] == [1, 2, 3]
    assert data["batches"][1]["ids"] == [4, 5]

@pytest.mark.asyncio
async def test_priority_and_rate_limit(client, setup_db):
    start_time = time.time()
    
    response1 = await client.post("/ingest", json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"})
    ingestion_id1 = response1.json()["ingestion_id"]
    
    await asyncio.sleep(4)
    
    response2 = await client.post("/ingest", json={"ids": [6, 7, 8, 9], "priority": "HIGH"})
    ingestion_id2 = response2.json()["ingestion_id"]
    
    await asyncio.sleep(2)
    response1_status = await client.get(f"/status/{ingestion_id1}")
    response2_status = await client.get(f"/status/{ingestion_id2}")
    
    assert response1_status.json()["batches"][0]["status"] == "completed"
    assert response1_status.json()["batches"][1]["status"] == "yet_to_start"
    
    await asyncio.sleep(5)
    response2_status = await client.get(f"/status/{ingestion_id2}")
    assert response2_status.json()["batches"][0]["status"] == "completed"
    
    await asyncio.sleep(5)
    response1_status = await client.get(f"/status/{ingestion_id1}")
    response2_status = await client.get(f"/status/{ingestion_id2}")
    assert response2_status.json()["batches"][1]["status"] == "completed"
    assert response1_status.json()["batches"][1]["status"] == "completed"
    
    assert time.time() - start_time >= 15

@pytest.mark.asyncio
async def test_invalid_ingestion_id(client, setup_db):
    response = await client.get("/status/invalid_id")
    assert response.status_code == 404
    assert "Ingestion ID not found" in response.json()["detail"]

if __name__ == "__main__":
    pytest.main(["-v"])