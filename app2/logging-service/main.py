from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any
import threading
import hazelcast
import os
import json
import time

app = FastAPI()

SERVICE_NAME = os.getenv("SERVICE_NAME", "logging-service")
HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
).split(",")

MAP_NAME = "transactions-map"

hz_client: Optional[Any] = None
tx_map: Optional[Any] = None


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float


def connect_hazelcast():
    global hz_client, tx_map

    for attempt in range(30):
        try:
            print(f"[{SERVICE_NAME}] connecting to Hazelcast, attempt {attempt + 1}")
            print(f"[{SERVICE_NAME}] members = {HAZELCAST_MEMBERS}")

            hz_client = hazelcast.HazelcastClient(
                cluster_name="bank-cluster",
                cluster_members=["hazelcast-1:5701"],
                cluster_connect_timeout=10.0,
            )

            tx_map = hz_client.get_map(MAP_NAME).blocking()

            print(f"[{SERVICE_NAME}] connected to Hazelcast")
            return

        except Exception as e:
            print(f"[{SERVICE_NAME}] Hazelcast error: {e}")
            time.sleep(2)

    tx_map = None
    print(f"[{SERVICE_NAME}] WARNING: Hazelcast unavailable")


@app.on_event("startup")
def startup():
    thread = threading.Thread(target=connect_hazelcast, daemon=True)
    thread.start()


@app.on_event("shutdown")
def shutdown():
    if hz_client:
        hz_client.shutdown()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "hazelcast_connected": tx_map is not None
    }


@app.post("/log")
def log_transaction(tx: Transaction):
    if tx_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast map unavailable")

    tx_data = {
        "transaction_id": tx.transaction_id,
        "user_id": tx.user_id,
        "amount": tx.amount,
        "timestamp": tx.timestamp
    }

    tx_map.set(tx.transaction_id, json.dumps(tx_data))

    return {
        "status": "logged",
        "service": SERVICE_NAME
    }


@app.get("/transactions/{user_id}")
def get_user_transactions(user_id: str):
    if tx_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast map unavailable")

    result: List[dict] = []

    for value in tx_map.values():
        tx = json.loads(value)
        if tx["user_id"] == user_id:
            result.append(tx)

    return {
        "service": SERVICE_NAME,
        "transactions": result
    }


@app.get("/transactions")
def get_all_transactions():
    if tx_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast map unavailable")

    result = []

    for value in tx_map.values():
        result.append(json.loads(value))

    return {
        "service": SERVICE_NAME,
        "transactions": result
    }