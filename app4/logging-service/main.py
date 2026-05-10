from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Any, List
import hazelcast
import consul
import os
import json
import time
import threading

app = FastAPI()

SERVICE_NAME = os.getenv("SERVICE_NAME", "logging-service")
SERVICE_ID = os.getenv("SERVICE_ID", "logging-service-1")
SERVICE_HOST = os.getenv("SERVICE_HOST", SERVICE_ID)
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))

hz_client: Optional[Any] = None
tx_map: Optional[Any] = None


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float


def get_consul_client():
    return consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)


def consul_kv_get(key: str, default: str):
    c = get_consul_client()

    for _ in range(30):
        try:
            _, data = c.kv.get(key)
            if data and data.get("Value"):
                return data["Value"].decode()
        except Exception as e:
            print(f"[{SERVICE_ID}] Consul KV error: {e}", flush=True)

        time.sleep(2)

    return default


def register_service():
    c = get_consul_client()

    for _ in range(30):
        try:
            c.agent.service.register(
                name=SERVICE_NAME,
                service_id=SERVICE_ID,
                address=SERVICE_HOST,
                port=SERVICE_PORT,
                check={
                    "http": f"http://{SERVICE_HOST}:{SERVICE_PORT}/health",
                    "interval": "10s",
                    "timeout": "5s"
                }
            )

            print(f"[{SERVICE_ID}] registered in Consul", flush=True)
            return

        except Exception as e:
            print(f"[{SERVICE_ID}] Consul register error: {e}", flush=True)
            time.sleep(2)


def connect_hazelcast():
    global hz_client, tx_map

    members = consul_kv_get(
        "config/hazelcast/members",
        "hazelcast-1:5701"
    ).split(",")

    map_name = consul_kv_get(
        "config/hazelcast/map",
        "transactions-map"
    )

    for attempt in range(30):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_name="bank-cluster",
                cluster_members=members,
                cluster_connect_timeout=10.0
            )

            tx_map = hz_client.get_map(map_name).blocking()

            print(
                f"[{SERVICE_ID}] connected to Hazelcast map={map_name}",
                flush=True
            )
            return

        except Exception as e:
            print(f"[{SERVICE_ID}] Hazelcast error: {e}", flush=True)
            time.sleep(2)


@app.on_event("startup")
def startup():
    threading.Thread(target=register_service, daemon=True).start()
    threading.Thread(target=connect_hazelcast, daemon=True).start()


@app.on_event("shutdown")
def shutdown():
    try:
        get_consul_client().agent.service.deregister(SERVICE_ID)
    except Exception:
        pass

    if hz_client:
        hz_client.shutdown()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service_id": SERVICE_ID,
        "hazelcast_connected": tx_map is not None
    }


@app.post("/log")
def log_transaction(tx: Transaction):
    if tx_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast unavailable")

    tx_data = {
        "transaction_id": tx.transaction_id,
        "user_id": tx.user_id,
        "amount": tx.amount,
        "timestamp": tx.timestamp
    }

    tx_map.set(tx.transaction_id, json.dumps(tx_data))

    print(
        f"[{SERVICE_ID}] received transaction {tx.transaction_id} user={tx.user_id}",
        flush=True
    )

    return {
        "status": "logged",
        "service": SERVICE_ID
    }


@app.get("/transactions/{user_id}")
def get_user_transactions(user_id: str):
    if tx_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast unavailable")

    result: List[dict] = []

    for value in tx_map.values():
        tx = json.loads(value)
        if tx["user_id"] == user_id:
            result.append(tx)

    return {
        "service": SERVICE_ID,
        "transactions": result
    }


@app.post("/reset")
def reset():
    if tx_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast unavailable")

    tx_map.clear()
    return {"status": "reset"}