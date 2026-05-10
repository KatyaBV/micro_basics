from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import hazelcast
import asyncio
import random
import uuid
import time
import json
import os
from typing import Optional, Any

app = FastAPI()

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8000")

HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_MEMBERS",
    "hazelcast-1:5701"
).split(",")

QUEUE_NAME = "counter-queue"

http_client: Optional[httpx.AsyncClient] = None
hz_client: Optional[Any] = None
counter_queue: Optional[Any] = None

logging_time_total = 0.0
counter_queue_time_total = 0.0


class ClientTransaction(BaseModel):
    user_id: str
    amount: float


class UserCreate(BaseModel):
    user_id: str
    initial_balance: float = 0


@app.on_event("startup")
async def startup():
    global http_client, hz_client, counter_queue

    http_client = httpx.AsyncClient(
        timeout=60.0,
        limits=httpx.Limits(
            max_connections=500,
            max_keepalive_connections=100
        )
    )

    hz_client = hazelcast.HazelcastClient(
        cluster_name="bank-cluster",
        cluster_members=HAZELCAST_MEMBERS,
        cluster_connect_timeout=10.0
    )

    counter_queue = hz_client.get_queue(QUEUE_NAME).blocking()

    print("[facade-service] connected to Hazelcast Queue")


@app.on_event("shutdown")
async def shutdown():
    if http_client:
        await http_client.aclose()

    if hz_client:
        hz_client.shutdown()


async def get_service_urls(service_name: str):
    response = await http_client.get(
        f"{CONFIG_SERVER_URL}/services/{service_name}"
    )

    response.raise_for_status()

    urls = response.json().get("urls", [])

    if not urls:
        raise HTTPException(
            status_code=503,
            detail=f"No registered instances for {service_name}"
        )

    return urls


async def call_logging_with_failover(path: str, payload: dict):
    urls = await get_service_urls("logging-service")
    random.shuffle(urls)

    last_error = None

    for url in urls:
        try:
            response = await http_client.post(
                f"{url}{path}",
                json=payload
            )

            response.raise_for_status()
            return response.json()

        except Exception as e:
            last_error = e
            print(f"[facade-service] logging failed: {url}, error={e}")

    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances unavailable: {last_error}"
    )


async def get_from_random_service(service_name: str, path: str):
    urls = await get_service_urls(service_name)
    random.shuffle(urls)

    last_error = None

    for url in urls:
        try:
            response = await http_client.get(f"{url}{path}")
            response.raise_for_status()
            return response.json()

        except Exception as e:
            last_error = e
            print(f"[facade-service] service failed: {url}, error={e}")

    raise HTTPException(
        status_code=503,
        detail=f"All {service_name} instances unavailable: {last_error}"
    )


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "queue_connected": counter_queue is not None
    }


@app.post("/users")
async def create_user(user: UserCreate):
    urls = await get_service_urls("counter-service")
    counter_url = random.choice(urls)

    response = await http_client.post(
        f"{counter_url}/users",
        json=user.model_dump()
    )

    response.raise_for_status()
    return response.json()


@app.post("/transaction")
async def create_transaction(tx: ClientTransaction):
    global logging_time_total, counter_queue_time_total

    if counter_queue is None:
        raise HTTPException(status_code=503, detail="Counter queue unavailable")

    payload = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": tx.user_id,
        "amount": tx.amount,
        "timestamp": time.time()
    }

    start_logging = time.perf_counter()
    await call_logging_with_failover("/log", payload)
    logging_time_total += time.perf_counter() - start_logging

    start_queue = time.perf_counter()
    counter_queue.put(json.dumps(payload))
    counter_queue_time_total += time.perf_counter() - start_queue

    return {
        "transaction_id": payload["transaction_id"],
        "status": "accepted"
    }


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    balance_task = get_from_random_service(
        "counter-service",
        f"/balance/{user_id}"
    )

    transactions_task = get_from_random_service(
        "logging-service",
        f"/transactions/{user_id}"
    )

    balance_json, tx_json = await asyncio.gather(
        balance_task,
        transactions_task
    )

    return {
        "balance": balance_json["balance"],
        "transactions": tx_json["transactions"],
        "logging_instance": tx_json.get("service")
    }


@app.get("/accounts")
async def get_accounts():
    return await get_from_random_service(
        "counter-service",
        "/balances"
    )


@app.get("/metrics")
async def metrics():
    return {
        "logging_time_total": logging_time_total,
        "counter_queue_time_total": counter_queue_time_total
    }


@app.post("/metrics/reset")
async def reset_metrics():
    global logging_time_total, counter_queue_time_total

    logging_time_total = 0.0
    counter_queue_time_total = 0.0

    return {"status": "reset"}


@app.post("/reset")
async def reset_all():
    global logging_time_total, counter_queue_time_total

    logging_time_total = 0.0
    counter_queue_time_total = 0.0

    logging_urls = await get_service_urls("logging-service")
    counter_urls = await get_service_urls("counter-service")

    tasks = []

    for url in logging_urls:
        tasks.append(http_client.post(f"{url}/reset"))

    for url in counter_urls:
        tasks.append(http_client.post(f"{url}/reset"))

    await asyncio.gather(*tasks, return_exceptions=True)

    return {"status": "reset"}