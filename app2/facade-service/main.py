from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import asyncio
import os
import random
import time
import uuid

app = FastAPI()

LOGGING_SERVICES = os.getenv(
    "LOGGING_SERVICES",
    "http://logging-service-1:8000,http://logging-service-2:8000,http://logging-service-3:8000"
).split(",")

COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8000")

logging_time_total = 0.0
counter_time_total = 0.0
http_client: httpx.AsyncClient | None = None


class ClientTransaction(BaseModel):
    user_id: str
    amount: float


class UserCreate(BaseModel):
    user_id: str
    initial_balance: float = 0


@app.on_event("startup")
async def startup():
    global http_client
    limits = httpx.Limits(
        max_connections=500,
        max_keepalive_connections=100
    )
    timeout = httpx.Timeout(60.0)
    http_client = httpx.AsyncClient(timeout=timeout, limits=limits)


@app.on_event("shutdown")
async def shutdown():
    if http_client:
        await http_client.aclose()


async def call_logging_with_failover(method: str, path: str, json_body=None):
    services = LOGGING_SERVICES.copy()
    random.shuffle(services)

    last_error = None

    for base_url in services:
        try:
            if method == "POST":
                response = await http_client.post(
                    f"{base_url}{path}",
                    json=json_body
                )
            else:
                response = await http_client.get(f"{base_url}{path}")

            response.raise_for_status()
            return response.json()

        except Exception as e:
            last_error = e
            print(f"[facade] logging failed: {base_url}, error={e}")

    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances unavailable: {last_error}"
    )


@app.post("/users")
async def create_user(user: UserCreate):
    response = await http_client.post(
        f"{COUNTER_URL}/users",
        json=user.model_dump()
    )
    response.raise_for_status()
    return response.json()


@app.post("/transaction")
async def create_transaction(tx: ClientTransaction):
    global logging_time_total, counter_time_total

    payload = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": tx.user_id,
        "amount": tx.amount,
        "timestamp": time.time()
    }

    async def call_logging():
        start = time.perf_counter()
        result = await call_logging_with_failover("POST", "/log", payload)
        return result, time.perf_counter() - start

    async def call_counter():
        start = time.perf_counter()
        response = await http_client.post(f"{COUNTER_URL}/update", json=payload)
        response.raise_for_status()
        return response.json(), time.perf_counter() - start

    logging_result, counter_result = await asyncio.gather(
        call_logging(),
        call_counter()
    )

    _, logging_duration = logging_result
    counter_json, counter_duration = counter_result

    logging_time_total += logging_duration
    counter_time_total += counter_duration

    return {
        "transaction_id": payload["transaction_id"],
        "balance": counter_json["balance"]
    }


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    global logging_time_total, counter_time_total

    async def get_balance():
        start = time.perf_counter()
        response = await http_client.get(f"{COUNTER_URL}/balance/{user_id}")
        response.raise_for_status()
        return response.json(), time.perf_counter() - start

    async def get_transactions():
        start = time.perf_counter()
        result = await call_logging_with_failover(
            "GET",
            f"/transactions/{user_id}"
        )
        return result, time.perf_counter() - start

    balance_result, tx_result = await asyncio.gather(
        get_balance(),
        get_transactions()
    )

    balance_json, counter_duration = balance_result
    tx_json, logging_duration = tx_result

    logging_time_total += logging_duration
    counter_time_total += counter_duration

    return {
        "balance": balance_json["balance"],
        "transactions": tx_json["transactions"],
        "logging_instance": tx_json["service"]
    }


@app.get("/accounts")
async def get_accounts():
    response = await http_client.get(f"{COUNTER_URL}/balances")
    response.raise_for_status()
    return response.json()


@app.get("/metrics")
async def get_metrics():
    return {
        "logging_time_total": logging_time_total,
        "counter_time_total": counter_time_total
    }


@app.post("/metrics/reset")
async def reset_metrics():
    global logging_time_total, counter_time_total
    logging_time_total = 0.0
    counter_time_total = 0.0
    return {"status": "reset"}


@app.post("/reset")
async def reset_all():
    global logging_time_total, counter_time_total
    logging_time_total = 0.0
    counter_time_total = 0.0

    response = await http_client.post(f"{COUNTER_URL}/reset")
    response.raise_for_status()

    return {"status": "reset"}