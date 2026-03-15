from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import asyncio
import uuid
import time

app = FastAPI()

LOGGING_URL = "http://logging-service:8000"
COUNTER_URL = "http://counter-service:8000"

logging_time_total = 0
counter_time_total = 0


class ClientTransaction(BaseModel):
    user_id: str
    amount: float


@app.post("/users")
async def create_user(user: dict):

    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{COUNTER_URL}/users",
            json=user
        )

    return r.json()


@app.post("/transaction")
async def create_transaction(tx: ClientTransaction):

    global logging_time_total
    global counter_time_total

    transaction_id = str(uuid.uuid4())
    timestamp = time.time()

    payload = {
        "transaction_id": transaction_id,
        "user_id": tx.user_id,
        "amount": tx.amount,
        "timestamp": timestamp
    }

    async with httpx.AsyncClient() as client:

        async def call_logging():
            start = time.perf_counter()
            await client.post(f"{LOGGING_URL}/log", json=payload)
            end = time.perf_counter()
            return end - start

        async def call_counter():
            start = time.perf_counter()
            r = await client.post(f"{COUNTER_URL}/update", json=payload)
            end = time.perf_counter()
            return r.json()["balance"], end - start

        logging_result, counter_result = await asyncio.gather(
            call_logging(),
            call_counter()
        )

        logging_time_total += logging_result
        balance, counter_time = counter_result
        counter_time_total += counter_time

    return {
        "transaction_id": transaction_id,
        "balance": balance
    }


@app.get("/user/{user_id}")
async def get_user(user_id: str):

    async with httpx.AsyncClient() as client:

        balance_resp = await client.get(
            f"{COUNTER_URL}/balance/{user_id}"
        )

        tx_resp = await client.get(
            f"{LOGGING_URL}/transactions/{user_id}"
        )

    return {
        "balance": balance_resp.json()["balance"],
        "transactions": tx_resp.json()
    }


@app.get("/accounts")
async def get_accounts():

    async with httpx.AsyncClient() as client:
        r = await client.get(f"{COUNTER_URL}/balances")

    return r.json()


@app.get("/metrics")
async def metrics():

    return {
        "logging_time_total": logging_time_total,
        "counter_time_total": counter_time_total
    }


@app.post("/metrics/reset")
async def reset_metrics():

    global logging_time_total
    global counter_time_total

    logging_time_total = 0
    counter_time_total = 0

    return {"status": "reset"}
