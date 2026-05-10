from fastapi import FastAPI
from pydantic import BaseModel
import asyncpg
import hazelcast
import requests
import os
import json
import time
import threading
import asyncio
from typing import Optional, Any

app = FastAPI()

SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")
SERVICE_URL = os.getenv("SERVICE_URL", "http://counter-service:8000")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8000")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "bankdb")
DB_USER = os.getenv("DB_USER", "bankuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "bankpass")

HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_MEMBERS",
    "hazelcast-1:5701"
).split(",")

QUEUE_NAME = "counter-queue"

db_pool: Optional[asyncpg.Pool] = None
hz_client: Optional[Any] = None
counter_queue: Optional[Any] = None
main_loop: Optional[asyncio.AbstractEventLoop] = None


class UserCreate(BaseModel):
    user_id: str
    initial_balance: float = 0


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float


def register_service():
    for attempt in range(30):
        try:
            requests.post(
                f"{CONFIG_SERVER_URL}/register",
                json={
                    "name": "counter-service",
                    "url": SERVICE_URL
                },
                timeout=5
            )
            print(f"[counter-service] registered in config-server as {SERVICE_URL}")
            return
        except Exception as e:
            print(f"[counter-service] config-server not ready: {e}")
            time.sleep(2)


def connect_hazelcast_queue():
    global hz_client, counter_queue

    for attempt in range(30):
        try:
            print(f"[counter-service] connecting to Hazelcast Queue, attempt {attempt + 1}")

            hz_client = hazelcast.HazelcastClient(
                cluster_name="bank-cluster",
                cluster_members=HAZELCAST_MEMBERS,
                cluster_connect_timeout=10.0
            )

            counter_queue = hz_client.get_queue(QUEUE_NAME).blocking()

            print("[counter-service] connected to Hazelcast Queue")
            return

        except Exception as e:
            print(f"[counter-service] Hazelcast error: {e}")
            time.sleep(2)

    print("[counter-service] WARNING: Hazelcast Queue unavailable")


async def init_db():
    global db_pool

    for attempt in range(30):
        try:
            db_pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                min_size=5,
                max_size=50
            )

            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        user_id TEXT PRIMARY KEY,
                        balance DOUBLE PRECISION NOT NULL DEFAULT 0
                    );
                """)

            print("[counter-service] connected to PostgreSQL")
            return

        except Exception as e:
            print(f"[counter-service] PostgreSQL not ready: {e}")
            await asyncio.sleep(2)

    raise RuntimeError("Cannot connect to PostgreSQL")


async def apply_transaction(tx: dict):
    if db_pool is None:
        print("[counter-service] DB pool is unavailable")
        return

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE accounts
            SET balance = balance + $1
            WHERE user_id = $2
            RETURNING balance
        """, tx["amount"], tx["user_id"])

        if row is None:
            await conn.execute("""
                INSERT INTO accounts(user_id, balance)
                VALUES($1, $2)
                ON CONFLICT (user_id)
                DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
            """, tx["user_id"], tx["amount"])

    print(
        f"[counter-service] applied transaction "
        f"{tx['transaction_id']} user={tx['user_id']} amount={tx['amount']}"
    )


def consume_queue():
    while True:
        try:
            if counter_queue is None or main_loop is None:
                time.sleep(1)
                continue

            item = counter_queue.take()
            tx = json.loads(item)

            future = asyncio.run_coroutine_threadsafe(
                apply_transaction(tx),
                main_loop
            )

            future.result()

        except Exception as e:
            print(f"[counter-service] consumer error: {e}")
            time.sleep(1)


@app.on_event("startup")
async def startup():
    global main_loop

    main_loop = asyncio.get_running_loop()

    await init_db()

    threading.Thread(target=register_service, daemon=True).start()
    threading.Thread(target=connect_hazelcast_queue, daemon=True).start()
    threading.Thread(target=consume_queue, daemon=True).start()


@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()

    if hz_client:
        hz_client.shutdown()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "queue_connected": counter_queue is not None,
        "db_connected": db_pool is not None
    }


@app.post("/users")
async def create_user(user: UserCreate):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO accounts(user_id, balance)
            VALUES($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET balance = EXCLUDED.balance
        """, user.user_id, user.initial_balance)

    return {
        "user_id": user.user_id,
        "balance": user.initial_balance
    }


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance FROM accounts WHERE user_id=$1",
            user_id
        )

    if row is None:
        return {"balance": None}

    return {"balance": row["balance"]}


@app.get("/balances")
async def get_balances():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, balance FROM accounts")

    return {
        row["user_id"]: row["balance"]
        for row in rows
    }


@app.post("/reset")
async def reset():
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM accounts")

    return {"status": "reset"}