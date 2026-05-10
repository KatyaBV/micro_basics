from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, Any, Dict, List
import asyncpg
import hazelcast
import consul
import os
import json
import time
import threading
import asyncio

app = FastAPI()

SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")
SERVICE_ID = os.getenv("SERVICE_ID", "counter-service-1")
SERVICE_HOST = os.getenv("SERVICE_HOST", "counter-service-1")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "bankdb")
DB_USER = os.getenv("DB_USER", "bankuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "bankpass")

db_pool: Optional[asyncpg.Pool] = None
hz_client: Optional[Any] = None
counter_queue: Optional[Any] = None
main_loop: Optional[asyncio.AbstractEventLoop] = None

BATCH_SIZE = 100
BATCH_TIMEOUT = 0.5


class UserCreate(BaseModel):
    user_id: str
    initial_balance: float = 0


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


async def init_db():
    global db_pool

    for _ in range(30):
        try:
            db_pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                min_size=20,
                max_size=100
            )

            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        user_id TEXT PRIMARY KEY,
                        balance DOUBLE PRECISION NOT NULL DEFAULT 0
                    );
                """)

            print(f"[{SERVICE_ID}] connected to PostgreSQL", flush=True)
            return

        except Exception as e:
            print(f"[{SERVICE_ID}] PostgreSQL error: {e}", flush=True)
            await asyncio.sleep(2)

    raise RuntimeError("Cannot connect to PostgreSQL")


def connect_hazelcast_queue():
    global hz_client, counter_queue

    members = consul_kv_get(
        "config/hazelcast/members",
        "hazelcast-1:5701"
    ).split(",")

    queue_name = consul_kv_get(
        "config/queue/name",
        "counter-queue"
    )

    for _ in range(30):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_name="bank-cluster",
                cluster_members=members,
                cluster_connect_timeout=10.0
            )

            counter_queue = hz_client.get_queue(queue_name).blocking()

            print(
                f"[{SERVICE_ID}] connected to Hazelcast queue={queue_name}",
                flush=True
            )
            return

        except Exception as e:
            print(f"[{SERVICE_ID}] Hazelcast Queue error: {e}", flush=True)
            time.sleep(2)


async def apply_batch(transactions: List[dict]):
    if not transactions or db_pool is None:
        return

    grouped: Dict[str, float] = {}

    for tx in transactions:
        user_id = tx["user_id"]
        amount = float(tx["amount"])
        grouped[user_id] = grouped.get(user_id, 0.0) + amount

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for user_id, total_amount in grouped.items():
                await conn.execute("""
                    INSERT INTO accounts(user_id, balance)
                    VALUES($1, $2)
                    ON CONFLICT (user_id)
                    DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
                """, user_id, total_amount)

    print(
        f"[{SERVICE_ID}] applied batch: {len(transactions)} tx, {len(grouped)} accounts",
        flush=True
    )


def consume_queue_batch():
    while True:
        batch = []

        try:
            if counter_queue is None or main_loop is None:
                time.sleep(1)
                continue

            start = time.perf_counter()

            while len(batch) < BATCH_SIZE:
                elapsed = time.perf_counter() - start

                if elapsed >= BATCH_TIMEOUT and batch:
                    break

                item = counter_queue.poll()

                if item is None:
                    if batch:
                        break
                    time.sleep(0.05)
                    continue

                batch.append(json.loads(item))

            if batch:
                future = asyncio.run_coroutine_threadsafe(
                    apply_batch(batch),
                    main_loop
                )

                future.result()

        except Exception as e:
            print(f"[{SERVICE_ID}] batch failed, requeue {len(batch)} tx: {e}", flush=True)

            if counter_queue is not None:
                for tx in batch:
                    try:
                        counter_queue.put(json.dumps(tx))
                    except Exception as requeue_error:
                        print(f"[{SERVICE_ID}] requeue error: {requeue_error}", flush=True)

            time.sleep(1)


@app.on_event("startup")
async def startup():
    global main_loop

    main_loop = asyncio.get_running_loop()

    await init_db()

    threading.Thread(target=register_service, daemon=True).start()
    threading.Thread(target=connect_hazelcast_queue, daemon=True).start()
    threading.Thread(target=consume_queue_batch, daemon=True).start()


@app.on_event("shutdown")
async def shutdown():
    try:
        get_consul_client().agent.service.deregister(SERVICE_ID)
    except Exception:
        pass

    if db_pool:
        await db_pool.close()

    if hz_client:
        hz_client.shutdown()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service_id": SERVICE_ID,
        "db_connected": db_pool is not None,
        "queue_connected": counter_queue is not None
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

    return {
        "balance": None if row is None else row["balance"]
    }


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