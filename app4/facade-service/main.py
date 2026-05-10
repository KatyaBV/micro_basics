from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Any, Dict, Tuple, List
import httpx
import hazelcast
import consul
import os
import json
import random
import uuid
import time
import asyncio
import threading

app = FastAPI()

SERVICE_NAME = os.getenv("SERVICE_NAME", "facade-service")
SERVICE_ID = os.getenv("SERVICE_ID", "facade-service-1")
SERVICE_HOST = os.getenv("SERVICE_HOST", "facade-service-1")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))

http_client: Optional[httpx.AsyncClient] = None
hz_client: Optional[Any] = None
counter_queue: Optional[Any] = None

logging_time_total = 0.0
counter_queue_time_total = 0.0

SERVICE_CACHE: Dict[str, Tuple[float, List[str]]] = {}
CACHE_TTL = 5.0


class ClientTransaction(BaseModel):
    user_id: str
    amount: float


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


def get_service_urls_from_consul(service_name: str):
    c = get_consul_client()
    _, services = c.health.service(service_name, passing=True)

    urls = []

    for item in services:
        service = item["Service"]
        address = service["Address"]
        port = service["Port"]
        urls.append(f"http://{address}:{port}")

    return urls


async def get_service_urls(service_name: str):
    now = time.time()

    if service_name in SERVICE_CACHE:
        ts, urls = SERVICE_CACHE[service_name]
        if now - ts < CACHE_TTL and urls:
            return urls

    urls = await asyncio.to_thread(get_service_urls_from_consul, service_name)

    SERVICE_CACHE[service_name] = (now, urls)

    return urls


async def get_random_service_url(service_name: str):
    urls = await get_service_urls(service_name)

    if not urls:
        raise HTTPException(
            status_code=503,
            detail=f"No healthy instances for {service_name}"
        )

    return random.choice(urls)


@app.on_event("startup")
async def startup():
    global http_client, hz_client, counter_queue

    http_client = httpx.AsyncClient(
        timeout=120.0,
        limits=httpx.Limits(
            max_connections=1000,
            max_keepalive_connections=300
        )
    )

    threading.Thread(target=register_service, daemon=True).start()

    members = await asyncio.to_thread(
        consul_kv_get,
        "config/hazelcast/members",
        "hazelcast-1:5701"
    )

    queue_name = await asyncio.to_thread(
        consul_kv_get,
        "config/queue/name",
        "counter-queue"
    )

    hz_client = hazelcast.HazelcastClient(
        cluster_name="bank-cluster",
        cluster_members=members.split(","),
        cluster_connect_timeout=10.0
    )

    counter_queue = hz_client.get_queue(queue_name).blocking()

    print(f"[{SERVICE_ID}] connected to queue={queue_name}", flush=True)


@app.on_event("shutdown")
async def shutdown():
    try:
        get_consul_client().agent.service.deregister(SERVICE_ID)
    except Exception:
        pass

    if http_client:
        await http_client.aclose()

    if hz_client:
        hz_client.shutdown()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service_id": SERVICE_ID,
        "queue_connected": counter_queue is not None
    }


async def call_logging(payload: dict):
    urls = await get_service_urls("logging-service")
    random.shuffle(urls)

    last_error = None

    for url in urls:
        try:
            response = await http_client.post(
                f"{url}/log",
                json=payload
            )

            response.raise_for_status()
            return response.json()

        except Exception as e:
            last_error = e
            print(f"[{SERVICE_ID}] logging failed {url}: {e}", flush=True)

            SERVICE_CACHE.pop("logging-service", None)

    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances failed: {last_error}"
    )


async def get_from_service(service_name: str, path: str):
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
            print(f"[{SERVICE_ID}] {service_name} failed {url}: {e}", flush=True)

            SERVICE_CACHE.pop(service_name, None)

    raise HTTPException(
        status_code=503,
        detail=f"All {service_name} instances failed: {last_error}"
    )


@app.post("/users")
async def create_user(user: UserCreate):
    counter_url = await get_random_service_url("counter-service")

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
        raise HTTPException(status_code=503, detail="Queue unavailable")

    payload = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": tx.user_id,
        "amount": tx.amount,
        "timestamp": time.time()
    }

    start = time.perf_counter()
    await call_logging(payload)
    logging_time_total += time.perf_counter() - start

    start = time.perf_counter()
    counter_queue.put(json.dumps(payload))
    counter_queue_time_total += time.perf_counter() - start

    return {
        "transaction_id": payload["transaction_id"],
        "status": "accepted"
    }


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    try:
        balance_json, tx_json = await asyncio.gather(
            get_from_service("counter-service", f"/balance/{user_id}"),
            get_from_service("logging-service", f"/transactions/{user_id}")
        )

        balance = balance_json["balance"]

    except Exception:
        tx_json = await get_from_service(
            "logging-service",
            f"/transactions/{user_id}"
        )
        balance = None

    return {
        "balance": balance,
        "transactions": tx_json["transactions"],
        "logging_instance": tx_json.get("service")
    }


@app.get("/accounts")
async def get_accounts():
    try:
        return await get_from_service("counter-service", "/balances")
    except Exception:
        return {}


@app.get("/metrics")
def metrics():
    return {
        "logging_time_total": logging_time_total,
        "counter_queue_time_total": counter_queue_time_total
    }


@app.post("/metrics/reset")
def reset_metrics():
    global logging_time_total, counter_queue_time_total

    logging_time_total = 0.0
    counter_queue_time_total = 0.0

    return {"status": "reset"}


@app.post("/reset")
async def reset_all():
    global logging_time_total, counter_queue_time_total

    logging_time_total = 0.0
    counter_queue_time_total = 0.0

    SERVICE_CACHE.clear()

    logging_urls = await get_service_urls("logging-service")
    counter_urls = await get_service_urls("counter-service")

    tasks = []

    for url in logging_urls:
        tasks.append(http_client.post(f"{url}/reset"))

    for url in counter_urls:
        tasks.append(http_client.post(f"{url}/reset"))

    await asyncio.gather(*tasks, return_exceptions=True)

    return {"status": "reset"}

@app.get("/balance/{user_id}")
async def get_balance_only(user_id: str):
    try:
        balance_json = await get_from_service(
            "counter-service",
            f"/balance/{user_id}"
        )

        return {
            "balance": balance_json["balance"]
        }

    except Exception:
        return {
            "balance": None
        }

@app.get("/queue/size")
def queue_size():
    if counter_queue is None:
        return {"queue_size": None}

    return {"queue_size": counter_queue.size()}