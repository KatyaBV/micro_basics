import threading
import requests
import time

FACADE = "http://localhost:8000"

CLIENTS = 10
REQUESTS_PER_CLIENT = 10000
AMOUNT = 1


def post_json(url, payload=None, timeout=120):
    if payload is None:
        payload = {}

    return requests.post(
        url,
        json=payload,
        timeout=timeout
    )


def reset_system():
    try:
        post_json(f"{FACADE}/reset", timeout=120)
    except Exception as e:
        print("Reset error:", e)


def create_user(user_id):
    r = post_json(
        f"{FACADE}/users",
        {
            "user_id": user_id,
            "initial_balance": 0
        }
    )

    if r.status_code not in (200, 201):
        print("Create user error:", r.status_code, r.text)


def worker(user_id, stats, lock):
    session = requests.Session()

    for _ in range(REQUESTS_PER_CLIENT):
        try:
            r = session.post(
                f"{FACADE}/transaction",
                json={
                    "user_id": user_id,
                    "amount": AMOUNT
                },
                timeout=120
            )

            with lock:
                if r.status_code == 200:
                    stats["ok"] += 1
                else:
                    stats["errors"] += 1
                    print("HTTP ERROR:", r.status_code, r.text)

        except Exception as e:
            with lock:
                stats["errors"] += 1
            print("REQUEST ERROR:", e)


def get_balance(user_id):
    try:
        r = requests.get(
            f"{FACADE}/balance/{user_id}",
            timeout=30
        )

        if r.status_code == 200:
            return r.json()["balance"]

        print("Get balance error:", r.status_code, r.text)
        return None

    except Exception as e:
        print("Get balance exception:", e)
        return None

def get_queue_size():
    try:
        r = requests.get(f"{FACADE}/queue/size", timeout=30)
        return r.json()["queue_size"]
    except Exception as e:
        print("Queue size error:", e)
        return None

def wait_for_balance(user_id, expected, timeout=600):
    start = time.perf_counter()

    while time.perf_counter() - start < timeout:
        balance = get_balance(user_id)
        qsize = get_queue_size()

        if balance == expected:
            return balance

        print(f"waiting {user_id}: current={balance}, expected={expected}, queue={qsize}")
        time.sleep(5)

    return get_balance(user_id)


def print_metrics(total_time, stats):
    total_attempts = CLIENTS * REQUESTS_PER_CLIENT

    print("\n--- Results ---")
    print("Total attempts:", total_attempts)
    print("Successful requests:", stats["ok"])
    print("Failed requests:", stats["errors"])
    print("Total time:", round(total_time, 2), "sec")

    if total_time > 0:
        print("Requests/sec by attempts:", round(total_attempts / total_time, 2))
        print("Requests/sec successful:", round(stats["ok"] / total_time, 2))

    try:
        metrics = requests.get(
            f"{FACADE}/metrics",
            timeout=60
        ).json()

        print("logging-service contribution:", round(metrics.get("logging_time_total", 0), 2))
        print("counter queue contribution:", round(metrics.get("counter_queue_time_total", 0), 2))

    except Exception as e:
        print("Metrics error:", e)


def scenario_separate_accounts():
    print("\n=== Scenario 1: 10 accounts ===")

    reset_system()

    for i in range(CLIENTS):
        create_user(f"user{i}")

    stats = {"ok": 0, "errors": 0}
    lock = threading.Lock()
    threads = []

    start = time.perf_counter()

    for i in range(CLIENTS):
        t = threading.Thread(
            target=worker,
            args=(f"user{i}", stats, lock)
        )

        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total_time = time.perf_counter() - start

    print_metrics(total_time, stats)

    print("\nBalances:")
    for i in range(CLIENTS):
        balance = wait_for_balance(f"user{i}", REQUESTS_PER_CLIENT, timeout=600)
        print(f"user{i}: {balance}")


def scenario_shared_account():
    print("\n=== Scenario 2: 1 account ===")

    reset_system()
    create_user("shared_user")

    stats = {"ok": 0, "errors": 0}
    lock = threading.Lock()
    threads = []

    start = time.perf_counter()

    for _ in range(CLIENTS):
        t = threading.Thread(
            target=worker,
            args=("shared_user", stats, lock)
        )

        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total_time = time.perf_counter() - start

    print_metrics(total_time, stats)

    expected = CLIENTS * REQUESTS_PER_CLIENT
    balance = wait_for_balance("shared_user", expected, timeout=300)

    print("\nshared_user balance:", balance)


if __name__ == "__main__":
    #scenario_separate_accounts()
    scenario_shared_account()