import threading
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FACADE = "http://localhost:8000"

CLIENTS = 10
REQUESTS_PER_CLIENT = 10000
AMOUNT = 1


def create_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=0.2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=50,
        pool_maxsize=50
    )

    session.mount("http://", adapter)
    return session


def reset_system():
    try:
        requests.post(f"{FACADE}/reset", timeout=180)
    except Exception as e:
        print("Reset error:", e)


def create_user(user_id):
    r = requests.post(
        f"{FACADE}/users",
        json={"user_id": user_id, "initial_balance": 0},
        timeout=180
    )

    if r.status_code not in (200, 201):
        print("Create user error:", r.status_code, r.text)


def send_transactions(user_id, stats, lock):
    session = create_session()

    for _ in range(REQUESTS_PER_CLIENT):
        try:
            r = session.post(
                f"{FACADE}/transaction",
                json={"user_id": user_id, "amount": AMOUNT},
                timeout=180
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
        r = requests.get(f"{FACADE}/user/{user_id}", timeout=180)
        if r.status_code == 200:
            return r.json()["balance"]
        print("Get balance error:", r.status_code, r.text)
        return None
    except Exception as e:
        print("Get balance exception:", e)
        return None


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
        metrics = requests.get(f"{FACADE}/metrics", timeout=180).json()

        logging_time = metrics.get("logging_time_total", 0)
        counter_time = metrics.get("counter_time_total", 0)

        print("logging-service contribution:", round(logging_time, 2), "sec")
        print("counter-service contribution:", round(counter_time, 2), "sec")

        if total_time > 0:
            print("logging-service %:", round(logging_time / total_time * 100, 2))
            print("counter-service %:", round(counter_time / total_time * 100, 2))

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
            target=send_transactions,
            args=(f"user{i}", stats, lock)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end = time.perf_counter()
    total_time = end - start

    print_metrics(total_time, stats)

    print("\nBalances:")
    for i in range(CLIENTS):
        balance = get_balance(f"user{i}")
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
            target=send_transactions,
            args=("shared_user", stats, lock)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end = time.perf_counter()
    total_time = end - start

    print_metrics(total_time, stats)

    balance = get_balance("shared_user")
    print("\nshared_user balance:", balance)


if __name__ == "__main__":
    #scenario_separate_accounts()
    scenario_shared_account()