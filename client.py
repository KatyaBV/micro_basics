import threading
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FACADE = "http://localhost:8000"

CLIENTS = 10
REQUESTS_PER_CLIENT = 10000



# HTTP session with retries

def create_session():

    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[500,502,503,504],
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=100,
        pool_maxsize=100
    )

    session.mount("http://", adapter)

    return session



# create user

def create_user(user):

    requests.post(
        f"{FACADE}/users",
        json={"user_id":user,"initial_balance":0}
    )



# client worker

def worker(user):

    session = create_session()

    for _ in range(REQUESTS_PER_CLIENT):

        session.post(
            f"{FACADE}/transaction",
            json={"user_id":user,"amount":1},
            timeout=5
        )



# scenario 1

def scenario_separate_accounts():

    print("\nSCENARIO 1")

    requests.post(f"{FACADE}/metrics/reset")

    for i in range(CLIENTS):
        create_user(f"user{i}")

    threads = []

    start = time.perf_counter()

    for i in range(CLIENTS):

        t = threading.Thread(
            target=worker,
            args=(f"user{i}",)
        )

        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end = time.perf_counter()

    total_requests = CLIENTS * REQUESTS_PER_CLIENT
    total_time = end - start

    print("Total requests:", total_requests)
    print("Total time:", round(total_time,2))
    print("Requests/sec:", round(total_requests/total_time,2))

    for i in range(CLIENTS):

        r = requests.get(f"{FACADE}/user/user{i}")
        print("user",i,"balance:",r.json()["balance"])

    metrics = requests.get(f"{FACADE}/metrics").json()

    print("logging_time_total:",metrics["logging_time_total"])
    print("counter_time_total:",metrics["counter_time_total"])


# scenario 2

def scenario_shared_account():

    print("\nSCENARIO 2")

    requests.post(f"{FACADE}/metrics/reset")

    create_user("shared")

    threads = []

    start = time.perf_counter()

    for _ in range(CLIENTS):

        t = threading.Thread(
            target=worker,
            args=("shared",)
        )

        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end = time.perf_counter()

    total_requests = CLIENTS * REQUESTS_PER_CLIENT
    total_time = end - start

    print("Total requests:", total_requests)
    print("Total time:", round(total_time,2))
    print("Requests/sec:", round(total_requests/total_time,2))

    r = requests.get(f"{FACADE}/user/shared")
    print("shared balance:",r.json()["balance"])

    metrics = requests.get(f"{FACADE}/metrics").json()

    print("logging_time_total:",metrics["logging_time_total"])
    print("counter_time_total:",metrics["counter_time_total"])


#choose the scenario
#scenario_separate_accounts()
scenario_shared_account()