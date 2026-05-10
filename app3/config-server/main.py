from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, List

app = FastAPI()

registry: Dict[str, List[str]] = {}


class ServiceRegister(BaseModel):
    name: str
    url: str


@app.post("/register")
def register(service: ServiceRegister):
    if service.name not in registry:
        registry[service.name] = []

    if service.url not in registry[service.name]:
        registry[service.name].append(service.url)

    return {
        "status": "registered",
        "name": service.name,
        "url": service.url,
        "registry": registry
    }


@app.get("/services/{name}")
def get_service(name: str):
    return {
        "name": name,
        "urls": registry.get(name, [])
    }


@app.get("/services")
def get_all_services():
    return registry


@app.delete("/services/{name}")
def clear_service(name: str):
    registry.pop(name, None)
    return {"status": "deleted", "name": name}


@app.get("/health")
def health():
    return {"status": "ok"}