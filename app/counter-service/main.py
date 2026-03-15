from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
import asyncio

app = FastAPI()

class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float

balances: Dict[str, float] = {}
lock = asyncio.Lock()

@app.post("/users")
async def create_user(user: dict):

    async with lock:
        user_id = user["user_id"]

        if user_id in balances:
            raise HTTPException(status_code=400, detail="User exists")

        balances[user_id] = user.get("initial_balance", 0)

    return {"user_id": user_id, "balance": balances[user_id]}

@app.post("/update")
async def update_balance(tx: Transaction):

    async with lock:
        if tx.user_id not in balances:
            raise HTTPException(status_code=404, detail="User not found")

        balances[tx.user_id] += tx.amount
        balance = balances[tx.user_id]

    return {"balance": balance}

@app.get("/balance/{user_id}")
async def get_balance(user_id: str):

    if user_id not in balances:
        raise HTTPException(status_code=404)

    return {"balance": balances[user_id]}

@app.get("/balances")
async def get_all_balances():
    return balances
