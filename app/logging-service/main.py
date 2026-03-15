from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict
import time

app = FastAPI()

class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float

transactions: Dict[str, Transaction] = {}

@app.post("/log")
async def log_transaction(tx: Transaction):
    transactions[tx.transaction_id] = tx
    return {"status": "logged"}

@app.get("/transactions/{user_id}")
async def get_user_transactions(user_id: str):
    return [
        tx for tx in transactions.values()
        if tx.user_id == user_id
    ]

