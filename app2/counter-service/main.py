from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg
import os
import time

app = FastAPI()

db_pool = None


class UserCreate(BaseModel):
    user_id: str
    initial_balance: float = 0


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float


@app.on_event("startup")
async def startup():
    global db_pool

    time.sleep(5)

    db_pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "bankdb"),
        user=os.getenv("DB_USER", "bankuser"),
        password=os.getenv("DB_PASSWORD", "bankpass"),
        min_size=10,
        max_size=50
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                user_id TEXT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS counter_transactions (
                transaction_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                timestamp DOUBLE PRECISION NOT NULL
            );
        """)

    print("[counter-service] connected to PostgreSQL")


@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()


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


@app.post("/update")
async def update_balance(tx: Transaction):
    async with db_pool.acquire() as conn:
        async with conn.transaction():

            await conn.execute("""
                INSERT INTO counter_transactions(transaction_id, user_id, amount, timestamp)
                VALUES($1, $2, $3, $4)
                ON CONFLICT (transaction_id) DO NOTHING
            """, tx.transaction_id, tx.user_id, tx.amount, tx.timestamp)

            row = await conn.fetchrow("""
                UPDATE accounts
                SET balance = balance + $1
                WHERE user_id = $2
                RETURNING balance
            """, tx.amount, tx.user_id)

            if row is None:
                raise HTTPException(status_code=404, detail="User not found")

            return {"balance": row["balance"]}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance FROM accounts WHERE user_id=$1",
            user_id
        )

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "balance": row["balance"]
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
        await conn.execute("DELETE FROM counter_transactions")
        await conn.execute("DELETE FROM accounts")

    return {"status": "reset"}