# webapp.py
from aiohttp import web
import aiosqlite
import json
import asyncio
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_NAME = "vitalik_bot_final.db"

async def get_user_balance(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT balance FROM players WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row[0] if row else None

async def update_balance_and_log(user_id: int, amount: int, txn_type: str, description: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE players SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, ?, ?, ?)",
            (user_id, txn_type, amount, description)
        )
        await db.commit()

async def api_balance(request):
    user_id = request.query.get('user_id')
    if not user_id:
        return web.json_response({"error": "No user_id"}, status=400)
    try:
        balance = await get_user_balance(int(user_id))
    except Exception as e:
        logger.error(f"Error getting balance for {user_id}: {e}")
        return web.json_response({"error": "Invalid user_id"}, status=400)
    if balance is None:
        return web.json_response({"error": "User not found"}, status=404)
    return web.json_response({"balance": balance})

async def api_save_spin(request):
    try:
        data = await request.json()
    except Exception as e:
        logger.error(f"Invalid JSON: {e}")
        return web.json_response({"error": "Invalid JSON"}, status=400)
    
    user_id = data.get('user_id')
    win = data.get('win', 0)
    bet = data.get('bet', 0)
    
    if not user_id:
        return web.json_response({"error": "No user_id"}, status=400)
    
    try:
        if win > 0:
            await update_balance_and_log(int(user_id), win, "sweet_bonanza", f"Выигрыш в Sweet Bonanza: {win}₽")
            logger.info(f"User {user_id} won {win} in Sweet Bonanza")
        else:
            await update_balance_and_log(int(user_id), -bet, "sweet_bonanza_bet", f"Ставка в Sweet Bonanza: {bet}₽")
            logger.info(f"User {user_id} bet {bet} in Sweet Bonanza")
    except Exception as e:
        logger.error(f"Error saving spin for {user_id}: {e}")
        return web.json_response({"error": "Database error"}, status=500)
    
    return web.json_response({"success": True})

async def main():
    app = web.Application()
    app.router.add_get('/api/balance', api_balance)
    app.router.add_post('/api/save_spin', api_save_spin)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 3000)
    await site.start()
    logger.info("✅ Sweet Bonanza веб-сервер запущен на порту 3000")
    
    # Бесконечно ждём
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
