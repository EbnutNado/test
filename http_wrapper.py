# http_wrapper.py
import asyncio
import sys
import os

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(__file__))

# Импортируем бота
from bot import dp, bot, on_startup, on_shutdown

async def main():
    # Запускаем бота
    await on_startup()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
