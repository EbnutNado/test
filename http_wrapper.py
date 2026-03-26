# http_wrapper.py
import asyncio
import sys
import os

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(__file__))

# Импортируем твой веб-сервер
from webapp import main

if __name__ == "__main__":
    asyncio.run(main())
