# start.py
import asyncio
import subprocess
import sys
import os
import signal
import time

async def run_bot():
    """Запускает основного бота"""
    print("🚀 Запускаем основного бота...")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "bot.py",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Читаем логи в реальном времени
    async def read_output(pipe, prefix):
        while True:
            line = await pipe.readline()
            if not line:
                break
            print(f"{prefix}: {line.decode().strip()}")

    await asyncio.gather(
        read_output(proc.stdout, "[BOT]"),
        read_output(proc.stderr, "[BOT-ERR]"),
        proc.wait()
    )

async def run_webapp():
    """Запускает веб-сервер"""
    print("🌐 Запускаем веб-сервер...")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "webapp.py",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    async def read_output(pipe, prefix):
        while True:
            line = await pipe.readline()
            if not line:
                break
            print(f"{prefix}: {line.decode().strip()}")

    await asyncio.gather(
        read_output(proc.stdout, "[WEB]"),
        read_output(proc.stderr, "[WEB-ERR]"),
        proc.wait()
    )

async def main():
    print("=" * 50)
    print("🔥 Запускаем Vitalik Bot + Sweet Bonanza WebApp")
    print("=" * 50)
    
    # Запускаем оба процесса параллельно
    await asyncio.gather(
        run_bot(),
        run_webapp()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановка всех процессов...")
