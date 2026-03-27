"""
Telegram бот "Виталик Штрафующий"
✅ Чеки исправлены | ✅ Дуэль без ухода в минус | ✅ Нагирт ужесточён
✅ БИЗНЕС-СИСТЕМА: таймер сбора, сумма дохода, кулдаун 1 час
"""

import asyncio
import logging
import os
import sqlite3
import random
import string
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Union, Tuple
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import CommandStart, StateFilter
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import aiosqlite

# ==================== КОНФИГУРАЦИЯ ====================
# Токен лучше задать в переменной окружения BOT_TOKEN (не хранить в коде в продакшене).
BOT_TOKEN = os.getenv("BOT_TOKEN", "8611222074:AAHYK7C9Y25pxAoxkOC1jUb4Zo8spXoMrpU")
ADMIN_ID = int(os.getenv("ADMIN_ID", "5775839902"))
# Telegram username бота для генерации ссылок вида https://t.me/<bot_username>?start=...
# Если не задан — будет использован bot.get_me().username.
# Формат допустим любой: "my_bot" или "@my_bot".
BOT_USERNAME_MANUAL = "terekonik22_bot"  # впишите сюда "my_bot" (или "@my_bot"), если хотите задавать username прямо в коде
BOT_USERNAME = (os.getenv("BOT_USERNAME", "").strip().lstrip("@") or BOT_USERNAME_MANUAL.strip().lstrip("@"))
# Коррекции username по умолчанию включены (на случай несовпадения "username" и отображаемого t.me).
ENABLE_USERNAME_CORRECTIONS = os.getenv("ENABLE_USERNAME_CORRECTIONS", "1").strip().lower() in ("1", "true", "yes", "on")
# Хроника: задай CHRONICLE_CHANNEL_ID=-100... (число) ИЛИ CHRONICLE_CHANNEL_USERNAME=mychannel (без @).
# Бот должен быть администратором канала с правом публикации.
def _load_chronicle_config() -> tuple:
    raw_id = os.getenv("CHRONICLE_CHANNEL_ID", "").strip()
    raw_user = os.getenv("CHRONICLE_CHANNEL_USERNAME", "").strip().lstrip("@")
    cid: Optional[int] = None
    if raw_id:
        try:
            cid = int(raw_id)
        except ValueError:
            logger_init = logging.getLogger(__name__)
            logger_init.warning(
                "CHRONICLE_CHANNEL_ID должен быть целым числом (например -1001234567890). "
                "Используй CHRONICLE_CHANNEL_USERNAME для публичного канала."
            )
    return cid, (raw_user or None)


CHRONICLE_CHANNEL_ID, CHRONICLE_CHANNEL_USERNAME = _load_chronicle_config()
_chronicle_resolved_id: Optional[int] = None
_TRANSIENT_BET_TXN_TYPES = frozenset({
    "roulette_bet",
    "dice_bet",
    "duel_bet",
})


def _load_subscribe_config() -> tuple:
    """
    Канал для обязательной подписки.
    Параметры:
    - SUBSCRIBE_CHANNEL_ID (например -1001234567890)
    - SUBSCRIBE_CHANNEL_USERNAME (например mychannel или @mychannel)
    """
    raw_id = os.getenv("SUBSCRIBE_CHANNEL_ID", "").strip()
    raw_user = os.getenv("SUBSCRIBE_CHANNEL_USERNAME", "").strip().lstrip("@")
    cid: Optional[int] = None
    if raw_id:
        try:
            cid = int(raw_id)
        except ValueError:
            cid = None
    return cid, (raw_user or None)


SUBSCRIBE_CHANNEL_ID, SUBSCRIBE_CHANNEL_USERNAME = _load_subscribe_config()
_subscribe_resolved_id: Optional[int] = None
MSK = ZoneInfo("Europe/Moscow")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Чтобы публикации в TG-канал шли в корректной хронологии (без “перемешивания” из-за параллельных задач).
chronicle_send_lock = asyncio.Lock()

# Подсказка без слэш-команд: регистрация только через кнопку Start у Telegram
NOT_REGISTERED_HINT = (
    "Сначала зарегистрируйся: нажми «Start» / «Запустить» под строкой ввода внизу экрана."
)
NOT_REGISTERED_ALERT = "Сначала зарегистрируйся (кнопка Start внизу)."

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Нормализация username для ссылок.
# Основной источник истины — BOT_USERNAME из окружения.
# Если не задан — используем bot.get_me().username и применяем небольшой fallback.
_USERNAME_CORRECTIONS = {
    "terekonik22bot": "terekonik22_bot",
}

async def get_bot_username_for_tme_links() -> Optional[str]:
    """
    Возвращает Telegram username бота (без '@') для генерации ссылок вида t.me/<username>.
    """
    if BOT_USERNAME:
        return BOT_USERNAME
    bot_info = await bot.get_me()
    bot_username = (bot_info.username or "").strip().lstrip("@")
    if not bot_username:
        return None
    if ENABLE_USERNAME_CORRECTIONS:
        return _USERNAME_CORRECTIONS.get(bot_username, bot_username)
    return bot_username


async def _get_subscribe_chat_id() -> Optional[int]:
    global _subscribe_resolved_id
    if SUBSCRIBE_CHANNEL_ID is not None:
        return SUBSCRIBE_CHANNEL_ID
    if not SUBSCRIBE_CHANNEL_USERNAME:
        return None
    if _subscribe_resolved_id is not None:
        return _subscribe_resolved_id
    try:
        ch = await bot.get_chat(f"@{SUBSCRIBE_CHANNEL_USERNAME}")
        _subscribe_resolved_id = ch.id
        return ch.id
    except Exception as e:
        logger.error(f"SUBSCRIBE: не удалось определить chat_id для @%s: %s", SUBSCRIBE_CHANNEL_USERNAME, e)
        return None


async def is_user_subscribed(user_id: int) -> bool:
    """
    Возвращает True, если пользователь подписан на обязательный канал.
    Если канал не настроен — всегда True.
    """
    chat_id = await _get_subscribe_chat_id()
    if chat_id is None:
        return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status not in ("left", "kicked")
    except Exception as e:
        logger.error(f"SUBSCRIBE: ошибка get_chat_member для {user_id}: {e}")
        # Строгое правило: если проверку выполнить не удалось — считаем, что подписки нет.
        return False


class RequireSubscriptionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)

        # Разрешаем /start и ручную проверку подписки.
        if isinstance(event, Message):
            txt = (event.text or "").strip().lower()
            if txt.startswith("/start"):
                return await handler(event, data)
        elif isinstance(event, CallbackQuery):
            cb = (event.data or "").strip()
            if cb == "sub_check":
                return await handler(event, data)

        if await is_user_subscribed(user.id):
            return await handler(event, data)

        if isinstance(event, Message):
            await event.answer(
                "Чтобы пользоваться ботом, подпишись на канал и подтверди подписку.",
                reply_markup=get_subscribe_keyboard(),
            )
            return

        if isinstance(event, CallbackQuery):
            try:
                await event.message.answer(
                    "Доступ ограничен: подпишись на канал и нажми «Проверить».",
                    reply_markup=get_subscribe_keyboard(),
                )
            except Exception:
                pass
            await event.answer("Нужна подписка на канал", show_alert=True)
            return

        return await handler(event, data)


dp.message.outer_middleware(RequireSubscriptionMiddleware())
dp.callback_query.outer_middleware(RequireSubscriptionMiddleware())


def get_subscribe_keyboard() -> InlineKeyboardMarkup:
    subscribe_url = None
    if SUBSCRIBE_CHANNEL_USERNAME:
        subscribe_url = f"https://t.me/{SUBSCRIBE_CHANNEL_USERNAME}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подписаться", url=subscribe_url if subscribe_url else "https://t.me/")],
        [InlineKeyboardButton(text="🔄 Проверить", callback_data="sub_check")],
    ])
    return kb

# ==================== УВЕДОМЛЕНИЯ О БИЗНЕСЕ ====================
last_business_notification = {}  # {user_id: timestamp последнего уведомления}
BUSINESS_NOTIFICATION_COOLDOWN = 3000  # 50 минут в секундах (чтобы не спамить)

# ==================== НАСТРОЙКИ ЭКОНОМИКИ ====================
ECONOMY_SETTINGS = {
    "start_balance": 5000,
    # Зарплата: чуть реалистичнее разброс и реже, но крупнее базовая вилка
    "salary_min": 1100,
    "salary_max": 4800,
    "salary_interval": 360,
    "fine_chance": 0.40,
    "random_fine_min": 450,
    "random_fine_max": 3200,
    "asphalt_earnings": 45,
    "asphalt_fine_min": 180,
    "asphalt_fine_max": 750,
    # Кости: чёт / нечёт (кубик 1–6, угадал → ×2)
    "dice_min_bet": 100,
    "dice_max_bet": 5000,
    # Рулетка: ставка, шанс победы и выплата ×2
    "roulette_min_bet": 100,
    "roulette_max_bet": 5000,
    "roulette_win_chance": 0.42,
    "min_transfer": 100,
    "random_fine_interval_min": 900,
    "random_fine_interval_max": 2400,
    "duel_min_bet": 200,
    "duel_max_bet": 10000,
    "duel_dice_sides": 6,
    # Топ заработка за вчера (МСК), выплата ежедневно в 10:00
    "daily_top_reward_1": 35000,
    "daily_top_reward_2": 20000,
    "daily_top_reward_3": 12000,
    "inventory_base_slots": 20,
    ECONOMY_SETTINGS = {
    # ... существующие настройки ...
    
    "mines_min_bet": 100,
    "mines_max_bet": 10000,
    "mines_field_size": 25,
    "mines_multipliers": {
        3: [1.0, 1.2, 1.5, 1.9, 2.4, 3.0, 3.8, 4.8, 6.0, 7.5, 9.5, 12.0, 15.0, 18.5, 23.0, 28.0, 34.0, 41.0, 49.0, 58.0, 68.0, 79.0, 91.0, 104.0, 118.0],
        5: [1.0, 1.3, 1.7, 2.3, 3.1, 4.1, 5.5, 7.3, 9.7, 12.9, 17.0, 22.5, 29.5, 38.5, 50.0, 65.0, 84.0, 109.0, 141.0, 183.0, 237.0, 308.0, 400.0, 520.0, 676.0],
        8: [1.0, 1.5, 2.3, 3.5, 5.3, 8.0, 12.0, 18.0, 27.0, 40.5, 60.7, 91.0, 136.5, 204.7, 307.0, 460.5, 690.7, 1036.0, 1554.0, 2331.0, 3496.5, 5244.7, 7867.0, 11800.5, 17700.7],
        12: [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0, 131072.0, 262144.0, 524288.0, 1048576.0, 2097152.0, 4194304.0, 8388608.0, 16777216.0],
    },
}

# ==================== МЕХАНИКИ БИЗНЕСОВ ====================
# Цены на покупку бизнесов снизить примерно в 1.5 раза.
BUSINESS_PRICE_DIVISOR: float = 1.5

# ==================== СОЦИАЛЬНЫЕ / РЕФЕРАЛЬНЫЕ СИСТЕМЫ ====================
REFERRAL_ACTIONS_MIN: int = 3  # "активный новичок" (облегчённый порог)
REFERRAL_DELAY_HOURS: int = 0  # начисление после выполнения условий (без ожидания)

# ==================== УВЕДОМЛЕНИЯ О "БЕДНОСТИ" ====================
# Если баланс упал ниже порога (и при этом был выше до операции) — отправляем событие в хроннику.
POOR_BALANCE_THRESHOLD: int = 2700
EXTREME_POOR_BALANCE_THRESHOLD: int = 500
POOR_ALERT_COOLDOWN_SEC: int = 6 * 3600  # защита от спама (раз в 6 часов максимум)

# Типы начислений, которые идут в «заработок за день» (МСК)
DAILY_EARN_TXN_TYPES = frozenset({
    "salary", "business_income", "asphalt", "dice_even_win", "duel_win",
    "roulette_win", "bank_loan", "bonus", "check", "instant", "lottery_shop", "bank_deposit_interest",
})

# ==================== БАНК «АСФАЛЬТ-КАПИТАЛ» (кредиты, %% , коллекторы) ====================
BANK_SETTINGS = {
    "name": "Асфальт-Капитал",
    "min_loan": 2_000,
    "max_loan": 200_000,
    "hourly_interest_rate": 0.004,  # 0.4% в час
    "term_hours": 48,  # после срока — режим коллекторов
    # За начислениями следим раз в час.
    "accrual_interval_sec": 3600,
    "collector_interval_sec": 3600,  # коллекторы "в час"
    # Коллекторы списывают 10-20% баланса за визит.
    "collector_seize_balance_min_pct": 0.10,
    "collector_seize_balance_max_pct": 0.20,
    "collector_min_seize": 1,
    "salary_garnish_if_defaulted": 0.25,  # с получки удерживается в погашение долга
    # Казна всегда полна (логически). Начальный "пул" оставляем под 10 000 000.
    "initial_pool_liquidity": int(os.getenv("BANK_INITIAL_POOL", "10000000")),
    "min_deposit": 5_000,
    "deposit_hourly_rate": 0.002,  # 0.2% в час
    "deposit_interest_interval_sec": 3600,
}

# ==================== ТОВАРЫ МАГАЗИНА ====================
SHOP_ITEMS = [
    {"id": "bonus_coin", "name": "🪙 Бонусная монета", "price": 1500,
     "description": "+15% к получке на 8 часов", "type": "boost", "category": "boosts",
     "value": 0.15, "hours": 8},
    {"id": "premium_boost", "name": "🚀 Премиум-Буст", "price": 5000,
     "description": "+30% к получке на 24 часа", "type": "boost", "category": "boosts",
     "value": 0.3, "hours": 24},
    {"id": "mega_boost", "name": "💎 Мега-Буст", "price": 15000,
     "description": "+50% к получке на 3 дня", "type": "boost", "category": "boosts",
     "value": 0.5, "hours": 72},
    {"id": "day_off", "name": "🎉 Выходной", "price": 3000,
     "description": "Полный иммунитет к штрафам на 12 часов", "type": "protection", "category": "protection", "hours": 12},
    {"id": "insurance", "name": "🛡️ Страховка", "price": 4000,
     "description": "Страховка от одного штрафа (возмещает 80%)", "type": "insurance", "category": "protection"},

    {"id": "nagirt_light", "name": "💊 Нагирт Лайт", "price": 2000,
     "description": "+15% к зарплате, +20% к играм на 2 часа. Риск штрафа +10%",
     "type": "pill", "category": "nagirt",
     "effect_salary": 0.15, "effect_game": 0.2, "hours": 2,
     "side_effect_chance": 25, "fine_bonus": 0.1},
    {"id": "nagirt_pro", "name": "💊💊 Нагирт Про", "price": 5000,
     "description": "+30% к зарплате, +40% к играм на 4 часа. Риск штрафа +25%",
     "type": "pill", "category": "nagirt",
     "effect_salary": 0.30, "effect_game": 0.4, "hours": 4,
     "side_effect_chance": 50, "fine_bonus": 0.25},
    {"id": "nagirt_extreme", "name": "💊💊💊 Нагирт Экстрим", "price": 12000,
     "description": "+50% к зарплате, +70% к играм на 6 часов. Риск штрафа +40%",
     "type": "pill", "category": "nagirt",
     "effect_salary": 0.50, "effect_game": 0.7, "hours": 6,
     "side_effect_chance": 75, "fine_bonus": 0.4},
    {"id": "antidote", "name": "💉 Антидот", "price": 2500,
     "description": "Снимает побочки и сбрасывает толерантность", "type": "antidote", "category": "misc"},
    {"id": "lottery_ticket", "name": "🎫 Лотерейный билет", "price": 1000,
     "description": "Шанс выиграть до 10000₽!", "type": "lottery", "category": "misc"},
    {"id": "instant_salary", "name": "⏱️ Мгновенная получка", "price": 8000,
     "description": "Сразу получаешь зарплату без ожидания", "type": "instant", "category": "misc"},
]

# ==================== БИЗНЕСЫ ====================
BUSINESS_TYPES = {
    "chair": {
        "name": "🪑 Офисное кресло",
        "description": "Мягкое, с подлокотниками. Начальник оценит.",
        "price": 3000,
        "base_income": 30,
        "salary_bonus": 0.01,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.0,
        "max_level": 3,
        "upgrades": {
            1: {"name": "Газлифт", "cost": 2000, "income_bonus": 10, "desc": "+10₽/ч"},
            2: {"name": "Массажная спинка", "cost": 5000, "income_bonus": 20, "desc": "+20₽/ч"},
            3: {"name": "Кожаная обивка", "cost": 8000, "income_bonus": 30, "salary_bonus": 0.005, "desc": "+30₽/ч, +0.5% зарплата"}
        }
    },
    "pc": {
        "name": "💻 Игровой ПК",
        "description": "RTX 5090, Intel i9. Для работы, конечно.",
        "price": 10000,
        "base_income": 150,
        "salary_bonus": 0.02,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.0,
        "max_level": 5,
        "upgrades": {
            1: {"name": "SSD на 1TB", "cost": 3000, "income_bonus": 40, "desc": "+40₽/ч"},
            2: {"name": "Механическая клавиатура", "cost": 5000, "income_bonus": 60, "desc": "+60₽/ч"},
            3: {"name": "Жидкостное охлаждение", "cost": 8000, "income_bonus": 80, "desc": "+80₽/ч"},
            4: {"name": "RGB подсветка", "cost": 3000, "income_bonus": 20, "salary_bonus": 0.005, "desc": "+20₽/ч, +0.5% ЗП"},
            5: {"name": "VR-шлем", "cost": 15000, "income_bonus": 120, "desc": "+120₽/ч"}
        }
    },
    "vending": {
        "name": "☕ Вендинговый аппарат",
        "description": "Кофе, снэки, доширак. Весь офис твой должник.",
        "price": 25000,
        "base_income": 400,
        "salary_bonus": 0.03,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.0,
        "max_level": 4,
        "upgrades": {
            1: {"name": "Кофе-машина", "cost": 8000, "income_bonus": 100, "desc": "+100₽/ч"},
            2: {"name": "Снэк-стеллаж", "cost": 12000, "income_bonus": 150, "desc": "+150₽/ч"},
            3: {"name": "Платежный терминал", "cost": 15000, "income_bonus": 200, "salary_bonus": 0.01, "desc": "+200₽/ч, +1% ЗП"},
            4: {"name": "Холодильная камера", "cost": 20000, "income_bonus": 250, "desc": "+250₽/ч"}
        }
    },
    "kiosk": {
        "name": "🏪 Ларёк у дома",
        "description": "Пиво, семечки, сим-карты. Торгуй, пока Виталик не пришёл.",
        "price": 60000,
        "base_income": 1200,
        "salary_bonus": 0.05,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.0,
        "max_level": 5,
        "upgrades": {
            1: {"name": "Вывеска", "cost": 15000, "income_bonus": 300, "desc": "+300₽/ч"},
            2: {"name": "Охрана", "cost": 25000, "income_bonus": 450, "desc": "+450₽/ч"},
            3: {"name": "Разливное пиво", "cost": 35000, "income_bonus": 600, "salary_bonus": 0.015, "desc": "+600₽/ч, +1.5% ЗП"},
            4: {"name": "Терминал оплаты", "cost": 20000, "income_bonus": 350, "desc": "+350₽/ч"},
            5: {"name": "Франшиза", "cost": 50000, "income_bonus": 800, "salary_bonus": 0.02, "desc": "+800₽/ч, +2% ЗП"}
        }
    },
    "truck": {
        "name": "🚛 Грузовой транспорт",
        "description": "Газель, рефрижератор, права с открытой категорией.",
        "price": 120000,
        "base_income": 2500,
        "salary_bonus": 0.07,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.10,
        "max_level": 5,
        "upgrades": {
            1: {"name": "Новые шины", "cost": 25000, "income_bonus": 500, "asphalt_bonus": 0.02, "desc": "+500₽/ч, +2% асфальт"},
            2: {"name": "Тахограф", "cost": 30000, "income_bonus": 700, "desc": "+700₽/ч"},
            3: {"name": "Рефрижератор", "cost": 50000, "income_bonus": 1000, "asphalt_bonus": 0.03, "desc": "+1000₽/ч, +3% асфальт"},
            4: {"name": "GPS-навигатор", "cost": 20000, "income_bonus": 400, "asphalt_bonus": 0.02, "desc": "+400₽/ч, +2% асфальт"},
            5: {"name": "Автопарк +1", "cost": 80000, "income_bonus": 1500, "salary_bonus": 0.02, "desc": "+1500₽/ч, +2% ЗП"}
        }
    },
    "factory": {
        "name": "🏭 Мини-завод",
        "description": "Штампуй детали, печатай деньги.",
        "price": 300000,
        "base_income": 6000,
        "salary_bonus": 0.10,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.0,
        "max_level": 6,
        "upgrades": {
            1: {"name": "Автоматизация", "cost": 60000, "income_bonus": 1500, "desc": "+1500₽/ч"},
            2: {"name": "Роботизация", "cost": 90000, "income_bonus": 2000, "salary_bonus": 0.02, "desc": "+2000₽/ч, +2% ЗП"},
            3: {"name": "Склад", "cost": 70000, "income_bonus": 1800, "desc": "+1800₽/ч"},
            4: {"name": "Конвейер", "cost": 80000, "income_bonus": 2200, "salary_bonus": 0.02, "desc": "+2200₽/ч, +2% ЗП"},
            5: {"name": "ИИ-контроль", "cost": 120000, "income_bonus": 3000, "desc": "+3000₽/ч"},
            6: {"name": "Экспорт", "cost": 150000, "income_bonus": 4000, "salary_bonus": 0.03, "desc": "+4000₽/ч, +3% ЗП"}
        }
    },
    "office": {
        "name": "🏢 Бизнес-центр",
        "description": "Сдавай этажи, собирай аренду. Вершина карьеры.",
        "price": 1000000,
        "base_income": 20000,
        "salary_bonus": 0.15,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.05,
        "max_level": 8,
        "upgrades": {
            1: {"name": "Охрана", "cost": 150000, "income_bonus": 4000, "desc": "+4000₽/ч"},
            2: {"name": "IT-инфраструктура", "cost": 200000, "income_bonus": 6000, "salary_bonus": 0.02, "desc": "+6000₽/ч, +2% ЗП"},
            3: {"name": "Фитнес-зал", "cost": 180000, "income_bonus": 5000, "desc": "+5000₽/ч"},
            4: {"name": "Ресепшн", "cost": 120000, "income_bonus": 3500, "desc": "+3500₽/ч"},
            5: {"name": "Конференц-зал", "cost": 250000, "income_bonus": 7000, "salary_bonus": 0.03, "desc": "+7000₽/ч, +3% ЗП"},
            6: {"name": "Кафетерий", "cost": 180000, "income_bonus": 5500, "asphalt_bonus": 0.02, "desc": "+5500₽/ч, +2% асфальт"},
            7: {"name": "Панорамные лифты", "cost": 200000, "income_bonus": 6000, "desc": "+6000₽/ч"},
            8: {"name": "Корпоративный музей", "cost": 300000, "income_bonus": 9000, "salary_bonus": 0.04, "desc": "+9000₽/ч, +4% ЗП"}
        }
    },
    "crypto_farm": {
        "name": "₿ Майнинг-ферма в гараже",
        "description": "Видеокарты жрут свет, Виталик жрёт налоги. Классика.",
        "price": 450000,
        "base_income": 8500,
        "salary_bonus": 0.08,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.0,
        "max_level": 5,
        "upgrades": {
            1: {"name": "Обдув 24/7", "cost": 80000, "income_bonus": 2000, "desc": "+2000₽/ч"},
            2: {"name": "Дешёвое электричество", "cost": 120000, "income_bonus": 3200, "salary_bonus": 0.015, "desc": "+3200₽/ч, +1.5% ЗП"},
            3: {"name": "Стеллажи GPU", "cost": 95000, "income_bonus": 2500, "desc": "+2500₽/ч"},
            4: {"name": "VPN в «дружественную» юрисдикцию", "cost": 150000, "income_bonus": 4000, "desc": "+4000₽/ч"},
            5: {"name": "Собственный пул", "cost": 200000, "income_bonus": 5500, "salary_bonus": 0.025, "desc": "+5500₽/ч, +2.5% ЗП"},
        }
    },
    "taxi": {
        "name": "🚕 Такси-парк",
        "description": "Жёлтые шашечки, чёрная бухгалтерия.",
        "price": 85000,
        "base_income": 1600,
        "salary_bonus": 0.055,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.12,
        "max_level": 4,
        "upgrades": {
            1: {"name": "Газ на газ", "cost": 22000, "income_bonus": 400, "asphalt_bonus": 0.02, "desc": "+400₽/ч, +2% асфальт"},
            2: {"name": "Яндекс-интеграция", "cost": 35000, "income_bonus": 550, "desc": "+550₽/ч"},
            3: {"name": "Каршеринг как прикрытие", "cost": 48000, "income_bonus": 700, "salary_bonus": 0.01, "desc": "+700₽/ч, +1% ЗП"},
            4: {"name": "Ночные тарифы", "cost": 60000, "income_bonus": 900, "asphalt_bonus": 0.03, "desc": "+900₽/ч, +3% асфальт"},
        }
    },
    "carwash": {
        "name": "🚿 Автомойка самообслуживания",
        "description": "Пена, вода, кэш. Легальнее некуда.",
        "price": 180000,
        "base_income": 3200,
        "salary_bonus": 0.06,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.05,
        "max_level": 4,
        "upgrades": {
            1: {"name": "Пылесосы", "cost": 40000, "income_bonus": 800, "desc": "+800₽/ч"},
            2: {"name": "Кофе с собой", "cost": 55000, "income_bonus": 1100, "salary_bonus": 0.01, "desc": "+1100₽/ч, +1% ЗП"},
            3: {"name": "Химчистка салона", "cost": 70000, "income_bonus": 1400, "desc": "+1400₽/ч"},
            4: {"name": "Абонементы", "cost": 90000, "income_bonus": 1800, "asphalt_bonus": 0.02, "desc": "+1800₽/ч, +2% асфальт"},
        }
    },
    "darknet_shop": {
        "name": "🕶 Ларёк «с серой зоны»",
        "description": "Никто ничего не видел. Особенно налоговая.",
        "price": 220000,
        "base_income": 4500,
        "salary_bonus": 0.04,
        "duel_bonus": 0.0,
        "asphalt_bonus": 0.0,
        "max_level": 3,
        "upgrades": {
            1: {"name": "Курьеры на байках", "cost": 65000, "income_bonus": 1200, "desc": "+1200₽/ч"},
            2: {"name": "Крипта только USDT", "cost": 90000, "income_bonus": 1800, "salary_bonus": 0.02, "desc": "+1800₽/ч, +2% ЗП"},
            3: {"name": "Подставной директор", "cost": 120000, "income_bonus": 2500, "desc": "+2500₽/ч"},
        }
    },
}

# ==================== ДОСТИЖЕНИЯ ====================
ACHIEVEMENTS: Dict[str, Dict[str, Any]] = {
    "millionaire": {
        "name": "Миллионер", "emoji": "🥇",
        "desc": "Накопить ≥ 1 000 000₽ на балансе",
        "income_bonus": 0.05,
    },
    "duel_master": {
        "name": "Убийца", "emoji": "⚔️",
        "desc": "100 побед в дуэлях",
        "title": "Мясник",
        "shop_discount": 0.05,
    },
    "nagirt100": {
        "name": "Наркоман", "emoji": "💊",
        "desc": "Использовать 100 таблеток Нагирта",
        "title": "Химик",
        "nagirt_effect_bonus": 0.10,
    },
    "oligarch": {
        "name": "Олигарх", "emoji": "🏢",
        "desc": "Иметь ≥ 10 бизнесов одновременно",
        "title": "Мафиози",
        "inventory_slots": 1,
    },
    "kidala": {
        "name": "Кидала", "emoji": "💸",
        "desc": "5+ раз попасть в просрочку по кредиту",
        "title": "Мошенник",
        "transfer_extra_fee": 0.05,
    },
}

# ==================== БАЗА ДАННЫХ ====================
DB_NAME = "vitalik_bot_final.db"

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Существующие таблицы
        await db.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                balance INTEGER DEFAULT 5000,
                total_earned INTEGER DEFAULT 0,
                total_fines INTEGER DEFAULT 0,
                salary_count INTEGER DEFAULT 0,
                last_salary TIMESTAMP,
                last_penalty TIMESTAMP,
                last_asphalt TIMESTAMP,
                penalty_immunity_until TIMESTAMP,
                asphalt_meters INTEGER DEFAULT 0,
                asphalt_earned INTEGER DEFAULT 0,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                type TEXT,
                amount INTEGER,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                item_name TEXT,
                price INTEGER,
                purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS boosts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                boost_type TEXT,
                boost_value REAL,
                expires_at TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS nagirt_pills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                pill_type TEXT,
                effect_strength REAL,
                expires_at TIMESTAMP,
                side_effects TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS nagirt_tolerance (
                user_id INTEGER PRIMARY KEY,
                tolerance REAL DEFAULT 1.0,
                last_used TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS gift_checks (
                check_id TEXT PRIMARY KEY,
                creator_id INTEGER,
                check_type TEXT,
                amount INTEGER,
                item_id TEXT,
                max_uses INTEGER DEFAULT 1,
                used_count INTEGER DEFAULT 0,
                created_at TIMESTAMP,
                expires_at TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                custom_message TEXT,
                last_used TIMESTAMP,
                activations_list TEXT DEFAULT '[]'
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS check_activations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_id TEXT,
                user_id INTEGER,
                activated_at TIMESTAMP,
                received_amount INTEGER,
                received_item TEXT
            )
        ''')

        # НОВЫЕ ТАБЛИЦЫ ДЛЯ БИЗНЕСОВ
        await db.execute('''
            CREATE TABLE IF NOT EXISTS businesses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                biz_type TEXT NOT NULL,
                level INTEGER DEFAULT 1,
                upgrade_level INTEGER DEFAULT 0,
                base_income INTEGER NOT NULL,
                collect_cooldown TIMESTAMP,
                health INTEGER DEFAULT 100,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (owner_id) REFERENCES players(user_id) ON DELETE CASCADE
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS business_upgrades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_id INTEGER NOT NULL,
                upgrade_name TEXT NOT NULL,
                upgrade_level INTEGER NOT NULL,
                bonus_income INTEGER DEFAULT 0,
                bonus_percent REAL DEFAULT 0.0,
                cost INTEGER NOT NULL,
                purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (business_id) REFERENCES businesses(id) ON DELETE CASCADE
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS asphalt_loans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                principal INTEGER NOT NULL,
                remaining INTEGER NOT NULL,
                issued_at TEXT NOT NULL,
                due_at TEXT NOT NULL,
                last_accrual_at TEXT NOT NULL,
                defaulted INTEGER DEFAULT 0,
                paid_off INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES players(user_id) ON DELETE CASCADE
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS global_economy (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                fine_scale REAL DEFAULT 1.0,
                transfer_commission_pct REAL DEFAULT 0.02,
                business_tax_chance REAL DEFAULT 0.15,
                business_tax_take_pct REAL DEFAULT 0.30
            )
        ''')
        await db.execute(
            "INSERT OR IGNORE INTO global_economy (id, fine_scale, transfer_commission_pct, business_tax_chance, business_tax_take_pct) "
            "VALUES (1, 1.0, 0.02, 0.15, 0.30)"
        )
        await db.execute('''
            CREATE TABLE IF NOT EXISTS daily_earnings (
                user_id INTEGER NOT NULL,
                day TEXT NOT NULL,
                earned INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, day)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS daily_top_paid (
                day TEXT PRIMARY KEY,
                paid_at TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS player_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_id TEXT NOT NULL,
                quantity INTEGER DEFAULT 1,
                acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS player_achievements (
                user_id INTEGER NOT NULL,
                achievement_id TEXT NOT NULL,
                unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, achievement_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS reputation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_user INTEGER NOT NULL,
                to_user INTEGER NOT NULL,
                stars INTEGER NOT NULL,
                context TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS bank_pool (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                liquidity INTEGER NOT NULL DEFAULT 0
            )
        ''')
        await db.execute(
            "INSERT OR IGNORE INTO bank_pool (id, liquidity) VALUES (1, ?)",
            (BANK_SETTINGS["initial_pool_liquidity"],),
        )
        await db.execute('''
            CREATE TABLE IF NOT EXISTS bank_deposits (
                user_id INTEGER PRIMARY KEY,
                amount INTEGER NOT NULL DEFAULT 0,
                total_interest INTEGER NOT NULL DEFAULT 0,
                last_interest_at TEXT,
                FOREIGN KEY (user_id) REFERENCES players(user_id)
            )
        ''')
        for alter in (
            "ALTER TABLE business_upgrades ADD COLUMN bonus_duel REAL DEFAULT 0",
            "ALTER TABLE business_upgrades ADD COLUMN bonus_asphalt REAL DEFAULT 0",
            "ALTER TABLE players ADD COLUMN duels_won INTEGER DEFAULT 0",
            "ALTER TABLE players ADD COLUMN loans_defaulted INTEGER DEFAULT 0",
            "ALTER TABLE players ADD COLUMN nagirt_uses INTEGER DEFAULT 0",
            "ALTER TABLE players ADD COLUMN rep_points INTEGER DEFAULT 0",
            "ALTER TABLE players ADD COLUMN rep_votes INTEGER DEFAULT 0",
            "ALTER TABLE bank_deposits ADD COLUMN total_interest INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE players ADD COLUMN poor_alerted_at TIMESTAMP",
            "ALTER TABLE players ADD COLUMN extreme_poor_alerted_at TIMESTAMP",
            "ALTER TABLE players ADD COLUMN mines_override REAL DEFAULT NULL",
            "ALTER TABLE players ADD COLUMN mines_override_active INTEGER DEFAULT 0",
        ):
            try:
                await db.execute(alter)
            except aiosqlite.OperationalError:
                pass
        await db.commit()
        logger.info("✅ База данных инициализирована (бизнесы, банк, инвентарь, топ дня, ачивки)")

        # ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================
        # Начисление бонусов происходит через 24 часа после регистрации,
        # и только если новичок активен (минимум REFERRAL_ACTIONS_MIN действий).
        await db.execute('''
            CREATE TABLE IF NOT EXISTS referral_invites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inviter_id INTEGER NOT NULL,
                invitee_id INTEGER NOT NULL UNIQUE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                credited_at TIMESTAMP,
                milestone INTEGER DEFAULT 0,
                reward_inviter INTEGER NOT NULL DEFAULT 0,
                reward_newcomer INTEGER NOT NULL DEFAULT 0
            )
        ''')
        await db.commit()


def moscow_date_str(when: Optional[datetime] = None) -> str:
    dt = when or datetime.now(MSK)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    else:
        dt = dt.astimezone(MSK)
    return dt.strftime("%Y-%m-%d")


async def get_global_economy() -> Dict[str, float]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM global_economy WHERE id = 1")
        row = await cur.fetchone()
        if not row:
            return {"fine_scale": 1.0, "transfer_commission_pct": 0.02, "business_tax_chance": 0.15, "business_tax_take_pct": 0.30}
        r = dict(row)
        return {
            "fine_scale": float(r["fine_scale"]),
            "transfer_commission_pct": float(r["transfer_commission_pct"]),
            "business_tax_chance": float(r["business_tax_chance"]),
            "business_tax_take_pct": float(r["business_tax_take_pct"]),
        }


async def adjust_global_economy(
    fine_scale_delta: float = 0.0,
    commission_delta: float = 0.0,
    tax_chance_delta: float = 0.0,
    tax_take_delta: float = 0.0,
) -> Dict[str, float]:
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """UPDATE global_economy SET
               fine_scale = MAX(0.3, MIN(3.0, fine_scale + ?)),
               transfer_commission_pct = MAX(0.0, MIN(0.25, transfer_commission_pct + ?)),
               business_tax_chance = MAX(0.05, MIN(0.55, business_tax_chance + ?)),
               business_tax_take_pct = MAX(0.1, MIN(0.7, business_tax_take_pct + ?))
               WHERE id = 1""",
            (fine_scale_delta, commission_delta, tax_chance_delta, tax_take_delta),
        )
        await db.commit()
    return await get_global_economy()


async def set_global_economy_param(param: str, value: float) -> Dict[str, float]:
    """Абсолютное (set) изменение параметров global_economy через ввод чисел в админке."""
    if param == "fine_scale":
        clamped = max(0.3, min(3.0, float(value)))
        col = "fine_scale"
    elif param == "transfer_commission_pct":
        clamped = max(0.0, min(0.25, float(value)))
        col = "transfer_commission_pct"
    elif param == "business_tax_chance":
        clamped = max(0.05, min(0.55, float(value)))
        col = "business_tax_chance"
    elif param == "business_tax_take_pct":
        clamped = max(0.1, min(0.7, float(value)))
        col = "business_tax_take_pct"
    else:
        return await get_global_economy()

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f"UPDATE global_economy SET {col} = ? WHERE id = 1", (clamped,))
        await db.commit()
    return await get_global_economy()


async def post_chronicle(text: str) -> None:
    """Публикация в канал без Markdown (имена игроков часто ломают разметку)."""
    global _chronicle_resolved_id
    chat_id: Optional[int] = CHRONICLE_CHANNEL_ID
    if chat_id is None:
        chat_id = _chronicle_resolved_id
    if chat_id is None and CHRONICLE_CHANNEL_USERNAME:
        try:
            ch = await bot.get_chat(f"@{CHRONICLE_CHANNEL_USERNAME}")
            _chronicle_resolved_id = ch.id
            chat_id = ch.id
            logger.info("Хроника: канал @%s → chat_id=%s", CHRONICLE_CHANNEL_USERNAME, chat_id)
        except Exception as e:
            logger.error(
                "Хроника: не удалось открыть канал @%s — добавь бота админом и проверь username. Ошибка: %s",
                CHRONICLE_CHANNEL_USERNAME,
                e,
            )
            return
    if not chat_id:
        logger.debug("Хроника не настроена: задай CHRONICLE_CHANNEL_ID или CHRONICLE_CHANNEL_USERNAME")
        return
    body = "📰 Криминальная хроника\n\n" + text
    try:
        async with chronicle_send_lock:
            await bot.send_message(chat_id, body, disable_web_page_preview=True)
    except Exception as e:
        logger.error(
            "Хроника: send_message(chat_id=%s) не удался. Проверь, что бот админ канала с правом постинга. %s",
            chat_id,
            e,
            exc_info=True,
        )


async def add_daily_earned(user_id: int, amount: int) -> None:
    if amount <= 0:
        return
    day = moscow_date_str()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """INSERT INTO daily_earnings (user_id, day, earned) VALUES (?, ?, ?)
               ON CONFLICT(user_id, day) DO UPDATE SET earned = earned + excluded.earned""",
            (user_id, day, amount),
        )
        await db.commit()


async def unlock_achievement(user_id: int, ach_id: str) -> bool:
    if ach_id not in ACHIEVEMENTS:
        return False
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute(
                "INSERT INTO player_achievements (user_id, achievement_id) VALUES (?, ?)",
                (user_id, ach_id),
            )
            await db.commit()
            return True
        except sqlite3.IntegrityError:
            return False


async def get_unlocked_achievement_ids(user_id: int) -> set:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT achievement_id FROM player_achievements WHERE user_id = ?", (user_id,)
        )
        rows = await cur.fetchall()
    return {r[0] for r in rows}


async def get_player_modifiers(user_id: int) -> Dict[str, Any]:
    unlocked = await get_unlocked_achievement_ids(user_id)
    mods = {
        "income_bonus": 0.0,
        "shop_discount": 0.0,
        "nagirt_effect_bonus": 0.0,
        "inventory_extra_slots": 0,
        "transfer_extra_fee": 0.0,
        "titles": [],
    }
    if "millionaire" in unlocked:
        mods["income_bonus"] += ACHIEVEMENTS["millionaire"].get("income_bonus", 0)
        mods["titles"].append(ACHIEVEMENTS["millionaire"]["name"])
    if "duel_master" in unlocked:
        mods["shop_discount"] += ACHIEVEMENTS["duel_master"].get("shop_discount", 0)
        mods["titles"].append(ACHIEVEMENTS["duel_master"].get("title", ACHIEVEMENTS["duel_master"]["name"]))
    if "nagirt100" in unlocked:
        mods["nagirt_effect_bonus"] += ACHIEVEMENTS["nagirt100"].get("nagirt_effect_bonus", 0)
        mods["titles"].append(ACHIEVEMENTS["nagirt100"].get("title", ACHIEVEMENTS["nagirt100"]["name"]))
    if "oligarch" in unlocked:
        mods["inventory_extra_slots"] += ACHIEVEMENTS["oligarch"].get("inventory_slots", 0)
        mods["titles"].append(ACHIEVEMENTS["oligarch"].get("title", ACHIEVEMENTS["oligarch"]["name"]))
    if "kidala" in unlocked:
        mods["transfer_extra_fee"] += ACHIEVEMENTS["kidala"].get("transfer_extra_fee", 0)
        mods["titles"].append(ACHIEVEMENTS["kidala"].get("title", ACHIEVEMENTS["kidala"]["name"]))
    return mods


# ==================== СОЦИАЛЬНЫЕ СТАТУСЫ ====================
_legend_cache_month: Optional[str] = None
_legend_cache_updated_at: Optional[datetime] = None
_legend_top3_ids: set[int] = set()


async def get_legend_top3_ids_for_month() -> set[int]:
    """Легенда: топ-3 по балансу за месяц; кэш обновляется каждые 5 минут."""
    global _legend_cache_month, _legend_cache_updated_at, _legend_top3_ids
    month_key = datetime.now(MSK).strftime("%Y-%m")
    now = datetime.now(MSK)
    if (
        _legend_cache_month == month_key
        and _legend_top3_ids
        and _legend_cache_updated_at
        and (now - _legend_cache_updated_at).total_seconds() < 300
    ):
        return _legend_top3_ids
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            cur = await db.execute(
                "SELECT user_id FROM players ORDER BY balance DESC LIMIT 3"
            )
            rows = await cur.fetchall()
    except Exception as e:
        logger.error("Ошибка при расчете топ-3 для статуса Легенда: %s", e)
        return _legend_top3_ids or set()
    _legend_top3_ids = {int(r[0]) for r in rows}
    _legend_cache_month = month_key
    _legend_cache_updated_at = now
    return _legend_top3_ids


async def get_social_status_for_user(user_id: int) -> Dict[str, Any]:
    """
    Статусы:
    🟢 Новичок
    🔵 Стажёр — 10 получок + 1 бизнес (комиссия переводов -13%)
    🟡 Мастер — 50 побед в дуэлях + 5 бизнесов (скидка в магазине -3%)
    🟠 Мафиози — 100 побед + 10 бизнесов + 1 млн ₽ (скидка -5%, кредитный лимит +50%)
    🔴 Легенда — топ-3 по балансу за месяц (скидка -10%, комиссия переводов 0%)
    🟣 Агитатор — 10 рефералов (меняет бонус рефералки +20%)
    """
    try:
        user = await get_user(user_id)
        if not user:
            return {
                "status_key": "novice",
                "status_name": "НОВИЧОК",
                "emoji": "🟢",
                "shop_discount_add": 0.0,
                "transfer_commission_multiplier": 1.0,
                "credit_limit_multiplier": 1.0,
                "referral_bonus_multiplier": 1.0,
            }

        duels_won = int(user.get("duels_won") or 0)
        salary_count = int(user.get("salary_count") or 0)
        balance = int(user.get("balance") or 0)

        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT COUNT(*) AS cnt FROM businesses WHERE owner_id = ? AND is_active = 1",
                (user_id,),
            )
            biz_cnt = int((await cur.fetchone())["cnt"] or 0)
            cur = await db.execute(
                "SELECT COUNT(*) AS cnt FROM referral_invites WHERE inviter_id = ? AND credited_at IS NOT NULL",
                (user_id,),
            )
            ref_cnt = int((await cur.fetchone())["cnt"] or 0)

        legend_ids = await get_legend_top3_ids_for_month()
        is_legend = user_id in legend_ids
        is_agitator = ref_cnt >= 10
        is_mafia = duels_won >= 100 and biz_cnt >= 10 and balance >= 1_000_000
        is_master = duels_won >= 50 and biz_cnt >= 5
        is_stajer = salary_count >= 10 and biz_cnt >= 1

        # Отображение приоритета.
        if is_legend:
            status_key, status_name, emoji = "legend", "ЛЕГЕНДА", "🔴"
        elif is_mafia:
            status_key, status_name, emoji = "mafia", "МАФИОЗИ", "🟠"
        elif is_master:
            status_key, status_name, emoji = "master", "МАСТЕР", "🟡"
        elif is_stajer:
            status_key, status_name, emoji = "trainee", "СТАЖЁР", "🔵"
        elif is_agitator:
            status_key, status_name, emoji = "agitator", "АГИТАТОР", "🟣"
        else:
            status_key, status_name, emoji = "novice", "НОВИЧОК", "🟢"

        shop_discount_add = 0.0
        transfer_commission_multiplier = 1.0
        credit_limit_multiplier = 1.0
        referral_bonus_multiplier = 1.2 if is_agitator else 1.0

        if status_key == "master":
            shop_discount_add = 0.03
        elif status_key == "mafia":
            shop_discount_add = 0.05
        elif status_key == "legend":
            shop_discount_add = 0.10

        if status_key == "trainee":
            transfer_commission_multiplier = 0.87
        elif status_key == "legend":
            transfer_commission_multiplier = 0.0

        if status_key == "mafia":
            credit_limit_multiplier = 1.5

        return {
            "status_key": status_key,
            "status_name": status_name,
            "emoji": emoji,
            "shop_discount_add": shop_discount_add,
            "transfer_commission_multiplier": transfer_commission_multiplier,
            "credit_limit_multiplier": credit_limit_multiplier,
            "referral_bonus_multiplier": referral_bonus_multiplier,
            "biz_cnt": biz_cnt,
            "ref_cnt": ref_cnt,
            "duels_won": duels_won,
            "is_legend": is_legend,
            "is_agitator": is_agitator,
        }
    except Exception as e:
        logger.error("Ошибка в get_social_status_for_user(user_id=%s): %s", user_id, e)
        return {
            "status_key": "novice",
            "status_name": "НОВИЧОК",
            "emoji": "🟢",
            "shop_discount_add": 0.0,
            "transfer_commission_multiplier": 1.0,
            "credit_limit_multiplier": 1.0,
            "referral_bonus_multiplier": 1.0,
            "biz_cnt": 0,
            "ref_cnt": 0,
            "duels_won": 0,
            "is_legend": False,
            "is_agitator": False,
        }


async def build_statuses_info_text(user_id: int) -> str:
    user = await get_user(user_id)
    if not user:
        return NOT_REGISTERED_HINT
    social = await get_social_status_for_user(user_id)
    legend_ids = await get_legend_top3_ids_for_month()

    salary_count = int(user.get("salary_count") or 0)
    duels_won = int(user.get("duels_won") or 0)
    balance = int(user.get("balance") or 0)
    biz_cnt = int(social.get("biz_cnt") or 0)
    ref_cnt = int(social.get("ref_cnt") or 0)
    in_top3 = user_id in legend_ids

    return (
        f"🏅 *СОЦИАЛЬНЫЕ СТАТУСЫ*\n\n"
        f"Текущий статус: *{social['status_name']}* {social['emoji']}\n\n"
        f"🟢 *Новичок*\n"
        f"• Условие: по умолчанию\n"
        f"• Бонусы: базовые условия\n\n"
        f"🔵 *Стажёр*\n"
        f"• Условия: получки {salary_count}/10, бизнесы {biz_cnt}/1\n"
        f"• Бонусы: комиссия переводов -13%\n\n"
        f"🟡 *Мастер*\n"
        f"• Условия: победы в дуэлях {duels_won}/50, бизнесы {biz_cnt}/5\n"
        f"• Бонусы: скидка в магазине -3%\n\n"
        f"🟠 *Мафиози*\n"
        f"• Условия: победы {duels_won}/100, бизнесы {biz_cnt}/10, баланс {format_money(balance)}/{format_money(1_000_000)}\n"
        f"• Бонусы: скидка -5%, лимит кредита +50%\n\n"
        f"🔴 *Легенда*\n"
        f"• Условие: войти в топ-3 по балансу за месяц (сейчас: {'да' if in_top3 else 'нет'})\n"
        f"• Бонусы: скидка -10%, комиссия переводов 0%\n\n"
        f"🟣 *Агитатор*\n"
        f"• Условие: активные рефералы {ref_cnt}/10\n"
        f"• Бонусы: +20% к реферальному бонусу\n\n"
        f"_Приоритет статусов: Легенда > Мафиози > Мастер > Стажёр > Агитатор > Новичок._"
    )


def get_statuses_info_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить статус", callback_data="refresh_statuses_info")]
        ]
    )


async def get_reputation_percent(user_id: int, user_row: Optional[Dict[str, Any]] = None) -> float:
    """
    Репутация хранится как текущий рейтинг 0..100 в players.rep_points.
    Если голосов ещё не было — показываем нейтральные 50/100.
    """
    user = user_row or await get_user(user_id) or {}
    votes = int(user.get("rep_votes") or 0)
    if votes <= 0:
        return 50.0
    score = float(user.get("rep_points") or 50)
    return max(0.0, min(100.0, score))


async def add_reputation_vote(from_uid: int, to_uid: int, stars: int, context: str = "") -> tuple[bool, str]:
    stars = max(1, min(3, int(stars)))
    # Пошаговая система:
    # 1★ -> -2, 2★ -> +2, 3★ -> +4
    delta = {1: -2, 2: 2, 3: 4}[stars]
    async with aiosqlite.connect(DB_NAME) as db:
        # Антиспам: одну и ту же цель можно оценить не чаще раза в 2 часа.
        cur = await db.execute(
            """
            SELECT created_at
            FROM reputation_events
            WHERE from_user = ? AND to_user = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (from_uid, to_uid),
        )
        last_vote_row = await cur.fetchone()
        if last_vote_row and last_vote_row[0]:
            last_vote_dt = safe_parse_datetime(last_vote_row[0])
            if last_vote_dt:
                wait_sec = int((last_vote_dt + timedelta(hours=2) - datetime.now()).total_seconds())
                if wait_sec > 0:
                    mins = max(1, wait_sec // 60)
                    return False, f"⏳ Этого пользователя можно оценить снова через ~{mins} мин."

        cur = await db.execute(
            "SELECT rep_points, rep_votes FROM players WHERE user_id = ?",
            (to_uid,),
        )
        row = await cur.fetchone()
        if not row:
            return False, "❌ Пользователь не найден."
        current_points = int(row[0] or 0)
        current_votes = int(row[1] or 0)
        # Старт от нейтрального 50/100 при первой оценке.
        base_score = 50 if current_votes <= 0 else current_points
        new_score = max(0, min(100, base_score + delta))

        await db.execute(
            "UPDATE players SET rep_points = ?, rep_votes = rep_votes + 1 WHERE user_id = ?",
            (new_score, to_uid),
        )
        await db.execute(
            "INSERT INTO reputation_events (from_user, to_user, stars, context) VALUES (?, ?, ?, ?)",
            (from_uid, to_uid, stars, context[:64]),
        )
        await db.commit()
    return True, "OK"


async def get_inventory_rows(user_id: int) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM player_inventory WHERE user_id = ? AND quantity > 0 ORDER BY id",
            (user_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def inventory_slots_used(user_id: int) -> int:
    rows = await get_inventory_rows(user_id)
    return sum(max(0, r["quantity"]) for r in rows)


async def max_inventory_slots_for_user(user_id: int) -> int:
    base = ECONOMY_SETTINGS["inventory_base_slots"]
    mods = await get_player_modifiers(user_id)
    return base + mods.get("inventory_extra_slots", 0)


async def add_inventory_item(user_id: int, item_id: str, qty: int = 1) -> tuple[bool, str]:
    item = next((i for i in SHOP_ITEMS if i["id"] == item_id), None)
    if not item:
        return False, "Неизвестный предмет"
    used = await inventory_slots_used(user_id)
    cap = await max_inventory_slots_for_user(user_id)
    if used + qty > cap:
        return False, f"Инвентарь полон ({used}/{cap} слотов). Расхламись или открой ачивку «Олигарх»."
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT id, quantity FROM player_inventory WHERE user_id = ? AND item_id = ?",
            (user_id, item_id),
        )
        ex = await cur.fetchone()
        if ex:
            await db.execute(
                "UPDATE player_inventory SET quantity = quantity + ? WHERE id = ?",
                (qty, ex[0]),
            )
        else:
            await db.execute(
                "INSERT INTO player_inventory (user_id, item_id, quantity) VALUES (?, ?, ?)",
                (user_id, item_id, qty),
            )
        await db.commit()
    return True, "OK"


async def remove_inventory_stack_row(inv_id: int, user_id: int, qty: int = 1) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT quantity FROM player_inventory WHERE id = ? AND user_id = ?",
            (inv_id, user_id),
        )
        row = await cur.fetchone()
        if not row or row[0] < qty:
            return False
        new_q = row[0] - qty
        if new_q <= 0:
            await db.execute("DELETE FROM player_inventory WHERE id = ?", (inv_id,))
        else:
            await db.execute("UPDATE player_inventory SET quantity = ? WHERE id = ?", (new_q, inv_id))
        await db.commit()
    return True


async def get_inventory_stack(inv_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM player_inventory WHERE id = ? AND user_id = ?",
            (inv_id, user_id),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def apply_shop_item_effect(user_id: int, item: Dict[str, Any]) -> str:
    """Применяет эффект товара сразу (из инвентаря или чека). Возвращает текст для игрока."""
    bonus_text = ""
    if item.get("type") == "boost":
        await add_boost(user_id, item["id"], item["value"], item["hours"])
        bonus_text = f"Буст: +{int(item['value']*100)}% к зарплате на {item['hours']}ч"
    elif item.get("type") == "protection":
        if item["id"] == "day_off":
            immunity_until = (datetime.now() + timedelta(hours=item["hours"])).isoformat()
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE players SET penalty_immunity_until = ? WHERE user_id = ?", (immunity_until, user_id))
                await db.commit()
            bonus_text = f"Иммунитет к штрафам на {item['hours']}ч"
        elif item["id"] == "insurance":
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "INSERT INTO boosts (user_id, boost_type, boost_value, expires_at) VALUES (?, ?, ?, ?)",
                    (user_id, "insurance", 0.8, (datetime.now() + timedelta(hours=24)).isoformat()),
                )
                await db.commit()
            bonus_text = "Страховка на 24ч (80% от следующего штрафа)"
    elif item.get("type") == "pill":
        tolerance = await get_nagirt_tolerance(user_id)
        real_salary_boost = item["effect_salary"] / tolerance
        real_game_boost = item["effect_game"] / tolerance
        mods = await get_player_modifiers(user_id)
        nb = 1.0 + mods.get("nagirt_effect_bonus", 0.0)
        real_salary_boost *= nb
        real_game_boost *= nb
        side_effects = ""
        if random.randint(1, 100) <= item.get("side_effect_chance", 0):
            side_effects = random.choice(
                ["Головокружение", "Тошнота", "Слабость", "Дрожь в руках", "Паранойя"]
            )
        await add_nagirt_pill(user_id, item["id"], (real_salary_boost + real_game_boost) / 2, item["hours"], side_effects)
        await update_nagirt_tolerance(user_id, increase=0.15)
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE players SET nagirt_uses = nagirt_uses + 1 WHERE user_id = ?", (user_id,)
            )
            await db.commit()
        bonus_text = f"Нагирт: +{int(real_salary_boost*100)}% ЗП, +{int(real_game_boost*100)}% игры на {item['hours']}ч"
        if side_effects:
            bonus_text += f" | Побочка: {side_effects}"
    elif item.get("type") == "antidote":
        await reset_nagirt_tolerance(user_id)
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("DELETE FROM nagirt_pills WHERE user_id = ?", (user_id,))
            await db.commit()
        bonus_text = "Антидот: эффекты Нагирта сняты"
    elif item.get("type") == "lottery":
        if random.random() <= 0.25:
            win_amount = random.randint(2000, 10000)
            await update_balance(user_id, win_amount, "lottery_shop", f"Лотерея из инвентаря")
            bonus_text = f"Выигрыш {format_money(win_amount)}!"
        else:
            bonus_text = "Лотерея не сыграла"
    elif item.get("type") == "instant":
        salary = random.randint(ECONOMY_SETTINGS["salary_min"], ECONOMY_SETTINGS["salary_max"])
        await update_balance(user_id, salary, "instant", "Мгновенная получка из инвентаря")
        bonus_text = f"Мгновенная получка: {format_money(salary)}"
    else:
        bonus_text = "Нечего применять"
    return bonus_text


async def check_achievements_for_user(user_id: int) -> List[str]:
    user = await get_user(user_id)
    if not user:
        return []
    have = await get_unlocked_achievement_ids(user_id)
    new_msgs: List[str] = []
    biz_n = len(await get_user_businesses(user_id))

    checks = []
    if user["balance"] >= 1_000_000:
        checks.append("millionaire")
    if (user.get("duels_won") or 0) >= 100:
        checks.append("duel_master")
    if (user.get("nagirt_uses") or 0) >= 100:
        checks.append("nagirt100")
    if biz_n >= 10:
        checks.append("oligarch")
    if (user.get("loans_defaulted") or 0) >= 5:
        checks.append("kidala")

    for aid in checks:
        if aid in have:
            continue
        if await unlock_achievement(user_id, aid):
            meta = ACHIEVEMENTS[aid]
            new_msgs.append(f"{meta.get('emoji', '🏅')} *{meta['name']}* — {meta['desc']}")
    return new_msgs


def _progress_bar(cur: int, need: int, width: int = 10) -> str:
    if need <= 0:
        return "░" * width + " 0%"
    pct = min(100, max(0, int(100 * cur / need)))
    filled = max(0, min(width, round(width * pct / 100)))
    return "█" * filled + "░" * (width - filled) + f" {pct}%"


async def format_achievements_screen(user_id: int) -> str:
    user = await get_user(user_id)
    if not user:
        return NOT_REGISTERED_HINT
    unlocked = await get_unlocked_achievement_ids(user_id)
    biz_n = len(await get_user_businesses(user_id))
    bal = int(user["balance"])
    dw = int(user.get("duels_won") or 0)
    nu = int(user.get("nagirt_uses") or 0)
    ld = int(user.get("loans_defaulted") or 0)

    rows = [
        ("millionaire", bal, 1_000_000),
        ("duel_master", dw, 100),
        ("nagirt100", nu, 100),
        ("oligarch", biz_n, 10),
        ("kidala", ld, 5),
    ]
    lines: List[str] = ["🏅 *Достижения*", "", "_Прогресс до разблокировки._", ""]
    for aid, cur, need in rows:
        meta = ACHIEVEMENTS.get(aid)
        if not meta:
            continue
        em = meta.get("emoji", "🏅")
        name = meta["name"]
        desc = meta["desc"]
        if aid in unlocked:
            lines.append(f"{em} *{name}* — получено ✅")
            lines.append(f"_{desc}_")
        else:
            lines.append(f"{em} *{name}*")
            lines.append(f"_{desc}_")
            lines.append(f"`{_progress_bar(cur, need)}`  `{cur} / {need}`")
        lines.append("")
    return "\n".join(lines).rstrip()


async def payout_daily_top_for_day(day_str: str) -> None:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT 1 FROM daily_top_paid WHERE day = ?", (day_str,))
        if await cur.fetchone():
            return
        cur = await db.execute(
            """SELECT user_id, earned FROM daily_earnings WHERE day = ? AND earned > 0
               ORDER BY earned DESC LIMIT 3""",
            (day_str,),
        )
        top = await cur.fetchall()
    rewards = (
        ECONOMY_SETTINGS["daily_top_reward_1"],
        ECONOMY_SETTINGS["daily_top_reward_2"],
        ECONOMY_SETTINGS["daily_top_reward_3"],
    )
    medals = ("🥇", "🥈", "🥉")
    lines = []
    for i, row in enumerate(top):
        uid, earned = int(row[0]), int(row[1])
        prize = rewards[i] if i < len(rewards) else 0
        if prize <= 0:
            continue
        await update_balance(uid, prize, "daily_top", f"Топ заработка за {day_str} ({medals[i]} место)")
        u = await get_user(uid)
        name = u["full_name"] if u else str(uid)
        lines.append(f"{medals[i]} {name}: +{format_money(prize)} (заработок дня {format_money(earned)})")
        try:
            await bot.send_message(
                uid,
                f"🏆 *Топ заработка дня ({day_str})*\n\n"
                f"Ты на {i+1} месте!\nНачислено: {format_money(prize)}",
                parse_mode="Markdown",
            )
        except Exception:
            pass
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO daily_top_paid (day, paid_at) VALUES (?, ?)",
            (day_str, datetime.now(MSK).isoformat()),
        )
        await db.commit()
    if lines:
        await post_chronicle("💰 *Топ заработка за сутки (МСК)*\n" + "\n".join(lines))
    else:
        await post_chronicle(f"📉 За {day_str} никто ничего не заработал — Виталик разочарован.")


async def daily_top_scheduler():
    await asyncio.sleep(5)
    while True:
        try:
            now = datetime.now(MSK)
            # На случай перезапуска бота после 10:00 пытаемся догнать выплату за вчера.
            # ВАЖНО: до 10:00 МСК не платим/не постим за "вчера", чтобы хроника выходила вовремя.
            today_10 = now.replace(hour=10, minute=0, second=0, microsecond=0)
            if now >= today_10:
                y = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                await payout_daily_top_for_day(y)
            nxt = now.replace(hour=10, minute=0, second=0, microsecond=0)
            if now >= nxt:
                nxt = nxt + timedelta(days=1)
            wait_sec = max(1.0, (nxt - now).total_seconds())
            await asyncio.sleep(wait_sec)
            y = (datetime.now(MSK) - timedelta(days=1)).strftime("%Y-%m-%d")
            await payout_daily_top_for_day(y)
        except Exception as e:
            logger.error(f"daily_top_scheduler: {e}")
            await asyncio.sleep(60)


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def safe_parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        try:
            return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return None


async def get_referral_activity_count(user_id: int) -> int:
    """
    Более надёжный счётчик активности для рефералки.
    Раньше считались только транзакции, из-за чего часть действий не засчитывалась.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*)
            FROM transactions
            WHERE user_id = ?
              AND type NOT IN ('registration')
            """,
            (user_id,),
        )
        tx_count = int((await cur.fetchone())[0] or 0)

        cur = await db.execute(
            "SELECT COUNT(*) FROM check_activations WHERE user_id = ?",
            (user_id,),
        )
        checks_count = int((await cur.fetchone())[0] or 0)

        cur = await db.execute(
            "SELECT COUNT(*) FROM purchases WHERE user_id = ?",
            (user_id,),
        )
        purchases_count = int((await cur.fetchone())[0] or 0)

        cur = await db.execute(
            "SELECT COUNT(*) FROM reputation_events WHERE from_user = ?",
            (user_id,),
        )
        rep_votes_count = int((await cur.fetchone())[0] or 0)

        cur = await db.execute(
            "SELECT COUNT(*) FROM businesses WHERE owner_id = ? AND is_active = 1",
            (user_id,),
        )
        active_businesses = int((await cur.fetchone())[0] or 0)

    # Бизнес считаем как активность (покупка/владение = участие в игре).
    return tx_count + checks_count + purchases_count + rep_votes_count + active_businesses


async def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM players WHERE user_id = ?", (user_id,))
        user = await cursor.fetchone()
        return dict(user) if user else None

async def register_user(
    user_id: int,
    username: str,
    full_name: str,
    referrer_id: Optional[int] = None,
) -> Tuple[bool, bool]:
    """
    Returns:
        (created_new_player, referral_invite_record_created)
    """
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT 1 FROM players WHERE user_id = ?", (user_id,))
        exists = await cursor.fetchone()
        if not exists:
            await db.execute(
                "INSERT INTO players (user_id, username, full_name, balance) VALUES (?, ?, ?, ?)",
                (user_id, username, full_name, ECONOMY_SETTINGS["start_balance"])
            )
            await db.execute(
                "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'registration', ?, 'Стартовый капитал')",
                (user_id, ECONOMY_SETTINGS["start_balance"])
            )

            referral_record_created = False
            # Если регистрация пришла по рефералке — фиксируем связь.
            if referrer_id is not None and referrer_id != user_id:
                ref_cur = await db.execute(
                    """
                    INSERT INTO referral_invites (inviter_id, invitee_id, credited_at, milestone, reward_inviter, reward_newcomer)
                    VALUES (?, ?, NULL, 0, 0, 0)
                    ON CONFLICT(invitee_id) DO NOTHING
                    """,
                    (referrer_id, user_id),
                )
                # При ON CONFLICT DO NOTHING rowcount обычно 1 или 0.
                try:
                    referral_record_created = (ref_cur.rowcount or 0) > 0
                except Exception:
                    referral_record_created = True
            await db.commit()
            return True, referral_record_created

        return False, False

async def update_balance(user_id: int, amount: int, txn_type: str, description: str):
    """Безопасное обновление баланса – баланс никогда не уходит в минус."""
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT balance, full_name, poor_alerted_at, extreme_poor_alerted_at FROM players WHERE user_id = ?",
            (user_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return
        current_balance = int(row[0] or 0)
        player_name = row[1] or str(user_id)
        poor_alerted_at = row[2]
        extreme_poor_alerted_at = row[3]
        new_balance = current_balance + amount
        if new_balance < 0:
            amount = -current_balance
            new_balance = 0

        await db.execute(
            "UPDATE players SET balance = ? WHERE user_id = ?",
            (new_balance, user_id)
        )
        if txn_type == "salary":
            await db.execute(
                "UPDATE players SET total_earned = total_earned + ?, salary_count = salary_count + 1 WHERE user_id = ?",
                (amount, user_id)
            )
        elif txn_type == "penalty":
            await db.execute(
                "UPDATE players SET total_fines = total_fines + ? WHERE user_id = ?",
                (-amount, user_id)
            )
        elif txn_type == "instant" and amount > 0:
            await db.execute(
                "UPDATE players SET last_salary = ? WHERE user_id = ?",
                (datetime.now().isoformat(), user_id),
            )
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, ?, ?, ?)",
            (user_id, txn_type, amount, description)
        )
        await db.commit()

    if amount > 0 and txn_type in DAILY_EARN_TXN_TYPES:
        await add_daily_earned(user_id, amount)

    try:
        crossed_extreme_poor = (
            amount < 0
            and txn_type not in _TRANSIENT_BET_TXN_TYPES
            and current_balance >= EXTREME_POOR_BALANCE_THRESHOLD
            and new_balance < EXTREME_POOR_BALANCE_THRESHOLD
        )
        crossed_poor = (
            amount < 0
            and txn_type not in _TRANSIENT_BET_TXN_TYPES
            and current_balance >= POOR_BALANCE_THRESHOLD
            and new_balance < POOR_BALANCE_THRESHOLD
            and new_balance >= EXTREME_POOR_BALANCE_THRESHOLD
        )

        # Уведомляем о бедности только при фактическом пересечении порога сверху вниз.
        if crossed_poor:
            now = datetime.now()
            last = safe_parse_datetime(poor_alerted_at)
            if not last or (now - last).total_seconds() >= POOR_ALERT_COOLDOWN_SEC:
                await post_chronicle(
                    f"😢 Игрок {player_name} стал бедным. Баланс: {format_money(new_balance)}."
                )
                async with aiosqlite.connect(DB_NAME) as db2:
                    await db2.execute(
                        "UPDATE players SET poor_alerted_at = ? WHERE user_id = ?",
                        (now.isoformat(), user_id),
                    )
                    await db2.commit()

        # Событие «нищета» только при первом падении ниже порога.
        if crossed_extreme_poor:
            now = datetime.now()
            last = safe_parse_datetime(extreme_poor_alerted_at)
            if not last or (now - last).total_seconds() >= POOR_ALERT_COOLDOWN_SEC:
                await post_chronicle(
                    f"💀 Игрок {player_name} обанкротился и продан в рабство на корпоративную стройку Виталика.\n"
                    f"Баланс: {format_money(new_balance)}"
                )
                async with aiosqlite.connect(DB_NAME) as db2:
                    await db2.execute(
                        "UPDATE players SET extreme_poor_alerted_at = ? WHERE user_id = ?",
                        (now.isoformat(), user_id),
                    )
                    await db2.commit()
    except Exception as e:
        logger.error("Ошибка в логике уведомлений бедности для user_id=%s: %s", user_id, e, exc_info=True)

    try:
        for line in await check_achievements_for_user(user_id):
            try:
                await bot.send_message(user_id, f"🎖️ *Новое достижение!*\n\n{line}", parse_mode="Markdown")
            except Exception:
                pass
    except Exception as e:
        logger.error("Ошибка проверки достижений для user_id=%s: %s", user_id, e, exc_info=True)


async def post_poverty_transition_if_needed(user_id: int, prev_balance: int, final_balance: int) -> None:
    """
    Отправка хроники по итоговому балансу (например, после завершения игры),
    чтобы не триггериться на промежуточное списание ставки.
    """
    if final_balance >= prev_balance:
        return

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT full_name, poor_alerted_at, extreme_poor_alerted_at FROM players WHERE user_id = ?",
            (user_id,),
        )
        row = await cur.fetchone()
        if not row:
            return
        player_name = row[0] or str(user_id)
        poor_alerted_at = row[1]
        extreme_poor_alerted_at = row[2]

    crossed_extreme_poor = (
        prev_balance >= EXTREME_POOR_BALANCE_THRESHOLD
        and final_balance < EXTREME_POOR_BALANCE_THRESHOLD
    )
    crossed_poor = (
        prev_balance >= POOR_BALANCE_THRESHOLD
        and final_balance < POOR_BALANCE_THRESHOLD
        and final_balance >= EXTREME_POOR_BALANCE_THRESHOLD
    )

    if crossed_poor:
        now = datetime.now()
        last = safe_parse_datetime(poor_alerted_at)
        if not last or (now - last).total_seconds() >= POOR_ALERT_COOLDOWN_SEC:
            await post_chronicle(f"😢 Игрок {player_name} стал бедным. Баланс: {format_money(final_balance)}.")
            async with aiosqlite.connect(DB_NAME) as db2:
                await db2.execute(
                    "UPDATE players SET poor_alerted_at = ? WHERE user_id = ?",
                    (now.isoformat(), user_id),
                )
                await db2.commit()

    if crossed_extreme_poor:
        now = datetime.now()
        last = safe_parse_datetime(extreme_poor_alerted_at)
        if not last or (now - last).total_seconds() >= POOR_ALERT_COOLDOWN_SEC:
            await post_chronicle(
                f"💀 Игрок {player_name} обанкротился и продан в рабство на корпоративную стройку Виталика.\n"
                f"Баланс: {format_money(final_balance)}"
            )
            async with aiosqlite.connect(DB_NAME) as db2:
                await db2.execute(
                    "UPDATE players SET extreme_poor_alerted_at = ? WHERE user_id = ?",
                    (now.isoformat(), user_id),
                )
                await db2.commit()

async def get_all_users() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT user_id, full_name, username, balance FROM players")
        users = await cursor.fetchall()
        return [dict(user) for user in users]

# ==================== НАГИРТ – ЖЁСТЧЕ ====================
async def add_nagirt_pill(user_id: int, pill_type: str, effect: float, hours: int, side_effects: str = ""):
    expires_at = datetime.now() + timedelta(hours=hours)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            '''INSERT INTO nagirt_pills (user_id, pill_type, effect_strength, expires_at, side_effects)
               VALUES (?, ?, ?, ?, ?)''',
            (user_id, pill_type, effect, expires_at.isoformat(), side_effects)
        )
        await db.commit()

async def get_active_nagirt_effects(user_id: int) -> Dict[str, Any]:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT pill_type, effect_strength, side_effects FROM nagirt_pills WHERE user_id = ? AND expires_at > ?",
            (user_id, datetime.now().isoformat())
        )
        rows = await cursor.fetchall()

    effects = {
        "salary_boost": 0.0,
        "game_boost": 0.0,
        "side_effects": [],
        "has_active": len(rows) > 0,
        "fine_chance_mod": 0.0
    }

    for row in rows:
        pill_type, strength, side_effects = row
        if pill_type == "nagirt_light":
            effects["salary_boost"] += 0.15
            effects["game_boost"] += 0.2
            effects["fine_chance_mod"] += 0.1
        elif pill_type == "nagirt_pro":
            effects["salary_boost"] += 0.3
            effects["game_boost"] += 0.4
            effects["fine_chance_mod"] += 0.25
        elif pill_type == "nagirt_extreme":
            effects["salary_boost"] += 0.5
            effects["game_boost"] += 0.7
            effects["fine_chance_mod"] += 0.4
        if side_effects:
            effects["side_effects"].append(side_effects)

    return effects

async def get_nagirt_tolerance(user_id: int) -> float:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT tolerance FROM nagirt_tolerance WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        return result[0] if result else 1.0

async def update_nagirt_tolerance(user_id: int, increase: float = 0.15):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT OR REPLACE INTO nagirt_tolerance (user_id, tolerance, last_used)
            VALUES (?, COALESCE((SELECT tolerance FROM nagirt_tolerance WHERE user_id = ?), 1.0) + ?, ?)
        ''', (user_id, user_id, increase, datetime.now().isoformat()))
        await db.commit()

async def reset_nagirt_tolerance(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO nagirt_tolerance (user_id, tolerance, last_used) VALUES (?, 1.0, ?)",
            (user_id, datetime.now().isoformat())
        )
        await db.commit()

async def cleanup_expired():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM boosts WHERE expires_at <= ?", (datetime.now().isoformat(),))
        await db.execute("DELETE FROM nagirt_pills WHERE expires_at <= ?", (datetime.now().isoformat(),))
        await db.commit()

async def add_boost(user_id: int, boost_type: str, value: float, hours: int):
    expires_at = datetime.now() + timedelta(hours=hours)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO boosts (user_id, boost_type, boost_value, expires_at) VALUES (?, ?, ?, ?)",
            (user_id, boost_type, value, expires_at.isoformat())
        )
        await db.commit()

async def get_active_boosts(user_id: int) -> float:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT SUM(boost_value) FROM boosts WHERE user_id = ? AND expires_at > ?",
            (user_id, datetime.now().isoformat())
        )
        result = await cursor.fetchone()
        return result[0] if result and result[0] else 0.0

async def has_fine_protection(user_id: int) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT 1 FROM players WHERE user_id = ? AND penalty_immunity_until > ?",
            (user_id, datetime.now().isoformat())
        )
        result = await cursor.fetchone()
        return result is not None

# ==================== БИЗНЕС-ФУНКЦИИ ====================
async def get_user_businesses(user_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM businesses WHERE owner_id = ? AND is_active = 1 ORDER BY created_at DESC",
            (user_id,)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def buy_business(user_id: int, biz_key: str) -> tuple[bool, str]:
    biz = BUSINESS_TYPES.get(biz_key)
    if not biz:
        return False, "❌ Такого бизнеса нет."

    user = await get_user(user_id)
    if not user:
        return False, "❌ Сначала зарегистрируйся."

    biz_price = int(biz["price"] / BUSINESS_PRICE_DIVISOR)
    if user['balance'] < biz_price:
        return False, f"❌ Не хватает денег. Нужно {format_money(biz_price)}."

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO businesses (owner_id, biz_type, base_income) VALUES (?, ?, ?)",
            (user_id, biz_key, biz['base_income'])
        )
        await db.execute(
            "UPDATE players SET balance = balance - ? WHERE user_id = ?",
            (biz_price, user_id)
        )
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'business_buy', -?, ?)",
            (user_id, biz_price, f"Покупка {biz['name']}")
        )
        await db.commit()

    return True, "✅ Бизнес куплен!"

async def calculate_business_income(business: Dict) -> int:
    biz_config = BUSINESS_TYPES[business['biz_type']]
    base = business['base_income']
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT SUM(bonus_income) FROM business_upgrades WHERE business_id = ?",
            (business['id'],)
        )
        row = await cursor.fetchone()
        bonus = row[0] if row and row[0] else 0
    return base + bonus

async def collect_business_income(user_id: int) -> int:
    """Собирает доход со всех бизнесов, у которых прошёл час. Возвращает сумму."""
    businesses = await get_user_businesses(user_id)
    total_income = 0
    now = datetime.now()

    for biz in businesses:
        last_collect = biz.get('collect_cooldown')
        if last_collect:
            last_time = safe_parse_datetime(last_collect)
            if last_time and (now - last_time).total_seconds() < 3600:
                continue

        income_per_hour = await calculate_business_income(biz)
        total_income += income_per_hour

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE businesses SET collect_cooldown = ? WHERE id = ?",
                (now.isoformat(), biz['id'])
            )
            await db.commit()

    if total_income > 0:
        ge = await get_global_economy()
        if random.random() < ge["business_tax_chance"]:
            tax = int(total_income * ge["business_tax_take_pct"])
            tax = max(1, tax)
            total_income -= tax
            await update_balance(user_id, -tax, 'vitalik_tax', 'Конфискация Виталика за бизнес')
            try:
                await bot.send_message(
                    user_id,
                    f"🚨 *ВИТАЛИК НАГРЯНУЛ!*\n\n"
                    f"Налоговая проверила твой бизнес.\n"
                    f"Конфисковано: {format_money(tax)}\n"
                    f"Осталось дохода: {format_money(total_income)}",
                    parse_mode="Markdown",
                )
            except Exception:
                pass
            if random.random() < 0.2:
                await post_chronicle(
                    f"📉 Налоговый рейд: конфискация *{format_money(tax)}* у предпринимателя (игрок `{user_id}`)."
                )

        await update_balance(user_id, total_income, 'business_income', 'Пассивный доход с бизнесов')

    return total_income

async def get_business_collect_status(user_id: int) -> Dict[str, Any]:
    """
    Возвращает статус сбора дохода:
    - total_income: сколько можно собрать сейчас (0 если ничего)
    - can_collect: bool (есть ли хоть один бизнес с готовым доходом)
    - next_collect_time: datetime самого близкого бизнеса, который станет доступен
    - seconds_left: секунд до следующего сбора (если can_collect=False)
    - total_per_hour: общий доход в час
    """
    businesses = await get_user_businesses(user_id)
    total_per_hour = 0
    now = datetime.now()
    next_collect_time = None
    total_income_now = 0

    for biz in businesses:
        income = await calculate_business_income(biz)
        total_per_hour += income

        last_collect = biz.get('collect_cooldown')
        if not last_collect:
            total_income_now += income
            continue

        last_time = safe_parse_datetime(last_collect)
        if not last_time:
            total_income_now += income
            continue

        time_passed = (now - last_time).total_seconds()
        if time_passed >= 3600:
            total_income_now += income
        else:
            collect_available = last_time + timedelta(hours=1)
            if next_collect_time is None or collect_available < next_collect_time:
                next_collect_time = collect_available

    result = {
        "total_income": total_income_now,
        "can_collect": total_income_now > 0,
        "total_per_hour": total_per_hour,
        "next_collect_time": next_collect_time,
        "seconds_left": int((next_collect_time - now).total_seconds()) if next_collect_time and not total_income_now else 0
    }
    return result

async def get_business_upgrades(business_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM business_upgrades WHERE business_id = ? ORDER BY upgrade_level",
            (business_id,)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def upgrade_business(user_id: int, business_id: int, upgrade_lvl: int) -> tuple[bool, str]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM businesses WHERE id = ? AND owner_id = ?",
                                 (business_id, user_id))
        biz = await cursor.fetchone()
        if not biz:
            return False, "❌ Бизнес не найден или не принадлежит тебе."
        biz = dict(biz)

    config = BUSINESS_TYPES[biz['biz_type']]
    upgrade = config['upgrades'].get(upgrade_lvl)
    if not upgrade:
        return False, "❌ Улучшение не найдено."

    existing = await get_business_upgrades(business_id)
    if any(u['upgrade_level'] == upgrade_lvl for u in existing):
        return False, "❌ Это улучшение уже установлено."

    if biz['upgrade_level'] + 1 != upgrade_lvl:
        return False, f"❌ Сначала нужно улучшить до уровня {biz['upgrade_level'] + 1}."

    user = await get_user(user_id)
    if user['balance'] < upgrade['cost']:
        return False, f"❌ Не хватает {format_money(upgrade['cost'])}."

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            '''INSERT INTO business_upgrades
               (business_id, upgrade_name, upgrade_level, bonus_income, bonus_percent, bonus_duel, bonus_asphalt, cost)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (business_id, upgrade['name'], upgrade_lvl,
             upgrade.get('income_bonus', 0),
             upgrade.get('salary_bonus', 0.0),
             upgrade.get('duel_bonus', 0.0),
             upgrade.get('asphalt_bonus', 0.0),
             upgrade['cost'])
        )
        await db.execute(
            "UPDATE businesses SET upgrade_level = ? WHERE id = ?",
            (upgrade_lvl, business_id)
        )
        await db.execute(
            "UPDATE players SET balance = balance - ? WHERE user_id = ?",
            (upgrade['cost'], user_id)
        )
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'business_upgrade', -?, ?)",
            (user_id, upgrade['cost'], f"Улучшение {upgrade['name']} для {config['name']}")
        )
        await db.commit()

    return True, f"✅ Улучшение '{upgrade['name']}' установлено!"

async def get_total_business_bonuses(user_id: int) -> Dict[str, float]:
    businesses = await get_user_businesses(user_id)
    bonuses = {"salary": 0.0, "duel": 0.0, "asphalt": 0.0}

    for biz in businesses:
        config = BUSINESS_TYPES[biz['biz_type']]
        bonuses["salary"] += config.get('salary_bonus', 0.0)
        bonuses["asphalt"] += config.get('asphalt_bonus', 0.0)

        upgrades = await get_business_upgrades(biz['id'])
        for up in upgrades:
            bonuses["salary"] += up.get('bonus_percent', 0.0)
            bonuses["asphalt"] += up.get('bonus_asphalt', 0.0)
    bonuses["duel"] = 0.0
    return bonuses

# ==================== БАНК (кредиты / проценты / коллекторы / пул) ====================
async def get_bank_pool_liquidity() -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT liquidity FROM bank_pool WHERE id = 1")
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def bank_pool_add(amount: int) -> None:
    if amount <= 0:
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE bank_pool SET liquidity = liquidity + ? WHERE id = 1", (amount,))
        await db.commit()


async def bank_pool_try_take(amount: int) -> bool:
    """Списать из кассы банка (выдача кредита)."""
    if amount <= 0:
        return True
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT liquidity FROM bank_pool WHERE id = 1")
        row = await cur.fetchone()
        liq = int(row[0]) if row else 0
        # Казна считается бесконечной: если не хватило ликвидности — "доначисляем".
        if liq < amount:
            await db.execute("UPDATE bank_pool SET liquidity = liquidity + ? WHERE id = 1", (amount - liq,))
            await db.commit()
        await db.execute("UPDATE bank_pool SET liquidity = liquidity - ? WHERE id = 1", (amount,))
        await db.commit()
    return True


async def get_user_deposit(user_id: int) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT amount FROM bank_deposits WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def get_user_deposit_details(user_id: int) -> Dict[str, Any]:
    """Для отображения: сумма депозита, сколько процентов накоплено и когда было последнее начисление."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT amount, total_interest, last_interest_at FROM bank_deposits WHERE user_id = ?",
            (user_id,),
        )
        row = await cur.fetchone()
        if not row:
            return {"amount": 0, "total_interest": 0, "last_interest_at": None}
        return {
            "amount": int(row["amount"] or 0),
            "total_interest": int(row["total_interest"] or 0),
            "last_interest_at": row["last_interest_at"],
        }


async def bank_player_deposit(user_id: int, amount: int) -> tuple[bool, str]:
    if amount < BANK_SETTINGS["min_deposit"]:
        return False, f"❌ Минимальный вклад: {format_money(BANK_SETTINGS['min_deposit'])}."
    user = await get_user(user_id)
    if not user or user["balance"] < amount:
        return False, "❌ Недостаточно средств на балансе."
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE players SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
            (amount, user_id, amount),
        )
        cur = await db.execute("SELECT changes()")
        ch = (await cur.fetchone())[0]
        if ch != 1:
            return False, "❌ Не удалось списать баланс."
        await db.execute(
            """INSERT INTO bank_deposits (user_id, amount, total_interest, last_interest_at)
               VALUES (?, ?, 0, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                   amount = amount + excluded.amount,
                   last_interest_at = excluded.last_interest_at""",
            (user_id, amount, now),
        )
        await db.execute("UPDATE bank_pool SET liquidity = liquidity + ? WHERE id = 1", (amount,))
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'bank_deposit', -?, ?)",
            (user_id, amount, "Вклад в кассу «Асфальт-Капитал»"),
        )
        await db.commit()
    return True, f"✅ В кассе банка +{format_money(amount)}. Спасибо, коллега!"

async def bank_accrue_deposit_interest_for_user(user_id: int) -> None:
    """Начисляет проценты на вклад конкретного пользователя (для 'снятия в любой момент' без потери процентов)."""
    now_dt = datetime.now()
    now_iso = now_dt.isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT amount, total_interest, last_interest_at FROM bank_deposits WHERE user_id = ? AND amount > 0",
            (user_id,),
        )
        row = await cur.fetchone()
        if not row:
            return
        amt = int(row["amount"])
        total_interest = int(row["total_interest"] or 0)
        last = safe_parse_datetime(row["last_interest_at"]) or now_dt
        hours = max(0.0, (now_dt - last).total_seconds() / 3600.0)
        if hours <= 0:
            return
        rate = float(BANK_SETTINGS["deposit_hourly_rate"])
        pay = int(amt * rate * hours)
        if pay < 1:
            # Всё равно обновляем last_interest_at, чтобы не копить "висящие" доли.
            await db.execute(
                "UPDATE bank_deposits SET last_interest_at = ? WHERE user_id = ?",
                (now_iso, user_id),
            )
            await db.commit()
            return

        await db.execute(
            "UPDATE bank_deposits SET amount = amount + ?, total_interest = total_interest + ?, last_interest_at = ? WHERE user_id = ?",
            (pay, pay, now_iso, user_id),
        )
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'bank_deposit_interest', ?, ?)",
            (user_id, pay, "%% по вкладу «Асфальт-Капитал»"),
        )
        await db.commit()

    await add_daily_earned(user_id, pay)
    try:
        await bot.send_message(user_id, f"🏦 Проценты по вкладу обновлены: {format_money(pay)}.")
    except Exception:
        pass


async def bank_player_withdraw(user_id: int, amount: int) -> tuple[bool, str]:
    if amount <= 0:
        return False, "❌ Сумма должна быть > 0."
    await bank_accrue_deposit_interest_for_user(user_id)
    dep = await get_user_deposit(user_id)
    if dep <= 0:
        return False, "❌ На вкладе нет средств."
    if amount > dep:
        return False, f"❌ На вкладе только {format_money(dep)}."
    async with aiosqlite.connect(DB_NAME) as db:
        # Убедимся, что касса (казна) может выплатить.
        pool = await get_bank_pool_liquidity()
        if pool < amount:
            await bank_pool_add(amount - pool)

        await db.execute(
            "UPDATE bank_pool SET liquidity = liquidity - ? WHERE id = 1 AND liquidity >= ?",
            (amount, amount),
        )
        cur = await db.execute("SELECT changes()")
        if (await cur.fetchone())[0] != 1:
            return False, "❌ Касса не подтвердила списание."
        await db.execute(
            "UPDATE bank_deposits SET amount = amount - ? WHERE user_id = ? AND amount >= ?",
            (amount, user_id, amount),
        )
        cur = await db.execute("SELECT changes()")
        if (await cur.fetchone())[0] != 1:
            await db.execute("UPDATE bank_pool SET liquidity = liquidity + ? WHERE id = 1", (amount,))
            await db.commit()
            return False, "❌ Ошибка вклада."
        await db.execute(
            "UPDATE players SET balance = balance + ? WHERE user_id = ?", (amount, user_id)
        )
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'bank_withdraw', ?, ?)",
            (user_id, amount, "Возврат вклада из «Асфальт-Капитал»"),
        )
        await db.commit()
    return True, f"✅ {format_money(amount)} возвращены на баланс."


async def bank_player_close_deposit(user_id: int) -> tuple[bool, str]:
    """Закрывает депозит и забирает всю сумму вместе с накопленными процентами."""
    await bank_accrue_deposit_interest_for_user(user_id)
    dep_details = await get_user_deposit_details(user_id)
    dep_amt = int(dep_details["amount"])
    dep_interest = int(dep_details["total_interest"])
    if dep_amt <= 0:
        return False, "❌ У тебя нет активного депозита."

    pool = await get_bank_pool_liquidity()
    if pool < dep_amt:
        await bank_pool_add(dep_amt - pool)

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE bank_pool SET liquidity = liquidity - ? WHERE id = 1 AND liquidity >= ?",
            (dep_amt, dep_amt),
        )
        cur = await db.execute("SELECT changes()")
        if (await cur.fetchone())[0] != 1:
            await db.rollback()
            return False, "❌ Касса банка не смогла подтвердить закрытие депозита."
        await db.execute(
            "UPDATE bank_deposits SET amount = 0, total_interest = 0, last_interest_at = NULL WHERE user_id = ?",
            (user_id,),
        )
        await db.execute("UPDATE players SET balance = balance + ? WHERE user_id = ?", (dep_amt, user_id))
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'bank_withdraw', ?, ?)",
            (user_id, dep_amt, "Возврат вклада из «Асфальт-Капитал» (закрытие депозита)"),
        )
        await db.commit()

    return True, (
        f"✅ Депозит закрыт!\n"
        f"Забрал: {format_money(dep_amt)}.\n"
        f"Проценты за всё время: {format_money(dep_interest)}."
    )


async def bank_deposit_interest_tick():
    """Проценты по вкладам: начисляем на сумму вклада (без прямого выдачи на баланс)."""
    now = datetime.now()
    rate = float(BANK_SETTINGS["deposit_hourly_rate"])
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT user_id, amount, total_interest, last_interest_at FROM bank_deposits WHERE amount > 0"
        )
        rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        uid = int(r["user_id"])
        amt = int(r["amount"])
        last = safe_parse_datetime(r.get("last_interest_at")) or now
        hours = max(0.0, (now - last).total_seconds() / 3600.0)
        if hours < 0.95:
            continue
        pay = int(amt * rate * hours)
        if pay < 1:
            continue
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE bank_deposits SET amount = amount + ?, total_interest = total_interest + ?, last_interest_at = ? WHERE user_id = ?",
                (pay, pay, now.isoformat(), uid),
            )
            await db.execute(
                "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'bank_deposit_interest', ?, ?)",
                (uid, pay, "%% по вкладу «Асфальт-Капитал»"),
            )
            await db.commit()
        await add_daily_earned(uid, pay)
        try:
            await bot.send_message(
                uid,
                f"🏦 Начислены проценты по вкладу: {format_money(pay)} (на вклад)."
            )
        except Exception:
            pass


async def get_active_bank_loan(user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM asphalt_loans WHERE user_id = ? AND paid_off = 0 ORDER BY id DESC LIMIT 1""",
            (user_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def issue_bank_loan(user_id: int, amount: int) -> tuple[bool, str]:
    if amount < BANK_SETTINGS["min_loan"]:
        return False, f"❌ Минимум {format_money(BANK_SETTINGS['min_loan'])}."
    if await get_active_bank_loan(user_id):
        return False, "❌ У тебя уже есть активный кредит. Погаси его в банке."
    user = await get_user(user_id)
    if not user:
        return False, "❌ Пользователь не найден."

    # Соц.статус «Мафиози» расширяет кредитный лимит.
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT COUNT(*) AS cnt FROM businesses WHERE owner_id = ? AND is_active = 1",
            (user_id,),
        )
        biz_cnt = int((await cur.fetchone())[0] or 0)
    is_mafia = int(user.get("duels_won") or 0) >= 100 and biz_cnt >= 10 and int(user.get("balance") or 0) >= 1_000_000
    max_cap = int(BANK_SETTINGS["max_loan"] * (1.5 if is_mafia else 1.0))
    if amount > max_cap:
        return False, f"❌ Максимум для твоего статуса: {format_money(max_cap)}."
    rep = await get_reputation_percent(user_id, user)
    # Честное решение без рандома:
    # - низкая репутация: отказ
    # - средняя: одобрение, но лимит урезан
    # - высокая: одобрение в полном лимите
    if rep < 40.0:
        return (
            False,
            "❌ Асфальт-Капитал отказал: репутация слишком низкая (< 40/100). "
            "Подними рейтинг после переводов и дуэлей.",
        )
    effective_cap = max_cap if rep >= 70.0 else int(max_cap * 0.6)
    if amount > effective_cap:
        return (
            False,
            f"❌ Для вашей репутации доступно до {format_money(effective_cap)}. "
            "Подними рейтинг, чтобы увеличить лимит.",
        )
    # Казна считается бесконечной: если не хватило, pool будет “доначислен” автоматически.
    await bank_pool_try_take(amount)
    now = datetime.now()
    due = now + timedelta(hours=BANK_SETTINGS["term_hours"])
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                """INSERT INTO asphalt_loans
                   (user_id, principal, remaining, issued_at, due_at, last_accrual_at, defaulted, paid_off)
                   VALUES (?, ?, ?, ?, ?, ?, 0, 0)""",
                (user_id, amount, amount, now.isoformat(), due.isoformat(), now.isoformat()),
            )
            await db.commit()
        await update_balance(user_id, amount, "bank_loan", "Кредит «Асфальт-Капитал»")
    except Exception as e:
        logger.error(f"Кредит: откат пула после ошибки: {e}")
        await bank_pool_add(amount)
        return False, "❌ Техническая ошибка при выдаче кредита. Попробуй позже."
    return True, f"✅ {format_money(amount)} выданы из казны банка. Вернёшь с процентами до {due.strftime('%d.%m %H:%M')}."


async def _set_loan_remaining(loan_id: int, remaining: int, paid_off: int = 0):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE asphalt_loans SET remaining = ?, paid_off = ? WHERE id = ?",
            (remaining, paid_off, loan_id),
        )
        await db.commit()


async def repay_bank_loan(user_id: int, amount: int) -> tuple[bool, str]:
    if amount <= 0:
        return False, "❌ Сумма должна быть больше нуля."
    loan = await get_active_bank_loan(user_id)
    if not loan:
        return False, "❌ Нет активного кредита."
    user = await get_user(user_id)
    if not user or user["balance"] < amount:
        return False, f"❌ Недостаточно средств (нужно {format_money(amount)})."
    pay = min(amount, loan["remaining"], user["balance"])
    new_rem = loan["remaining"] - pay
    await update_balance(user_id, -pay, "bank_repay", "Погашение кредита Асфальт-Капитал")
    await bank_pool_add(pay)
    if new_rem <= 0:
        await _set_loan_remaining(loan["id"], 0, 1)
        return True, f"✅ Долг закрыт! Внесено {format_money(pay)}."
    await _set_loan_remaining(loan["id"], new_rem, 0)
    return True, f"✅ Внесено {format_money(pay)}. Остаток долга: {format_money(new_rem)}."


async def _on_loan_first_default(user_id: int) -> None:
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE players SET loans_defaulted = loans_defaulted + 1 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()
    u = await get_user(user_id)
    nm = u["full_name"] if u else str(user_id)
    await post_chronicle(
        f"📉 *{nm}* влетел в просрочку по кредиту — коллекторы «Асфальт-Капитал» уже в пути."
    )
    for line in await check_achievements_for_user(user_id):
        try:
            await bot.send_message(user_id, f"🎖️ *Новое достижение!*\n\n{line}", parse_mode="Markdown")
        except Exception:
            pass


async def bank_accrue_interest_tick():
    """Начисляет проценты пропорционально времени с last_accrual_at; помечает просрочку."""
    now = datetime.now()
    rate = BANK_SETTINGS["hourly_interest_rate"]
    first_defaults: List[int] = []
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM asphalt_loans WHERE paid_off = 0 AND remaining > 0"
        )
        rows = await cursor.fetchall()
        for row in rows:
            loan = dict(row)
            last = safe_parse_datetime(loan["last_accrual_at"]) or now
            elapsed_h = max(0.0, (now - last).total_seconds() / 3600.0)
            if elapsed_h < 1 / 60:  # меньше минуты — пропуск
                continue
            add = int(loan["remaining"] * rate * elapsed_h)
            if add < 1 and loan["remaining"] > 0 and elapsed_h >= 0.25:
                add = 1
            new_rem = loan["remaining"] + add
            due = safe_parse_datetime(loan["due_at"])
            was_def = int(loan["defaulted"])
            defaulted = was_def
            if due and now > due and new_rem > 0:
                defaulted = 1
            if defaulted == 1 and was_def == 0:
                first_defaults.append(int(loan["user_id"]))
            await db.execute(
                "UPDATE asphalt_loans SET remaining = ?, last_accrual_at = ?, defaulted = ? WHERE id = ?",
                (new_rem, now.isoformat(), defaulted, loan["id"]),
            )
        await db.commit()
    for uid in dict.fromkeys(first_defaults):
        await _on_loan_first_default(uid)

    second_defaults: List[int] = []
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM asphalt_loans WHERE paid_off = 0 AND remaining > 0 AND defaulted = 0"
        )
        for row in await cursor.fetchall():
            loan = dict(row)
            due = safe_parse_datetime(loan["due_at"])
            if due and now > due:
                cur = await db.execute(
                    "UPDATE asphalt_loans SET defaulted = 1 WHERE id = ? AND defaulted = 0",
                    (loan["id"],),
                )
                if getattr(cur, "rowcount", 0) and cur.rowcount > 0:
                    second_defaults.append(int(loan["user_id"]))
        await db.commit()
    for uid in dict.fromkeys(second_defaults):
        await _on_loan_first_default(uid)


async def bank_collector_tick():
    """Коллекторы списывают часть баланса в счёт долга при просрочке."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM asphalt_loans WHERE paid_off = 0 AND remaining > 0 AND defaulted = 1"
        )
        loans = [dict(r) for r in await cursor.fetchall()]
    mn = int(BANK_SETTINGS.get("collector_min_seize", 1))
    min_pct = float(BANK_SETTINGS.get("collector_seize_balance_min_pct", 0.10))
    max_pct = float(BANK_SETTINGS.get("collector_seize_balance_max_pct", 0.20))
    for loan in loans:
        uid = loan["user_id"]
        user = await get_user(uid)
        if not user or user["balance"] <= 0:
            continue
        seize_pct = random.uniform(min_pct, max_pct)
        seize = int(user["balance"] * seize_pct)
        seize = max(mn, seize)
        seize = min(seize, user["balance"], loan["remaining"])
        if seize <= 0:
            continue
        await update_balance(
            uid,
            -seize,
            "collector",
            "Коллекторы Асфальт-Капитал (просрочка)",
        )
        await bank_pool_add(seize)
        new_rem = loan["remaining"] - seize
        if new_rem <= 0:
            await _set_loan_remaining(loan["id"], 0, 1)
        else:
            await _set_loan_remaining(loan["id"], new_rem, 0)
        try:
            await bot.send_message(
                uid,
                f"🕴️ *Коллекторы «Асфальт-Капитал»*\n\n"
                f"Просрочка по кредиту. С баланса удержано {format_money(seize)}.\n"
                f"Остаток долга: {format_money(max(new_rem, 0))}\n\n"
                f"_Погаси долг в меню банка, пока не стало больнее._",
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Коллектор: не удалось написать {uid}: {e}")


async def bank_scheduler():
    """Проценты по кредитам, коллекторы, %% по вкладам."""
    last_accrual = time.monotonic()
    last_collector = time.monotonic()
    last_dep_int = time.monotonic()
    dep_int_sec = float(BANK_SETTINGS.get("deposit_interest_interval_sec", 3600))
    while True:
        try:
            await asyncio.sleep(60)
            now_ts = time.monotonic()
            if now_ts - last_accrual >= BANK_SETTINGS["accrual_interval_sec"]:
                await bank_accrue_interest_tick()
                last_accrual = now_ts
            if now_ts - last_collector >= BANK_SETTINGS["collector_interval_sec"]:
                await bank_collector_tick()
                last_collector = now_ts
            if now_ts - last_dep_int >= dep_int_sec:
                await bank_deposit_interest_tick()
                last_dep_int = now_ts
        except Exception as e:
            logger.error(f"Ошибка планировщика банка: {e}")
            await asyncio.sleep(120)


def get_bank_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💰 Инвестировать", callback_data="bank_dep_start"),
                InlineKeyboardButton(text="📈 Взять кредит", callback_data="bank_loan_start"),
            ],
            [
                InlineKeyboardButton(text="💳 Погасить долг", callback_data="bank_repay_start"),
                InlineKeyboardButton(text="🏦 Закрыть депозит", callback_data="bank_dep_close"),
            ],
            [
                InlineKeyboardButton(text="📋 Статус", callback_data="bank_status"),
            ],
            [InlineKeyboardButton(text="🔙 Закрыть", callback_data="bank_close")],
        ]
    )


# ==================== ФОРМАТИРОВАНИЕ ====================
def format_money(amount: int) -> str:
    return f"{amount:,}₽".replace(",", " ")

def format_time(seconds: int) -> str:
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes}:{secs:02d}"

# ==================== КЛАВИАТУРЫ ====================
def get_main_keyboard(user_id: int = None) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="💰 Получка"), KeyboardButton(text="🛒 Магазин")],
        [KeyboardButton(text="🔁 Перевод"), KeyboardButton(text="🎮 Мини-игры")],
        [KeyboardButton(text="🏢 Бизнес"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="🎒 Инвентарь"), KeyboardButton(text="🏅 Ачивки")],
        [KeyboardButton(text="👥 Рефералы")],
        [KeyboardButton(text="💊 Эффекты"), KeyboardButton(text="🏦 Асфальт-Капитал")],
    ]
    if user_id == ADMIN_ID:
        keyboard.append([KeyboardButton(text="👑 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_shop_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    boosts = [item for item in SHOP_ITEMS if item.get("type") == "boost"]
    pills = [item for item in SHOP_ITEMS if item.get("type") == "pill"]
    protection = [item for item in SHOP_ITEMS if item.get("type") in ["protection", "insurance"]]
    other = [item for item in SHOP_ITEMS if item.get("type") in ["antidote", "lottery", "instant"]]

    if boosts:
        buttons.append([InlineKeyboardButton(text="📈 БУСТЫ К ЗАРПЛАТЕ", callback_data="none")])
        for item in boosts:
            buttons.append([InlineKeyboardButton(text=f"{item['name']} - {format_money(item['price'])}", callback_data=f"buy_{item['id']}")])
    if pills:
        buttons.append([InlineKeyboardButton(text="💊 ТАБЛЕТКИ НАГИРТ", callback_data="none")])
        for item in pills:
            buttons.append([InlineKeyboardButton(text=f"{item['name']} - {format_money(item['price'])}", callback_data=f"buy_{item['id']}")])
    if protection:
        buttons.append([InlineKeyboardButton(text="🛡️ ЗАЩИТА", callback_data="none")])
        for item in protection:
            buttons.append([InlineKeyboardButton(text=f"{item['name']} - {format_money(item['price'])}", callback_data=f"buy_{item['id']}")])
    for item in other:
        buttons.append([InlineKeyboardButton(text=f"{item['name']} - {format_money(item['price'])}", callback_data=f"buy_{item['id']}")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main"), InlineKeyboardButton(text="❌ Закрыть", callback_data="shop_close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ==================== МАГАЗИН: КАТЕГОРИИ ====================
SHOP_CATEGORY_ORDER = ["boosts", "nagirt", "protection", "inventory", "special", "misc"]
SHOP_CATEGORY_META = {
    "boosts": "📈 Бусты",
    "nagirt": "💊 Нагирт",
    "protection": "🛡️ Защита",
    "inventory": "📦 Инвентарь",
    "special": "🎲 Особое",
    "misc": "🎁 Разное",
}


def get_shop_categories_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for key in SHOP_CATEGORY_ORDER:
        if any(item.get("category") == key for item in SHOP_ITEMS):
            buttons.append([InlineKeyboardButton(text=SHOP_CATEGORY_META.get(key, key), callback_data=f"shop_cat_{key}")])
    buttons.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main"),
        InlineKeyboardButton(text="❌ Закрыть", callback_data="shop_close"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_minigames_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🎰 Рулетка", callback_data="game_roulette")],
        [InlineKeyboardButton(text="🎲 Кости (чёт / нечёт)", callback_data="game_dice")],
        [InlineKeyboardButton(text="🛣️ Укладка асфальта", callback_data="game_asphalt")],
        [InlineKeyboardButton(text="⚔️ Дуэль", callback_data="game_duel")],
        [InlineKeyboardButton(text="💣 Мины", callback_data="game_mines")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_asphalt_keyboard(can_work: bool = True) -> InlineKeyboardMarkup:
    if can_work:
        buttons = [[InlineKeyboardButton(text="🛣️ Уложить асфальт", callback_data="lay_asphalt")]]
    else:
        buttons = [[InlineKeyboardButton(text="⏳ Жди 30 сек", callback_data="asphalt_wait")]]
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_games")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_users_keyboard(
    users: List[Dict[str, Any]],
    exclude_id: int,
    callback_prefix: str = "transfer_to_",
    cancel_callback: str = "cancel_transfer",
) -> InlineKeyboardMarkup:
    buttons = []
    for user in users:
        if user['user_id'] != exclude_id:
            name = user['full_name']
            if len(name) > 20:
                name = name[:17] + "..."
            buttons.append([InlineKeyboardButton(
                text=f"👤 {name} ({format_money(user['balance'])})",
                callback_data=f"{callback_prefix}{user['user_id']}"
            )])
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="⚡ Штраф", callback_data="admin_fine")],
        [InlineKeyboardButton(text="🎁 Бонус", callback_data="admin_bonus")],
        [InlineKeyboardButton(text="🏦 Влить в кассу банка", callback_data="admin_bank_inject")],
        [InlineKeyboardButton(text="📈 Экономика (штрафы/налоги/комиссия)", callback_data="admin_economy")],
        [InlineKeyboardButton(text="💣 Настройка Mines", callback_data="admin_mines")],
        [InlineKeyboardButton(text="🧾 Чеки", callback_data="admin_checks")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_broadcast_cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена рассылки")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_back_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔙 Назад")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_state_back_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="state_back")]]
    )

def get_admin_checks_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="💰 Создать денежный чек", callback_data="admin_check_money")],
        [InlineKeyboardButton(text="🎁 Создать товарный чек", callback_data="admin_check_item")],
        [InlineKeyboardButton(text="📊 Список активных чеков", callback_data="admin_checks_list")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_items_for_checks() -> InlineKeyboardMarkup:
    buttons = []
    boosts = [item for item in SHOP_ITEMS if item.get("type") == "boost"]
    pills = [item for item in SHOP_ITEMS if item.get("type") == "pill"]
    other = [item for item in SHOP_ITEMS if item.get("type") in ["antidote", "insurance", "lottery", "instant"]]
    if boosts:
        buttons.append([InlineKeyboardButton(text="📈 БУСТЫ", callback_data="none")])
        for item in boosts[:3]:
            buttons.append([InlineKeyboardButton(text=f"{item['name']}", callback_data=f"check_item_{item['id']}")])
    if pills:
        buttons.append([InlineKeyboardButton(text="💊 ТАБЛЕТКИ", callback_data="none")])
        for item in pills:
            buttons.append([InlineKeyboardButton(text=f"{item['name']}", callback_data=f"check_item_{item['id']}")])
    if other:
        for item in other:
            buttons.append([InlineKeyboardButton(text=f"{item['name']}", callback_data=f"check_item_{item['id']}")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_check_item")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==================== МАШИНЫ СОСТОЯНИЙ ====================
class TransferStates(StatesGroup):
    choosing_recipient = State()
    entering_amount = State()
    confirming = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()

class AdminFineStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_amount = State()

class AdminBonusStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_amount = State()


class AdminBankInjectStates(StatesGroup):
    waiting_amount = State()

class AdminEconomySetStates(StatesGroup):
    waiting_value = State()


class CheckStates(StatesGroup):
    waiting_for_check_amount = State()
    waiting_for_check_uses = State()
    waiting_for_check_hours = State()
    waiting_for_check_message = State()

class DuelStates(StatesGroup):
    choosing_opponent = State()
    waiting_bet_amount = State()
    waiting_confirmation = State()


class BankStates(StatesGroup):
    waiting_custom_loan = State()
    waiting_repay = State()
    waiting_deposit_amt = State()
    waiting_withdraw_amt = State()


class DiceStates(StatesGroup):
    waiting_for_bet = State()


class RouletteStates(StatesGroup):
    waiting_for_bet = State()


class MinesStates(StatesGroup):
    choosing_mines = State()
    waiting_bet = State()
    playing = State()


class InventoryStates(StatesGroup):
    choosing_gift_target = State()
    choosing_gift_stack = State()


@dp.message(StateFilter("*"), F.text.in_({"🔙 Назад", "Назад", "назад"}))
async def handle_global_back_from_state(message: Message, state: FSMContext):
    if await state.get_state() is None:
        return
    await state.clear()
    await message.answer("↩️ Действие отменено. Главное меню:", reply_markup=get_main_keyboard(message.from_user.id))


@dp.callback_query(F.data == "state_back")
async def handle_global_inline_back(callback: CallbackQuery, state: FSMContext):
    if await state.get_state() is None:
        await callback.answer("Нет активного действия", show_alert=False)
        return
    await state.clear()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer("↩️ Действие отменено. Главное меню:", reply_markup=get_main_keyboard(callback.from_user.id))
    await callback.answer()


# ==================== АКТИВНЫЕ ДУЭЛИ ====================
active_duels = {}
DUEL_TIMEOUT = 60
active_mines_games = {}  # {user_id: {bet, mines_count, mines, opened, message_id}}

# ==================== СИСТЕМА ЧЕКОВ ====================
def generate_check_id() -> str:
    chars = string.ascii_uppercase + string.digits
    return 'CHK_' + ''.join(random.choices(chars, k=12))

async def create_gift_check(creator_id: int, check_type: str, amount: int = 0,
                           item_id: str = None, max_uses: int = 1, hours: int = 24,
                           message: str = "") -> str:
    check_id = generate_check_id()
    created_at = datetime.now()
    expires_at = created_at + timedelta(hours=hours)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT INTO gift_checks 
            (check_id, creator_id, check_type, amount, item_id, max_uses, 
             created_at, expires_at, custom_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (check_id, creator_id, check_type, amount, item_id, max_uses,
              created_at.isoformat(), expires_at.isoformat(), message))
        await db.commit()
    return check_id

async def activate_gift_check_by_link(user_id: int, check_id: str) -> Dict[str, Any]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM gift_checks 
            WHERE check_id = ? AND is_active = 1 
            AND (expires_at IS NULL OR expires_at > ?)
        ''', (check_id, datetime.now().isoformat()))
        check = await cursor.fetchone()
        if not check:
            return {"success": False, "error": "Чек не найден или недействителен"}
        check = dict(check)
        if check['used_count'] >= check['max_uses']:
            return {"success": False, "error": "Лимит использований исчерпан"}
        cursor = await db.execute('''
            SELECT 1 FROM check_activations 
            WHERE check_id = ? AND user_id = ?
        ''', (check_id, user_id))
        already_used = await cursor.fetchone()
        if already_used:
            return {"success": False, "error": "Вы уже активировали этот чек"}

        await db.execute('''
            UPDATE gift_checks 
            SET used_count = used_count + 1, last_used = ?
            WHERE check_id = ?
        ''', (datetime.now().isoformat(), check_id))
        await db.execute('''
            INSERT INTO check_activations (check_id, user_id, activated_at)
            VALUES (?, ?, ?)
        ''', (check_id, user_id, datetime.now().isoformat()))
        await db.commit()

        reward_text = ""
        success = True
        error_message = None

        try:
            if check['check_type'] == 'money':
                amount = check['amount']
                await update_balance(user_id, amount, "check", f"Активация чека {check_id}")
                await db.execute('''
                    UPDATE check_activations 
                    SET received_amount = ?
                    WHERE check_id = ? AND user_id = ?
                ''', (amount, check_id, user_id))
                await db.commit()
                reward_text = f"{format_money(amount)}"
            elif check['check_type'] == 'item':
                item_id = check['item_id']
                item = next((i for i in SHOP_ITEMS if i["id"] == item_id), None)
                if item:
                    ok_inv, _ = await add_inventory_item(user_id, item["id"], 1)
                    if ok_inv:
                        await db.execute('''
                            UPDATE check_activations 
                            SET received_item = ?
                            WHERE check_id = ? AND user_id = ?
                        ''', (item['name'], check_id, user_id))
                        await db.commit()
                        reward_text = f"{item['name']} (в инвентарь)"
                    else:
                        reward_text = "инвентарь полон — предмет не выдан"
                else:
                    reward_text = "неизвестный предмет"
        except Exception as e:
            logger.error(f"Ошибка выдачи награды чека {check_id}: {e}")
            success = False
            error_message = "Техническая ошибка при активации"

        cursor = await db.execute('''
            SELECT full_name FROM players WHERE user_id = ?
        ''', (check['creator_id'],))
        creator = await cursor.fetchone()
        creator_name = creator[0] if creator else "Администрация"

        return {
            "success": success,
            "amount": check.get('amount'),
            "item": check.get('item_id'),
            "reward_text": reward_text,
            "message": check.get('custom_message', ''),
            "creator_name": creator_name,
            "used_count": check['used_count'] + 1,
            "max_uses": check['max_uses'],
            "error": error_message
        }

async def get_active_checks() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM gift_checks 
            WHERE is_active = 1 AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY created_at DESC
        ''', (datetime.now().isoformat(),))
        checks = await cursor.fetchall()
        return [dict(check) for check in checks]

async def get_check_stats(check_id: str) -> Dict[str, Any]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT g.*, u.full_name as creator_name 
            FROM gift_checks g
            LEFT JOIN players u ON g.creator_id = u.user_id
            WHERE g.check_id = ?
        ''', (check_id,))
        check = await cursor.fetchone()
        if not check:
            return None
        check = dict(check)
        cursor = await db.execute('''
            SELECT ca.*, p.full_name as user_name 
            FROM check_activations ca
            LEFT JOIN players p ON ca.user_id = p.user_id
            WHERE ca.check_id = ?
            ORDER BY ca.activated_at DESC
        ''', (check_id,))
        activations = await cursor.fetchall()
        check['activations'] = [dict(act) for act in activations]
        return check

async def deactivate_check(check_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            UPDATE gift_checks SET is_active = 0 WHERE check_id = ?
        ''', (check_id,))
        await db.commit()


# ==================== ОСНОВНЫЕ ОБРАБОТЧИКИ ====================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    args = message.text.split()
    referrer_id: Optional[int] = None
    # Проверка обязательной подписки на канал.
    if not await is_user_subscribed(message.from_user.id):
        await message.answer(
            "Чтобы пользоваться ботом, подпишись на канал и подтверди подписку.",
            reply_markup=get_subscribe_keyboard(),
        )
        return
    if len(args) > 1:
        payload = args[1].strip()
        tmp = payload.lower()
        # Поддерживаем оба формата: `ref_<id>` и (иногда из-за форматирования) `ref<id>`.
        if tmp.startswith("ref"):
            suffix = payload[3:]
            if suffix.startswith("_"):
                suffix = suffix[1:]
            try:
                referrer_id = int(suffix)
            except ValueError:
                # Если не получилось разобрать число — дальше пробуем трактовать как check_id.
                referrer_id = None
        else:
            check_id = payload.upper()
            async with aiosqlite.connect(DB_NAME) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT 1 FROM gift_checks 
                    WHERE check_id = ? AND is_active = 1
                    AND (expires_at IS NULL OR expires_at > ?)
                ''', (check_id, datetime.now().isoformat()))
                check_exists = await cursor.fetchone()
            if check_exists:
                await handle_check_activation(message, check_id)
                return
        # Если это `ref...` и referrer_id разобрался — дальше регистрируем пользователя.
        # Если referrer_id не разобрался — продолжаем, как будто параметр не относится к рефералке.
    user_id = message.from_user.id
    username = message.from_user.username or "Без username"
    full_name = message.from_user.full_name
    created_new_player, referral_record_created = await register_user(
        user_id,
        username,
        full_name,
        referrer_id=referrer_id,
    )

    # Уведомления при регистрации по реферальной ссылке.
    if created_new_player and referral_record_created and referrer_id is not None:
        inviter = await get_user(referrer_id)
        inviter_name = (inviter or {}).get("full_name") or str(referrer_id)
        try:
            await bot.send_message(
                referrer_id,
                f"🎉 По вашей реферальной ссылке зарегистрировался новый пользователь: {full_name} (ID: {user_id})."
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пригласившему {referrer_id}: {e}")

        try:
            await message.answer(
                f"🎁 Вы зарегистрировались по реферальной ссылке!\n"
                f"Вам начислен стартовый капитал: {format_money(ECONOMY_SETTINGS['start_balance'])}"
            )
        except Exception:
            pass
    user = await get_user(user_id)
    nagirt_effects = await get_active_nagirt_effects(user_id)
    tolerance = await get_nagirt_tolerance(user_id)
    welcome_text = (
        f"👋 Добро пожаловать на работу, {full_name}!\n\n"
        f"Я *Виталик* — ваш генеральный директор! 👔\n\n"
        f"💰 *Начальный капитал:* {format_money(user['balance'] if user else ECONOMY_SETTINGS['start_balance'])}\n"
        f"💼 *Зарплата:* каждые 5 минут\n"
        f"⚡ *Случайные проверки:* каждые 20-30 минут\n\n"
    )
    if nagirt_effects["has_active"]:
        welcome_text += f"💊 *Активные таблетки:* +{int(nagirt_effects['salary_boost']*100)}% к зарплате\n"
        welcome_text += f"⚠️ Риск штрафа: {ECONOMY_SETTINGS['fine_chance']+nagirt_effects['fine_chance_mod']:.0%}\n\n"
    welcome_text += (
        f"📊 *Доступные функции:*\n"
        f"• 💰 Получка ({format_money(ECONOMY_SETTINGS['salary_min'])}-{format_money(ECONOMY_SETTINGS['salary_max'])})\n"
        f"• 🛒 Магазин (реалистичные цены)\n"
        f"• 🏦 Банк «Асфальт-Капитал» — кредит под проценты\n"
        f"• 🔁 Переводы между сотрудниками\n"
        f"• 🎮 Мини-игры (кости, асфальт, дуэль)\n"
        f"• 💊 Таблетки Нагирт (риск/награда)\n"
        f"• 🏢 БИЗНЕСЫ — пассивный доход, бонусы, прокачка!\n"
        f"• 📊 Статистика и рейтинг\n\n"
    )
    if tolerance > 1.0:
        welcome_text += f"📈 Толерантность к Нагирту: +{int((tolerance-1)*100)}%\n\n"
    welcome_text += (
        "*Внимание! Злоупотребление таблетками может привести к увольнению!* 💊\n\n"
        "_Дальше всё — кнопками меню внизу; слэш-команды не нужны._"
    )
    await message.answer(welcome_text, parse_mode="Markdown", reply_markup=get_main_keyboard(user_id))

@dp.callback_query(F.data == "sub_check")
async def sub_check_callback(callback: CallbackQuery):
    ok = await is_user_subscribed(callback.from_user.id)
    if ok:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.answer("Подписка подтверждена!", show_alert=False)
        await callback.message.answer("Теперь можешь пользоваться ботом. Нажми `Start` ещё раз.", parse_mode="Markdown")
    else:
        await callback.answer(
            "Ты ещё не подписан. Нажми «Подписаться», затем «Проверить».",
            show_alert=True,
        )


async def handle_check_activation(message: Message, check_id: str):
    user_id = message.from_user.id
    username = message.from_user.username or "Без username"
    full_name = message.from_user.full_name
    await register_user(user_id, username, full_name)
    result = await activate_gift_check_by_link(user_id, check_id)
    if not result['success']:
        extra_text = f"\n\n❌ *Не удалось активировать чек:* {result['error']}"
        user = await get_user(user_id)
        nagirt_effects = await get_active_nagirt_effects(user_id)
        tolerance = await get_nagirt_tolerance(user_id)
        welcome_text = (
            f"👋 Добро пожаловать на работу, {full_name}!\n\n"
            f"Я *Виталик* — ваш генеральный директор! 👔\n\n"
            f"💰 *Начальный капитал:* {format_money(user['balance'] if user else ECONOMY_SETTINGS['start_balance'])}\n"
            f"💼 *Зарплата:* каждые 5 минут\n"
            f"⚡ *Случайные проверки:* каждые 20-30 минут\n\n"
        )
        if nagirt_effects["has_active"]:
            welcome_text += f"💊 *Активные таблетки:* +{int(nagirt_effects['salary_boost']*100)}%\n"
            welcome_text += f"⚠️ Риск штрафа: {ECONOMY_SETTINGS['fine_chance']+nagirt_effects['fine_chance_mod']:.0%}\n\n"
        welcome_text += (
            f"📊 *Доступные функции:*\n"
            f"• 💰 Получка ({format_money(ECONOMY_SETTINGS['salary_min'])}-{format_money(ECONOMY_SETTINGS['salary_max'])})\n"
            f"• 🛒 Магазин (реалистичные цены)\n"
            f"• 🔁 Переводы между сотрудниками\n"
            f"• 🎮 Мини-игры (кости, асфальт, дуэль)\n"
            f"• 💊 Таблетки Нагирт (риск/награда)\n"
            f"• 🏢 БИЗНЕСЫ — пассивный доход, бонусы, прокачка!\n"
            f"• 📊 Статистика и рейтинг\n\n"
        )
        if tolerance > 1.0:
            welcome_text += f"📈 Толерантность к Нагирту: +{int((tolerance-1)*100)}%\n\n"
        welcome_text += (
            "*Внимание! Злоупотребление таблетками может привести к увольнению!* 💊\n\n"
            "_Дальше всё — кнопками меню внизу; слэш-команды не нужны._"
        )
        welcome_text += extra_text
        await message.answer(welcome_text, parse_mode="Markdown", reply_markup=get_main_keyboard(user_id))
        return
    if result['amount']:
        reward_text = f"💰 *{format_money(result['amount'])}*"
    else:
        reward_text = f"🎁 *{result['reward_text']}*"
    response = (
        f"🎉 *ЧЕК АКТИВИРОВАН!*\n\n"
        f"✅ Вы получили: {reward_text}\n"
        f"👤 От: {result['creator_name']}\n"
        f"🔢 {result['used_count']}/{result['max_uses']} использований\n"
    )
    if result['message']:
        response += f"💌 Сообщение: {result['message']}\n"
    response += f"\n🏦 *Баланс обновлён!*\n"
    user = await get_user(user_id)
    response += f"💰 Ваш баланс: {format_money(user['balance'])}\n\n"
    response += (
        f"🎮 *Доступные функции:*\n"
        f"• 💰 Получка каждые 5 минут\n"
        f"• 🛒 Магазин с бустами и таблетками\n"
        f"• 🎮 Мини-игры (кости, асфальт, дуэль)\n"
        f"• 🔁 Переводы другим игрокам\n"
        f"• 🏢 Бизнес-империя — пассивный доход!\n\n"
        f"*Добро пожаловать в компанию Виталика!* 👔\n"
        f"_Управление — кнопками меню внизу._"
    )
    await message.answer(response, parse_mode="Markdown", reply_markup=get_main_keyboard(user_id))

# ----- ЗАРПЛАТА -----
@dp.message(F.text == "💰 Получка")
async def handle_paycheck(message: Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        return
    current_time = datetime.now()
    last_salary = user.get('last_salary')
    if last_salary:
        last_salary_time = safe_parse_datetime(last_salary)
        if last_salary_time:
            time_since_last = current_time - last_salary_time
            min_wait = timedelta(seconds=ECONOMY_SETTINGS["salary_interval"])
            if time_since_last < min_wait:
                wait_seconds = int((min_wait - time_since_last).total_seconds())
                wait_time = format_time(wait_seconds)
                await message.answer(f"⏳ *Слишком рано!*\n\nЖди еще *{wait_time}* (мм:сс)")
                return

    await cleanup_expired()
    boost_multiplier = await get_active_boosts(user_id)
    nagirt_effects = await get_active_nagirt_effects(user_id)
    biz_bonuses = await get_total_business_bonuses(user_id)
    salary_bonus = biz_bonuses["salary"]
    mods = await get_player_modifiers(user_id)
    ach_income_bonus = float(mods.get("income_bonus", 0.0))
    nagirt_ach_mult = 1.0 + float(mods.get("nagirt_effect_bonus", 0.0))

    base_salary = random.randint(ECONOMY_SETTINGS["salary_min"], ECONOMY_SETTINGS["salary_max"])
    base_salary = int(base_salary * random.uniform(0.94, 1.09))

    pill_fine = 0
    if nagirt_effects["has_active"]:
        actual_fine_chance = ECONOMY_SETTINGS["fine_chance"] + nagirt_effects.get("fine_chance_mod", 0)
        if random.random() <= actual_fine_chance:
            pill_fine = random.randint(int(base_salary * 0.3), int(base_salary * 0.6))
            fine_reasons = [
                "Обнаружены следы Нагирта в крови!",
                "Работа в состоянии измененного сознания!",
                "Нарушение техники безопасности из-за таблеток!",
                "Неконтролируемая агрессия под Нагиртом!",
                "Прогул после приёма Нагирта!"
            ]
            await update_balance(user_id, -pill_fine, "penalty", f"💊 {random.choice(fine_reasons)}")

    nag_salary_part = nagirt_effects["salary_boost"] * nagirt_ach_mult
    total_multiplier = (
        1.0 + boost_multiplier + nag_salary_part + salary_bonus + ach_income_bonus
    )
    final_salary = int(base_salary * total_multiplier)
    await update_balance(user_id, final_salary, "salary", f"💸 Зарплата (x{total_multiplier:.2f})")
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE players SET last_salary = ? WHERE user_id = ?", (current_time.isoformat(), user_id))
        await db.commit()
    garnish_amt = 0
    loan = await get_active_bank_loan(user_id)
    if loan and loan.get("defaulted") and loan["remaining"] > 0:
        u2 = await get_user(user_id)
        if u2 and u2["balance"] > 0:
            g = int(final_salary * BANK_SETTINGS["salary_garnish_if_defaulted"])
            g = min(max(0, g), loan["remaining"], u2["balance"])
            if g > 0:
                await update_balance(
                    user_id,
                    -g,
                    "bank_garnish",
                    "Удержание с получки (Асфальт-Капитал, просрочка)",
                )
                new_rem = loan["remaining"] - g
                if new_rem <= 0:
                    await _set_loan_remaining(loan["id"], 0, 1)
                else:
                    await _set_loan_remaining(loan["id"], new_rem, 0)
                garnish_amt = g
    user = await get_user(user_id)
    response = f"💸 *ЗАРПЛАТА НАЧИСЛЕНА!*\n\n"
    response += f"📊 *Детализация:*\n"
    response += f"• Базовая ставка: {format_money(base_salary)}\n"
    details = []
    if boost_multiplier > 0:
        details.append(f"Бусты: +{int(boost_multiplier*100)}%")
    if nagirt_effects["salary_boost"] > 0:
        details.append(f"Нагирт: +{int(nag_salary_part*100)}%")
    if salary_bonus > 0:
        details.append(f"Бизнес: +{int(salary_bonus*100)}%")
    if ach_income_bonus > 0:
        details.append(f"Ачивки: +{int(ach_income_bonus*100)}%")
    if details:
        response += f"• Доплаты: {', '.join(details)}\n"
    response += f"• Итоговый коэффициент: x{total_multiplier:.2f}\n\n"
    if pill_fine > 0:
        response += f"⚠️ *ШТРАФ ЗА НАГИРТ:* -{format_money(pill_fine)}\n\n"
    if garnish_amt > 0:
        response += f"🕴️ *Удержание «Асфальт-Капитал»:* -{format_money(garnish_amt)} (просрочка кредита)\n\n"
    response += f"✅ *Итого начислено:* {format_money(final_salary)}\n"
    response += f"💳 *Новый баланс:* {format_money(user['balance'])}\n\n"
    comments = [
        "Могло бы быть и больше...", "На такую сумму даже пиццу не купишь!", "Работай лучше!",
        "Отличная работа!", "Так держать!", "Вы заслужили эту премию!",
        "Нормально работаешь.", "Продолжай в том же духе.", "Стабильно, но можно лучше."
    ]
    if nagirt_effects["has_active"]:
        pill_comments = ["Таблетки не заменят профессионализм!", "Осторожнее с Нагиртом!", "Лекарства должны помогать, а не мешать работе!", "Вы думаете, Нагирт делает из вас супермена?"]
        response += f"💬 *Виталик:* '{random.choice(pill_comments)}'"
    else:
        response += f"💬 *Виталик:* '{random.choice(comments)}'"
    await message.answer(response, parse_mode="Markdown")

# ----- МАГАЗИН -----
@dp.message(F.text == "🛒 Магазин")
async def handle_shop(message: Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        return
    active_boosts = await get_active_boosts(user_id)
    nagirt_effects = await get_active_nagirt_effects(user_id)
    shop_text = (
        "🏪 *Antonov-Shop*\n\n"
        f"💰 *Ваш баланс:* {format_money(user['balance'])}\n\n"
    )
    if active_boosts > 0:
        shop_text += f"📈 *Активные бусты:* +{int(active_boosts*100)}%\n"
    if nagirt_effects["has_active"]:
        shop_text += f"💊 *Активные таблетки:* +{int(nagirt_effects['salary_boost']*100)}% к зарплате, +{int(nagirt_effects['game_boost']*100)}% к играм\n"
    shop_text += (
        "\n*Выберите категорию товара:*"
        "\n\n🎒 *Товары кладутся в инвентарь* — применяй оттуда."
    )
    await message.answer(shop_text, parse_mode="Markdown", reply_markup=get_shop_categories_keyboard())


@dp.callback_query(F.data == "shop_back_categories")
async def shop_back_categories(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    active_boosts = await get_active_boosts(user_id)
    nagirt_effects = await get_active_nagirt_effects(user_id)
    shop_text = (
        "🏪 *Antonov-Shop*\n\n"
        f"💰 *Ваш баланс:* {format_money(user['balance'])}\n\n"
    )
    if active_boosts > 0:
        shop_text += f"📈 *Активные бусты:* +{int(active_boosts*100)}%\n"
    if nagirt_effects["has_active"]:
        shop_text += f"💊 *Активные таблетки:* +{int(nagirt_effects['salary_boost']*100)}% к зарплате, +{int(nagirt_effects['game_boost']*100)}% к играм\n"
    shop_text += "\n*Выберите категорию товара:*"
    try:
        await callback.message.edit_text(shop_text, parse_mode="Markdown", reply_markup=get_shop_categories_keyboard())
    except Exception:
        await callback.message.answer(shop_text, parse_mode="Markdown", reply_markup=get_shop_categories_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("shop_cat_"))
async def shop_cat(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    category_key = callback.data[len("shop_cat_"):]
    category_title = SHOP_CATEGORY_META.get(category_key, category_key)
    items = [i for i in SHOP_ITEMS if i.get("category") == category_key]
    if not items:
        await callback.answer("В этой категории пока нет товаров", show_alert=True)
        return

    mods = await get_player_modifiers(user_id)
    social = await get_social_status_for_user(user_id)
    discount = float(mods.get("shop_discount", 0.0)) + float(social.get("shop_discount_add", 0.0))
    discount = max(0.0, min(0.85, discount))

    shop_lines = [f"🏪 *Antonov-Shop*", f"📦 *Категория:* {category_title}", ""]
    kb_buttons = []
    for item in items:
        base_price = int(item["price"])
        if discount > 0:
            final_price = max(1, int(round(base_price * (1.0 - discount))))
            shop_lines.append(
                f"{item['name']} — {format_money(base_price)}\n"
                f"🏷 Скидка {int(discount*100)}% → {format_money(final_price)}\n"
            )
            kb_buttons.append([InlineKeyboardButton(text=f"{item['name']} - {format_money(final_price)}", callback_data=f"buy_{item['id']}")])
        else:
            shop_lines.append(f"{item['name']} - {format_money(base_price)}\n")
            kb_buttons.append([InlineKeyboardButton(text=f"{item['name']} - {format_money(base_price)}", callback_data=f"buy_{item['id']}")])

    kb_buttons.append([
        InlineKeyboardButton(text="🔙 Назад в категории", callback_data="shop_back_categories"),
        InlineKeyboardButton(text="❌ Закрыть", callback_data="shop_close"),
    ])
    shop_text = "\n".join(shop_lines).replace("\n\n\n", "\n\n")
    try:
        await callback.message.edit_text(shop_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons))
    except Exception:
        await callback.message.answer(shop_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons))
    await callback.answer()

@dp.callback_query(F.data.startswith("buy_"))
async def handle_buy_item(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден!", show_alert=True)
        return
    item_id = callback.data[4:]
    item = next((i for i in SHOP_ITEMS if i["id"] == item_id), None)
    if not item:
        await callback.answer("❌ Товар не найден!", show_alert=True)
        return
    mods = await get_player_modifiers(user_id)
    social = await get_social_status_for_user(user_id)
    discount = float(mods.get("shop_discount", 0.0)) + float(social.get("shop_discount_add", 0.0))
    discount = max(0.0, min(0.85, discount))
    price = max(1, int(round(item["price"] * (1.0 - discount))))
    if user["balance"] < price:
        await callback.answer(
            f"❌ Недостаточно средств! Нужно {format_money(price)}", show_alert=True
        )
        return

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE players SET balance = balance - ? WHERE user_id = ?", (price, user_id))
        await db.execute(
            "INSERT INTO purchases (user_id, item_name, price) VALUES (?, ?, ?)",
            (user_id, item["name"], price),
        )
        await db.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'purchase', -?, ?)",
            (user_id, price, f"Покупка: {item['name']}"),
        )
        await db.commit()

    ok, inv_msg = await add_inventory_item(user_id, item["id"], 1)
    if not ok:
        await update_balance(user_id, price, "purchase_refund", f"Возврат: инвентарь — {inv_msg}")
        await callback.answer(inv_msg[:180], show_alert=True)
        return

    user = await get_user(user_id)
    disc_note = f"\n🏷 Итоговая скидка: −{int(discount*100)}%" if discount > 0 else ""
    response = (
        f"✅ *Покупка*\n\n"
        f"📦 *{item['name']}* добавлен в инвентарь (×1)\n"
        f"💰 Списано: {format_money(price)}{disc_note}\n\n"
        f"Открой *🎒 Инвентарь* — используй, выбрось или передай другому.\n\n"
        f"💳 Баланс: {format_money(user['balance'])}"
    )
    try:
        await callback.message.edit_text(response, parse_mode="Markdown")
    except:
        await callback.message.answer(response, parse_mode="Markdown")
    await callback.answer()

# ==================== МИНИ-ИГРЫ ====================
@dp.message(F.text == "🎮 Мини-игры")
async def handle_minigames(message: Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        return
    games_text = (
        "🎮 *КОРПОРАТИВНЫЕ МИНИ-ИГРЫ*\n\n"
        "🎰 *Рулетка*\n"
        f"• Ставка: {format_money(ECONOMY_SETTINGS['roulette_min_bet'])} — {format_money(ECONOMY_SETTINGS['roulette_max_bet'])}\n"
        f"• Шанс выигрыша: {int(ECONOMY_SETTINGS['roulette_win_chance']*100)}%\n"
        f"• Выплата при победе: ×2\n\n"
        "🎲 *Кости: чёт / нечёт*\n"
        f"• Кубик 1–6; угадал чётность — выигрыш ×2\n"
        f"• Ставка: {format_money(ECONOMY_SETTINGS['dice_min_bet'])} — "
        f"{format_money(ECONOMY_SETTINGS['dice_max_bet'])}\n"
        f"• Шанс 50% (честный кубик)\n\n"
        "🛣️ *Укладка асфальта*\n"
        f"• Заработок за метр: {format_money(ECONOMY_SETTINGS['asphalt_earnings'])}\n"
        f"• Штраф за брак: {format_money(ECONOMY_SETTINGS['asphalt_fine_min'])}-{format_money(ECONOMY_SETTINGS['asphalt_fine_max'])}\n"
        f"• Шанс успеха: 70% (с Нагиртом до 95%)\n"
        f"• Время работы: 30 секунд\n\n"
        "⚔️ *Дуэль*\n"
        f"• Ставка: от {format_money(ECONOMY_SETTINGS['duel_min_bet'])} до {format_money(ECONOMY_SETTINGS['duel_max_bet'])}\n"
        f"• Правила: вызов → ставка → бросок кубика по очереди\n"
        f"• Таймаут: {DUEL_TIMEOUT} сек на ход\n"
        f"• **Честная дуэль — без бонусов от бизнеса.** 🎲\n\n"
    )
    await message.answer(games_text, parse_mode="Markdown", reply_markup=get_minigames_keyboard())

# ----- КОСТИ (ЧЁТ / НЕЧЁТ) -----
@dp.callback_query(F.data == "game_roulette")
async def game_roulette_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer(NOT_REGISTERED_ALERT, show_alert=True)
        return
    min_bet = int(ECONOMY_SETTINGS["roulette_min_bet"])
    max_bet = min(int(ECONOMY_SETTINGS["roulette_max_bet"]), int(user["balance"]))
    if int(user["balance"]) < min_bet:
        await callback.answer("❌ Недостаточно денег для минимальной ставки.", show_alert=True)
        return
    await state.set_state(RouletteStates.waiting_for_bet)
    await callback.message.edit_text(
        f"🎰 *Рулетка*\n\n"
        f"💳 Баланс: {format_money(user['balance'])}\n"
        f"🎯 Шанс выигрыша: {int(ECONOMY_SETTINGS['roulette_win_chance']*100)}%\n"
        f"💰 Ставка: от {format_money(min_bet)} до {format_money(max_bet)}\n"
        f"🏆 Победа: выплата ×2\n\n"
        f"Введи сумму ставки числом:",
        parse_mode="Markdown",
    )
    await callback.message.answer(
        "↩️ Для отмены нажми «🔙 Назад».",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await callback.answer()


@dp.message(RouletteStates.waiting_for_bet)
async def game_roulette_bet(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await state.clear()
        return
    balance_before_game = int(user["balance"])

    try:
        bet = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Введи целое число — сумму ставки.")
        return

    min_bet = int(ECONOMY_SETTINGS["roulette_min_bet"])
    max_bet = int(ECONOMY_SETTINGS["roulette_max_bet"])
    if bet < min_bet:
        await message.answer(f"❌ Минимальная ставка: {format_money(min_bet)}.")
        return
    if bet > max_bet:
        await message.answer(f"❌ Максимальная ставка: {format_money(max_bet)}.")
        return
    if bet > int(user["balance"]):
        await message.answer("❌ Недостаточно денег на балансе.")
        return

    await update_balance(user_id, -bet, "roulette_bet", "Ставка в рулетке")
    win = random.random() < float(ECONOMY_SETTINGS["roulette_win_chance"])

    if win:
        prize = bet * 2
        await update_balance(user_id, prize, "roulette_win", "Выигрыш в рулетке ×2")
        u2 = await get_user(user_id)
        txt = (
            f"🎰 *Победа!*\n\n"
            f"Ставка: {format_money(bet)}\n"
            f"Начислено: +{format_money(prize)}\n"
            f"Чистая прибыль: +{format_money(bet)}\n"
            f"💳 Баланс: {format_money(u2['balance'])}"
        )
    else:
        u2 = await get_user(user_id)
        txt = (
            f"🎰 *Проигрыш*\n\n"
            f"Ставка: {format_money(bet)}\n"
            f"Списано: -{format_money(bet)}\n"
            f"💳 Баланс: {format_money(u2['balance'])}"
        )

    await message.answer(txt, parse_mode="Markdown", reply_markup=get_minigames_keyboard())
    try:
        await post_poverty_transition_if_needed(user_id, balance_before_game, int(u2["balance"]))
    except Exception as e:
        logger.error("Ошибка пост-обработки бедности (roulette) user_id=%s: %s", user_id, e, exc_info=True)
    await state.clear()


@dp.callback_query(F.data == "game_dice")
async def game_dice_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer(NOT_REGISTERED_ALERT, show_alert=True)
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⚪ Чёт (2,4,6)", callback_data="dice_pick_even"),
                InlineKeyboardButton(text="⚫ Нечёт (1,3,5)", callback_data="dice_pick_odd"),
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_games")],
        ]
    )
    await callback.message.edit_text(
        f"🎲 *Кости: чёт / нечёт*\n\n"
        f"Баланс: {format_money(user['balance'])}\n"
        f"Ставка: от {format_money(ECONOMY_SETTINGS['dice_min_bet'])} "
        f"до {format_money(min(ECONOMY_SETTINGS['dice_max_bet'], user['balance']))}\n"
        f"Угадал — получаешь ×2 (чистая удача, без комиссии).\n\n"
        f"Выбери, на что ставишь:",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    await state.clear()
    await callback.answer()


@dp.callback_query(F.data.in_({"dice_pick_even", "dice_pick_odd"}))
async def game_dice_parity(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer(NOT_REGISTERED_ALERT, show_alert=True)
        return
    want_even = callback.data == "dice_pick_even"
    await state.update_data(dice_even=want_even)
    await state.set_state(DiceStates.waiting_for_bet)
    label = "ЧЁТ (2, 4, 6)" if want_even else "НЕЧЁТ (1, 3, 5)"
    await callback.message.edit_text(
        f"🎲 Ставка на *{label}*\n\n"
        f"Введи сумму ставки числом:\n"
        f"мин. {format_money(ECONOMY_SETTINGS['dice_min_bet'])}, "
        f"макс. {format_money(min(ECONOMY_SETTINGS['dice_max_bet'], user['balance']))}",
        parse_mode="Markdown",
    )
    await callback.message.answer(
        "↩️ Для отмены нажми «🔙 Назад».",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await callback.answer()


@dp.message(DiceStates.waiting_for_bet)
async def game_dice_bet(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    want_even = data.get("dice_even")
    if want_even is None:
        await state.clear()
        return
    try:
        bet = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Введи целое число — сумму ставки.")
        return
    user = await get_user(user_id)
    if not user:
        await state.clear()
        return
    balance_before_game = int(user["balance"])
    mx = min(ECONOMY_SETTINGS["dice_max_bet"], user["balance"])
    if bet < ECONOMY_SETTINGS["dice_min_bet"] or bet > mx:
        await message.answer(
            f"❌ Ставка от {format_money(ECONOMY_SETTINGS['dice_min_bet'])} до {format_money(mx)}."
        )
        return
    if bet > user["balance"]:
        await message.answer("❌ Недостаточно денег.")
        return

    await update_balance(user_id, -bet, "dice_bet", "Ставка в костях чёт/нечёт")
    roll = random.randint(1, 6)
    is_even = roll % 2 == 0
    win = is_even == want_even
    label_guess = "чёт" if want_even else "нечёт"
    label_roll = "чётное" if is_even else "нечёт"

    if win:
        prize = bet * 2
        await update_balance(user_id, prize, "dice_even_win", f"Кости: выпало {roll}, ставка {label_guess}")
        u2 = await get_user(user_id)
        txt = (
            f"🎲 *Победа!*\n\n"
            f"Кубик: *{roll}* ({label_roll})\n"
            f"Ты ставил на: {label_guess}\n"
            f"💰 +{format_money(prize)} (ставка ×2)\n"
            f"💳 Баланс: {format_money(u2['balance'])}"
        )
    else:
        u2 = await get_user(user_id)
        txt = (
            f"🎲 *Проигрыш*\n\n"
            f"Кубик: *{roll}* ({label_roll})\n"
            f"Ты ставил на: {label_guess}\n"
            f"💸 −{format_money(bet)}\n"
            f"💳 Баланс: {format_money(u2['balance'])}"
        )
    await message.answer(txt, parse_mode="Markdown", reply_markup=get_minigames_keyboard())
    try:
        await post_poverty_transition_if_needed(user_id, balance_before_game, int(u2["balance"]))
    except Exception as e:
        logger.error("Ошибка пост-обработки бедности (dice) user_id=%s: %s", user_id, e, exc_info=True)
    await state.clear()

# ----- АСФАЛЬТ -----
@dp.callback_query(F.data == "game_asphalt")
async def handle_game_asphalt(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("❌ Ошибка: пользователь не найден", show_alert=True)
        return
    nagirt_effects = await get_active_nagirt_effects(user_id)
    can_work = True
    last_asphalt = user.get('last_asphalt')
    if last_asphalt:
        last_time = safe_parse_datetime(last_asphalt)
        if last_time:
            time_passed = (datetime.now() - last_time).total_seconds()
            if time_passed < 30:
                can_work = False
    asphalt_text = (
        f"🛣️ *Укладка асфальта*\n\n"
        f"💰 Баланс: {format_money(user['balance'])}\n"
        f"📏 Уложено метров: {user.get('asphalt_meters', 0):,}\n"
        f"💵 Заработано: {format_money(user.get('asphalt_earned', 0))}\n\n"
    )
    if nagirt_effects["has_active"]:
        asphalt_text += f"💊 *Активный Нагирт:* +{int(nagirt_effects['game_boost']*100)}% к заработку\n"
        if nagirt_effects["side_effects"]:
            asphalt_text += f"⚠️ *Побочки:* {', '.join(nagirt_effects['side_effects'][:2])}\n"
        asphalt_text += "\n"
    if can_work:
        asphalt_text += "Нажми кнопку ниже, чтобы уложить 1 метр асфальта!"
    else:
        asphalt_text += "⏳ *Асфальт еще сохнет!*\nПодожди 30 секунд между укладками."
    try:
        await callback.message.edit_text(asphalt_text, parse_mode="Markdown", reply_markup=get_asphalt_keyboard(can_work))
    except:
        await callback.message.answer(asphalt_text, parse_mode="Markdown", reply_markup=get_asphalt_keyboard(can_work))
    await callback.answer()

@dp.callback_query(F.data == "lay_asphalt")
async def handle_lay_asphalt(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("❌ Ошибка: пользователь не найден", show_alert=True)
        return
    nagirt_effects = await get_active_nagirt_effects(user_id)
    biz_bonuses = await get_total_business_bonuses(user_id)
    asphalt_bonus = biz_bonuses["asphalt"]

    current_time = datetime.now()
    last_asphalt = user.get('last_asphalt')
    if last_asphalt:
        last_time = safe_parse_datetime(last_asphalt)
        if last_time:
            time_passed = (current_time - last_time).total_seconds()
            if time_passed < 30:
                wait_time = 30 - int(time_passed)
                await callback.answer(f"⏳ Отдыхай еще {wait_time} секунд!", show_alert=True)
                return

    base_success_chance = 0.7
    success_chance = base_success_chance
    if nagirt_effects["has_active"]:
        success_chance = min(0.95, base_success_chance + (nagirt_effects["game_boost"] * 0.15))
        if nagirt_effects["side_effects"]:
            success_chance = max(0.3, success_chance - (len(nagirt_effects["side_effects"]) * 0.05))

    success = random.random() <= success_chance
    if success:
        base_earnings = ECONOMY_SETTINGS["asphalt_earnings"]
        earnings_multiplier = 1.0 + nagirt_effects.get("game_boost", 0) + asphalt_bonus
        earnings = int(base_earnings * earnings_multiplier)

        jackpot_message = ""
        if random.random() <= 0.01:
            jackpot_bonus = earnings * 5
            earnings += jackpot_bonus
            jackpot_message = f"\n🎰 ДЖЕКПОТ! +{format_money(jackpot_bonus)}"

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute('''
                UPDATE players 
                SET balance = balance + ?,
                    asphalt_meters = asphalt_meters + 1,
                    asphalt_earned = asphalt_earned + ?,
                    last_asphalt = ?
                WHERE user_id = ?
            ''', (earnings, earnings, current_time.isoformat(), user_id))
            await db.execute('''
                INSERT INTO transactions (user_id, type, amount, description)
                VALUES (?, ?, ?, ?)
            ''', (user_id, 'asphalt', earnings, 'Укладка асфальта' + (' + Нагирт' if nagirt_effects["has_active"] else '')))
            await db.commit()

        user = await get_user(user_id)
        result_text = (
            f"✅ *Асфальт уложен!*\n\n"
            f"🛣️ Уложен 1 метр асфальта\n"
        )
        if nagirt_effects["has_active"]:
            result_text += f"💊 *Эффект Нагирта:* +{int(nagirt_effects['game_boost']*100)}%\n"
        if asphalt_bonus > 0:
            result_text += f"🏢 *Бонус бизнеса:* +{int(asphalt_bonus*100)}%\n"
        result_text += (
            f"💰 Заработано: {format_money(earnings)}\n"
            f"📏 Всего метров: {user.get('asphalt_meters', 0):,}\n"
            f"💵 Заработано всего: {format_money(user.get('asphalt_earned', 0))}\n"
            f"💳 Баланс: {format_money(user['balance'])}"
        ) + jackpot_message + "\n\nОтличная работа! 🏗️"
    else:
        base_penalty = random.randint(ECONOMY_SETTINGS["asphalt_fine_min"], ECONOMY_SETTINGS["asphalt_fine_max"])
        if nagirt_effects["has_active"] and nagirt_effects["side_effects"]:
            penalty_multiplier = 1.0 + (len(nagirt_effects["side_effects"]) * 0.2)
            penalty = int(base_penalty * penalty_multiplier)
            penalty_reason = f"Штраф за плохую укладку + побочки Нагирта"
        else:
            penalty = base_penalty
            penalty_reason = "Штраф за плохую укладку"
        if nagirt_effects["has_active"] and not nagirt_effects["side_effects"]:
            penalty = max(ECONOMY_SETTINGS["asphalt_fine_min"], int(penalty * 0.7))
            penalty_reason = "Штраф смягчен (Нагирт без побочек)"
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute('''
                UPDATE players 
                SET balance = balance - ?,
                    last_asphalt = ?,
                    total_fines = total_fines + ?
                WHERE user_id = ?
            ''', (penalty, current_time.isoformat(), penalty, user_id))
            await db.execute('''
                INSERT INTO transactions (user_id, type, amount, description)
                VALUES (?, ?, ?, ?)
            ''', (user_id, 'penalty', -penalty, penalty_reason))
            await db.commit()
        user = await get_user(user_id)
        result_text = (
            f"⚠️ *ВИТАЛИК ШТРАФУЕТ!*\n\n"
            f"🛣️ Асфальт уложен криво!\n"
        )
        if nagirt_effects["has_active"]:
            result_text += f"💊 *Влияние Нагирта:* {int((success_chance - base_success_chance)*100)}% к шансу\n"
        result_text += (
            f"💸 Штраф: {format_money(penalty)}\n"
            f"💳 Баланс: {format_money(user['balance'])}\n\n"
            f"Будь внимательнее! ⚠️"
        )
        if nagirt_effects["side_effects"]:
            result_text += f"\n\n💊 *Побочки:* {', '.join(nagirt_effects['side_effects'])}"
    await callback.message.answer(result_text, parse_mode="Markdown")
    menu_text = (
        f"🛣️ *Укладка асфальта*\n\n"
        f"💰 Баланс: {format_money(user['balance'])}\n"
        f"📏 Уложено метров: {user.get('asphalt_meters', 0):,}\n"
        f"💵 Заработано: {format_money(user.get('asphalt_earned', 0))}\n"
    )
    if nagirt_effects["has_active"]:
        menu_text += f"\n💊 *Нагирт активен:* +{int(nagirt_effects['game_boost']*100)}% к заработку"
        if nagirt_effects["side_effects"]:
            menu_text += f"\n⚠️ Побочки: {', '.join(nagirt_effects['side_effects'][:2])}"
    menu_text += f"\n\n⏳ Асфальт сохнет...\nЖди 30 секунд перед следующей укладкой."
    try:
        await callback.message.edit_text(menu_text, parse_mode="Markdown", reply_markup=get_asphalt_keyboard(False))
    except:
        await callback.message.answer(menu_text, parse_mode="Markdown", reply_markup=get_asphalt_keyboard(False))
    await callback.answer()

@dp.callback_query(F.data == "asphalt_wait")
async def handle_asphalt_wait(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    last_asphalt = user.get('last_asphalt')
    if last_asphalt:
        last_time = safe_parse_datetime(last_asphalt)
        if last_time:
            time_passed = (datetime.now() - last_time).total_seconds()
            if time_passed < 30:
                wait_time = 30 - int(time_passed)
                await callback.answer(f"⏳ Жди еще {wait_time} секунд!", show_alert=True)
            else:
                await callback.answer("✅ Можно укладывать асфальт!", show_alert=True)
        else:
            await callback.answer("✅ Можно укладывать асфальт!", show_alert=True)
    else:
        await callback.answer("✅ Можно укладывать асфальт!", show_alert=True)

# ==================== ДУЭЛЬ (ПОШАГОВАЯ, БЕЗ УХОДА В МИНУС) ====================
async def duel_cancel_by_timeout(duel_id: str, challenger_id: int, acceptor_id: int, bet: int):
    await asyncio.sleep(DUEL_TIMEOUT)
    if duel_id not in active_duels:
        return
    duel = active_duels[duel_id]
    if duel["status"] != "finished":
        # Возвращаем ставки
        await update_balance(challenger_id, bet, "duel_refund", "Возврат ставки (таймаут)")
        await update_balance(acceptor_id, bet, "duel_refund", "Возврат ставки (таймаут)")
        try:
            await bot.send_message(challenger_id, "⏰ Дуэль отменена из-за бездействия. Ставки возвращены.")
            await bot.send_message(acceptor_id, "⏰ Дуэль отменена из-за бездействия. Ставки возвращены.")
        except:
            pass
        del active_duels[duel_id]

@dp.callback_query(F.data == "game_duel")
async def handle_duel_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("❌ Вы не зарегистрированы!", show_alert=True)
        return
    all_users = await get_all_users()
    if len(all_users) <= 1:
        await callback.answer("❌ Нет других игроков для дуэли", show_alert=True)
        return
    await callback.message.edit_text(
        "⚔️ *ДУЭЛЬ*\n\nВыберите противника:",
        parse_mode="Markdown",
        reply_markup=get_users_keyboard(
            all_users, user_id, "duel_opponent_", cancel_callback="duel_cancel_choose"
        )
    )
    await state.set_state(DuelStates.choosing_opponent)
    await callback.answer()

@dp.callback_query(F.data.startswith("duel_opponent_"), DuelStates.choosing_opponent)
async def duel_choose_opponent(callback: CallbackQuery, state: FSMContext):
    try:
        opponent_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Некорректный выбор противника", show_alert=True)
        return
    challenger_id = callback.from_user.id
    if opponent_id == challenger_id:
        await callback.answer("❌ Нельзя вызвать самого себя", show_alert=True)
        return
    opponent = await get_user(opponent_id)
    if not opponent:
        await callback.answer("❌ Противник не найден", show_alert=True)
        return
    await state.update_data(opponent_id=opponent_id, opponent_name=opponent['full_name'])
    await callback.message.edit_text(
        f"⚔️ *Дуэль с {opponent['full_name']}*\n\n"
        f"💰 Ваш баланс: {format_money((await get_user(challenger_id))['balance'])}\n"
        f"💰 Баланс противника: {format_money(opponent['balance'])}\n\n"
        f"💸 Введите сумму ставки:\n"
        f"Минимум: {format_money(ECONOMY_SETTINGS['duel_min_bet'])}\n"
        f"Максимум: {format_money(min(ECONOMY_SETTINGS['duel_max_bet'], (await get_user(challenger_id))['balance']))}",
        parse_mode="Markdown"
    )
    await callback.message.answer(
        "↩️ Для отмены нажми «🔙 Назад».",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await state.set_state(DuelStates.waiting_bet_amount)
    await callback.answer()

@dp.message(DuelStates.waiting_bet_amount)
async def duel_enter_bet(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    opponent_id = data.get('opponent_id')
    if not opponent_id:
        await message.answer("❌ Ошибка: противник не выбран")
        await state.clear()
        return
    try:
        bet = int(message.text)
        user = await get_user(user_id)
        if not user:
            await message.answer("❌ Ошибка")
            await state.clear()
            return
        if bet < ECONOMY_SETTINGS['duel_min_bet']:
            await message.answer(f"❌ Минимальная ставка: {format_money(ECONOMY_SETTINGS['duel_min_bet'])}")
            return
        if bet > ECONOMY_SETTINGS['duel_max_bet']:
            await message.answer(f"❌ Максимальная ставка: {format_money(ECONOMY_SETTINGS['duel_max_bet'])}")
            return
        if bet > user['balance']:
            await message.answer(f"❌ У вас недостаточно средств. Ваш баланс: {format_money(user['balance'])}")
            return
        opponent = await get_user(opponent_id)
        if not opponent:
            await message.answer("❌ Противник не найден")
            await state.clear()
            return
        if bet > opponent['balance']:
            await message.answer(f"❌ У противника недостаточно средств для такой ставки.")
            return
        await state.update_data(bet=bet)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="duel_confirm"),
             InlineKeyboardButton(text="❌ Отмена", callback_data="duel_cancel")]
        ])
        await message.answer(
            f"⚔️ *Дуэль с {opponent['full_name']}*\n\n"
            f"💰 Ставка: {format_money(bet)}\n\n"
            f"Подтвердите вызов:",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        await state.set_state(DuelStates.waiting_confirmation)
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "duel_confirm", DuelStates.waiting_confirmation)
async def duel_confirm_challenge(callback: CallbackQuery, state: FSMContext):
    challenger_id = callback.from_user.id
    data = await state.get_data()
    opponent_id = data['opponent_id']
    bet = data['bet']
    challenger = await get_user(challenger_id)
    if challenger['balance'] < bet:
        await callback.message.edit_text("❌ Недостаточно средств для ставки. Дуэль отменена.")
        await state.clear()
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"duel_accept_{challenger_id}_{bet}"),
         InlineKeyboardButton(text="❌ Отклонить", callback_data="duel_decline")]
    ])
    try:
        await bot.send_message(
            opponent_id,
            f"⚔️ *ВЫЗОВ НА ДУЭЛЬ!*\n\n"
            f"👤 Противник: {challenger['full_name']}\n"
            f"💰 Ставка: {format_money(bet)}\n\n"
            f"Принять вызов?",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        await callback.message.edit_text("✅ Вызов отправлен! Ожидайте ответа противника.")
        await state.clear()
    except Exception as e:
        await callback.message.edit_text("❌ Не удалось отправить вызов. Возможно, противник заблокировал бота.")
        await state.clear()

@dp.callback_query(F.data.startswith("duel_accept_"))
async def duel_accept(callback: CallbackQuery):
    acceptor_id = callback.from_user.id
    parts = callback.data.split('_')
    challenger_id = int(parts[2])
    bet = int(parts[3])

    if acceptor_id == challenger_id:
        await callback.answer("❌ Нельзя принять свой вызов", show_alert=True)
        return

    challenger = await get_user(challenger_id)
    acceptor = await get_user(acceptor_id)

    if not challenger or not acceptor:
        await callback.answer("❌ Ошибка: пользователь не найден", show_alert=True)
        return

    # Жёсткая проверка баланса
    if challenger['balance'] < bet:
        await callback.message.edit_text("❌ У противника уже нет денег для дуэли. Вызов отменён.")
        return
    if acceptor['balance'] < bet:
        await callback.message.edit_text("❌ У вас недостаточно средств для участия в дуэли.")
        return

    # Списываем ставки через update_balance (с защитой от минуса)
    await update_balance(challenger_id, -bet, "duel_bet", f"Ставка в дуэли против {acceptor['full_name']}")
    await update_balance(acceptor_id, -bet, "duel_bet", f"Ставка в дуэли против {challenger['full_name']}")

    duel_id = f"{challenger_id}_{acceptor_id}_{int(datetime.now().timestamp())}"
    active_duels[duel_id] = {
        "challenger_id": challenger_id,
        "acceptor_id": acceptor_id,
        "challenger_name": challenger['full_name'],
        "acceptor_name": acceptor['full_name'],
        "bet": bet,
        "challenger_roll": None,
        "acceptor_roll": None,
        "status": "waiting_challenger",
        "last_action": datetime.now(),
        "message_ids": [],
        "challenger_balance_before": int(challenger["balance"]),
        "acceptor_balance_before": int(acceptor["balance"]),
    }

    challenger_msg = await bot.send_message(
        challenger_id,
        f"⚔️ *ДУЭЛЬ ПРИНЯТА!*\n\n"
        f"Противник: {acceptor['full_name']}\n"
        f"💰 Ставка: {format_money(bet)}\n\n"
        f"🎲 Ваш ход! Нажмите кнопку, чтобы бросить кубик.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Бросить кубик", callback_data=f"duel_roll_{duel_id}")]
        ])
    )

    acceptor_msg = await callback.message.edit_text(
        f"⚔️ *ВЫ ПРИНЯЛИ ДУЭЛЬ!*\n\n"
        f"Противник: {challenger['full_name']}\n"
        f"💰 Ставка: {format_money(bet)}\n\n"
        f"⏳ Ожидайте, пока противник бросит кубик...",
        parse_mode="Markdown"
    )

    active_duels[duel_id]["message_ids"] = [challenger_msg.message_id, acceptor_msg.message_id]
    asyncio.create_task(duel_cancel_by_timeout(duel_id, challenger_id, acceptor_id, bet))
    await callback.answer()

@dp.callback_query(F.data.startswith("duel_roll_"))
async def duel_roll(callback: CallbackQuery):
    user_id = callback.from_user.id
    duel_id = callback.data[10:]

    if duel_id not in active_duels:
        await callback.answer("❌ Дуэль уже завершена или не существует", show_alert=True)
        return

    duel = active_duels[duel_id]

    if duel["status"] == "waiting_challenger" and user_id == duel["challenger_id"]:
        player = "challenger"
        opponent_id = duel["acceptor_id"]
        player_name = duel["challenger_name"]
        opponent_name = duel["acceptor_name"]
    elif duel["status"] == "waiting_acceptor" and user_id == duel["acceptor_id"]:
        player = "acceptor"
        opponent_id = duel["challenger_id"]
        player_name = duel["acceptor_name"]
        opponent_name = duel["challenger_name"]
    else:
        await callback.answer("❌ Сейчас не ваш ход или дуэль уже завершена", show_alert=True)
        return

    roll = random.randint(1, ECONOMY_SETTINGS['duel_dice_sides'])

    duel[f"{player}_roll"] = roll
    duel["last_action"] = datetime.now()

    await callback.message.edit_text(
        f"🎲 *ВЫ БРОСИЛИ КУБИК!*\n\n"
        f"Результат: {roll}\n\n"
        f"⏳ Ожидайте броска противника...",
        parse_mode="Markdown"
    )

    if duel["status"] == "waiting_challenger":
        duel["status"] = "waiting_acceptor"
        opponent_msg = await bot.send_message(
            opponent_id,
            f"⚔️ *ВАШ ХОД!*\n\n"
            f"Противник {player_name} уже бросил кубик.\n"
            f"💰 Ставка: {format_money(duel['bet'])}\n\n"
            f"🎲 Нажмите кнопку, чтобы бросить кубик!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎲 Бросить кубик", callback_data=f"duel_roll_{duel_id}")]
            ])
        )
        asyncio.create_task(duel_cancel_by_timeout(duel_id, duel["challenger_id"], duel["acceptor_id"], duel["bet"]))

    elif duel["status"] == "waiting_acceptor":
        duel["status"] = "finished"
        challenger_roll = duel["challenger_roll"]
        acceptor_roll = duel["acceptor_roll"]
        bet = duel["bet"]

        if challenger_roll > acceptor_roll:
            winner_id = duel["challenger_id"]
            loser_id = duel["acceptor_id"]
            winner_name = duel["challenger_name"]
            loser_name = duel["acceptor_name"]
            winner_roll = challenger_roll
            loser_roll = acceptor_roll
        elif acceptor_roll > challenger_roll:
            winner_id = duel["acceptor_id"]
            loser_id = duel["challenger_id"]
            winner_name = duel["acceptor_name"]
            loser_name = duel["challenger_name"]
            winner_roll = acceptor_roll
            loser_roll = challenger_roll
        else:
            # Ничья – возвращаем ставки
            await update_balance(duel["challenger_id"], bet, "duel_refund", "Возврат ставки (ничья)")
            await update_balance(duel["acceptor_id"], bet, "duel_refund", "Возврат ставки (ничья)")
            await bot.send_message(
                duel["challenger_id"],
                f"🤝 *НИЧЬЯ!*\n\n"
                f"Ваш бросок: {challenger_roll}\n"
                f"Бросок {duel['acceptor_name']}: {acceptor_roll}\n\n"
                f"Ставки возвращены."
            )
            await bot.send_message(
                duel["acceptor_id"],
                f"🤝 *НИЧЬЯ!*\n\n"
                f"Ваш бросок: {acceptor_roll}\n"
                f"Бросок {duel['challenger_name']}: {challenger_roll}\n\n"
                f"Ставки возвращены."
            )
            del active_duels[duel_id]
            await callback.answer()
            return

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE players SET duels_won = duels_won + 1 WHERE user_id = ?",
                (winner_id,),
            )
            await db.commit()

        prize = bet * 2
        await update_balance(winner_id, prize, "duel_win", f"Победа в дуэли против {loser_name}, ставка {bet}")

        rep_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="⭐1", callback_data=f"rep_duel_{loser_id}_1"),
                    InlineKeyboardButton(text="⭐2", callback_data=f"rep_duel_{loser_id}_2"),
                    InlineKeyboardButton(text="⭐3", callback_data=f"rep_duel_{loser_id}_3"),
                ]
            ]
        )
        rep_kb2 = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="⭐1", callback_data=f"rep_duel_{winner_id}_1"),
                    InlineKeyboardButton(text="⭐2", callback_data=f"rep_duel_{winner_id}_2"),
                    InlineKeyboardButton(text="⭐3", callback_data=f"rep_duel_{winner_id}_3"),
                ]
            ]
        )

        await bot.send_message(
            winner_id,
            f"🏆 *ВЫ ПОБЕДИЛИ В ДУЭЛИ!*\n\n"
            f"🎲 Ваш бросок: {winner_roll}\n"
            f"🎲 Бросок {loser_name}: {loser_roll}\n\n"
            f"💰 Выигрыш: {format_money(prize)}\n\n"
            f"Оцени честность противника (1–3):",
            parse_mode="Markdown",
            reply_markup=rep_kb,
        )
        await bot.send_message(
            loser_id,
            f"💥 *ВЫ ПРОИГРАЛИ В ДУЭЛИ!*\n\n"
            f"🎲 Ваш бросок: {loser_roll}\n"
            f"🎲 Бросок {winner_name}: {winner_roll}\n\n"
            f"💸 Потеряно: {format_money(bet)}\n\n"
            f"Оцени честность противника (1–3):",
            parse_mode="Markdown",
            reply_markup=rep_kb2,
        )
        if bet >= 5000:
            await post_chronicle(
                f"⚔️ Громкая дуэль: *{winner_name}* обыграл *{loser_name}* на {format_money(bet)}."
            )
        try:
            challenger_final = await get_user(duel["challenger_id"])
            acceptor_final = await get_user(duel["acceptor_id"])
            if challenger_final:
                await post_poverty_transition_if_needed(
                    duel["challenger_id"],
                    int(duel.get("challenger_balance_before", challenger_final["balance"])),
                    int(challenger_final["balance"]),
                )
            if acceptor_final:
                await post_poverty_transition_if_needed(
                    duel["acceptor_id"],
                    int(duel.get("acceptor_balance_before", acceptor_final["balance"])),
                    int(acceptor_final["balance"]),
                )
        except Exception as e:
            logger.error("Ошибка пост-обработки бедности (duel) duel_id=%s: %s", duel_id, e, exc_info=True)
        del active_duels[duel_id]

    await callback.answer()

# ==================== БИЗНЕС-СИСТЕМА (ПОЛНЫЙ ИНТЕРФЕЙС С ТАЙМЕРОМ) ====================
async def cmd_business_menu(target: Union[Message, CallbackQuery], user_id: int = None):
    """Универсальная функция для показа меню бизнесов."""
    if isinstance(target, CallbackQuery):
        message = target.message
        if user_id is None:
            user_id = target.from_user.id
        is_callback = True
    else:
        message = target
        if user_id is None:
            user_id = message.from_user.id
        is_callback = False

    user = await get_user(user_id)
    if not user:
        if is_callback:
            await target.answer(NOT_REGISTERED_ALERT, show_alert=True)
        else:
            await message.answer(NOT_REGISTERED_HINT)
        return

    biz_list = await get_user_businesses(user_id)
    status = await get_business_collect_status(user_id)

    text = (
        f"🏢 *КОРПОРАЦИЯ ВИТАЛИКА*\n\n"
        f"💰 Баланс: {format_money(user['balance'])}\n"
        f"🏭 Твоих бизнесов: {len(biz_list)}\n"
        f"💵 Пассивный доход: {format_money(status['total_per_hour'])}/час\n\n"
    )

    if status['can_collect']:
        text += f"✅ *Доступно к сбору:* {format_money(status['total_income'])}\n"
        collect_text = "💰 Собрать доход"
    else:
        if status['seconds_left'] > 0:
            minutes = status['seconds_left'] // 60
            seconds = status['seconds_left'] % 60
            time_str = f"{minutes} мин {seconds} сек"
            text += f"⏳ *Следующий сбор:* через {time_str}\n"
        else:
            text += "⏳ *Следующий сбор:* скоро...\n"
        collect_text = "⏳ Сбор недоступен"

    text += f"\n{'—' * 20}\n"
    if biz_list:
        text += "Управляй империей 👇"
    else:
        text += "Пока пусто. Купи первый бизнес! 👇"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Купить бизнес", callback_data="biz_shop")],
        [InlineKeyboardButton(text="📋 Мои предприятия", callback_data="biz_my")],
    ])

    if status['can_collect']:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=collect_text, callback_data="biz_collect")])
    else:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=collect_text, callback_data="biz_collect_wait")])

    if is_callback:
        await target.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
    else:
        await message.answer(text, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(F.text == "🏢 Бизнес")
async def handle_business_button(message: Message):
    await cmd_business_menu(message)

@dp.callback_query(F.data == "biz_shop")
async def biz_shop(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)

    text = "🏪 *МАГАЗИН БИЗНЕСОВ*\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])

    for key, biz in BUSINESS_TYPES.items():
        display_price = int(biz["price"] / BUSINESS_PRICE_DIVISOR)
        text += f"**{biz['name']}** — {format_money(display_price)}\n"
        text += f"_{biz['description']}_\n"
        text += f"💰 Доход: {format_money(biz['base_income'])}/ч\n"
        if biz.get('salary_bonus'):
            text += f"📈 +{int(biz['salary_bonus']*100)}% к зарплате\n"
        if biz.get('asphalt_bonus'):
            text += f"🛣️ +{int(biz['asphalt_bonus']*100)}% к асфальту\n"
        text += "\n"

        kb.inline_keyboard.append([
            InlineKeyboardButton(text=f"✅ Купить {biz['name']}", callback_data=f"biz_buy_{key}")
        ])

    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="biz_back_to_menu")
    ])

    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("biz_buy_"))
async def biz_buy(callback: CallbackQuery):
    biz_key = callback.data[8:]
    success, msg = await buy_business(callback.from_user.id, biz_key)
    await callback.answer(msg, show_alert=True)
    if success:
        await cmd_business_menu(callback, user_id=callback.from_user.id)

@dp.callback_query(F.data == "biz_my")
async def biz_my(callback: CallbackQuery):
    user_id = callback.from_user.id
    biz_list = await get_user_businesses(user_id)

    if not biz_list:
        await callback.message.edit_text(
            "❌ У тебя ещё нет бизнеса.\nКупи первый через меню!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛒 Магазин бизнесов", callback_data="biz_shop")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="biz_back_to_menu")]
            ])
        )
        await callback.answer()
        return

    text = "📋 *МОИ ПРЕДПРИЯТИЯ*\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])

    total_income = 0
    for biz in biz_list:
        config = BUSINESS_TYPES[biz['biz_type']]
        income = await calculate_business_income(biz)
        total_income += income

        text += f"**{config['name']}** (ур. {biz['upgrade_level']})\n"
        text += f"💰 Доход: {format_money(income)}/ч\n"
        text += f"❤️ Прочность: {biz['health']}%\n\n"

        kb.inline_keyboard.append([
            InlineKeyboardButton(text=f"🔧 {config['name']}", callback_data=f"biz_info_{biz['id']}")
        ])

    text += f"💵 **Общий доход:** {format_money(total_income)}/ч"

    kb.inline_keyboard.append([
        InlineKeyboardButton(text="💰 Собрать доход", callback_data="biz_collect"),
        InlineKeyboardButton(text="🔙 Назад", callback_data="biz_back_to_menu")
    ])

    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("biz_info_"))
async def biz_info(callback: CallbackQuery):
    biz_id = int(callback.data[9:])
    user_id = callback.from_user.id

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM businesses WHERE id = ? AND owner_id = ?",
            (biz_id, user_id)
        )
        biz = await cursor.fetchone()

    if not biz:
        await callback.answer("❌ Бизнес не найден или не принадлежит тебе", show_alert=True)
        return

    biz = dict(biz)
    config = BUSINESS_TYPES[biz['biz_type']]
    income = await calculate_business_income(biz)
    upgrades_installed = await get_business_upgrades(biz_id)
    installed_levels = [u['upgrade_level'] for u in upgrades_installed]

    text = (
        f"🏭 **{config['name']}**\n"
        f"📊 Уровень прокачки: {biz['upgrade_level']}/{config.get('max_level', 3)}\n"
        f"💰 Текущий доход: {format_money(income)}/ч\n"
        f"❤️ Состояние: {biz['health']}%\n\n"
        f"**📈 Улучшения:**\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[])

    for lvl, up in config.get('upgrades', {}).items():
        status = "✅" if lvl in installed_levels else "❌"
        if lvl in installed_levels:
            text += f"• {status} {up['name']} (установлено)\n"
        elif lvl == biz['upgrade_level'] + 1:
            text += f"• {up['name']} — {format_money(up['cost'])}\n  _{up['desc']}_\n"
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=f"⬆️ Купить {up['name']}",
                                   callback_data=f"biz_upgrade_{biz_id}_{lvl}")
            ])
        else:
            text += f"• 🔒 Уровень {lvl} (требуется прокачка)\n"

    if not config.get('upgrades'):
        text += "Нет доступных улучшений.\n"

    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 К списку", callback_data="biz_my")
    ])

    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("biz_upgrade_"))
async def biz_upgrade(callback: CallbackQuery):
    parts = callback.data.split('_')
    biz_id = int(parts[2])
    lvl = int(parts[3])
    user_id = callback.from_user.id

    success, msg = await upgrade_business(user_id, biz_id, lvl)
    await callback.answer(msg, show_alert=True)

    if success:
        await biz_info(callback)

@dp.callback_query(F.data == "biz_collect")
async def biz_collect(callback: CallbackQuery):
    user_id = callback.from_user.id
    amount = await collect_business_income(user_id)

    if amount > 0:
        status = await get_business_collect_status(user_id)
        await callback.answer(f"💰 Собрано {format_money(amount)}!", show_alert=False)

        text = (
            f"💰 *ДОХОД СОБРАН!*\n\n"
            f"✅ Вы получили: {format_money(amount)}\n"
            f"💵 Текущий пассивный доход: {format_money(status['total_per_hour'])}/час\n\n"
            f"⏳ *Следующий сбор:* через 1 час\n"
            f"📅 (после нажатия кнопки)"
        )
        await callback.message.answer(text, parse_mode="Markdown")
        await cmd_business_menu(callback, user_id=user_id)
    else:
        await callback.answer("❌ Нет дохода для сбора (кулдаун 1 час)", show_alert=True)

@dp.callback_query(F.data == "biz_collect_wait")
async def biz_collect_wait(callback: CallbackQuery):
    user_id = callback.from_user.id
    status = await get_business_collect_status(user_id)
    if status['can_collect']:
        # Уже можно собрать, обновим меню
        await cmd_business_menu(callback, user_id=user_id)
        await callback.answer()
        return

    if status['seconds_left'] > 0:
        minutes = status['seconds_left'] // 60
        seconds = status['seconds_left'] % 60
        await callback.answer(f"⏳ До сбора: {minutes} мин {seconds} сек", show_alert=True)
    else:
        await callback.answer("⏳ Скоро будет доступно...", show_alert=True)

@dp.callback_query(F.data == "biz_back_to_menu")
async def biz_back_to_menu(callback: CallbackQuery):
    await cmd_business_menu(callback, user_id=callback.from_user.id)
    await callback.answer()

# ==================== ПЕРЕВОДЫ ====================
@dp.message(F.text == "🔁 Перевод")
async def handle_transfer_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        return
    all_users = await get_all_users()
    if len(all_users) <= 1:
        await message.answer("❌ Нет других сотрудников для перевода")
        return
    await message.answer(
        "👥 *Выберите получателя:*\n\n"
        f"Минимальный перевод: {format_money(ECONOMY_SETTINGS['min_transfer'])}\n"
        "Нажмите на сотрудника для перевода:",
        parse_mode="Markdown",
        reply_markup=get_users_keyboard(all_users, user_id, "transfer_to_")
    )
    await state.set_state(TransferStates.choosing_recipient)

@dp.callback_query(F.data.startswith("transfer_to_"), TransferStates.choosing_recipient)
async def handle_transfer_recipient(callback: CallbackQuery, state: FSMContext):
    try:
        recipient_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Некорректный получатель", show_alert=True)
        return
    sender_id = callback.from_user.id
    await state.update_data(recipient_id=recipient_id)
    recipient = await get_user(recipient_id)
    sender = await get_user(sender_id)
    if recipient and sender:
        await callback.message.edit_text(
            f"📤 *Перевод пользователю:*\n\n"
            f"👤 *{recipient['full_name']}*\n"
            f"💰 Баланс: {format_money(recipient['balance'])}\n"
            f"💼 Ваш баланс: {format_money(sender['balance'])}\n\n"
            f"💸 *Введите сумму перевода:*\n"
            f"Минимум: {format_money(ECONOMY_SETTINGS['min_transfer'])}\n"
            f"Максимум: {format_money(sender['balance'])}",
            parse_mode="Markdown"
        )
        await callback.message.answer(
            "↩️ Для отмены нажми «🔙 Назад».",
            reply_markup=get_state_back_inline_keyboard(),
        )
    await state.set_state(TransferStates.entering_amount)
    await callback.answer()

@dp.callback_query(F.data == "admin_cancel_pick")
async def handle_admin_cancel_pick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await state.clear()
    try:
        await callback.message.edit_text("❌ Действие отменено.")
    except Exception:
        await callback.message.answer("❌ Действие отменено.")
    await callback.answer()


@dp.callback_query(F.data == "duel_cancel_choose")
async def duel_cancel_choose(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.edit_text(
            "❌ Дуэль отменена.",
            parse_mode="Markdown",
            reply_markup=get_minigames_keyboard(),
        )
    except Exception:
        await callback.message.answer(
            "❌ Дуэль отменена.",
            parse_mode="Markdown",
            reply_markup=get_minigames_keyboard(),
        )
    await callback.answer()


@dp.callback_query(F.data == "cancel_transfer")
async def handle_cancel_transfer(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.edit_text(
            "❌ Перевод отменен."
        )
        await callback.message.answer(
            "Главное меню:",
            reply_markup=get_main_keyboard(callback.from_user.id),
        )
    except Exception:
        await callback.message.answer(
            "❌ Перевод отменен.",
            reply_markup=get_main_keyboard(callback.from_user.id),
        )
    await callback.answer()

@dp.message(TransferStates.entering_amount)
async def handle_transfer_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    sender = await get_user(user_id)
    if not sender:
        await message.answer("❌ Ошибка: отправитель не найден")
        await state.clear()
        return
    try:
        amount = int(message.text)
        if amount < ECONOMY_SETTINGS["min_transfer"]:
            await message.answer(f"❌ Минимальная сумма перевода - {format_money(ECONOMY_SETTINGS['min_transfer'])}")
            return

        data = await state.get_data()
        recipient_id = data.get("recipient_id")
        if not recipient_id:
            await message.answer("❌ Ошибка: получатель не выбран")
            await state.clear()
            return

        recipient = await get_user(recipient_id)
        if not recipient:
            await message.answer("❌ Ошибка: получатель не найден")
            await state.clear()
            return

        ge = await get_global_economy()
        mods = await get_player_modifiers(user_id)
        social = await get_social_status_for_user(user_id)

        # Комиссия по ТЗ:
        # - базовая: amount * transfer_commission_pct
        # - "Легенда" => 0%
        # - "Кидала" => +5% (у вас хранится в transfer_extra_fee)
        if social.get("status_key") == "legend":
            fee = 0
        else:
            fee_pct = float(ge.get("transfer_commission_pct", 0.0)) + float(mods.get("transfer_extra_fee", 0.0))
            fee = max(0, int(round(amount * fee_pct)))

        total_debit = amount + fee
        if total_debit > sender["balance"]:
            await message.answer(
                f"❌ Не хватает с учётом комиссии.\n"
                f"Нужно всего: {format_money(total_debit)} "
                f"(перевод {format_money(amount)} + комиссия {format_money(fee)}).\n"
                f"Твой баланс: {format_money(sender['balance'])}"
            )
            await state.clear()
            return

        await state.update_data(
            recipient_id=recipient_id,
            amount=amount,
            fee=fee,
            total_debit=total_debit,
        )
        await state.set_state(TransferStates.confirming)

        fee_base_pct = int(float(ge.get("transfer_commission_pct", 0.0)) * 100)
        kidala_extra_pct = float(mods.get("transfer_extra_fee", 0.0))
        if social.get("status_key") == "legend":
            fee_note = " (Легенда — 0%)"
        elif kidala_extra_pct > 0:
            fee_note = f" (база {fee_base_pct}% + Кидала {int(round(kidala_extra_pct * 100))}%)"
        else:
            fee_note = f" (база {fee_base_pct}%)"

        await message.answer(
            "📤 *ПОДТВЕРЖДЕНИЕ ПЕРЕВОДА*\n\n"
            f"👤 Получатель: {recipient['full_name']}\n"
            f"💰 Сумма перевода: {format_money(amount)}\n"
            f"💸 Комиссия: {format_money(fee)}{fee_note}\n"
            f"📦 Итого к списанию: {format_money(total_debit)}\n\n"
            "Подтвердите операцию:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_transfer")],
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_transfer")],
                ]
            ),
        )
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")
        return


@dp.callback_query(F.data == "confirm_transfer", TransferStates.confirming)
async def confirm_transfer(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = await state.get_data()
    recipient_id = data.get("recipient_id")
    amount = data.get("amount")
    fee = data.get("fee")
    total_debit = data.get("total_debit")

    if not recipient_id or amount is None or fee is None or total_debit is None:
        await callback.answer("❌ Ошибка подтверждения, повторите перевод.", show_alert=True)
        await state.clear()
        return

    sender = await get_user(user_id)
    recipient = await get_user(int(recipient_id))
    if not sender or not recipient:
        await callback.answer("❌ Ошибка данных, повторите перевод.", show_alert=True)
        await state.clear()
        return
    if int(total_debit) > int(sender["balance"]):
        await callback.answer("❌ Недостаточно средств.", show_alert=True)
        await state.clear()
        return

    sender_balance_before = int(sender["balance"])
    await update_balance(
        user_id,
        -int(amount),
        "transfer_out",
        f"Перевод {recipient['full_name']} (−{int(amount)}₽)",
    )
    if int(fee) > 0:
        await update_balance(
            user_id,
            -int(fee),
            "transfer_fee",
            f"Комиссия перевода (−{int(fee)}₽) → {recipient['full_name']}",
        )
    await update_balance(
        int(recipient_id),
        int(amount),
        "transfer_in",
        f"Перевод от {sender['full_name']}",
    )

    sender_updated = await get_user(user_id)
    recipient_updated = await get_user(int(recipient_id))
    if sender_updated:
        try:
            await post_poverty_transition_if_needed(
                user_id,
                sender_balance_before,
                int(sender_updated["balance"]),
            )
        except Exception as e:
            logger.error("Ошибка пост-обработки бедности (transfer) user_id=%s: %s", user_id, e, exc_info=True)
    rep_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"{s}⭐", callback_data=f"rep_tr_{user_id}_{s}")
                for s in (1, 2, 3)
            ]
        ]
    )

    await callback.message.edit_text(
        f"✅ *Перевод выполнен*\n\n"
        f"📤 Получатель получит: {format_money(int(amount))}\n"
        f"💸 Комиссия: {format_money(int(fee))}\n"
        f"💰 Списано с тебя: {format_money(int(total_debit))}\n"
        f"💳 Твой баланс: {format_money(sender_updated['balance'])}\n",
        parse_mode="Markdown",
    )
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_keyboard(user_id),
    )

    try:
        await bot.send_message(
            int(recipient_id),
            f"💰 *Перевод*\n\n"
            f"📥 Получено: {format_money(int(amount))}\n"
            f"👤 От: {sender['full_name']}\n"
            f"💳 Баланс: {format_money(recipient_updated['balance'])}\n\n"
            f"Оцени надёжность отправителя (1–3):",
            parse_mode="Markdown",
            reply_markup=rep_kb,
        )
    except Exception:
        pass

    await state.clear()


def get_inv_gift_users_keyboard(
    inv_id: int, users: List[Dict[str, Any]], exclude_id: int
) -> InlineKeyboardMarkup:
    buttons = []
    for u in users:
        if u["user_id"] == exclude_id:
            continue
        name = u["full_name"]
        if len(name) > 22:
            name = name[:19] + "..."
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"👤 {name}",
                    callback_data=f"inv_target_{inv_id}_{u['user_id']}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="inv_gift_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_inventory_keyboard(rows: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    buttons = []
    for r in rows:
        item = next((i for i in SHOP_ITEMS if i["id"] == r["item_id"]), None)
        label = (item["name"] if item else r["item_id"])[:18]
        if len((item["name"] if item else r["item_id"])) > 18:
            label += "…"
        iid = r["id"]
        q = r["quantity"]
        buttons.append(
            [
                InlineKeyboardButton(text=f"▶️ {label} ×{q}", callback_data=f"inv_use_{iid}"),
                InlineKeyboardButton(text="🗑", callback_data=f"inv_drop_{iid}"),
                InlineKeyboardButton(text="🎁", callback_data=f"inv_give_{iid}"),
            ]
        )
    if not buttons:
        buttons.append([InlineKeyboardButton(text="— Пусто —", callback_data="inv_empty")])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="inv_refresh")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.message(F.text == "🎒 Инвентарь")
async def handle_inventory(message: Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        return
    rows = await get_inventory_rows(user_id)
    used = await inventory_slots_used(user_id)
    cap = await max_inventory_slots_for_user(user_id)
    lines = [f"🎒 *Инвентарь* ({used}/{cap} слотов)\n"]
    if not rows:
        lines.append("_Пусто._ Купи товары в магазине.")
    else:
        for r in rows:
            it = next((i for i in SHOP_ITEMS if i["id"] == r["item_id"]), None)
            nm = it["name"] if it else r["item_id"]
            lines.append(f"• {nm} ×{r['quantity']}")
    await message.answer(
        "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=build_inventory_keyboard(rows),
    )


@dp.callback_query(F.data == "inv_empty")
async def inv_empty_cb(callback: CallbackQuery):
    await callback.answer("Инвентарь пуст", show_alert=True)


@dp.callback_query(F.data == "inv_refresh")
async def inv_refresh_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    rows = await get_inventory_rows(user_id)
    used = await inventory_slots_used(user_id)
    cap = await max_inventory_slots_for_user(user_id)
    lines = [f"🎒 *Инвентарь* ({used}/{cap} слотов)\n"]
    if not rows:
        lines.append("_Пусто._")
    else:
        for r in rows:
            it = next((i for i in SHOP_ITEMS if i["id"] == r["item_id"]), None)
            nm = it["name"] if it else r["item_id"]
            lines.append(f"• {nm} ×{r['quantity']}")
    try:
        await callback.message.edit_text(
            "\n".join(lines), parse_mode="Markdown", reply_markup=build_inventory_keyboard(rows)
        )
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("inv_use_"))
async def inv_use_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        inv_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Некорректный предмет", show_alert=True)
        return
    stack = await get_inventory_stack(inv_id, user_id)
    if not stack or stack["quantity"] < 1:
        await callback.answer("❌ Нет такого стака", show_alert=True)
        return
    item = next((i for i in SHOP_ITEMS if i["id"] == stack["item_id"]), None)
    if not item:
        await callback.answer("❌ Неизвестный предмет", show_alert=True)
        return
    if not await remove_inventory_stack_row(inv_id, user_id, 1):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    effect = await apply_shop_item_effect(user_id, item)
    await callback.answer("✅ Применено", show_alert=False)
    try:
        await callback.message.answer(f"🎒 *{item['name']}*\n{effect}", parse_mode="Markdown")
    except Exception:
        pass
    rows = await get_inventory_rows(user_id)
    used = await inventory_slots_used(user_id)
    cap = await max_inventory_slots_for_user(user_id)
    hdr = f"🎒 *Инвентарь* ({used}/{cap} слотов)\n"
    body_lines = []
    for r in rows:
        it = next((i for i in SHOP_ITEMS if i["id"] == r["item_id"]), None)
        nm = it["name"] if it else r["item_id"]
        body_lines.append(f"• {nm} ×{r['quantity']}")
    body = "\n".join(body_lines) if body_lines else "_Пусто._"
    try:
        await callback.message.edit_text(
            hdr + body,
            parse_mode="Markdown",
            reply_markup=build_inventory_keyboard(rows),
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("inv_drop_"))
async def inv_drop_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        inv_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Некорректный предмет", show_alert=True)
        return
    if not await remove_inventory_stack_row(inv_id, user_id, 1):
        await callback.answer("❌ Не вышло", show_alert=True)
        return
    await callback.answer("Выброшено", show_alert=False)
    rows = await get_inventory_rows(user_id)
    used = await inventory_slots_used(user_id)
    cap = await max_inventory_slots_for_user(user_id)
    hdr = f"🎒 *Инвентарь* ({used}/{cap} слотов)\n"
    body_lines = []
    for r in rows:
        it = next((i for i in SHOP_ITEMS if i["id"] == r["item_id"]), None)
        nm = it["name"] if it else r["item_id"]
        body_lines.append(f"• {nm} ×{r['quantity']}")
    body = "\n".join(body_lines) if body_lines else "_Пусто._"
    try:
        await callback.message.edit_text(
            hdr + body,
            parse_mode="Markdown",
            reply_markup=build_inventory_keyboard(rows),
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("inv_give_"))
async def inv_give_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        inv_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Некорректный предмет", show_alert=True)
        return
    stack = await get_inventory_stack(inv_id, user_id)
    if not stack:
        await callback.answer("❌ Нет предмета", show_alert=True)
        return
    all_u = await get_all_users()
    await callback.message.answer(
        "🎁 *Передача предмета*\nВыбери получателя:",
        parse_mode="Markdown",
        reply_markup=get_inv_gift_users_keyboard(inv_id, all_u, user_id),
    )
    await callback.answer()


@dp.callback_query(F.data == "inv_gift_cancel")
async def inv_gift_cancel_cb(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer("Отменено")


@dp.callback_query(F.data.startswith("inv_target_"))
async def inv_target_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    rest = callback.data[len("inv_target_") :]
    inv_part, _, rid_part = rest.rpartition("_")
    try:
        inv_id = int(inv_part)
        target_id = int(rid_part)
    except ValueError:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    if target_id == user_id:
        await callback.answer("❌ Себе нельзя", show_alert=True)
        return
    stack = await get_inventory_stack(inv_id, user_id)
    if not stack or stack["quantity"] < 1:
        await callback.answer("❌ Предмета нет", show_alert=True)
        return
    ok, msg = await add_inventory_item(target_id, stack["item_id"], 1)
    if not ok:
        await callback.answer(msg[:160], show_alert=True)
        return
    await remove_inventory_stack_row(inv_id, user_id, 1)
    item = next((i for i in SHOP_ITEMS if i["id"] == stack["item_id"]), None)
    iname = item["name"] if item else stack["item_id"]
    giver = await get_user(user_id)
    getter = await get_user(target_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer("Передано!", show_alert=False)
    await callback.message.answer(f"✅ Ты передал: *{iname}*", parse_mode="Markdown")
    try:
        await bot.send_message(
            target_id,
            f"🎁 *Подарок от {giver['full_name'] if giver else 'коллеги'}:* {iname}",
            parse_mode="Markdown",
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("rep_duel_"))
async def handle_rep_duel(callback: CallbackQuery):
    rest = callback.data[len("rep_duel_") :]
    opp_part, _, stars_part = rest.rpartition("_")
    try:
        opponent_id = int(opp_part)
        stars = int(stars_part)
    except ValueError:
        await callback.answer()
        return
    from_uid = callback.from_user.id
    if opponent_id == from_uid or stars < 1 or stars > 3:
        await callback.answer("❌ Некорректно", show_alert=True)
        return
    ok, msg = await add_reputation_vote(from_uid, opponent_id, stars, "duel")
    if not ok:
        await callback.answer(msg, show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer(f"✅ Оценка *{stars}/3* записана.", parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data.startswith("rep_tr_"))
async def handle_rep_transfer(callback: CallbackQuery):
    rest = callback.data[len("rep_tr_") :]
    sender_part, _, stars_part = rest.rpartition("_")
    try:
        sender_id = int(sender_part)
        stars = int(stars_part)
    except ValueError:
        await callback.answer()
        return
    from_uid = callback.from_user.id
    if sender_id == from_uid or stars < 1 or stars > 3:
        await callback.answer("❌ Некорректно", show_alert=True)
        return
    ok, msg = await add_reputation_vote(from_uid, sender_id, stars, "transfer")
    if not ok:
        await callback.answer(msg, show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer(f"✅ Репутация отправителя: *{stars}/3*.", parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "admin_economy")
async def admin_economy_menu(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    ge = await get_global_economy()
    txt = (
        "📈 *Глобальная экономика*\n\n"
        "Введите значение числами (шаги кнопками не используются).\n\n"
        f"• Множитель штрафов (`fine_scale`): ×{ge['fine_scale']:.2f}\n"
        f"• Комиссия переводов (`transfer_commission_pct`): {ge['transfer_commission_pct']*100:.2f}%\n"
        f"• Шанс налога на бизнес (`business_tax_chance`): {ge['business_tax_chance']*100:.2f}%\n"
        f"• Доля конфискации (`business_tax_take_pct`): {ge['business_tax_take_pct']*100:.2f}%\n\n"
        "Выберите параметр:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Штрафы (fine_scale)", callback_data="admin_econ_choose_fine_scale"),
                InlineKeyboardButton(text="Комиссия переводов", callback_data="admin_econ_choose_transfer_commission_pct"),
            ],
            [
                InlineKeyboardButton(text="Налог: шанс", callback_data="admin_econ_choose_business_tax_chance"),
                InlineKeyboardButton(text="Налог: доля", callback_data="admin_econ_choose_business_tax_take_pct"),
            ],
            [InlineKeyboardButton(text="🔙 Админка", callback_data="admin_back")],
        ]
    )
    try:
        await callback.message.edit_text(txt, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        await callback.message.answer(txt, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_econ_choose_"))
async def admin_economy_choose_param(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    param = callback.data[len("admin_econ_choose_") :]
    if param not in {"fine_scale", "transfer_commission_pct", "business_tax_chance", "business_tax_take_pct"}:
        await callback.answer("❌ Неизвестный параметр", show_alert=True)
        return
    await state.update_data(param=param)
    await state.set_state(AdminEconomySetStates.waiting_value)
    await callback.message.answer(
        "🔢 Введи новое значение числом.\n"
        "Пример для `fine_scale`: 1.20\n"
        "Пример для `transfer_commission_pct`: 0.025 (это 2.5%)\n\n"
        "Отмена: напиши `отмена`.",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await callback.answer()


@dp.message(AdminEconomySetStates.waiting_value)
async def admin_economy_set_value(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    if not message.text:
        return
    if message.text.strip().lower() == "отмена":
        await state.clear()
        await message.answer("Ок, отменено.", reply_markup=get_admin_keyboard())
        return
    try:
        val = float(message.text.strip().replace(",", "."))
    except ValueError:
        await message.answer("❌ Нужно число. Например: 1.20 или 0.025")
        return

    data = await state.get_data()
    param = data.get("param")
    if not param:
        await state.clear()
        await message.answer("❌ Не выбран параметр. Попробуй заново.")
        return

    ge = await set_global_economy_param(param, val)
    await post_chronicle(
        f"📢 *Виталик крутит гайки:* обновлены глобальные штрафы/налоги/комиссии. "
        f"Штрафы ×{ge['fine_scale']:.2f}, комиссия {ge['transfer_commission_pct']*100:.2f}%, "
        f"налог: шанс {ge['business_tax_chance']*100:.2f}%, изъятие {ge['business_tax_take_pct']*100:.2f}%."
    )
    await state.clear()

    txt = (
        "📈 *Глобальная экономика*\n\n"
        "Текущие значения:\n\n"
        f"• Множитель штрафов (`fine_scale`): ×{ge['fine_scale']:.2f}\n"
        f"• Комиссия переводов (`transfer_commission_pct`): {ge['transfer_commission_pct']*100:.2f}%\n"
        f"• Шанс налога на бизнес (`business_tax_chance`): {ge['business_tax_chance']*100:.2f}%\n"
        f"• Доля конфискации (`business_tax_take_pct`): {ge['business_tax_take_pct']*100:.2f}%\n\n"
        "Выберите параметр:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Штрафы (fine_scale)", callback_data="admin_econ_choose_fine_scale"),
                InlineKeyboardButton(text="Комиссия переводов", callback_data="admin_econ_choose_transfer_commission_pct"),
            ],
            [
                InlineKeyboardButton(text="Налог: шанс", callback_data="admin_econ_choose_business_tax_chance"),
                InlineKeyboardButton(text="Налог: доля", callback_data="admin_econ_choose_business_tax_take_pct"),
            ],
            [InlineKeyboardButton(text="🔙 Админка", callback_data="admin_back")],
        ]
    )
    await message.answer(txt, parse_mode="Markdown", reply_markup=kb)


@dp.callback_query(F.data.startswith("adeca_"))
async def admin_economy_adjust(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    d = callback.data
    fs = comm = tc = tt = 0.0
    if d == "adeca_fine_p1":
        fs = 0.10
    elif d == "adeca_fine_p25":
        fs = 0.25
    elif d == "adeca_comm_p005":
        comm = 0.005
    elif d == "adeca_comm_p01":
        comm = 0.01
    elif d == "adeca_taxch_p05":
        tc = 0.05
    elif d == "adeca_taxtk_p05":
        tt = 0.05
    ge = await adjust_global_economy(fs, comm, tc, tt)
    await post_chronicle(
        f"📢 *Виталик крутит гайки:* обновлены штрафы/налоги/комиссии. "
        f"Штрафы ×{ge['fine_scale']:.2f}, комиссия {ge['transfer_commission_pct']*100:.1f}%, "
        f"налог: шанс {ge['business_tax_chance']*100:.1f}%, изъятие {ge['business_tax_take_pct']*100:.1f}%."
    )
    await admin_economy_menu(callback)


# ==================== БАНК «АСФАЛЬТ-КАПИТАЛ» ====================
async def format_bank_menu_text(user_id: int) -> Optional[str]:
    user = await get_user(user_id)
    if not user:
        return None
    await bank_accrue_interest_tick()
    loan = await get_active_bank_loan(user_id)
    dep_details = await get_user_deposit_details(user_id)
    dep_amt = int(dep_details["amount"])
    dep_interest = int(dep_details["total_interest"])

    rep_v = await get_reputation_percent(user_id, user)

    dep_rate_h = float(BANK_SETTINGS["deposit_hourly_rate"])
    dep_rate_day = dep_rate_h * 24.0
    loan_rate_h = float(BANK_SETTINGS["hourly_interest_rate"])
    loan_rate_day = loan_rate_h * 24.0

    interval_sec = int(BANK_SETTINGS.get("deposit_interest_interval_sec", 3600))
    last_dt = safe_parse_datetime(dep_details["last_interest_at"]) if dep_details["last_interest_at"] else None
    next_in_sec = None
    if dep_amt > 0 and last_dt:
        elapsed = (datetime.now() - last_dt).total_seconds()
        next_in_sec = max(0, int(interval_sec - elapsed))

    next_in_s = "—"
    if next_in_sec is not None:
        mins = next_in_sec // 60
        next_in_s = f"через {mins} мин" if mins > 0 else "сейчас"

    # Лимит кредита расширяется для «Мафиози».
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT COUNT(*) AS cnt FROM businesses WHERE owner_id = ? AND is_active = 1",
            (user_id,),
        )
        biz_cnt = int((await cur.fetchone())[0] or 0)
    is_mafia = int(user.get("duels_won") or 0) >= 100 and biz_cnt >= 10 and int(user.get("balance") or 0) >= 1_000_000
    max_loan_cap = int(BANK_SETTINGS["max_loan"] * (1.5 if is_mafia else 1.0))

    text = (
        "🏦 *АСФАЛЬТ-КАПИТАЛ*\n"
        f"💰 Баланс: {format_money(user['balance'])}\n"
        f"⭐ Репутация: {rep_v:.0f}/100 _(влияет на одобрение кредитов: <40 отказ, 40–69 средний лимит, 70+ полный лимит)_\n\n"
        "📊 *Депозиты (вклады)*\n"
        f"• Ставка: {dep_rate_h*100:.1f}% в час (~{dep_rate_day*100:.0f}% в сутки)\n"
        f"• Минимум: {format_money(BANK_SETTINGS['min_deposit'])}\n"
        f"• Твой депозит: {format_money(dep_amt)}\n"
    )

    if dep_amt > 0:
        text += (
            f"• Начислено за всё время: {format_money(dep_interest)}\n"
            f"• Следующее начисление: {next_in_s}\n"
            "\n"
        )
    else:
        text += "• Начислений пока нет\n\n"

    text += (
        "📈 *Кредиты (заёмы)*\n"
        f"• Ставка: {loan_rate_h*100:.1f}% в час (~{loan_rate_day*100:.0f}% в сутки)\n"
        f"• Минимум: {format_money(BANK_SETTINGS['min_loan'])}\n"
        f"• Максимум: {format_money(max_loan_cap)}\n"
        f"• Срок: {BANK_SETTINGS['term_hours']} ч\n"
        "\n"
    )

    if loan:
        due = safe_parse_datetime(loan["due_at"])
        due_s = due.strftime("%d.%m %H:%M") if due else "—"
        st = "🔴 ПРОСРОЧКА / коллекторы" if loan.get("defaulted") else "🟢 В сроке"
        text += (
            "📋 *Твой долг*\n"
            f"• Остаток долга: {format_money(loan['remaining'])}\n"
            f"• Взято: {format_money(loan['principal'])}\n"
            f"• До: {due_s}\n"
            f"• Статус: {st}\n"
        )
    else:
        text += "✅ Активного долга нет — можешь взять кредит.\n"
    return text


async def send_bank_menu(message: Message, user_id: int):
    text = await format_bank_menu_text(user_id)
    if text is None:
        await message.answer(NOT_REGISTERED_HINT)
        return
    await message.answer(text, parse_mode="Markdown", reply_markup=get_bank_menu_keyboard())


@dp.message(F.text == "🏦 Асфальт-Капитал")
async def handle_bank_button(message: Message):
    await send_bank_menu(message, message.from_user.id)


@dp.message(F.text == "🏅 Ачивки")
async def handle_achievements_button(message: Message):
    txt = await format_achievements_screen(message.from_user.id)
    await message.answer(txt, parse_mode="Markdown")


@dp.callback_query(F.data == "bank_menu_refresh")
async def bank_menu_refresh_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    text = await format_bank_menu_text(uid)
    if text is None:
        await callback.answer(NOT_REGISTERED_ALERT, show_alert=True)
        return
    try:
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=get_bank_menu_keyboard())
    except Exception:
        await callback.message.answer(text, parse_mode="Markdown", reply_markup=get_bank_menu_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "bank_info_pool")
async def bank_info_pool_cb(callback: CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    pool = await get_bank_pool_liquidity()
    dep = await get_user_deposit(uid)
    txt = (
        f"🏦 *Касса банка*\n\n"
        f"💵 Свободная ликвидность: *{format_money(pool)}*\n"
        f"Твой вклад: *{format_money(dep)}*\n\n"
        "Кредиты забирают деньги из этой кассы. Вклады игроков её пополняют. "
        "Если касса пуста — новые кредиты не выдаются, пока кто-то не внесёт вклад "
        "или заёмщики не вернут долги."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 В меню банка", callback_data="bank_menu_refresh")],
        ]
    )
    try:
        await callback.message.edit_text(txt, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        await callback.message.answer(txt, parse_mode="Markdown", reply_markup=kb)


@dp.callback_query(F.data == "bank_dep_start")
async def bank_dep_start_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    user = await get_user(uid)
    if not user:
        return
    await state.set_state(BankStates.waiting_deposit_amt)
    await callback.message.answer(
        f"💰 *Инвестировать в «{BANK_SETTINGS['name']}»*\n\n"
        f"Минимум: {format_money(BANK_SETTINGS['min_deposit'])}\n"
        f"Ставка: {float(BANK_SETTINGS['deposit_hourly_rate'])*100:.1f}% в час (~{float(BANK_SETTINGS['deposit_hourly_rate'])*24*100:.0f}% в сутки)\n"
        f"Баланс: {format_money(user['balance'])}\n\n"
        f"Введи сумму одним числом (деньги замораживаются на вкладе).",
        parse_mode="Markdown",
        reply_markup=get_state_back_inline_keyboard(),
    )

@dp.callback_query(F.data == "bank_loan_start")
async def bank_loan_start_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    user = await get_user(uid)
    if not user:
        await callback.message.answer(NOT_REGISTERED_HINT)
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT COUNT(*) AS cnt FROM businesses WHERE owner_id = ? AND is_active = 1",
            (uid,),
        )
        biz_cnt = int((await cur.fetchone())[0] or 0)
    is_mafia = int(user.get("duels_won") or 0) >= 100 and biz_cnt >= 10 and int(user.get("balance") or 0) >= 1_000_000
    max_loan = int(BANK_SETTINGS["max_loan"] * (1.5 if is_mafia else 1.0))
    await state.set_state(BankStates.waiting_custom_loan)
    await callback.message.answer(
        "📈 *Взять кредит*\n\n"
        f"Минимум: {format_money(BANK_SETTINGS['min_loan'])}\n"
        f"Максимум: {format_money(max_loan)}\n"
        f"Ставка: {float(BANK_SETTINGS['hourly_interest_rate'])*100:.1f}% в час\n"
        f"Срок: {BANK_SETTINGS['term_hours']} ч\n\n"
        "Введи сумму одним числом.",
        parse_mode="Markdown",
        reply_markup=get_state_back_inline_keyboard(),
    )

@dp.callback_query(F.data == "bank_dep_close")
async def bank_dep_close_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    ok, msg = await bank_player_close_deposit(uid)
    # Быстрый фидбек в виде pop-up, а подробности — сообщением.
    await callback.answer("✅" if ok else "❌", show_alert=False)
    await callback.message.answer(msg)
    if not ok:
        return
    # Обновляем меню банка (по возможности — редактированием текущего сообщения),
    # чтобы после закрытия депозита сразу был актуальный статус.
    text = await format_bank_menu_text(uid)
    if text is None:
        return
    try:
        await callback.message.edit_text(
            text,
            parse_mode="Markdown",
            reply_markup=get_bank_menu_keyboard(),
        )
    except Exception:
        await callback.message.answer(
            text,
            parse_mode="Markdown",
            reply_markup=get_bank_menu_keyboard(),
        )


@dp.callback_query(F.data == "bank_wd_start")
async def bank_wd_start_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    dep = await get_user_deposit(uid)
    pool = await get_bank_pool_liquidity()
    if dep <= 0:
        await callback.message.answer("❌ У тебя нет вклада для вывода.")
        return
    await state.set_state(BankStates.waiting_withdraw_amt)
    await callback.message.answer(
        f"📤 *Вывод вклада*\n\n"
        f"На вкладе: {format_money(dep)}\n"
        f"В кассе сейчас: {format_money(pool)}\n\n"
        f"Введи сумму вывода (не больше вклада и доступной кассы).",
        parse_mode="Markdown",
        reply_markup=get_state_back_inline_keyboard(),
    )


@dp.message(BankStates.waiting_deposit_amt)
async def bank_deposit_amount_msg(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not message.text:
        return
    try:
        amt = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Нужно целое число.")
        return
    ok, msg = await bank_player_deposit(user_id, amt)
    await message.answer(msg)
    await state.clear()
    if ok:
        await send_bank_menu(message, user_id)


@dp.message(BankStates.waiting_withdraw_amt)
async def bank_withdraw_amount_msg(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not message.text:
        return
    try:
        amt = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Нужно целое число.")
        return
    ok, msg = await bank_player_withdraw(user_id, amt)
    await message.answer(msg)
    await state.clear()
    if ok:
        await send_bank_menu(message, user_id)


@dp.callback_query(F.data.startswith("bank_take_"))
async def bank_take_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if callback.data == "bank_take_custom":
        if await get_active_bank_loan(user_id):
            await callback.answer("❌ Сначала погаси текущий кредит.", show_alert=True)
            return
        await callback.message.answer(
            f"✏️ Введи сумму кредита ({format_money(BANK_SETTINGS['min_loan'])} — "
            f"{format_money(BANK_SETTINGS['max_loan'])}), одним числом:",
            reply_markup=get_state_back_inline_keyboard(),
        )
        await state.set_state(BankStates.waiting_custom_loan)
        await callback.answer()
        return
    preset = {"bank_take_5000": 5000, "bank_take_15000": 15000, "bank_take_40000": 40000}.get(callback.data)
    if not preset:
        await callback.answer()
        return
    ok, msg = await issue_bank_loan(user_id, preset)
    if ok:
        await callback.answer("✅ Кредит выдан", show_alert=False)
        await send_bank_menu(callback.message, user_id)
    else:
        await callback.answer(msg[:180], show_alert=True)


@dp.message(BankStates.waiting_custom_loan)
async def bank_custom_loan_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not message.text:
        return
    try:
        amt = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Нужно целое число.")
        return
    ok, msg = await issue_bank_loan(user_id, amt)
    await message.answer(msg)
    await state.clear()
    if ok:
        await send_bank_menu(message, user_id)


@dp.callback_query(F.data == "bank_repay_start")
async def bank_repay_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await get_active_bank_loan(user_id):
        await callback.answer("❌ Нет долга для погашения.", show_alert=True)
        return
    await callback.message.answer(
        "💳 Введи сумму погашения (целое число, не больше баланса и долга):",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await state.set_state(BankStates.waiting_repay)
    await callback.answer()


@dp.message(BankStates.waiting_repay)
async def bank_repay_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not message.text:
        return
    try:
        amt = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Нужно целое число.")
        return
    ok, msg = await repay_bank_loan(user_id, amt)
    await message.answer(msg)
    await state.clear()
    if ok or await get_active_bank_loan(user_id):
        await send_bank_menu(message, user_id)


@dp.callback_query(F.data == "bank_status")
async def bank_status_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    await bank_accrue_interest_tick()
    loan = await get_active_bank_loan(user_id)
    if not loan:
        await callback.answer("Нет активного кредита", show_alert=True)
        return
    due = safe_parse_datetime(loan["due_at"])
    due_s = due.strftime("%d.%m %H:%M") if due else "—"
    st = "просрочка" if loan.get("defaulted") else "в сроке"
    await callback.answer(
        f"Долг {format_money(loan['remaining'])}, до {due_s}, {st}",
        show_alert=True,
    )


@dp.callback_query(F.data == "bank_close")
async def bank_close_callback(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer()


# ==================== АДМИН-ПАНЕЛЬ ====================
@dp.message(F.text == "👑 Админ-панель")
async def handle_admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещен!")
        return
    admin_text = (
        "👑 *Админ-панель*\n\n"
        "Управление только кнопками ниже — слэш-команды не используются."
    )
    await message.answer(admin_text, parse_mode="Markdown", reply_markup=get_admin_keyboard())

@dp.callback_query(F.data == "admin_broadcast")
async def handle_admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    await callback.message.answer(
        "📢 *Режим рассылки*\n\n"
        "Отправь текст объявления — его получат все игроки.\n"
        "Отмена: кнопка «❌ Отмена рассылки» под полем ввода.",
        parse_mode="Markdown",
        reply_markup=get_broadcast_cancel_keyboard(),
    )
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()


@dp.callback_query(F.data == "admin_bank_inject")
async def handle_admin_bank_inject_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    pool = await get_bank_pool_liquidity()
    await callback.message.answer(
        f"🏦 *Вливание в кассу банка*\n\n"
        f"Сейчас в кассе: {format_money(pool)}\n"
        f"Введи сумму целым числом (добавится в пул ликвидности).",
        parse_mode="Markdown",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await state.set_state(AdminBankInjectStates.waiting_amount)
    await callback.answer()


@dp.message(AdminBankInjectStates.waiting_amount)
async def handle_admin_bank_inject_amount(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    if not message.text:
        return
    try:
        amount = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Нужно целое число.")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля.")
        return
    await bank_pool_add(amount)
    pool = await get_bank_pool_liquidity()
    await state.clear()
    await message.answer(
        f"✅ В кассу влито {format_money(amount)}.\n"
        f"Текущая ликвидность: {format_money(pool)}.",
        reply_markup=get_main_keyboard(ADMIN_ID),
    )
    logger.info("Админ %s влил в bank_pool %s", message.from_user.id, amount)


@dp.message(BroadcastStates.waiting_for_message)
async def handle_broadcast_message(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    if message.text == "❌ Отмена рассылки":
        await state.clear()
        await message.answer("❌ Рассылка отменена.", reply_markup=get_main_keyboard(ADMIN_ID))
        return
    all_users = await get_all_users()
    if not all_users:
        await message.answer("❌ Нет пользователей для рассылки", reply_markup=get_main_keyboard(ADMIN_ID))
        await state.clear()
        return
    await message.answer(
        f"⏳ Начинаю рассылку для {len(all_users)} пользователей...",
        reply_markup=get_main_keyboard(ADMIN_ID),
    )
    success_count = 0
    fail_count = 0
    failed: List[str] = []
    for user in all_users:
        try:
            # Копируем исходное сообщение админа (текст/медиа/username-упоминания и entities сохраняются).
            await bot.copy_message(
                chat_id=user["user_id"],
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
            success_count += 1
            await asyncio.sleep(0.1)
        except Exception:
            fail_count += 1
            uname = (user.get("username") or "").strip()
            label = f"@{uname}" if uname and uname.lower() != "без username" else str(user.get("user_id"))
            failed.append(label)
    report = (
        f"📊 *Отчет о рассылке*\n\n"
        f"✅ Успешно отправлено: {success_count}\n"
        f"❌ Не отправлено: {fail_count}\n"
        f"📈 Общий охват: {len(all_users)} пользователей"
    )
    if failed:
        preview = ", ".join(failed[:25])
        tail = "" if len(failed) <= 25 else f"\n…и еще {len(failed) - 25}"
        report += f"\n\n👤 Не дошло до:\n{preview}{tail}"
    await message.answer(report, parse_mode="Markdown", reply_markup=get_main_keyboard(ADMIN_ID))
    await state.clear()

@dp.callback_query(F.data == "admin_fine")
async def handle_admin_fine_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    all_users = await get_all_users()
    await callback.message.answer(
        "⚡ *Штраф пользователя*\n\n"
        "Выберите пользователя для штрафа:",
        reply_markup=get_users_keyboard(
            all_users, ADMIN_ID, "admin_fine_", cancel_callback="admin_cancel_pick"
        )
    )
    await state.set_state(AdminFineStates.waiting_for_user_id)
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_fine_"), AdminFineStates.waiting_for_user_id)
async def handle_admin_fine_user(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    try:
        user_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Некорректный пользователь", show_alert=True)
        return
    await state.update_data(fine_user_id=user_id)
    user = await get_user(user_id)
    if user:
        await callback.message.answer(
            f"⚡ *Штраф пользователя:* {user['full_name']}\n\n"
            f"💰 Текущий баланс: {format_money(user['balance'])}\n\n"
            f"💸 *Введите сумму штрафа:*\n"
            f"Минимум: 1₽\n"
            f"Максимум: {format_money(user['balance'])}",
            parse_mode="Markdown",
            reply_markup=get_state_back_inline_keyboard(),
        )
    await state.set_state(AdminFineStates.waiting_for_amount)
    await callback.answer()

@dp.message(AdminFineStates.waiting_for_amount)
async def handle_admin_fine_amount(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            await message.answer("❌ Сумма штрафа должна быть положительной!")
            return
        data = await state.get_data()
        user_id = data.get('fine_user_id')
        if not user_id:
            await message.answer("❌ Ошибка: пользователь не выбран")
            await state.clear()
            return
        user = await get_user(user_id)
        if not user:
            await message.answer("❌ Ошибка: пользователь не найден")
            await state.clear()
            return
        if amount > user['balance']:
            amount = user['balance']
        await update_balance(user_id, -amount, "penalty", f"⚡ Штраф от администратора")
        user_updated = await get_user(user_id)
        await message.answer(
            f"✅ *Штраф выписан!*\n\n"
            f"👤 Пользователь: {user['full_name']}\n"
            f"💸 Сумма штрафа: {format_money(amount)}\n"
            f"💰 Новый баланс: {format_money(user_updated['balance'])}",
            parse_mode="Markdown"
        )
        try:
            await bot.send_message(user_id,
                f"⚡ *ВЫ ПОЛУЧИЛИ ШТРАФ ОТ АДМИНИСТРАЦИИ!*\n\n"
                f"💸 Сумма штрафа: {format_money(amount)}\n"
                f"💰 Новый баланс: {format_money(user_updated['balance'])}\n\n"
                f"Соблюдайте правила!",
                parse_mode="Markdown"
            )
        except:
            pass
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")
        return
    await state.clear()

@dp.callback_query(F.data == "admin_bonus")
async def handle_admin_bonus_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    all_users = await get_all_users()
    await callback.message.answer(
        "🎁 *Бонус пользователю*\n\n"
        "Выберите пользователя для бонуса:",
        reply_markup=get_users_keyboard(
            all_users, ADMIN_ID, "admin_bonus_", cancel_callback="admin_cancel_pick"
        )
    )
    await state.set_state(AdminBonusStates.waiting_for_user_id)
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_bonus_"), AdminBonusStates.waiting_for_user_id)
async def handle_admin_bonus_user(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    try:
        user_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Некорректный пользователь", show_alert=True)
        return
    await state.update_data(bonus_user_id=user_id)
    user = await get_user(user_id)
    if user:
        await callback.message.answer(
            f"🎁 *Бонус пользователю:* {user['full_name']}\n\n"
            f"💰 Текущий баланс: {format_money(user['balance'])}\n\n"
            f"💸 *Введите сумму бонуса:*\n"
            f"Минимум: 1₽\n"
            f"Максимум: 1.000.000₽",
            parse_mode="Markdown",
            reply_markup=get_state_back_inline_keyboard(),
        )
    await state.set_state(AdminBonusStates.waiting_for_amount)
    await callback.answer()

@dp.message(AdminBonusStates.waiting_for_amount)
async def handle_admin_bonus_amount(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            await message.answer("❌ Сумма бонуса должна быть положительной!")
            return
        if amount > 1000000:
            await message.answer("❌ Максимальная сумма бонуса - 1.000.000₽")
            return
        data = await state.get_data()
        user_id = data.get('bonus_user_id')
        if not user_id:
            await message.answer("❌ Ошибка: пользователь не выбран")
            await state.clear()
            return
        user = await get_user(user_id)
        if not user:
            await message.answer("❌ Ошибка: пользователь не найден")
            await state.clear()
            return
        await update_balance(user_id, amount, "bonus", f"🎁 Бонус от администратора")
        user_updated = await get_user(user_id)
        await message.answer(
            f"✅ *Бонус выдан!*\n\n"
            f"👤 Пользователь: {user['full_name']}\n"
            f"💰 Сумма бонуса: {format_money(amount)}\n"
            f"💳 Новый баланс: {format_money(user_updated['balance'])}",
            parse_mode="Markdown"
        )
        try:
            await bot.send_message(user_id,
                f"🎁 *ВЫ ПОЛУЧИЛИ БОНУС ОТ АДМИНИСТРАЦИИ!*\n\n"
                f"💰 Сумма бонуса: {format_money(amount)}\n"
                f"💳 Новый баланс: {format_money(user_updated['balance'])}\n\n"
                f"Поздравляем! 🎉",
                parse_mode="Markdown"
            )
        except:
            pass
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")
        return
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def handle_admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    all_users = await get_all_users()
    total_balance = sum(u['balance'] for u in all_users)
    total_players = len(all_users)
    avg_balance = total_balance // total_players if total_players > 0 else 0
    stats_text = (
        f"📊 *Статистика системы*\n\n"
        f"👥 Всего игроков: {total_players}\n"
        f"💰 Общий баланс: {format_money(total_balance)}\n"
        f"📈 Средний баланс: {format_money(avg_balance)}\n\n"
        f"🏆 *Топ-10 по балансу:*\n"
    )
    sorted_users = sorted(all_users, key=lambda x: x['balance'], reverse=True)[:10]
    for i, user in enumerate(sorted_users, 1):
        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"][i-1]
        name = user['full_name'][:15] + "..." if len(user['full_name']) > 15 else user['full_name']
        stats_text += f"{medal} {name}: {format_money(user['balance'])}\n"
    if all_users:
        richest = max(all_users, key=lambda x: x["balance"])
        poorest = min(all_users, key=lambda x: x["balance"])
        stats_text += (
            f"\n💎 Самый богатый: {richest['full_name']} ({format_money(richest['balance'])})\n"
            f"📉 Самый бедный: {poorest['full_name']} ({format_money(poorest['balance'])})"
        )
    await callback.message.answer(stats_text, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data == "admin_close")
async def handle_admin_close(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await callback.answer()

# ==================== АДМИН-ЧЕКИ ====================
@dp.callback_query(F.data == "admin_checks")
async def handle_admin_checks(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    checks_text = (
        "🧾 *АДМИН: СИСТЕМА ЧЕКОВ*\n\n"
        "Создавайте подарочные чеки-ссылки:\n"
        "• 🎁 **Денежные чеки** - фиксированная сумма\n"
        "• 🎁 **Товарные чеки** - бусты, таблетки, предметы\n\n"
        "Игроки активируют чеки простым переходом по ссылке!\n"
        "Один человек = одна активация ⚠️"
    )
    try:
        await callback.message.edit_text(
            checks_text,
            parse_mode="Markdown",
            reply_markup=get_admin_checks_keyboard()
        )
    except Exception:
        await callback.message.answer(
            checks_text,
            parse_mode="Markdown",
            reply_markup=get_admin_checks_keyboard()
        )
    await callback.answer()

@dp.callback_query(F.data == "admin_checks_back")
async def handle_admin_checks_back(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    checks_text = (
        "🧾 *АДМИН: СИСТЕМА ЧЕКОВ*\n\n"
        "Создавайте подарочные чеки-ссылки:\n"
        "• 🎁 **Денежные чеки** - фиксированная сумма\n"
        "• 🎁 **Товарные чеки** - бусты, таблетки, предметы\n\n"
        "Игроки активируют чеки простым переходом по ссылке!\n"
        "Один человек = одна активация ⚠️"
    )
    try:
        await callback.message.edit_text(
            checks_text,
            parse_mode="Markdown",
            reply_markup=get_admin_checks_keyboard()
        )
    except Exception:
        await callback.message.answer(
            checks_text,
            parse_mode="Markdown",
            reply_markup=get_admin_checks_keyboard()
        )
    await callback.answer()

@dp.callback_query(F.data == "admin_check_money")
async def handle_admin_check_money(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    check_money_text = (
        "💰 *Создание денежного чека*\n\n"
        "💸 Введите сумму чека (от 100 до 100000₽):"
    )
    try:
        await callback.message.edit_text(
            check_money_text,
            parse_mode="Markdown",
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await callback.message.answer(check_money_text, parse_mode="Markdown")
    await callback.message.answer(
        "↩️ Для отмены нажми «🔙 Назад».",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await state.update_data(check_type="money")
    await state.set_state(CheckStates.waiting_for_check_amount)
    await callback.answer()

@dp.callback_query(F.data == "admin_check_item")
async def handle_admin_check_item(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    check_item_text = (
        "🎁 *Создание товарного чека*\n\n"
        "Выберите товар для чека:"
    )
    check_item_kb = get_items_for_checks()
    try:
        await callback.message.edit_text(
            check_item_text,
            parse_mode="Markdown",
            reply_markup=check_item_kb
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await callback.message.answer(
                check_item_text,
                parse_mode="Markdown",
                reply_markup=check_item_kb
            )
    await callback.answer()

@dp.callback_query(F.data.startswith("check_item_"))
async def handle_check_item_select(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    item_id = callback.data[11:]
    item = next((i for i in SHOP_ITEMS if i["id"] == item_id), None)
    if not item:
        await callback.answer("❌ Товар не найден", show_alert=True)
        return
    await state.update_data(check_type="item", item_id=item_id)
    check_item_selected_text = (
        f"🎁 *Создание чека на товар*\n\n"
        f"📦 Товар: {item['name']}\n"
        f"💵 Стоимость в магазине: {format_money(item['price'])}\n\n"
        f"🔢 Введите количество использований чека (1-100):"
    )
    try:
        await callback.message.edit_text(
            check_item_selected_text,
            parse_mode="Markdown"
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await callback.message.answer(check_item_selected_text, parse_mode="Markdown")
    await callback.message.answer(
        "↩️ Для отмены нажми «🔙 Назад».",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await state.set_state(CheckStates.waiting_for_check_uses)
    await callback.answer()

@dp.message(CheckStates.waiting_for_check_amount)
async def handle_check_amount(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    try:
        amount = int(message.text)
        if amount < 100:
            await message.answer("❌ Минимальная сумма - 100₽")
            return
        if amount > 100000:
            await message.answer("❌ Максимальная сумма - 100000₽")
            return
        await state.update_data(amount=amount)
        await message.answer(
            f"💰 Сумма: {format_money(amount)}\n\n"
            f"🔢 Введите количество использований чека (1-1000):",
            parse_mode="Markdown"
        )
        await state.set_state(CheckStates.waiting_for_check_uses)
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.message(CheckStates.waiting_for_check_uses)
async def handle_check_uses(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    try:
        max_uses = int(message.text)
        if max_uses < 1:
            await message.answer("❌ Минимум 1 использование")
            return
        if max_uses > 1000:
            await message.answer("❌ Максимум 1000 использований")
            return
        await state.update_data(max_uses=max_uses)
        await message.answer(
            f"🔢 Использований: {max_uses}\n\n"
            f"⏳ Введите срок действия в часах (1-720):",
            parse_mode="Markdown"
        )
        await state.set_state(CheckStates.waiting_for_check_hours)
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.message(CheckStates.waiting_for_check_hours)
async def handle_check_hours(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    try:
        hours = int(message.text)
        if hours < 1:
            await message.answer("❌ Минимум 1 час")
            return
        if hours > 720:
            await message.answer("❌ Максимум 720 часов (30 дней)")
            return
        await state.update_data(hours=hours)
        await message.answer(
            f"⏳ Срок действия: {hours} часов\n\n"
            f"💌 Введите сообщение для получателей (необязательно):\n"
            f"Или отправьте '-' чтобы пропустить",
            parse_mode="Markdown"
        )
        await state.set_state(CheckStates.waiting_for_check_message)
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.message(CheckStates.waiting_for_check_message)
async def handle_check_message(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    data = await state.get_data()
    check_type = data.get('check_type', 'money')
    amount = data.get('amount', 0)
    item_id = data.get('item_id')
    max_uses = data.get('max_uses', 1)
    hours = data.get('hours', 24)
    custom_message = message.text if message.text != "-" else ""

    bot_username = await get_bot_username_for_tme_links()
    if not bot_username:
        await message.answer(
            "❌ *ОШИБКА!*\n\n"
            "У бота нет username! Без username нельзя создать ссылку.\n"
            "Установите username в @BotFather и перезапустите бота.",
            parse_mode="Markdown"
        )
        await state.clear()
        return

    check_id = await create_gift_check(
        creator_id=ADMIN_ID,
        check_type=check_type,
        amount=amount,
        item_id=item_id,
        max_uses=max_uses,
        hours=hours,
        message=custom_message
    )

    check_link = f"https://t.me/{bot_username}?start={check_id}"

    if check_type == 'money':
        check_info = f"💰 *Денежный чек на {format_money(amount)}*"
        reward_text = f"{format_money(amount)}"
    else:
        item_name = next((i['name'] for i in SHOP_ITEMS if i["id"] == item_id), "Неизвестный товар")
        check_info = f"🎁 *Товарный чек на {item_name}*"
        reward_text = item_name

    expires_at = datetime.now() + timedelta(hours=hours)
    check_text = (
        f"✅ *ЧЕК УСПЕШНО СОЗДАН!*\n\n"
        f"{check_info}\n"
        f"🔢 Использований: {max_uses}\n"
        f"⏳ Действует до: {expires_at.strftime('%d.%m.%Y %H:%M')}\n\n"
    )
    if custom_message:
        check_text += f"💌 Сообщение: {custom_message}\n\n"
    check_text += (
        f"🔗 *ССЫЛКА ДЛЯ АКТИВАЦИИ:*\n"
        f"`{check_link}`\n\n"
        f"📋 *ИНСТРУКЦИЯ:*\n"
        f"1. Отправьте эту ссылку в чат\n"
        f"2. Игроки переходят по ссылке\n"
        f"3. Первые {max_uses} человек получат {reward_text}\n"
        f"4. Остальные увидят, что чек уже использован\n\n"
        f"🆔 Код чека: `{check_id}`"
    )
    buttons = [
        [InlineKeyboardButton(text="📋 Отправить ссылку в чат", callback_data=f"send_link_{check_id}")],
        [InlineKeyboardButton(text="🧾 К списку чеков", callback_data="admin_checks_list")]
    ]
    await message.answer(check_text, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await state.clear()

@dp.callback_query(F.data.startswith("send_link_"))
async def handle_send_link(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    check_id = callback.data[10:]
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT check_type, amount, item_id, max_uses, used_count FROM gift_checks WHERE check_id = ?",
            (check_id,)
        )
        check = await cursor.fetchone()
    if not check:
        await callback.answer("❌ Чек не найден", show_alert=True)
        return
    check = dict(check)
    bot_username = await get_bot_username_for_tme_links()
    if not bot_username:
        await callback.answer("❌ У бота нет username!", show_alert=True)
        return
    check_link = f"https://t.me/{bot_username}?start={check_id}"
    remaining_uses = check['max_uses'] - check['used_count']
    if check['check_type'] == 'money':
        reward_text = f"{format_money(check['amount'])}"
        message_text = (
            f"🎁 *ПОДАРОЧНЫЙ ЧЕК ОТ АДМИНИСТРАЦИИ!*\n\n"
            f"💰 Сумма: {reward_text}\n"
            f"👥 Доступно использований: {remaining_uses}/{check['max_uses']}\n\n"
            f"🔗 *Активировать:* {check_link}\n\n"
            f"📱 *Как использовать:*\n"
            f"1. Нажмите на ссылку выше\n"
            f"2. Нажмите START в боте\n"
            f"3. Получите деньги на баланс!"
        )
    else:
        item_name = next((i['name'] for i in SHOP_ITEMS if i["id"] == check['item_id']), "Неизвестный товар")
        message_text = (
            f"🎁 *ПОДАРОЧНЫЙ ЧЕК ОТ АДМИНИСТРАЦИИ!*\n\n"
            f"📦 Награда: {item_name}\n"
            f"👥 Доступно использований: {remaining_uses}/{check['max_uses']}\n\n"
            f"🔗 *Активировать:* {check_link}\n\n"
            f"📱 *Как использовать:*\n"
            f"1. Нажмите на ссылку выше\n"
            f"2. Нажмите START в боте\n"
            f"3. Получите предмет в инвентарь!"
        )
    await callback.message.answer(message_text, parse_mode="Markdown")
    await callback.answer("✅ Ссылка отправлена в чат!")

@dp.callback_query(F.data == "admin_checks_list")
async def handle_admin_checks_list(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    try:
        active_checks = await get_active_checks()
        if not active_checks:
            await callback.message.edit_text(
                "📭 Активных чеков нет\n\nСоздайте первый чек через меню!",
                reply_markup=get_admin_checks_keyboard()
            )
            await callback.answer()
            return
        checks_text = "🧾 АКТИВНЫЕ ЧЕКИ:\n\n"
        total_amount = 0
        for i, check in enumerate(active_checks[:10], 1):
            expires_at = safe_parse_datetime(check.get('expires_at'))
            if expires_at:
                time_left = expires_at - datetime.now()
                hours_left = int(time_left.total_seconds() // 3600)
                expires_text = expires_at.strftime('%d.%m %H:%M')
            else:
                hours_left = "?"
                expires_text = "⚠️ дата неизвестна"
            if check['check_type'] == 'money':
                amount = check.get('amount', 0)
                check_info = f"💰 {format_money(amount)}"
                remaining = check['max_uses'] - check['used_count']
                total_amount += amount * remaining
            else:
                item_id = check.get('item_id', '?')
                item = next((i for i in SHOP_ITEMS if i["id"] == item_id), None)
                item_name = item['name'] if item else item_id
                check_info = f"🎁 {item_name}"
            checks_text += (
                f"{i}. {check['check_id'][:12]}...\n"
                f"   {check_info} | 👥 {check['used_count']}/{check['max_uses']}\n"
            )
            if isinstance(hours_left, int):
                checks_text += f"   ⏳ {hours_left}ч | 📅 {expires_text}\n"
            else:
                checks_text += f"   ⏳ {expires_text}\n"
        checks_text += f"\n📊 Итого в обороте: {format_money(total_amount)}"
        buttons = []
        for i, check in enumerate(active_checks[:5], 1):
            buttons.append([InlineKeyboardButton(
                text=f"📊 Статистика {check['check_id'][:8]}...",
                callback_data=f"check_stats_{check['check_id']}"
            )])
        buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_checks_back")])
        await callback.message.edit_text(
            checks_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в списке чеков: {e}")
        await callback.message.answer(
            "❌ Произошла ошибка при загрузке списка чеков.\nПроверьте логи бота.",
            reply_markup=get_admin_checks_keyboard()
        )
        await callback.answer("❌ Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("check_stats_"))
async def handle_check_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    check_id = callback.data[12:]
    stats = await get_check_stats(check_id)
    if not stats:
        await callback.answer("❌ Чек не найден", show_alert=True)
        return

    expires_at = safe_parse_datetime(stats.get('expires_at'))
    created_at = safe_parse_datetime(stats.get('created_at'))

    if stats['check_type'] == 'money':
        check_info = f"💰 Денежный чек на {format_money(stats['amount'])}"
    else:
        item = next((i for i in SHOP_ITEMS if i["id"] == stats['item_id']), None)
        item_name = item['name'] if item else stats['item_id']
        check_info = f"🎁 Товарный чек на {item_name}"

    bot_username = await get_bot_username_for_tme_links()
    if bot_username:
        check_link = f"https://t.me/{bot_username}?start={check_id}"
        link_text = f"🔗 Ссылка: {check_link}"
    else:
        link_text = "❌ У бота нет username!"

    stats_text = (
        f"📊 СТАТИСТИКА ЧЕКА\n\n"
        f"{check_info}\n"
        f"👤 Создатель: {stats.get('creator_name', 'Админ')}\n"
        f"📅 Создан: {created_at.strftime('%d.%m.%Y %H:%M') if created_at else 'неизвестно'}\n"
        f"⏳ Действует до: {expires_at.strftime('%d.%m.%Y %H:%M') if expires_at else 'неизвестно'}\n"
        f"👥 Использовано: {stats['used_count']}/{stats['max_uses']}\n"
        f"{link_text}\n"
    )
    if stats.get('custom_message'):
        stats_text += f"💌 Сообщение: {stats['custom_message']}\n"

    if stats['activations']:
        stats_text += f"\n🎯 Активировали ({len(stats['activations'])}):\n"
        for i, act in enumerate(stats['activations'][:5], 1):
            act_time = safe_parse_datetime(act.get('activated_at'))
            act_time_str = act_time.strftime('%H:%M') if act_time else '??'
            user_name = act.get('user_name', f'ID:{act["user_id"]}')
            stats_text += f"{i}. {user_name} - {act_time_str}\n"
        if len(stats['activations']) > 5:
            stats_text += f"... и ещё {len(stats['activations']) - 5} человек\n"
    else:
        stats_text += "\n🎯 Пока никто не активировал этот чек"

    buttons = [
        [InlineKeyboardButton(text="📤 Отправить ссылку", callback_data=f"send_link_{check_id}")],
        [InlineKeyboardButton(text="🔙 К списку чеков", callback_data="admin_checks_list")],
        [InlineKeyboardButton(text="❌ Деактивировать чек", callback_data=f"check_deactivate_{check_id}")]
    ]

    await callback.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("check_deactivate_"))
async def handle_check_deactivate(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    check_id = callback.data[16:]
    await deactivate_check(check_id)
    await callback.answer(f"✅ Чек {check_id} деактивирован!", show_alert=True)
    await handle_admin_checks_list(callback)

@dp.callback_query(F.data == "admin_back")
async def handle_admin_back(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    admin_text = (
        "👑 *Админ-панель*\n\n"
        "Управление только кнопками ниже — слэш-команды не используются."
    )
    try:
        await callback.message.edit_text(
            admin_text,
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
    except Exception:
        await callback.message.answer(
            admin_text,
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
    await callback.answer()

# ==================== СТАТИСТИКА И ЭФФЕКТЫ ====================
@dp.message(F.text == "📊 Статистика")
async def handle_statistics(message: Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        return
    day_msk = moscow_date_str()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, full_name, balance, total_earned, asphalt_meters FROM players ORDER BY balance DESC LIMIT 10"
        )
        top_players = await cursor.fetchall()
        cursor = await db.execute("SELECT COUNT(*) as total, SUM(balance) as total_balance FROM players")
        total_stats = await cursor.fetchone()
        cursor = await db.execute(
            "SELECT earned FROM daily_earnings WHERE user_id = ? AND day = ?", (user_id, day_msk)
        )
        te_row = await cursor.fetchone()
        today_earned = int(te_row["earned"]) if te_row else 0
        cursor = await db.execute(
            """SELECT d.user_id, d.earned, p.full_name FROM daily_earnings d
               JOIN players p ON p.user_id = d.user_id
               WHERE d.day = ? AND d.earned > 0 ORDER BY d.earned DESC LIMIT 5""",
            (day_msk,),
        )
        top_day_rows = await cursor.fetchall()
    rep_val = await get_reputation_percent(user_id, user)
    social = await get_social_status_for_user(user_id)
    ach_ids = await get_unlocked_achievement_ids(user_id)
    ach_line = ""
    if ach_ids:
        parts = []
        for aid in sorted(ach_ids):
            meta = ACHIEVEMENTS.get(aid)
            if meta:
                parts.append(f"{meta.get('emoji', '🏅')}{meta['name']}")
        ach_line = "• Достижения: " + ", ".join(parts) + "\n"
    stats_text = (
        f"📊 *КОРПОРАТИВНАЯ СТАТИСТИКА*\n\n"
        f"👤 *Ваш профиль:*\n"
        f"• Имя: {user['full_name']}\n"
        f"• Баланс: {format_money(user['balance'])}\n"
        f"• Репутация: {rep_val:.0f}/100 _(кредит: <40 отказ, 40–69 средний лимит, 70+ полный лимит)_\n"
        f"• 🏅 Статус: {social['status_name']} {social['emoji']}\n"
        f"• Заработок сегодня (МСК): {format_money(today_earned)}\n"
        f"• Побед в дуэлях: {user.get('duels_won', 0)}\n"
        f"{ach_line}"
        f"• Заработано всего: {format_money(user.get('total_earned', 0))}\n"
        f"• Штрафов получено: {format_money(user.get('total_fines', 0))}\n"
        f"• Получок: {user.get('salary_count', 0)}\n"
        f"• Уложено асфальта: {user.get('asphalt_meters', 0):,} метров\n"
        f"• Заработано на асфальте: {format_money(user.get('asphalt_earned', 0))}\n\n"
    )
    if top_day_rows:
        stats_text += f"📈 *Топ заработка сегодня (МСК):*\n"
        medals = ("🥇", "🥈", "🥉", "4️⃣", "5️⃣")
        for i, row in enumerate(top_day_rows):
            nm = row["full_name"][:14] + "…" if len(row["full_name"]) > 14 else row["full_name"]
            stats_text += f"{medals[i]} {nm}: {format_money(row['earned'])}\n"
        stats_text += (
            f"\n_Награды топ-3 за вчерашний день начисляются в 10:00 МСК "
            f"({format_money(ECONOMY_SETTINGS['daily_top_reward_1'])}/"
            f"{format_money(ECONOMY_SETTINGS['daily_top_reward_2'])}/"
            f"{format_money(ECONOMY_SETTINGS['daily_top_reward_3'])})._ \n\n"
        )
    loan = await get_active_bank_loan(user_id)
    if loan:
        due = safe_parse_datetime(loan["due_at"])
        due_s = due.strftime("%d.%m %H:%M") if due else "—"
        st = "просрочка ⚠️" if loan.get("defaulted") else "в сроке"
        stats_text += (
            f"🏦 *Кредит «{BANK_SETTINGS['name']}»:*\n"
            f"• Долг: {format_money(loan['remaining'])}\n"
            f"• До: {due_s} ({st})\n\n"
        )
    # Бизнес-статистика
    biz_list = await get_user_businesses(user_id)
    if biz_list:
        total_income = 0
        biz_names = []
        for biz in biz_list:
            config = BUSINESS_TYPES[biz['biz_type']]
            biz_names.append(config['name'])
            total_income += await calculate_business_income(biz)
        stats_text += f"🏢 *Бизнес-империя:*\n"
        stats_text += f"• Предприятий: {len(biz_list)}\n"
        stats_text += f"• Доход/час: {format_money(total_income)}\n"
        stats_text += f"• Активы: {', '.join(biz_names[:3])}"
        if len(biz_list) > 3:
            stats_text += f" и ещё {len(biz_list)-3}"
        stats_text += "\n\n"
    if total_stats:
        stats_text += (
            f"🏢 *Общая статистика:*\n"
            f"• Всего сотрудников: {total_stats['total']}\n"
            f"• Общий капитал: {format_money(total_stats['total_balance'] or 0)}\n\n"
        )
    if top_players:
        stats_text += "🏆 *ТОП-10 СОТРУДНИКОВ:*\n"
        for i, player in enumerate(top_players, 1):
            medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"][i-1]
            name = player['full_name'][:15] + "..." if len(player['full_name']) > 15 else player['full_name']
            sp = await get_social_status_for_user(int(player["user_id"]))
            stats_text += f"{medal} {name}: {format_money(player['balance'])} — {sp['status_name']} {sp['emoji']}\n"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏅 Статусы: условия и бонусы", callback_data="show_statuses_info")]
        ]
    )
    await message.answer(stats_text, parse_mode="Markdown", reply_markup=kb)


@dp.callback_query(F.data == "show_statuses_info")
async def handle_show_statuses_info(callback: CallbackQuery):
    try:
        text = await build_statuses_info_text(callback.from_user.id)
        await callback.message.answer(
            text,
            parse_mode="Markdown",
            reply_markup=get_statuses_info_keyboard(),
        )
    except Exception as e:
        logger.error("Ошибка при открытии экрана статусов user_id=%s: %s", callback.from_user.id, e)
        await callback.message.answer(
            "❌ Не удалось загрузить социальные статусы. Попробуй еще раз через пару секунд.",
            reply_markup=get_statuses_info_keyboard(),
        )
    await callback.answer()


@dp.callback_query(F.data == "refresh_statuses_info")
async def handle_refresh_statuses_info(callback: CallbackQuery):
    text = await build_statuses_info_text(callback.from_user.id)
    try:
        await callback.message.edit_text(
            text,
            parse_mode="Markdown",
            reply_markup=get_statuses_info_keyboard(),
        )
    except Exception as e:
        if "message is not modified" in str(e).lower():
            await callback.answer("Статус уже актуален", show_alert=False)
            return
        logger.error("Ошибка при обновлении экрана статусов user_id=%s: %s", callback.from_user.id, e)
        await callback.message.answer(
            text,
            parse_mode="Markdown",
            reply_markup=get_statuses_info_keyboard(),
        )
    await callback.answer()

@dp.message(F.text == "👥 Рефералы")
async def handle_referrals_menu(message: Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        return

    bot_username = await get_bot_username_for_tme_links()
    if not bot_username:
        await message.answer("❌ У бота нет username — невозможно показать реферальную ссылку.")
        return

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT
                COUNT(*) AS invited,
                COALESCE(SUM(reward_inviter), 0) AS earned
            FROM referral_invites
            WHERE inviter_id = ? AND credited_at IS NOT NULL
            """,
            (user_id,),
        )
        r = await cur.fetchone()
        invited = int(r["invited"] or 0)
        earned = int(r["earned"] or 0)

        # Топ приглашающих.
        cur = await db.execute(
            """
            SELECT inviter_id, COUNT(*) AS cnt
            FROM referral_invites
            WHERE credited_at IS NOT NULL
            GROUP BY inviter_id
            ORDER BY cnt DESC
            LIMIT 3
            """
        )
        top_rows = [dict(x) for x in await cur.fetchall()]

    milestones = [
        (1, 5000, 5000, ""),
        (3, 15000, 5000, "буст 24ч"),
        (5, 30000, 5000, "редкий предмет"),
        # Агитатор: +20% к реферальному бонусу (применяется на 10-й кредит).
        (10, 120000, 5000, "титул «Агитатор»"),
    ]

    lines = [
        "👥 *РЕФЕРАЛЬНАЯ СИСТЕМА*",
        "",
        # Backticks нужны, чтобы Markdown не "съел" подчёркивание в `ref_<id>`.
        f"Ссылка: `https://t.me/{bot_username}?start=ref_{user_id}`",
        f"Приглашено (активных): {invited}",
        f"Заработано: {format_money(earned)}",
        "",
        "Доступно:",
        f"Новичок (активный): +5 000 ₽ после {REFERRAL_ACTIONS_MIN}+ действий (начисляется сразу).",
        "Пригласившему: +2 000 ₽ за каждого активного реферала (+вехи 1/3/5/10).",
    ]

    for m, inv_reward, new_reward, meta in milestones:
        if invited >= m:
            extra = ""
            lines.append(f"✅ {m} друзей — получено (пригласившему {format_money(inv_reward)}{(' ' + meta) if meta else ''}){extra}")
        else:
            need = m - invited
            lines.append(
                f"⏳ {m} друзей — нужно ещё {need} (пригласившему {format_money(inv_reward)}"
                f"{(' ' + meta) if meta else ''})"
            )

    lines.append("")
    lines.append(f"⏳ Начисление: сразу после выполнения условий новичком (минимум {REFERRAL_ACTIONS_MIN} действий).")

    if top_rows:
        lines.append("")
        lines.append("🥇 *Топ приглашающих:*")
        for i, row in enumerate(top_rows, 1):
            nm = await get_user(row["inviter_id"])
            nm = nm["full_name"] if nm else str(row["inviter_id"])
            medal = ("🥇", "🥈", "🥉")[i - 1] if i <= 3 else "🏅"
            lines.append(f"{medal} {nm} — {row['cnt']}")

    await message.answer("\n".join(lines), parse_mode="Markdown")

@dp.message(F.text == "💊 Эффекты")
async def handle_effects(message: Message):
    user_id = message.from_user.id
    effects = await get_active_nagirt_effects(user_id)
    tolerance = await get_nagirt_tolerance(user_id)
    boosts = await get_active_boosts(user_id)
    biz_bonuses = await get_total_business_bonuses(user_id)
    effects_text = "⚡ *АКТИВНЫЕ ЭФФЕКТЫ*\n\n"
    if boosts > 0:
        effects_text += f"📈 *Бусты к зарплате:* +{int(boosts*100)}%\n\n"
    else:
        effects_text += "📈 *Бусты к зарплате:* нет\n\n"
    if biz_bonuses['salary'] > 0 or biz_bonuses['asphalt'] > 0:
        effects_text += "🏢 *Бонусы от бизнесов:*\n"
        if biz_bonuses['salary'] > 0:
            effects_text += f"• Зарплата: +{int(biz_bonuses['salary']*100)}%\n"
        if biz_bonuses['asphalt'] > 0:
            effects_text += f"• Асфальт: +{int(biz_bonuses['asphalt']*100)}%\n"
        effects_text += "\n"
    if effects["has_active"]:
        effects_text += "💊 *Таблетки Нагирт:*\n"
        if effects["salary_boost"] > 0:
            effects_text += f"• Зарплата: +{int(effects['salary_boost']*100)}%\n"
            effects_text += f"  ⚠️ Риск штрафа: {ECONOMY_SETTINGS['fine_chance']+effects['fine_chance_mod']:.0%}\n"
        if effects["game_boost"] > 0:
            effects_text += f"• Мини-игры: +{int(effects['game_boost']*100)}%\n"
        if effects["side_effects"]:
            effects_text += "\n⚠️ *Побочные эффекты:*\n"
            for effect in effects["side_effects"]:
                effects_text += f"• {effect}\n"
        effects_text += "\n"
    else:
        effects_text += "💊 *Таблетки Нагирт:* нет\n\n"
    effects_text += f"📊 *Толерантность к Нагирту:* +{int((tolerance-1)*100)}%\n"
    if tolerance > 1.5:
        effects_text += "\n🚨 *ВНИМАНИЕ!* Высокая толерантность!\nЭффект таблеток слабеет. Рекомендуется использовать антидот.\n"
    elif tolerance > 1.2:
        effects_text += "\n⚠️ *Предупреждение:* Толерантность повышена.\n"
    await message.answer(effects_text, parse_mode="Markdown")

# ==================== ОСТАЛЬНЫЕ ОБРАБОТЧИКИ ====================
@dp.callback_query(F.data == "back_to_main")
async def handle_back_to_main(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard(callback.from_user.id))
    await callback.answer()

@dp.callback_query(F.data == "back_to_games")
async def handle_back_to_games(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("🎮 Мини-игры:", reply_markup=get_minigames_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "shop_close")
async def handle_shop_close(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await callback.answer()

# ==================== СЛУЧАЙНЫЕ ШТРАФЫ ====================
async def penalty_scheduler():
    while True:
        try:
            wait_time = random.randint(
                ECONOMY_SETTINGS["random_fine_interval_min"],
                ECONOMY_SETTINGS["random_fine_interval_max"]
            )
            await asyncio.sleep(wait_time)
            all_users = await get_all_users()
            logger.info(f"🔍 Проверка на штрафы: {len(all_users)} пользователей")
            for user in all_users:
                user_data = await get_user(user['user_id'])
                if not user_data:
                    continue
                if await has_fine_protection(user_data['user_id']):
                    continue
                if random.random() <= 0.32 and user_data['balance'] > ECONOMY_SETTINGS["random_fine_min"]:
                    ge = await get_global_economy()
                    scale = float(ge.get("fine_scale", 1.0))
                    pmin = int(ECONOMY_SETTINGS["random_fine_min"] * scale)
                    pmax = int(
                        min(
                            ECONOMY_SETTINGS["random_fine_max"] * scale,
                            int(user_data["balance"] * random.uniform(0.22, 0.38)),
                        )
                    )
                    if pmax < pmin:
                        pmax = pmin
                    penalty = random.randint(pmin, pmax)
                    penalty_reasons = [
                        "Внеплановая проверка! Обнаружены нарушения.",
                        "Неправильно заполнена отчетность.",
                        "Опоздание на работу.",
                        "Использование рабочего времени в личных целях.",
                        "Нарушение дресс-кода.",
                        "Невыполнение плана продаж.",
                        "Поломка корпоративного оборудования.",
                        "Конфликт с коллегами.",
                        "Утечка конфиденциальной информации.",
                        "Несанкционированный доступ к данным."
                    ]
                    reason = random.choice(penalty_reasons)
                    await update_balance(
                        user_data['user_id'], 
                        -penalty, 
                        "penalty",
                        f"⚡ Случайная проверка: {reason}"
                    )
                    fresh = await get_user(user_data['user_id'])
                    new_bal = fresh["balance"] if fresh else max(0, user_data["balance"] - penalty)
                    try:
                        await bot.send_message(
                            user_data['user_id'],
                            f"⚠️ *СЛУЧАЙНАЯ ПРОВЕРКА ОТ ВИТАЛИКА!*\n\n"
                            f"📛 Причина: {reason}\n"
                            f"💸 Штраф: {format_money(penalty)}\n"
                            f"💰 Новый баланс: {format_money(new_bal)}\n\n"
                            f"Купите 'Выходной' в магазине для защиты!",
                            parse_mode="Markdown"
                        )
                        logger.info(f"Штраф {penalty}₽ пользователю {user_data['user_id']}")
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление: {e}")
        except Exception as e:
            logger.error(f"Ошибка в планировщике штрафов: {e}")
            await asyncio.sleep(300)

async def business_notification_scheduler():
    """Планировщик уведомлений о готовом доходе с бизнесов."""
    while True:
        try:
            await asyncio.sleep(60)  # проверяем раз в минуту

            now = datetime.now()
            one_hour_ago = now - timedelta(hours=1)

            async with aiosqlite.connect(DB_NAME) as db:
                db.row_factory = aiosqlite.Row
                # Ищем все бизнесы, у которых collect_cooldown < 1 час назад
                # то есть уже можно собрать доход, но возможно ещё не собрали
                cursor = await db.execute('''
                    SELECT DISTINCT owner_id 
                    FROM businesses 
                    WHERE collect_cooldown IS NOT NULL 
                      AND collect_cooldown <= ? 
                      AND is_active = 1
                ''', (one_hour_ago.isoformat(),))
                rows = await cursor.fetchall()

            for row in rows:
                user_id = row['owner_id']
                now_ts = now.timestamp()

                # Проверяем, не отправляли ли уведомление недавно
                last_notify = last_business_notification.get(user_id, 0)
                if now_ts - last_notify < BUSINESS_NOTIFICATION_COOLDOWN:
                    continue

                # Убедимся, что у пользователя действительно есть бизнесы с готовым доходом
                status = await get_business_collect_status(user_id)
                if status['can_collect'] and status['total_income'] > 0:
                    try:
                        # Отправляем личное сообщение
                        await bot.send_message(
                            user_id,
                            f"🏢 *ВАШ БИЗНЕС ПРИНЁС ПРИБЫЛЬ!*\n\n"
                            f"💰 Доступно к сбору: {format_money(status['total_income'])}\n"
                            f"💵 Пассивный доход: {format_money(status['total_per_hour'])}/час\n\n"
                            f"👇 Заберите деньги прямо сейчас:",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="💰 Забрать доход", callback_data="biz_collect")]
                            ])
                        )
                        last_business_notification[user_id] = now_ts
                        logger.info(f"📨 Уведомление о бизнесе отправлено пользователю {user_id}")
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление {user_id}: {e}")

        except Exception as e:
            logger.error(f"Ошибка в планировщике уведомлений о бизнесе: {e}")
            await asyncio.sleep(300)  # при ошибке ждём 5 минут

# ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================
async def referral_credit_scheduler():
    """Начисляет реферальные бонусы через 24ч после регистрации,
    и только если новичок активен (минимум REFERRAL_ACTIONS_MIN действий)."""
    await asyncio.sleep(10)
    while True:
        try:
            now_dt = datetime.now()
            delay_td = timedelta(hours=REFERRAL_DELAY_HOURS)

            async with aiosqlite.connect(DB_NAME) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute(
                    "SELECT id, inviter_id, invitee_id, created_at FROM referral_invites WHERE credited_at IS NULL"
                )
                pending = [dict(r) for r in await cur.fetchall()]

            for row in pending:
                pending_created = safe_parse_datetime(row.get("created_at")) or now_dt
                if now_dt - pending_created < delay_td:
                    continue

                inviter_id = int(row["inviter_id"])
                invitee_id = int(row["invitee_id"])

                invitee = await get_user(invitee_id)
                inviter = await get_user(inviter_id)
                if not invitee or not inviter:
                    continue

                # Защита от накрутки: только активный новичок.
                action_count = await get_referral_activity_count(invitee_id)
                if action_count < REFERRAL_ACTIONS_MIN:
                    continue

                async with aiosqlite.connect(DB_NAME) as db:
                    cur = await db.execute(
                        "SELECT COUNT(*) FROM referral_invites WHERE inviter_id = ? AND credited_at IS NOT NULL",
                        (inviter_id,),
                    )
                    credited_before = int((await cur.fetchone())[0] or 0)

                after_credited = credited_before + 1

                inviter_reward = 0
                newcomer_reward = 0
                milestone = after_credited

                if after_credited == 1:
                    inviter_reward = 5000
                    newcomer_reward = 2000
                elif after_credited == 3:
                    inviter_reward = 15000
                    newcomer_reward = 5000
                elif after_credited == 5:
                    inviter_reward = 30000
                    newcomer_reward = 10000
                elif after_credited == 10:
                    inviter_reward = 100000
                    newcomer_reward = 20000

                # Агитатор (+20% к реферальному бонусу) — учитываем при 10-м приглашении.
                if after_credited >= 10 and (inviter_reward > 0 or newcomer_reward > 0):
                    inviter_reward = int(inviter_reward * 1.2)
                    newcomer_reward = int(newcomer_reward * 1.2)

                if inviter_reward > 0:
                    await update_balance(
                        inviter_id,
                        inviter_reward,
                        "referral_inviter",
                        "Реферальный бонус (приглашение)",
                    )
                if newcomer_reward > 0:
                    await update_balance(
                        invitee_id,
                        newcomer_reward,
                        "referral_newcomer",
                        "Реферальный бонус (новичок)",
                    )

                # Доп. награды/эффекты по ТЗ.
                if after_credited == 3 and inviter_reward > 0:
                    await add_boost(inviter_id, "premium_boost", 0.3, 24)
                if after_credited == 5 and inviter_reward > 0:
                    await add_inventory_item(inviter_id, "antidote", 1)
                if after_credited == 10:
                    await add_inventory_item(invitee_id, "antidote", 1)

                async with aiosqlite.connect(DB_NAME) as db:
                    await db.execute(
                        "UPDATE referral_invites SET credited_at = ?, milestone = ?, reward_inviter = ?, reward_newcomer = ? WHERE id = ?",
                        (
                            now_dt.isoformat(),
                            milestone,
                            inviter_reward,
                            newcomer_reward,
                            row["id"],
                        ),
                    )
                    await db.commit()

        except Exception as e:
            logger.error(f"referral_credit_scheduler ошибка: {e}")

        await asyncio.sleep(300)


# ==================== ЗАПУСК БОТА ====================
async def referral_credit_scheduler_immediate():
    """Начисляет реферальные бонусы сразу после выполнения условий новичком (5+ действий).
    Бонус новичка всегда фиксированный: +5000."""
    await asyncio.sleep(10)
    while True:
        try:
            now_dt = datetime.now()
            async with aiosqlite.connect(DB_NAME) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute(
                    "SELECT id, inviter_id, invitee_id FROM referral_invites WHERE credited_at IS NULL"
                )
                pending = [dict(r) for r in await cur.fetchall()]

            for row in pending:
                inviter_id = int(row["inviter_id"])
                invitee_id = int(row["invitee_id"])

                # Активность новичка: считаем по нескольким источникам, не только по transactions.
                action_count = await get_referral_activity_count(invitee_id)
                if action_count < REFERRAL_ACTIONS_MIN:
                    continue

                # Считаем сколько уже реально "зачтённых" приглашений у пригласившего.
                async with aiosqlite.connect(DB_NAME) as db:
                    cur = await db.execute(
                        "SELECT COUNT(*) FROM referral_invites WHERE inviter_id = ? AND credited_at IS NOT NULL",
                        (inviter_id,),
                    )
                    credited_before = int((await cur.fetchone())[0] or 0)
                after_credited = credited_before + 1

                # Бонус новичка фиксированный.
                newcomer_reward = 5000
                milestone = after_credited

                # Бонус пригласившего: всегда есть базовый бонус, чтобы не было эффекта "через раз".
                inviter_reward = 2000
                milestone_bonus = 0
                if after_credited == 1:
                    milestone_bonus = 3000
                elif after_credited == 3:
                    milestone_bonus = 13000
                elif after_credited == 5:
                    milestone_bonus = 28000
                elif after_credited == 10:
                    milestone_bonus = 98000
                inviter_reward += milestone_bonus

                # Агитатор: 10 рефералов -> +20% к бонусу пригласившего.
                if after_credited >= 10 and inviter_reward > 0:
                    inviter_reward = int(inviter_reward * 1.2)

                if inviter_reward > 0:
                    await update_balance(
                        inviter_id,
                        inviter_reward,
                        "referral_inviter",
                        "Реферальный бонус (приглашение)",
                    )
                await update_balance(
                    invitee_id,
                    newcomer_reward,
                    "referral_newcomer",
                    "Реферальный бонус (новичок)",
                )

                # Уведомление новичка о начислении бонуса.
                if newcomer_reward > 0:
                    try:
                        await bot.send_message(
                            invitee_id,
                            f"🎁 Реферальный бонус!\n"
                            f"Вы выполнили условия (минимум {REFERRAL_ACTIONS_MIN} действий).\n"
                            f"Начислено: +{format_money(newcomer_reward)}."
                        )
                    except Exception as e:
                        logger.error(f"Не удалось отправить бонус новичку {invitee_id}: {e}")

                # Уведомление пригласившему о том, что реферал выполнил условия и бонус зачислен.
                if inviter_reward > 0:
                    try:
                        await bot.send_message(
                            inviter_id,
                            f"✅ Ваш реферал (ID: {invitee_id}) выполнил условия "
                            f"(минимум {REFERRAL_ACTIONS_MIN} действий).\n"
                            f"Начислено вам: +{format_money(inviter_reward)}.\n"
                            f"Зачтено активных рефералов: {after_credited}."
                        )
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление пригласившему {inviter_id}: {e}")

                # Дополнительные награды по ТЗ.
                if after_credited == 3 and inviter_reward > 0:
                    await add_boost(inviter_id, "premium_boost", 0.3, 24)
                if after_credited == 5 and inviter_reward > 0:
                    await add_inventory_item(inviter_id, "antidote", 1)
                if after_credited == 10:
                    await add_inventory_item(invitee_id, "antidote", 1)

                async with aiosqlite.connect(DB_NAME) as db:
                    await db.execute(
                        "UPDATE referral_invites SET credited_at = ?, milestone = ?, reward_inviter = ?, reward_newcomer = ? WHERE id = ?",
                        (now_dt.isoformat(), milestone, inviter_reward, newcomer_reward, row["id"]),
                    )
                    await db.commit()

        except Exception as e:
            logger.error(f"referral_credit_scheduler_immediate ошибка: {e}")

        await asyncio.sleep(20)

# ==================== АДМИН-ПОДКРУТКА ДЛЯ MINES ====================

async def get_mines_override(user_id: int) -> dict:
    """Получает настройки подкрутки для игрока"""
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT mines_override, mines_override_active FROM players WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        if row and row[1]:
            return {"active": True, "win_chance": row[0]}
        return {"active": False, "win_chance": None}

async def set_mines_override(user_id: int, win_chance: float, active: bool = True) -> None:
    """Устанавливает подкрутку для игрока"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE players SET mines_override = ?, mines_override_active = ? WHERE user_id = ?",
            (win_chance, 1 if active else 0, user_id)
        )
        await db.commit()

async def disable_mines_override(user_id: int) -> None:
    """Отключает подкрутку для игрока"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE players SET mines_override_active = 0 WHERE user_id = ?",
            (user_id,)
        )
        await db.commit()

# ==================== ИГРА МИНЫ ====================

def generate_mines_field(mines_count: int, exclude_cell: int = None, user_id: int = None) -> list:
    """Генерирует позиции мин с учётом админ-подкрутки"""
    all_cells = list(range(25))
    if exclude_cell is not None and exclude_cell in all_cells:
        all_cells.remove(exclude_cell)
    
    # Проверяем подкрутку (синхронный вызов — нужно передавать user_id)
    if user_id:
        # Используем asyncio.run_coroutine_threadsafe, но проще передать уже готовое значение
        pass
    
    return random.sample(all_cells, min(mines_count, len(all_cells)))

def get_mines_multiplier(mines_count: int, opened_count: int) -> float:
    """Возвращает текущий множитель по количеству открытых ячеек"""
    multipliers = ECONOMY_SETTINGS["mines_multipliers"].get(mines_count, [])
    if opened_count >= len(multipliers):
        return multipliers[-1] if multipliers else 1.0
    return multipliers[opened_count]

async def show_mines_field(message: Message, user_id: int, first_time: bool = False) -> Message:
    """Отображает игровое поле с кнопками"""
    game = active_mines_games.get(user_id)
    if not game:
        return None
    
    mines_count = game["mines_count"]
    opened = game["opened"]
    bet = game["bet"]
    multiplier = get_mines_multiplier(mines_count, len(opened))
    current_win = int(bet * multiplier)
    
    # Создаём клавиатуру 5x5
    kb_buttons = []
    for i in range(25):
        row = i // 5
        col = i % 5
        if len(kb_buttons) <= row:
            kb_buttons.append([])
        
        if i in opened:
            kb_buttons[row].append(InlineKeyboardButton(text="💎", callback_data=f"mines_opened_{i}"))
        else:
            kb_buttons[row].append(InlineKeyboardButton(text="⬛", callback_data=f"mines_cell_{i}"))
    
    # Добавляем кнопки управления
    kb_buttons.append([
        InlineKeyboardButton(text=f"💰 ЗАБРАТЬ {format_money(current_win)}", callback_data="mines_cashout"),
        InlineKeyboardButton(text="❌ Выход", callback_data="mines_exit")
    ])
    
    # Прогресс-бар
    progress = int(len(opened) / 25 * 20)
    progress_bar = "█" * progress + "░" * (20 - progress)
    
    text = (
        f"💣 *МИНЫ — {mines_count} мин*\n\n"
        f"┌{'─' * 20}┐\n"
        f"│ {progress_bar} │\n"
        f"└{'─' * 20}┘\n\n"
        f"💰 Ставка: {format_money(bet)}\n"
        f"🔓 Открыто: {len(opened)}/25 ячеек\n"
        f"📈 Множитель: x{multiplier:.2f}\n"
        f"💎 Выигрыш при выходе: {format_money(current_win)}\n\n"
        f"*Нажми на ⬛, чтобы открыть ячейку.*\n"
        f"Наступишь на 💣 — проиграешь!"
    )
    
    if first_time:
        return await message.answer(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons))
    else:
        await message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons))
        return message

# ----- НАЧАЛО ИГРЫ -----
@dp.callback_query(F.data == "game_mines")
async def mines_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer(NOT_REGISTERED_ALERT, show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💣 3 мины (x118 макс)", callback_data="mines_count_3")],
        [InlineKeyboardButton(text="💣💣 5 мин (x676 макс)", callback_data="mines_count_5")],
        [InlineKeyboardButton(text="💣💣💣 8 мин (x17700 макс)", callback_data="mines_count_8")],
        [InlineKeyboardButton(text="💣💣💣💣 12 мин (x16.7 млн макс)", callback_data="mines_count_12")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_games")]
    ])
    
    await callback.message.edit_text(
        "💣 *ИГРА «МИНЫ»*\n\n"
        "Выбери количество мин на поле 5×5:\n"
        "• 3 мины — низкий риск, низкий множитель\n"
        "• 5 мин — средний риск\n"
        "• 8 мин — высокий риск, высокий множитель\n"
        "• 12 мин — экстремальный риск, огромные множители!\n\n"
        "_Чем больше мин — тем выше множитель за каждую открытую ячейку._",
        parse_mode="Markdown",
        reply_markup=kb
    )
    await state.set_state(MinesStates.choosing_mines)
    await callback.answer()

@dp.callback_query(F.data.startswith("mines_count_"), MinesStates.choosing_mines)
async def mines_choose_count(callback: CallbackQuery, state: FSMContext):
    mines_count = int(callback.data.split("_")[2])
    await state.update_data(mines_count=mines_count)
    await state.set_state(MinesStates.waiting_bet)
    
    user = await get_user(callback.from_user.id)
    max_bet = min(ECONOMY_SETTINGS["mines_max_bet"], user['balance'])
    
    await callback.message.edit_text(
        f"💣 *МИНЫ — {mines_count} мин*\n\n"
        f"💰 Баланс: {format_money(user['balance'])}\n"
        f"💸 Ставка: от {format_money(ECONOMY_SETTINGS['mines_min_bet'])} до {format_money(max_bet)}\n\n"
        f"Введи сумму ставки числом:",
        parse_mode="Markdown"
    )
    await callback.message.answer(
        "↩️ Для отмены нажми «🔙 Назад».",
        reply_markup=get_state_back_inline_keyboard(),
    )
    await callback.answer()

@dp.message(MinesStates.waiting_bet)
async def mines_place_bet(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await message.answer(NOT_REGISTERED_HINT)
        await state.clear()
        return
    
    try:
        bet = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введи число!")
        return
    
    min_bet = ECONOMY_SETTINGS["mines_min_bet"]
    max_bet = ECONOMY_SETTINGS["mines_max_bet"]
    
    if bet < min_bet or bet > max_bet:
        await message.answer(f"❌ Ставка от {format_money(min_bet)} до {format_money(max_bet)}")
        return
    
    if bet > user['balance']:
        await message.answer(f"❌ Недостаточно средств! Баланс: {format_money(user['balance'])}")
        return
    
    data = await state.get_data()
    mines_count = data.get('mines_count')
    
    # Списываем ставку
    await update_balance(user_id, -bet, "mines_bet", f"Ставка в Минах: {bet}₽")
    
    # Создаём игру
    first_cell = random.randint(0, 24)
    
    # Проверяем подкрутку
    override = await get_mines_override(user_id)
    if override["active"]:
        win_chance = override["win_chance"]
        if win_chance >= 1.0:
            mines = []  # без мин
        elif win_chance <= 0.0:
            mines = list(range(25))
            if first_cell in mines:
                mines.remove(first_cell)
        else:
            # Пропорционально уменьшаем количество мин
            adjusted = max(1, int(mines_count * (1 - win_chance)))
            all_cells = list(range(25))
            all_cells.remove(first_cell)
            mines = random.sample(all_cells, min(adjusted, len(all_cells)))
    else:
        all_cells = list(range(25))
        all_cells.remove(first_cell)
        mines = random.sample(all_cells, mines_count)
    
    game_data = {
        "bet": bet,
        "mines_count": mines_count,
        "mines": mines,
        "opened": [first_cell],
        "status": "playing",
        "message_id": None
    }
    
    active_mines_games[user_id] = game_data
    
    # Отправляем игровое поле
    msg = await show_mines_field(message, user_id, first_time=True)
    game_data["message_id"] = msg.message_id
    
    await state.set_state(MinesStates.playing)

# ----- ОТКРЫТИЕ ЯЧЕЙКИ -----
@dp.callback_query(F.data.startswith("mines_cell_"), MinesStates.playing)
async def mines_open_cell(callback: CallbackQuery):
    user_id = callback.from_user.id
    cell = int(callback.data.split("_")[2])
    game = active_mines_games.get(user_id)
    
    if not game or game["status"] != "playing":
        await callback.answer("❌ Игра не найдена или завершена", show_alert=True)
        return
    
    if cell in game["opened"]:
        await callback.answer("❌ Эта ячейка уже открыта", show_alert=True)
        return
    
    # Проверяем, не мина ли
    if cell in game["mines"]:
        # Проигрыш
        game["status"] = "lost"
        await callback.answer("💥 БАХ! Ты наступил на мину!", show_alert=True)
        
        # Показываем все мины
        kb_buttons = []
        for i in range(25):
            row = i // 5
            col = i % 5
            if len(kb_buttons) <= row:
                kb_buttons.append([])
            
            if i in game["mines"]:
                kb_buttons[row].append(InlineKeyboardButton(text="💣", callback_data="mines_noop"))
            elif i in game["opened"]:
                kb_buttons[row].append(InlineKeyboardButton(text="💎", callback_data="mines_noop"))
            else:
                kb_buttons[row].append(InlineKeyboardButton(text="⬛", callback_data="mines_noop"))
        
        kb_buttons.append([InlineKeyboardButton(text="🔄 Играть снова", callback_data="game_mines")])
        kb_buttons.append([InlineKeyboardButton(text="🔙 В меню", callback_data="back_to_games")])
        
        await callback.message.edit_text(
            f"💣 *ВЫ ПРОИГРАЛИ!*\n\n"
            f"💰 Ставка: {format_money(game['bet'])}\n"
            f"💥 Наступил на мину!\n"
            f"💸 Потеряно: {format_money(game['bet'])}\n\n"
            f"*Красные 💣 — мины*\n"
            f"*Зелёные 💎 — открытые ячейки*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons)
        )
        del active_mines_games[user_id]
        await callback.answer()
        return
    
    # Безопасная ячейка
    game["opened"].append(cell)
    multiplier = get_mines_multiplier(game["mines_count"], len(game["opened"]))
    
    # Если открыты все ячейки — победа
    if len(game["opened"]) == 25:
        win = int(game["bet"] * multiplier)
        await update_balance(user_id, win, "mines_win", f"Выигрыш в Минах: {win}₽")
        
        await callback.message.edit_text(
            f"🎉 *ПОБЕДА! ВСЕ ЯЧЕЙКИ ОТКРЫТЫ!*\n\n"
            f"💰 Ставка: {format_money(game['bet'])}\n"
            f"📈 Множитель: x{multiplier:.2f}\n"
            f"💎 Выигрыш: {format_money(win)}\n\n"
            f"Поздравляю! 🔥",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Играть снова", callback_data="game_mines")],
                [InlineKeyboardButton(text="🔙 В меню", callback_data="back_to_games")]
            ])
        )
        del active_mines_games[user_id]
        await callback.answer()
        return
    
    # Обновляем поле
    await show_mines_field(callback.message, user_id, first_time=False)
    await callback.answer(f"✅ Безопасно! Множитель x{multiplier:.2f}", show_alert=False)

# ----- ЗАБРАТЬ ВЫИГРЫШ -----
@dp.callback_query(F.data == "mines_cashout", MinesStates.playing)
async def mines_cashout(callback: CallbackQuery):
    user_id = callback.from_user.id
    game = active_mines_games.get(user_id)
    
    if not game or game["status"] != "playing":
        await callback.answer("❌ Игра не найдена", show_alert=True)
        return
    
    multiplier = get_mines_multiplier(game["mines_count"], len(game["opened"]))
    win = int(game["bet"] * multiplier)
    
    await update_balance(user_id, win, "mines_win", f"Выигрыш в Минах: {win}₽")
    
    await callback.message.edit_text(
        f"💰 *ВЫ ЗАБРАЛИ ВЫИГРЫШ!*\n\n"
        f"💰 Ставка: {format_money(game['bet'])}\n"
        f"🔓 Открыто ячеек: {len(game['opened'])}/25\n"
        f"📈 Множитель: x{multiplier:.2f}\n"
        f"💎 Выигрыш: {format_money(win)}\n\n"
        f"Отличная игра! 🎉",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Играть снова", callback_data="game_mines")],
            [InlineKeyboardButton(text="🔙 В меню", callback_data="back_to_games")]
        ])
    )
    del active_mines_games[user_id]
    await callback.answer()

# ----- ВЫХОД ИЗ ИГРЫ -----
@dp.callback_query(F.data == "mines_exit", MinesStates.playing)
async def mines_exit(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    game = active_mines_games.get(user_id)
    
    if game:
        await update_balance(user_id, game["bet"], "mines_refund", "Возврат ставки (выход из игры)")
        del active_mines_games[user_id]
    
    await callback.message.edit_text(
        "❌ *Игра прервана*\n\nСтавка возвращена.",
        parse_mode="Markdown",
        reply_markup=get_minigames_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "mines_noop")
async def mines_noop(callback: CallbackQuery):
    await callback.answer()

# ==================== АДМИН-ПАНЕЛЬ MINES ====================

@dp.callback_query(F.data == "admin_mines")
async def admin_mines_menu(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    active_games = len(active_mines_games)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Подкрутка удачи", callback_data="admin_mines_override")],
        [InlineKeyboardButton(text="👥 Активные игры", callback_data="admin_mines_active")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(
        f"💣 *Админ-панель MINES*\n\n"
        f"📊 Активных игр: {active_games}\n\n"
        f"Выберите действие:",
        parse_mode="Markdown",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_mines_override")
async def admin_mines_override(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    
    all_users = await get_all_users()
    await callback.message.answer(
        "🎲 *Подкрутка удачи в Mines*\n\n"
        "Выберите игрока:",
        parse_mode="Markdown",
        reply_markup=get_users_keyboard(all_users, ADMIN_ID, "admin_mines_override_user_")
    )
    await state.set_state("admin_mines_override_user")
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_mines_override_user_"))
async def admin_mines_override_user(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[5])
    await state.update_data(target_user=user_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Всегда выигрывает (100%)", callback_data="mines_override_100")],
        [InlineKeyboardButton(text="💀 Всегда проигрывает (0%)", callback_data="mines_override_0")],
        [InlineKeyboardButton(text="📊 75% побед", callback_data="mines_override_75")],
        [InlineKeyboardButton(text="📊 50% побед", callback_data="mines_override_50")],
        [InlineKeyboardButton(text="📊 25% побед", callback_data="mines_override_25")],
        [InlineKeyboardButton(text="🔧 Своё значение (0.0-1.0)", callback_data="mines_override_custom")],
        [InlineKeyboardButton(text="❌ Отключить подкрутку", callback_data="mines_override_off")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_mines")]
    ])
    
    user = await get_user(user_id)
    await callback.message.edit_text(
        f"🎲 *Подкрутка удачи*\n\n"
        f"👤 Игрок: {user['full_name']}\n\n"
        f"Выберите режим:",
        parse_mode="Markdown",
        reply_markup=kb
    )
    await state.set_state("admin_mines_override_value")
    await callback.answer()

@dp.callback_query(F.data.startswith("mines_override_"))
async def admin_mines_set_override(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    
    data = await state.get_data()
    target_user = data.get("target_user")
    if not target_user:
        await callback.answer("Ошибка: игрок не выбран", show_alert=True)
        return
    
    action = callback.data.split("_")[2]
    
    if action == "100":
        win_chance = 1.0
        mode_text = "ВСЕГДА ВЫИГРЫВАЕТ"
    elif action == "0":
        win_chance = 0.0
        mode_text = "ВСЕГДА ПРОИГРЫВАЕТ"
    elif action == "75":
        win_chance = 0.75
        mode_text = "75% побед"
    elif action == "50":
        win_chance = 0.5
        mode_text = "50% побед"
    elif action == "25":
        win_chance = 0.25
        mode_text = "25% побед"
    elif action == "off":
        await disable_mines_override(target_user)
        await callback.message.edit_text(
            f"✅ Подкрутка для игрока отключена.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 В админку Mines", callback_data="admin_mines")]
            ])
        )
        await state.clear()
        await callback.answer()
        return
    elif action == "custom":
        await callback.message.answer(
            "🔧 Введи число от 0.0 до 1.0 (например 0.85 = 85% побед):\n"
            "0.0 — всегда проигрывает\n"
            "1.0 — всегда выигрывает",
            reply_markup=get_state_back_inline_keyboard()
        )
        await state.set_state("admin_mines_custom_value")
        await callback.answer()
        return
    else:
        await callback.answer("❌ Неизвестная команда", show_alert=True)
        return
    
    await set_mines_override(target_user, win_chance, True)
    
    await callback.message.edit_text(
        f"✅ *Подкрутка установлена!*\n\n"
        f"👤 Игрок: {target_user}\n"
        f"🎲 Режим: {mode_text}\n"
        f"📊 Шанс победы: {win_chance*100:.0f}%",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 В админку Mines", callback_data="admin_mines")]
        ])
    )
    await state.clear()
    await callback.answer()

@dp.message(StateFilter("admin_mines_custom_value"))
async def admin_mines_custom_value(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    
    try:
        win_chance = float(message.text.strip().replace(",", "."))
        if win_chance < 0 or win_chance > 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи число от 0.0 до 1.0 (например 0.75)")
        return
    
    data = await state.get_data()
    target_user = data.get("target_user")
    if not target_user:
        await message.answer("❌ Ошибка: игрок не выбран")
        await state.clear()
        return
    
    await set_mines_override(target_user, win_chance, True)
    
    await message.answer(
        f"✅ *Подкрутка установлена!*\n\n"
        f"👤 Игрок: {target_user}\n"
        f"🎲 Шанс победы: {win_chance*100:.0f}%",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 В админку Mines", callback_data="admin_mines")]
        ])
    )
    await state.clear()

@dp.callback_query(F.data == "admin_mines_active")
async def admin_mines_active(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔", show_alert=True)
        return
    
    if not active_mines_games:
        await callback.message.edit_text(
            "📭 *Нет активных игр*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_mines")]
            ])
        )
        await callback.answer()
        return
    
    text = "👥 *Активные игры в Mines:*\n\n"
    for uid, game in active_mines_games.items():
        user = await get_user(uid)
        name = user['full_name'] if user else str(uid)
        multiplier = get_mines_multiplier(game["mines_count"], len(game["opened"]))
        win = int(game["bet"] * multiplier)
        text += f"• {name}\n"
        text += f"  💰 Ставка: {format_money(game['bet'])} | 💣 {game['mines_count']} мин\n"
        text += f"  🔓 Открыто: {len(game['opened'])}/25 | x{multiplier:.2f} = {format_money(win)}\n\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_mines_active")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_mines")]
    ])
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()


async def on_startup():  # startup
    await init_db()
    bot_info = await bot.get_me()
    if not bot_info.username:
        logger.error("❌ У бота нет username! Чеки не будут работать.")
        logger.error("Установите username в @BotFather и перезапустите бота.")
    else:
        logger.info(f"✅ Username бота: @{bot_info.username}")
    asyncio.create_task(penalty_scheduler())
    asyncio.create_task(business_notification_scheduler())
    asyncio.create_task(bank_scheduler())
    asyncio.create_task(daily_top_scheduler())
    asyncio.create_task(referral_credit_scheduler_immediate())
    logger.info(
        "✅ Бот запущен: бизнес, банк (пул + вклады), топ дня (10:00 МСК), инвентарь, кости, хроника."
    )

async def on_shutdown():
    logger.info("🛑 Бот останавливается...")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
