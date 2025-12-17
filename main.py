import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from database import Database

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Получаем токен из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Проверяем токен
if not BOT_TOKEN:
    print("❌ ОШИБКА: BOT_TOKEN не найден!")
    print("💡 Добавьте переменную BOT_TOKEN в настройках bothost.ru")
    exit(1)

# Инициализируем бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db = Database()

# Клавиатура
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚜 Начать смену")],
            [KeyboardButton(text="ℹ️ Информация"), KeyboardButton(text="📊 Мои смены")]
        ],
        resize_keyboard=True
    )

# Команда /start
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 Добро пожаловать в систему управления парком спецтехники!\n\n"
        "Выберите действие:",
        reply_markup=get_main_keyboard()
    )

# Кнопка "Начать смену"
@dp.message(lambda message: message.text == "🚜 Начать смену")
async def start_shift(message: types.Message):
    await message.answer("✅ Функция 'Начать смену' готова к работе!")

# Кнопка "Информация"
@dp.message(lambda message: message.text == "ℹ️ Информация")
async def show_info(message: types.Message):
    await message.answer("🤖 Бот для управления парком спецтехники\nВерсия: 1.0")

# Запуск бота и базы данных
async def main():
    try:
        # Подключаем БД
        await db.connect()
        await db.add_test_data()
        
        print("✅ База данных инициализирована")
        print(f"🤖 Бот запускается с токеном: {BOT_TOKEN[:10]}...")
        
        # Запускаем бота
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")
        await db.close()

if __name__ == "__main__":
    print("🚀 Запуск бота...")
    asyncio.run(main())
