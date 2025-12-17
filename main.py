import os
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart

# Получаем токен из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Проверяем токен
if not BOT_TOKEN:
    print("❌ ОШИБКА: BOT_TOKEN не найден!")
    print("Добавьте переменную BOT_TOKEN в настройках bothost.ru")
    exit(1)

# Инициализируем бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Команда /start
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer("✅ Бот работает! Система управления парком спецтехники готова к работе.")

# Запуск
async def main():
    print("🚀 Бот запускается...")
    print(f"✅ Токен получен: {BOT_TOKEN[:10]}...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
