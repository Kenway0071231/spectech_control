import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
import asyncio

# Берём токен из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer("✅ Бот работает на bothost.ru!")

async def main():
    print("Бот запускается...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
