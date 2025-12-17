import os
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv

# Импортируем нашу базу данных
from database import db

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Создаем состояния (шаги) для FSM
class ShiftStates(StatesGroup):
    choosing_equipment = State()  # Выбор техники
    safety_instruction = State()  # Инструктаж по безопасности
    pre_inspection = State()      # Предсменный осмотр

# ========== ПРОСТАЯ ИНИЦИАЛИЗАЦИЯ БОТА ==========
# Используем прокси сразу (самый надежный способ)
session = AiohttpSession()
bot = Bot(token=os.getenv('BOT_TOKEN'), session=session)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
# ===============================================

# ========== ОБРАБОТЧИКИ КОМАНД (остальной код БЕЗ изменений) ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    # Регистрируем водителя в базе
    driver_id = await db.register_driver(
        telegram_id=message.from_user.id,
        full_name=f"{message.from_user.first_name} {message.from_user.last_name or ''}"
    )
    
    keyboard = [
        [types.KeyboardButton(text="🚛 Начать смену")],
        [types.KeyboardButton(text="📋 Мои смены")],
        [types.KeyboardButton(text="ℹ️  Информация")]
    ]
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        f"Привет, {message.from_user.first_name}!\n"
        f"Твой ID: {driver_id}\n"
        f"Я бот для контроля спецтехники.\n"
        f"Выберите действие:",
        reply_markup=reply_markup
    )

@dp.message(F.text == "🚛 Начать смену")
async def start_shift_process(message: types.Message, state: FSMContext):
    """Начинаем процесс начала смены"""
    
    # Получаем список техники из базы
    equipment_list = await db.get_equipment_list()
    
    if not equipment_list:
        await message.answer("В базе нет техники. Обратитесь к администратору.")
        return
    
    # Создаем клавиатуру с техникой
    keyboard = []
    for eq in equipment_list:
        eq_id, name, model = eq
        keyboard.append([types.KeyboardButton(text=f"{name} ({model})")])
    
    # Добавляем кнопку отмены
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        "Выберите технику для начала смены:",
        reply_markup=reply_markup
    )
    
    # Сохраняем список техники в состоянии
    await state.update_data(equipment_list=equipment_list)
    await state.set_state(ShiftStates.choosing_equipment)

@dp.message(ShiftStates.choosing_equipment)
async def process_equipment_choice(message: types.Message, state: FSMContext):
    """Обрабатываем выбор техники"""
    
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    # Получаем сохраненный список техники
    data = await state.get_data()
    equipment_list = data.get('equipment_list', [])
    
    # Ищем выбранную технику
    selected_eq = None
    for eq in equipment_list:
        eq_id, name, model = eq
        if message.text == f"{name} ({model})":
            selected_eq = eq
            break
    
    if not selected_eq:
        await message.answer("Пожалуйста, выберите технику из списка.")
        return
    
    eq_id, name, model = selected_eq
    
    # Сохраняем выбранную технику
    await state.update_data(selected_equipment=selected_eq)
    
    # Инструктаж по безопасности
    keyboard = [
        [types.KeyboardButton(text="✅ Ознакомлен, приступаю")],
        [types.KeyboardButton(text="❌ Отмена")]
    ]
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        f"📋 ИНСТРУКТАЖ ПО ТЕХНИКЕ БЕЗОПАСНОСТИ\n\n"
        f"Техника: {name} ({model})\n\n"
        f"1. Проверьте наличие средств пожаротушения\n"
        f"2. Убедитесь в исправности ремней безопасности\n"
        f"3. Проверьте работоспособность сигналов и огней\n"
        f"4. Осмотрите технику на наличие утечек\n"
        f"5. Проверьте давление в шинах\n\n"
        f"Прочитайте и подтвердите ознакомление:",
        reply_markup=reply_markup
    )
    
    await state.set_state(ShiftStates.safety_instruction)

@dp.message(ShiftStates.safety_instruction)
async def process_safety_instruction(message: types.Message, state: FSMContext):
    """Обрабатываем подтверждение инструктажа"""
    
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    if message.text != "✅ Ознакомлен, приступаю":
        await message.answer("Пожалуйста, подтвердите ознакомление с инструктажем.")
        return
    
    # Переходим к предсменному осмотру
    keyboard = [
        [types.KeyboardButton(text="✅ Осмотр завершен, начинаю смену")],
        [types.KeyboardButton(text="🔄 Запросить чек-лист осмотра")],
        [types.KeyboardButton(text="❌ Отмена")]
    ]
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        "🔍 ПРЕДСМЕННЫЙ ОСМОТР\n\n"
        "1. Проверьте уровень масла в двигателе\n"
        "2. Проверьте уровень охлаждающей жидкости\n"
        "3. Осмотрите гидравлические шланги на предмет утечек\n"
        "4. Проверьте работу всех приборов\n"
        "5. Сделайте фото основных узлов\n\n"
        "После осмотра нажмите кнопку ниже:",
        reply_markup=reply_markup
    )
    
    await state.set_state(ShiftStates.pre_inspection)

@dp.message(ShiftStates.pre_inspection)
async def process_pre_inspection(message: types.Message, state: FSMContext):
    """Завершаем предсменный осмотр и начинаем смену"""
    
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    if message.text == "🔄 Запросить чек-лист осмотра":
        await message.answer(
            "📋 ЧЕК-ЛИСТ ПРЕДСМЕННОГО ОСМОТРА:\n\n"
            "1. Двигатель:\n"
            "   - Уровень масла\n"
            "   - Уровень охлаждающей жидкости\n"
            "   - Состояние ремней\n\n"
            "2. Гидравлика:\n"
            "   - Уровень гидравлической жидкости\n"
            "   - Состояние шлангов\n"
            "   - Проверка на утечки\n\n"
            "3. Ходовая часть:\n"
            "   - Давление в шинах\n"
            "   - Состояние гусениц (если есть)\n\n"
            "4. Безопасность:\n"
            "   - Ремни безопасности\n"
            "   - Огнетушитель\n"
            "   - Аптечка\n"
            "   - Знаки аварийной остановки\n"
        )
        return
    
    if message.text == "✅ Осмотр завершен, начинаю смену":
        # Получаем данные из состояния
        data = await state.get_data()
        selected_eq = data.get('selected_equipment')
        
        if not selected_eq:
            await message.answer("Ошибка: данные о технике не найдены.")
            await state.clear()
            return
        
        eq_id, name, model = selected_eq
        
        # Начинаем смену в базе данных
        shift_id = await db.start_shift(
            driver_id=message.from_user.id,
            equipment_id=eq_id
        )
        
        # Очищаем состояние
        await state.clear()
        
        # Возвращаем основное меню
        keyboard = [
            [types.KeyboardButton(text="⏹️ Завершить смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="ℹ️  Информация")]
        ]
        reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
        
        await message.answer(
            f"✅ СМЕНА НАЧАТА!\n\n"
            f"Техника: {name} ({model})\n"
            f"ID смены: {shift_id}\n"
            f"Время начала: {message.date.strftime('%H:%M %d.%m.%Y')}\n\n"
            f"Удачной работы! Будьте внимательны.",
            reply_markup=reply_markup
        )
        return
    
    await message.answer("Пожалуйста, используйте кнопки меню.")

@dp.message(F.text == "📋 Мои смены")
async def show_my_shifts(message: types.Message):
    """Показываем историю смен водителя"""
    # Временно заглушка
    await message.answer(
        "📊 ИСТОРИЯ СМЕН\n\n"
        "Этот раздел в разработке.\n"
        "Скоро здесь появится:\n"
        "- История ваших смен\n"
        "- Статистика\n"
        "- Отчеты\n\n"
        "Сейчас вы можете начать новую смену."
    )

@dp.message(F.text == "ℹ️  Информация")
async def show_info(message: types.Message):
    await message.answer(
        "🤖 ТЕХКОНТРОЛЬ MVP v1.0\n\n"
        "Это тестовая версия бота для управления спецтехникой.\n\n"
        "Функции в разработке:\n"
        "✅ Начало смены\n"
        "✅ Инструктаж по безопасности\n"
        "✅ Предсменный осмотр\n"
        "🔄 История смен\n"
        "🔄 Интеграция с AI\n"
        "🔄 Веб-админка\n\n"
        "По вопросам: свяжитесь с разработчиком."
    )

# ========== ЗАПУСК БОТА ==========

async def on_startup():
    """Действия при запуске бота"""
    # Подключаемся к базе данных
    await db.connect()
    
    # Добавляем тестовые данные (если их нет)
    await db.add_test_data()
    
    logging.info("Бот и база данных готовы к работе")

async def on_shutdown():
    """Действия при остановке бота"""
    # Закрываем соединение с базой
    await db.close()
    logging.info("Бот остановлен, база данных закрыта")

async def main():
    # Запускаем действия при старте
    await on_startup()
    
    # Запускаем бота
    logging.info("Бот ЗАПУЩЕН! Ищет новые сообщения...")
    await dp.start_polling(bot)
    
    # Действия при остановке
    await on_shutdown()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())