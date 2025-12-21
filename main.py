import os
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import ContentType
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
    waiting_for_photos = State()  # Ожидание фотографий

# ========== ПРОСТАЯ ИНИЦИАЛИЗАЦИЯ БОТА ==========
session = AiohttpSession()
bot = Bot(token=os.getenv('BOT_TOKEN'), session=session)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
# ===============================================

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    # Регистрируем водителя в базе
    driver_id = await db.register_driver(
        telegram_id=message.from_user.id,
        full_name=f"{message.from_user.first_name} {message.from_user.last_name or ''}"
    )
    
    # Проверяем, есть ли активная смена
    active_shift = await db.get_active_shift(message.from_user.id)
    
    if active_shift:
        # Если есть активная смена - показываем кнопку завершения
        keyboard = [
            [types.KeyboardButton(text="⏹️ Завершить смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="📸 Осмотры с фото")],
            [types.KeyboardButton(text="ℹ️  Информация")]
        ]
    else:
        # Если нет активной смены - показываем кнопку начала
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
        [types.KeyboardButton(text="📸 Добавить фото осмотра")],
        [types.KeyboardButton(text="✅ Завершить осмотр без фото")],
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
        "Вы можете добавить фото или завершить осмотр:",
        reply_markup=reply_markup
    )
    
    # Инициализируем список фото в состоянии
    await state.update_data(inspection_photos=[])
    await state.set_state(ShiftStates.pre_inspection)

@dp.message(ShiftStates.pre_inspection)
async def process_pre_inspection(message: types.Message, state: FSMContext):
    """Обрабатываем действия в состоянии осмотра"""
    
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
    
    if message.text == "📸 Добавить фото осмотра":
        await message.answer(
            "Отправьте фотографию осмотра. "
            "Вы можете отправить несколько фото подряд.\n\n"
            "После отправки фото нажмите '✅ Завершить осмотр с фото'."
        )
        await state.set_state(ShiftStates.waiting_for_photos)
        return
    
    if message.text == "✅ Завершить осмотр без фото":
        # Завершаем осмотр без фото
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
        
        # Создаем запись об осмотре без фото
        await db.add_inspection_with_photos(shift_id, [], "Осмотр без фото")
        
        # Очищаем состояние
        await state.clear()
        
        # Возвращаем основное меню
        keyboard = [
            [types.KeyboardButton(text="⏹️ Завершить смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="📸 Осмотры с фото")],
            [types.KeyboardButton(text="ℹ️  Информация")]
        ]
        reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
        
        await message.answer(
            f"✅ СМЕНА НАЧАТА!\n\n"
            f"Техника: {name} ({model})\n"
            f"ID смены: {shift_id}\n"
            f"Время начала: {message.date.strftime('%H:%M %d.%m.%Y')}\n"
            f"Фото осмотра: не добавлено\n\n"
            f"Удачной работы! Будьте внимательны.",
            reply_markup=reply_markup
        )
        return
    
    await message.answer("Пожалуйста, используйте кнопки меню.")

@dp.message(ShiftStates.waiting_for_photos, F.content_type == ContentType.PHOTO)
async def process_inspection_photo(message: types.Message, state: FSMContext):
    """Обрабатываем фото осмотра"""
    
    # Получаем file_id самой качественной версии фото
    photo = message.photo[-1]
    photo_id = photo.file_id
    
    # Получаем текущий список фото из состояния
    data = await state.get_data()
    photos = data.get('inspection_photos', [])
    
    # Добавляем новое фото
    photos.append(photo_id)
    await state.update_data(inspection_photos=photos)
    
    # Показываем превью фото
    await message.answer_photo(
        photo_id,
        caption=f"✅ Фото #{len(photos)} сохранено!\n"
                f"Вы можете отправить ещё фото или завершить осмотр."
    )
    
    # Показываем клавиатуру для продолжения
    keyboard = [
        [types.KeyboardButton(text="📸 Добавить ещё фото")],
        [types.KeyboardButton(text="✅ Завершить осмотр с фото")],
        [types.KeyboardButton(text="❌ Отмена")]
    ]
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        f"Добавлено фото: {len(photos)} шт.\n"
        f"Что дальше?",
        reply_markup=reply_markup
    )
    
    # Возвращаемся в состояние осмотра
    await state.set_state(ShiftStates.pre_inspection)

@dp.message(ShiftStates.waiting_for_photos)
async def handle_non_photo_in_waiting_state(message: types.Message, state: FSMContext):
    """Обрабатываем не-фото сообщения в состоянии ожидания фото"""
    await message.answer("Пожалуйста, отправьте фотографию или используйте кнопки меню.")

@dp.message(F.text == "✅ Завершить осмотр с фото")
async def complete_inspection_with_photos(message: types.Message, state: FSMContext):
    """Завершаем осмотр с добавленными фото"""
    
    # Получаем данные из состояния
    data = await state.get_data()
    selected_eq = data.get('selected_equipment')
    photos = data.get('inspection_photos', [])
    
    if not selected_eq:
        await message.answer("Ошибка: данные о технике не найдены.")
        await state.clear()
        return
    
    if not photos:
        await message.answer("Вы не добавили фото. Используйте '✅ Завершить осмотр без фото'.")
        return
    
    eq_id, name, model = selected_eq
    
    # Начинаем смену в базе данных
    shift_id = await db.start_shift(
        driver_id=message.from_user.id,
        equipment_id=eq_id
    )
    
    # Создаем запись об осмотре с фото
    await db.add_inspection_with_photos(shift_id, photos, f"Осмотр {name} ({model})")
    
    # Очищаем состояние
    await state.clear()
    
    # Возвращаем основное меню
    keyboard = [
        [types.KeyboardButton(text="⏹️ Завершить смену")],
        [types.KeyboardButton(text="📋 Мои смены")],
        [types.KeyboardButton(text="📸 Осмотры с фото")],
        [types.KeyboardButton(text="ℹ️  Информация")]
    ]
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        f"✅ СМЕНА НАЧАТА!\n\n"
        f"Техника: {name} ({model})\n"
        f"ID смены: {shift_id}\n"
        f"Время начала: {message.date.strftime('%H:%M %d.%m.%Y')}\n"
        f"Фото осмотра: {len(photos)} шт.\n\n"
        f"Удачной работы! Будьте внимательны.",
        reply_markup=reply_markup
    )

@dp.message(F.text == "⏹️ Завершить смену")
async def end_shift_process(message: types.Message):
    """Завершаем активную смену"""
    
    # Получаем активную смену
    active_shift = await db.get_active_shift(message.from_user.id)
    
    if not active_shift:
        await message.answer("❌ У вас нет активной смены.")
        return
    
    shift_id, equipment_id = active_shift
    
    # Завершаем смену в базе
    await db.end_shift(shift_id)
    
    # Получаем название техники для красивого ответа
    cursor = await db.connection.execute(
        'SELECT name, model FROM equipment WHERE id = ?', 
        (equipment_id,)
    )
    equipment = await cursor.fetchone()
    await cursor.close()
    
    if equipment:
        eq_name, eq_model = equipment
        equipment_text = f"{eq_name} ({eq_model})"
    else:
        equipment_text = "неизвестная техника"
    
    await message.answer(
        f"✅ СМЕНА ЗАВЕРШЕНА!\n\n"
        f"Техника: {equipment_text}\n"
        f"ID смены: {shift_id}\n"
        f"Время окончания: {message.date.strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Спасибо за работу! Отдыхайте."
    )
    
    # Обновляем меню (уберем кнопку завершения)
    await cmd_start(message)

@dp.message(F.text == "📋 Мои смены")
async def show_my_shifts(message: types.Message):
    """Показываем историю смен водителя"""
    
    # Получаем смены из базы
    shifts = await db.get_driver_shifts(message.from_user.id, limit=5)
    
    if not shifts:
        await message.answer("📭 У вас ещё не было смен.")
        return
    
    # Формируем сообщение
    text = "📊 ПОСЛЕДНИЕ СМЕНЫ:\n\n"
    
    for shift in shifts:
        shift_id, start_time, end_time, status, eq_name, eq_model = shift
        
        # Форматируем время
        start_str = start_time[:16] if start_time else "—"
        end_str = end_time[:16] if end_time else "в процессе"
        
        # Статус
        status_icon = "✅" if status == "completed" else "🟡"
        
        text += f"{status_icon} {eq_name} ({eq_model})\n"
        text += f"   Начало: {start_str}\n"
        text += f"   Окончание: {end_str}\n"
        text += f"   ID: {shift_id}\n\n"
    
    text += "Всего смен: " + str(len(shifts))
    
    await message.answer(text)

@dp.message(F.text == "📸 Осмотры с фото")
async def show_inspections_with_photos(message: types.Message):
    """Показываем осмотры с фотографиями"""
    
    # Получаем последнюю активную или завершенную смену
    shifts = await db.get_driver_shifts(message.from_user.id, limit=3)
    
    if not shifts:
        await message.answer("📭 У вас ещё не было смен с осмотрами.")
        return
    
    text = "📸 ОСМОТРЫ С ФОТОГРАФИЯМИ:\n\n"
    
    for shift in shifts:
        shift_id, start_time, end_time, status, eq_name, eq_model = shift
        
        # Получаем осмотры для этой смены
        inspections = await db.get_shift_inspections(shift_id)
        
        if inspections:
            for inspection in inspections:
                photo_count = len(inspection['photos'])
                text += f"🔍 {eq_name} ({eq_model})\n"
                text += f"   ID смены: {shift_id}\n"
                text += f"   Фото: {photo_count} шт.\n"
                text += f"   Дата: {inspection['created_at'][:16]}\n"
                
                if photo_count > 0:
                    # Отправляем первое фото как превью
                    await message.answer_photo(
                        inspection['photos'][0],
                        caption=f"Осмотр {eq_name} ({eq_model})\n"
                                f"Фото 1 из {photo_count}\n"
                                f"ID смены: {shift_id}"
                    )
                
                text += "\n"
    
    if text == "📸 ОСМОТРЫ С ФОТОГРАФИЯМИ:\n\n":
        text += "Нет осмотров с фотографиями."
    
    await message.answer(text)

@dp.message(F.text == "ℹ️  Информация")
async def show_info(message: types.Message):
    await message.answer(
        "🤖 ТЕХКОНТРОЛЬ MVP v1.2\n\n"
        "Версия с загрузкой фото при осмотре.\n\n"
        "Доступные функции:\n"
        "✅ Начало смены\n"
        "✅ Инструктаж по безопасности\n"
        "✅ Предсменный осмотр с фото\n"
        "✅ Завершение смены\n"
        "✅ История смен (5 последних)\n"
        "✅ Просмотр осмотров с фото\n"
        "🔄 Интеграция с AI (анализ фото)\n"
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
