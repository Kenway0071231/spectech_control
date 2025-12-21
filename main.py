import os
import logging
import asyncio
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

# Состояния для админ-панели
class AdminStates(StatesGroup):
    waiting_for_equipment_name = State()
    waiting_for_equipment_model = State()
    waiting_for_equipment_vin = State()

# ========== ПРОСТАЯ ИНИЦИАЛИЗАЦИЯ БОТА ==========
session = AiohttpSession()
bot = Bot(token=os.getenv('BOT_TOKEN'), session=session)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
# ===============================================

# ========== ФУНКЦИИ ДЛЯ УВЕДОМЛЕНИЙ ==========

async def notify_admins_shift_started(shift_id):
    """Отправляем уведомление всем администраторам о начале смены"""
    try:
        # Получаем детали смены
        shift_details = await db.get_shift_details(shift_id)
        if not shift_details:
            return
        
        # Получаем всех администраторов
        admins = await db.get_all_admins()
        
        if not admins:
            logging.info("Нет администраторов для уведомлений")
            return
        
        message_text = (
            "🔔 *НОВАЯ СМЕНА НАЧАТА*\n\n"
            f"*Водитель:* {shift_details['driver_name']}\n"
            f"*Техника:* {shift_details['equipment_name']} ({shift_details['equipment_model']})\n"
            f"*Время начала:* {shift_details['start_time'][:16]}\n"
            f"*ID смены:* {shift_id}\n\n"
            f"Для просмотра активных смен: /admin"
        )
        
        # Отправляем уведомление каждому администратору
        for admin in admins:
            admin_id, admin_name = admin
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=message_text,
                    parse_mode="Markdown"
                )
                logging.info(f"Уведомление отправлено администратору: {admin_name}")
            except Exception as e:
                logging.error(f"Ошибка отправки уведомления администратору {admin_name}: {e}")
        
    except Exception as e:
        logging.error(f"Ошибка в функции notify_admins_shift_started: {e}")

async def notify_admins_shift_ended(shift_id):
    """Отправляем уведомление всем администраторам о завершении смены"""
    try:
        # Получаем детали смены
        shift_details = await db.get_shift_details(shift_id)
        if not shift_details:
            return
        
        # Рассчитываем продолжительность смены
        start_time = shift_details['start_time']
        end_time = shift_details['end_time']
        
        duration = "неизвестно"
        if start_time and end_time:
            try:
                from datetime import datetime
                start_dt = datetime.strptime(start_time[:19], "%Y-%m-%d %H:%M:%S")
                end_dt = datetime.strptime(end_time[:19], "%Y-%m-%d %H:%M:%S")
                diff = end_dt - start_dt
                hours = diff.seconds // 3600
                minutes = (diff.seconds % 3600) // 60
                duration = f"{hours} ч {minutes} мин"
            except:
                pass
        
        # Получаем всех администраторов
        admins = await db.get_all_admins()
        
        if not admins:
            logging.info("Нет администраторов для уведомлений")
            return
        
        message_text = (
            "🔔 *СМЕНА ЗАВЕРШЕНА*\n\n"
            f"*Водитель:* {shift_details['driver_name']}\n"
            f"*Техника:* {shift_details['equipment_name']} ({shift_details['equipment_model']})\n"
            f"*Время начала:* {shift_details['start_time'][:16]}\n"
            f"*Время окончания:* {shift_details['end_time'][:16]}\n"
            f"*Продолжительность:* {duration}\n"
            f"*ID смены:* {shift_id}\n\n"
            f"Для просмотра статистики: /admin"
        )
        
        # Отправляем уведомление каждому администратору
        for admin in admins:
            admin_id, admin_name = admin
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=message_text,
                    parse_mode="Markdown"
                )
                logging.info(f"Уведомление отправлено администратору: {admin_name}")
            except Exception as e:
                logging.error(f"Ошибка отправки уведомления администратору {admin_name}: {e}")
        
    except Exception as e:
        logging.error(f"Ошибка в функции notify_admins_shift_ended: {e}")

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    # Регистрируем водителя в базе (по умолчанию driver)
    driver_id = await db.register_driver(
        telegram_id=message.from_user.id,
        full_name=f"{message.from_user.first_name} {message.from_user.last_name or ''}"
    )
    
    # Проверяем, есть ли активная смена
    active_shift = await db.get_active_shift(message.from_user.id)
    
    # Проверяем роль пользователя
    user_role = await db.get_user_role(message.from_user.id)
    
    if user_role == 'admin':
        # Меню для админа
        if active_shift:
            keyboard = [
                [types.KeyboardButton(text="⏹️ Завершить смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="📸 Осмотры с фото")],
                [types.KeyboardButton(text="👨‍💼 Админ-панель")],
                [types.KeyboardButton(text="ℹ️  Информация")]
            ]
        else:
            keyboard = [
                [types.KeyboardButton(text="🚛 Начать смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="📸 Осмотры с фото")],
                [types.KeyboardButton(text="👨‍💼 Админ-панель")],
                [types.KeyboardButton(text="ℹ️  Информация")]
            ]
    else:
        # Меню для водителя
        if active_shift:
            keyboard = [
                [types.KeyboardButton(text="⏹️ Завершить смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="📸 Осмотры с фото")],
                [types.KeyboardButton(text="ℹ️  Информация")]
            ]
        else:
            keyboard = [
                [types.KeyboardButton(text="🚛 Начать смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="ℹ️  Информация")]
            ]
    
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        f"Привет, {message.from_user.first_name}!\n"
        f"Твой ID: {driver_id}\n"
        f"Роль: {user_role}\n"
        f"Я бот для контроля спецтехники.\n"
        f"Выберите действие:",
        reply_markup=reply_markup
    )

@dp.message(F.text == "👨‍💼 Админ-панель")
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    """Админ-панель для руководителя"""
    
    # Проверяем права доступа
    user_role = await db.get_user_role(message.from_user.id)
    
    if user_role != 'admin':
        await message.answer("⛔ У вас нет доступа к админ-панели.")
        return
    
    keyboard = [
        [types.KeyboardButton(text="📊 Активные смены")],
        [types.KeyboardButton(text="📈 Статистика за сегодня")],
        [types.KeyboardButton(text="👥 Все водители")],
        [types.KeyboardButton(text="🚜 Вся техника")],
        [types.KeyboardButton(text="➕ Добавить технику")],
        [types.KeyboardButton(text="🔔 Тест уведомлений")],
        [types.KeyboardButton(text="🔙 Назад")]
    ]
    
    reply_markup = types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        "👨‍💼 АДМИН-ПАНЕЛЬ\n\n"
        "Выберите действие:",
        reply_markup=reply_markup
    )

@dp.message(F.text == "📊 Активные смены")
async def show_active_shifts(message: types.Message):
    """Показываем все активные смены"""
    
    user_role = await db.get_user_role(message.from_user.id)
    if user_role != 'admin':
        await message.answer("⛔ У вас нет прав для просмотра активных смен.")
        return
    
    active_shifts = await db.get_all_active_shifts()
    
    if not active_shifts:
        await message.answer("✅ На данный момент активных смен нет.")
        return
    
    text = "📊 АКТИВНЫЕ СМЕНЫ:\n\n"
    
    for shift in active_shifts:
        shift_id, start_time, driver_name, eq_name, eq_model = shift
        
        # Форматируем время
        start_str = start_time[:16] if start_time else "—"
        
        text += f"🟢 *ID:* {shift_id}\n"
        text += f"   *Водитель:* {driver_name}\n"
        text += f"   *Техника:* {eq_name} ({eq_model})\n"
        text += f"   *Начало:* {start_str}\n\n"
    
    text += f"*Всего активных смен:* {len(active_shifts)}"
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "📈 Статистика за сегодня")
async def show_today_stats(message: types.Message):
    """Показываем статистику за сегодня"""
    
    user_role = await db.get_user_role(message.from_user.id)
    if user_role != 'admin':
        await message.answer("⛔ У вас нет прав для просмотра статистики.")
        return
    
    # Временно простая статистика
    active_shifts = await db.get_all_active_shifts()
    
    text = (
        "📈 *СТАТИСТИКА ЗА СЕГОДНЯ*\n\n"
        f"*Активных смен:* {len(active_shifts)}\n"
        f"*Всего водителей:* {len(await db.get_all_drivers())}\n"
        f"*Всего техники:* {len(await db.get_equipment_list())}\n\n"
        "*Администраторов:*\n"
    )
    
    # Список администраторов
    admins = await db.get_all_admins()
    for admin in admins:
        admin_id, admin_name = admin
        text += f"👑 {admin_name}\n"
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "🔔 Тест уведомлений")
async def test_notifications(message: types.Message):
    """Тестовая отправка уведомлений"""
    
    user_role = await db.get_user_role(message.from_user.id)
    if user_role != 'admin':
        await message.answer("⛔ У вас нет прав для теста уведомлений.")
        return
    
    await message.answer("📡 Отправляю тестовое уведомление...")
    
    try:
        # Отправляем тестовое сообщение самому себе
        test_text = (
            "🔔 *ТЕСТ УВЕДОМЛЕНИЙ*\n\n"
            "Это тестовое уведомление от системы.\n"
            "Если вы видите это сообщение, значит система уведомлений работает корректно.\n\n"
            f"*Время:* {message.date.strftime('%H:%M %d.%m.%Y')}"
        )
        
        await bot.send_message(
            chat_id=message.from_user.id,
            text=test_text,
            parse_mode="Markdown"
        )
        
        await message.answer("✅ Тестовое уведомление отправлено успешно!")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки уведомления: {e}")

# ========== ОСНОВНЫЕ ФУНКЦИИ С УВЕДОМЛЕНИЯМИ ==========

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
        
        # Отправляем уведомление администраторам
        asyncio.create_task(notify_admins_shift_started(shift_id))
        
        # Очищаем состояние
        await state.clear()
        
        # Возвращаем основное меню
        reply_markup = await get_admin_keyboard(message.from_user.id)
        
        await message.answer(
            f"✅ СМЕНА НАЧАТА!\n\n"
            f"Техника: {name} ({model})\n"
            f"ID смены: {shift_id}\n"
            f"Время начала: {message.date.strftime('%H:%M %d.%m.%Y')}\n"
            f"Фото осмотра: не добавлено\n\n"
            f"Уведомление отправлено руководителям.\n"
            f"Удачной работы! Будьте внимательны.",
            reply_markup=reply_markup
        )
        return
    
    await message.answer("Пожалуйста, используйте кнопки меню.")

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
    
    # Отправляем уведомление администраторам
    asyncio.create_task(notify_admins_shift_started(shift_id))
    
    # Очищаем состояние
    await state.clear()
    
    # Возвращаем основное меню
    reply_markup = await get_admin_keyboard(message.from_user.id)
    
    await message.answer(
        f"✅ СМЕНА НАЧАТА!\n\n"
        f"Техника: {name} ({model})\n"
        f"ID смены: {shift_id}\n"
        f"Время начала: {message.date.strftime('%H:%M %d.%m.%Y')}\n"
        f"Фото осмотра: {len(photos)} шт.\n\n"
        f"Уведомление отправлено руководителям.\n"
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
    
    # Отправляем уведомление администраторам
    asyncio.create_task(notify_admins_shift_ended(shift_id))
    
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
        f"Уведомление отправлено руководителям.\n"
        f"Спасибо за работу! Отдыхайте."
    )
    
    # Обновляем меню (уберем кнопку завершения)
    await cmd_start(message)

# ========== ОСТАЛЬНЫЕ ФУНКЦИИ (без изменений) ==========

# [Остальной код оставляем без изменений: 
# show_my_shifts, show_inspections_with_photos, add_equipment_start, 
# process_equipment_name, process_equipment_model, process_equipment_vin,
# back_to_main_menu, get_admin_keyboard, show_all_drivers, show_all_equipment,
# process_inspection_photo, handle_non_photo_in_waiting_state, show_info]

# ========== ЗАПУСК БОТА С СОЗДАНИЕМ ТЕСТОВОГО АДМИНА ==========

async def on_startup():
    """Действия при запуске бота"""
    # Подключаемся к базе данных
    await db.connect()
    
    # Добавляем тестовые данные (если их нет)
    await db.add_test_data()
    
    # Создаем тестового администратора (ЗАМЕНИ 123456789 на СВОЙ telegram ID)
    # Чтобы узнать свой ID: напиши боту /start, он покажет твой ID
    YOUR_TELEGRAM_ID = 123456789  # <-- ЗАМЕНИ ЭТО ЧИСЛО НА СВОЙ ID
    await db.register_driver(YOUR_TELEGRAM_ID, "Администратор", "admin")
    
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
    asyncio.run(main())
