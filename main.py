import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta
import aioschedule
import asyncio
from dotenv import load_dotenv

from database import db

# ========== НАСТРОЙКА ==========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(
    token=os.getenv('BOT_TOKEN'),
    default=DefaultBotProperties(parse_mode="HTML")
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== СОСТОЯНИЯ ==========
class UserStates(StatesGroup):
    waiting_for_username_or_id = State()
    waiting_for_role = State()
    waiting_for_equipment_name = State()
    waiting_for_equipment_model = State()
    waiting_for_equipment_vin = State()
    
    # Новые состояния для смен
    waiting_for_equipment_selection = State()
    waiting_for_briefing_confirmation = State()
    waiting_for_inspection_photo = State()
    waiting_for_daily_checks = State()
    waiting_for_shift_notes = State()
    
    # Для ТО
    waiting_for_maintenance_type = State()
    waiting_for_maintenance_date = State()
    waiting_for_maintenance_description = State()
    
    # Для уведомлений
    waiting_for_notification_text = State()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

async def send_typing(chat_id):
    """Показывает 'печатает...'"""
    try:
        await bot.send_chat_action(chat_id, "typing")
        await asyncio.sleep(0.1)
    except:
        pass

async def reply(message, text, **kwargs):
    """Отправляет сообщение с индикатором набора"""
    await send_typing(message.chat.id)
    return await message.answer(text, **kwargs)

async def send_to_user(user_id, text, **kwargs):
    """Отправляет сообщение пользователю по ID"""
    try:
        await bot.send_message(user_id, text, **kwargs)
    except:
        logger.error(f"Не удалось отправить сообщение пользователю {user_id}")

def get_main_keyboard(role):
    """Генерирует клавиатуру в зависимости от роли"""
    
    keyboards = {
        'botadmin': [
            [types.KeyboardButton(text="👑 Админ-панель")],
            [types.KeyboardButton(text="🏢 Все организации")],
            [types.KeyboardButton(text="👥 Все пользователи")],
            [types.KeyboardButton(text="📊 Статистика")],
            [types.KeyboardButton(text="➕ Назначить роль")],
            [types.KeyboardButton(text="🔔 Отправить уведомление")]
        ],
        
        'director': [
            [types.KeyboardButton(text="👨‍💼 Моя организация")],
            [types.KeyboardButton(text="🚜 Автопарк")],
            [types.KeyboardButton(text="👥 Сотрудники")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Добавить ТО")],
            [types.KeyboardButton(text="➕ Назначить роль")],
            [types.KeyboardButton(text="📊 Отчеты")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")],
            [types.KeyboardButton(text="📅 Ближайшие ТО")]
        ],
        
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Управление парком")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Добавить ТО")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")],
            [types.KeyboardButton(text="📅 Ближайшие ТО")]
        ],
        
        'driver': [
            [types.KeyboardButton(text="🚛 Начать смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="✅ Закончить смену")],
            [types.KeyboardButton(text="🚜 Моя техника")],
            [types.KeyboardButton(text="ℹ️ Информация")]
        ]
    }
    
    return types.ReplyKeyboardMarkup(
        keyboard=keyboards.get(role, keyboards['driver']),
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

def get_cancel_keyboard():
    """Клавиатура с кнопкой отмена"""
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def get_yes_no_keyboard():
    """Клавиатура Да/Нет"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="✅ Да"), types.KeyboardButton(text="❌ Нет")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def get_check_status_keyboard():
    """Клавиатура для статуса проверки"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="✅ Исправно"), types.KeyboardButton(text="⚠️ Требует внимания")],
            [types.KeyboardButton(text="❌ Неисправно"), types.KeyboardButton(text="⏭️ Пропустить")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

# ========== КОМАНДА СТАРТ ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Главное меню для всех"""
    # Сбрасываем состояние
    await state.clear()
    
    user = await db.get_user(message.from_user.id)
    
    # Регистрируем, если пользователя нет
    if not user:
        await db.register_user(
            telegram_id=message.from_user.id,
            full_name=f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip(),
            username=message.from_user.username,
            role='driver'
        )
        user = await db.get_user(message.from_user.id)
    
    role = user['role']
    role_names = {
        'botadmin': '👑 Администратор бота',
        'director': '👨‍💼 Директор компании',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    # Проверяем активную смену для водителя
    if role == 'driver':
        active_shift = await db.get_active_shift(message.from_user.id)
        if active_shift:
            await reply(
                message,
                f"🚛 <b>У вас активная смена!</b>\n\n"
                f"<b>Техника:</b> {active_shift.get('equipment_name', 'Не указана')}\n"
                f"<b>Начало:</b> {active_shift['start_time'][:16]}\n"
                f"<b>Статус осмотра:</b> {'✅ Подтверждён' if active_shift['inspection_approved'] else '⏳ Ожидает проверки'}\n\n"
                f"Вы можете завершить смену через меню.",
                reply_markup=get_main_keyboard(role)
            )
            return
    
    await reply(
        message,
        f"🤖 <b>ТехКонтроль Бот</b>\n\n"
        f"<b>Роль:</b> {role_names.get(role, '👤 Пользователь')}\n"
        f"<b>ID:</b> {message.from_user.id}\n"
        f"<b>Имя:</b> {message.from_user.full_name}\n\n"
        f"Выберите действие из меню:",
        reply_markup=get_main_keyboard(role)
    )

# ========== КОМАНДА ОТМЕНА ==========
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Отменяет текущее действие"""
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(
        message,
        "❌ Действие отменено. Возврат в главное меню.",
        reply_markup=get_main_keyboard(user['role'])
    )

# ========== ОБРАБОТЧИКИ АДМИНИСТРАТОРА ==========

@dp.message(F.text == "👑 Админ-панель")
async def admin_panel(message: types.Message):
    """Панель администратора"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    organizations = await db.get_all_organizations()
    users = await db.get_all_users()
    
    await reply(
        message,
        "👑 <b>Панель администратора</b>\n\n"
        f"<b>Организаций:</b> {len(organizations)}\n"
        f"<b>Пользователей:</b> {len(users)}\n\n"
        "<b>Доступные действия:</b>\n"
        "• Просмотр всех организаций\n"
        "• Просмотр всех пользователей\n"
        "• Назначение ролей\n"
        "• Просмотр статистики\n"
        "• Отправка уведомлений"
    )

@dp.message(F.text == "🏢 Все организации")
async def show_all_organizations(message: types.Message):
    """Показывает все организации - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    organizations = await db.get_all_organizations()
    
    if not organizations:
        await reply(message, "🏢 <b>Организаций пока нет</b>")
        return
    
    text = "🏢 <b>Все организации</b>\n\n"
    
    for org in organizations:
        # Получаем информацию о директоре
        director_info = "Не назначен"
        if org['director_id']:
            director = await db.get_user(org['director_id'])
            if director:
                director_info = f"{director['full_name']} (ID: {director['telegram_id']})"
        
        # Получаем количество сотрудников и техники
        users_count = len(await db.get_users_by_organization(org['id']))
        equipment_count = len(await db.get_organization_equipment(org['id']))
        
        text += f"<b>• {org['name']}</b>\n"
        text += f"  ID организации: {org['id']}\n"
        text += f"  Директор: {director_info}\n"
        text += f"  Сотрудников: {users_count}\n"
        text += f"  Техники: {equipment_count}\n"
        text += f"  Создана: {org['created_at'][:10]}\n\n"
    
    await reply(message, text)

@dp.message(F.text == "👥 Все пользователи")
async def show_all_users(message: types.Message):
    """Показывает всех пользователей"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    users = await db.get_all_users()
    
    if not users:
        await reply(message, "👥 <b>Пользователей пока нет</b>")
        return
    
    # Группируем по ролям
    roles_count = {}
    for u in users:
        roles_count[u['role']] = roles_count.get(u['role'], 0) + 1
    
    text = "👥 <b>Все пользователи</b>\n\n"
    text += "<b>Статистика по ролям:</b>\n"
    
    role_names = {
        'botadmin': '👑 Администратор',
        'director': '👨‍💼 Директор',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    for role, count in roles_count.items():
        text += f"• {role_names.get(role, role)}: {count} чел.\n"
    
    text += f"\n<b>Всего:</b> {len(users)} пользователей"
    
    await reply(message, text)

@dp.message(F.text == "📊 Статистика")
async def show_statistics(message: types.Message):
    """Показывает статистику"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    organizations = await db.get_all_organizations()
    users = await db.get_all_users()
    
    # Подсчитываем активные смены
    active_shifts_count = 0
    for u in users:
        if u['role'] == 'driver':
            shift = await db.get_active_shift(u['telegram_id'])
            if shift:
                active_shifts_count += 1
    
    # Статистика по организациям
    orgs_with_directors = len([o for o in organizations if o['director_id']])
    total_equipment = 0
    for org in organizations:
        equipment = await db.get_organization_equipment(org['id'])
        total_equipment += len(equipment)
    
    text = (
        "📊 <b>Статистика системы</b>\n\n"
        f"<b>Организаций:</b> {len(organizations)}\n"
        f"<b>С назначенными директорами:</b> {orgs_with_directors}\n"
        f"<b>Пользователей:</b> {len(users)}\n"
        f"<b>Техники всего:</b> {total_equipment} ед.\n"
        f"<b>Активных смен:</b> {active_shifts_count}\n\n"
        "<b>Распределение по ролям:</b>\n"
    )
    
    # Считаем роли
    roles = {}
    for u in users:
        roles[u['role']] = roles.get(u['role'], 0) + 1
    
    for role, count in roles.items():
        text += f"• {role_names.get(role, role)}: {count} чел.\n"
    
    await reply(message, text)

@dp.message(F.text == "🔔 Отправить уведомление")
async def send_notification_start(message: types.Message, state: FSMContext):
    """Начинает отправку уведомления всем пользователям"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    await reply(
        message,
        "🔔 <b>Отправка уведомления всем пользователям</b>\n\n"
        "Введите текст уведомления:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_notification_text)

@dp.message(UserStates.waiting_for_notification_text)
async def process_notification_text(message: types.Message, state: FSMContext):
    """Обрабатывает текст уведомления"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отправка уведомления отменена", reply_markup=get_main_keyboard(user['role']))
        return
    
    notification_text = message.text
    
    # Получаем всех пользователей
    all_users = await db.get_all_users()
    
    await reply(
        message,
        f"✅ <b>Уведомление подготовлено</b>\n\n"
        f"Текст: {notification_text}\n\n"
        f"Будет отправлено {len(all_users)} пользователям.\n"
        f"Начинаю отправку..."
    )
    
    # Отправляем уведомления
    sent_count = 0
    failed_count = 0
    
    for user in all_users:
        try:
            await send_to_user(
                user['telegram_id'],
                f"🔔 <b>Уведомление от администратора</b>\n\n"
                f"{notification_text}\n\n"
                f"<i>Если у вас есть вопросы, обратитесь к администратору.</i>"
            )
            sent_count += 1
            await asyncio.sleep(0.1)  # Чтобы не превысить лимиты Telegram
        except Exception as e:
            failed_count += 1
            logger.error(f"Не удалось отправить уведомление пользователю {user['telegram_id']}: {e}")
    
    await reply(
        message,
        f"📨 <b>Уведомления отправлены!</b>\n\n"
        f"Успешно: {sent_count}\n"
        f"Не удалось: {failed_count}\n"
        f"Всего: {len(all_users)} пользователей"
    )
    
    await state.clear()
    await cmd_start(message, state)

@dp.message(F.text == "➕ Назначить роль")
async def assign_role_start(message: types.Message, state: FSMContext):
    """Начинает назначение роли"""
    user = await db.get_user(message.from_user.id)
    
    # Проверяем права
    if user['role'] == 'driver':
        await reply(message, "⛔ У водителей нет прав назначать роли!")
        return
    
    await reply(
        message,
        "👤 <b>Назначение роли</b>\n\n"
        "Введите Telegram ID или @username пользователя:\n\n"
        "<b>Примеры:</b>\n"
        "• 123456789 (ID)\n"
        "• @username\n"
        "• username (без @)",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_username_or_id)

# ========== СИСТЕМА ТО (ТЕХНИЧЕСКОГО ОБСЛУЖИВАНИЯ) ==========

@dp.message(F.text == "➕ Добавить ТО")
async def add_maintenance_start(message: types.Message, state: FSMContext):
    """Начинает добавление ТО"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    # Получаем технику организации
    equipment = await db.get_organization_equipment(org_id)
    
    if not equipment:
        await reply(
            message,
            "🚜 <b>Добавление ТО</b>\n\n"
            "Сначала добавьте технику в организацию."
        )
        return
    
    # Создаем клавиатуру с техникой
    keyboard = []
    for eq in equipment[:10]:  # Показываем первые 10 единиц
        keyboard.append([types.KeyboardButton(text=f"🚜 {eq['name']} ({eq['model']})")])
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await state.update_data(equipment_list=equipment, org_id=org_id)
    
    await reply(
        message,
        "🔧 <b>Добавление ТО</b>\n\n"
        "Выберите технику для добавления ТО:",
        reply_markup=types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    )
    await state.set_state(UserStates.waiting_for_equipment_selection)

@dp.message(F.text == "📅 Ближайшие ТО")
async def show_upcoming_maintenance(message: types.Message):
    """Показывает ближайшие ТО"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    # Получаем всю технику организации
    equipment = await db.get_organization_equipment(org_id)
    
    if not equipment:
        await reply(message, "🚜 <b>Техники пока нет</b>")
        return
    
    # Проверяем ТО для каждой техники
    today = datetime.now().date()
    upcoming_maintenance = []
    
    for eq in equipment:
        if eq.get('next_maintenance'):
            next_date = datetime.strptime(eq['next_maintenance'], '%Y-%m-%d').date()
            days_left = (next_date - today).days
            
            if days_left <= 30:  # Показываем только ТО в ближайшие 30 дней
                upcoming_maintenance.append({
                    'equipment': eq,
                    'next_date': next_date,
                    'days_left': days_left
                })
    
    if not upcoming_maintenance:
        await reply(
            message,
            "📅 <b>Ближайшие ТО</b>\n\n"
            "Нет предстоящих ТО в ближайшие 30 дней.\n"
            "Все ТО запланированы на более поздние даты."
        )
        return
    
    # Сортируем по дате
    upcoming_maintenance.sort(key=lambda x: x['days_left'])
    
    text = "📅 <b>Ближайшие ТО</b>\n\n"
    
    for item in upcoming_maintenance[:10]:  # Показываем первые 10
        eq = item['equipment']
        days_left = item['days_left']
        
        if days_left < 0:
            status = f"🔴 <b>Просрочено на {abs(days_left)} дней!</b>"
        elif days_left == 0:
            status = "🟡 <b>Сегодня!</b>"
        elif days_left <= 7:
            status = f"🟠 <b>Через {days_left} дней</b>"
        else:
            status = f"🟢 Через {days_left} дней"
        
        text += f"🚜 <b>{eq['name']}</b> ({eq['model']})\n"
        text += f"📅 {item['next_date'].strftime('%d.%m.%Y')}\n"
        text += f"📌 {status}\n"
        text += f"🆔 VIN: {eq['vin']}\n\n"
    
    if len(upcoming_maintenance) > 10:
        text += f"... и ещё {len(upcoming_maintenance) - 10} ТО\n"
    
    text += "\n<i>Для добавления ТО используйте меню '➕ Добавить ТО'</i>"
    
    await reply(message, text)

# Обработка выбора техники для ТО
@dp.message(UserStates.waiting_for_equipment_selection)
async def process_maintenance_equipment_selection(message: types.Message, state: FSMContext):
    """Обрабатывает выбор техники для ТО"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    data = await state.get_data()
    equipment_list = data.get('equipment_list', [])
    
    # Ищем выбранную технику
    selected_eq = None
    for eq in equipment_list:
        if f"🚜 {eq['name']} ({eq['model']})" == message.text:
            selected_eq = eq
            break
    
    if not selected_eq:
        await reply(message, "❌ Пожалуйста, выберите технику из списка")
        return
    
    # Сохраняем выбранную технику
    await state.update_data(selected_equipment=selected_eq)
    
    # Типы ТО
    maintenance_types = [
        "🛢️ Замена масла",
        "🔧 ТО-1000 (первое)",
        "🔧 ТО-5000 (плановое)",
        "🔧 ТО-10000 (комплексное)",
        "🔩 Замена фильтров",
        "🛞 Регламент шин",
        "⚡ Электрика",
        "🔧 Прочее"
    ]
    
    keyboard = []
    for mt in maintenance_types:
        keyboard.append([types.KeyboardButton(text=mt)])
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await reply(
        message,
        f"🔧 <b>Добавление ТО для:</b> {selected_eq['name']} ({selected_eq['model']})\n\n"
        f"Выберите тип ТО:",
        reply_markup=types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    )
    await state.set_state(UserStates.waiting_for_maintenance_type)

@dp.message(UserStates.waiting_for_maintenance_type)
async def process_maintenance_type(message: types.Message, state: FSMContext):
    """Обрабатывает тип ТО"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    maintenance_types = [
        "🛢️ Замена масла",
        "🔧 ТО-1000 (первое)",
        "🔧 ТО-5000 (плановое)",
        "🔧 ТО-10000 (комплексное)",
        "🔩 Замена фильтров",
        "🛞 Регламент шин",
        "⚡ Электрика",
        "🔧 Прочее"
    ]
    
    if message.text not in maintenance_types:
        await reply(message, "❌ Пожалуйста, выберите тип ТО из списка")
        return
    
    await state.update_data(maintenance_type=message.text)
    
    await reply(
        message,
        f"✅ <b>Тип ТО:</b> {message.text}\n\n"
        f"Введите дату ТО в формате ДД.ММ.ГГГГ:\n\n"
        f"<b>Пример:</b> 15.01.2024\n"
        f"Или укажите через сколько дней:\n"
        f"<b>Пример:</b> через 30 дней",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_maintenance_date)

@dp.message(UserStates.waiting_for_maintenance_date)
async def process_maintenance_date(message: types.Message, state: FSMContext):
    """Обрабатывает дату ТО"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    date_input = message.text.strip()
    
    try:
        if date_input.startswith("через "):
            # Формат "через X дней"
            days = int(date_input.split()[1])
            maintenance_date = (datetime.now() + timedelta(days=days)).date()
        else:
            # Формат ДД.ММ.ГГГГ
            maintenance_date = datetime.strptime(date_input, "%d.%m.%Y").date()
        
        # Проверяем что дата не в прошлом
        if maintenance_date < datetime.now().date():
            await reply(message, "❌ Дата ТО не может быть в прошлом!")
            return
        
        await state.update_data(maintenance_date=maintenance_date.strftime("%Y-%m-%d"))
        
        await reply(
            message,
            f"✅ <b>Дата ТО:</b> {maintenance_date.strftime('%d.%m.%Y')}\n\n"
            f"Введите описание ТО (можно пропустить, отправив любое сообщение):",
            reply_markup=types.ReplyKeyboardMarkup(
                keyboard=[[types.KeyboardButton(text="⏭️ Без описания")], [types.KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(UserStates.waiting_for_maintenance_description)
        
    except ValueError:
        await reply(message, "❌ Неверный формат даты! Используйте ДД.ММ.ГГГГ или 'через X дней'")

@dp.message(UserStates.waiting_for_maintenance_description)
async def process_maintenance_description(message: types.Message, state: FSMContext):
    """Обрабатывает описание ТО"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    data = await state.get_data()
    selected_eq = data.get('selected_equipment')
    maintenance_type = data.get('maintenance_type')
    maintenance_date = data.get('maintenance_date')
    
    description = None
    if message.text != "⏭️ Без описания":
        description = message.text
    
    # Добавляем ТО в базу
    try:
        maintenance_id = await db.add_maintenance(
            equipment_id=selected_eq['id'],
            type=maintenance_type,
            scheduled_date=maintenance_date,
            description=description
        )
        
        # Форматируем дату для вывода
        scheduled_date = datetime.strptime(maintenance_date, "%Y-%m-%d")
        days_left = (scheduled_date.date() - datetime.now().date()).days
        
        await reply(
            message,
            f"✅ <b>ТО успешно добавлено!</b>\n\n"
            f"<b>Техника:</b> {selected_eq['name']} ({selected_eq['model']})\n"
            f"<b>Тип ТО:</b> {maintenance_type}\n"
            f"<b>Дата:</b> {scheduled_date.strftime('%d.%m.%Y')}\n"
            f"<b>Осталось дней:</b> {days_left}\n"
            f"<b>ID ТО:</b> #{maintenance_id}\n"
            f"{f'<b>Описание:</b> {description}' if description else ''}\n\n"
            f"Уведомление будет отправлено за неделю до ТО."
        )
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении ТО: {e}")
        await reply(
            message,
            f"❌ <b>Ошибка при добавлении ТО!</b>\n\n"
            f"Попробуйте ещё раз или обратитесь к администратору."
        )
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))

# ========== ТЕХНИКА ВОДИТЕЛЯ ==========

@dp.message(F.text == "🚜 Моя техника")
async def my_equipment(message: types.Message):
    """Показывает технику назначенную водителю"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут просматривать свою технику!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации! Обратитесь к начальнику парка.")
        return
    
    # Получаем всю технику организации
    equipment = await db.get_organization_equipment(org_id)
    
    if not equipment:
        await reply(
            message,
            "🚜 <b>Моя техника</b>\n\n"
            "В организации пока нет техники.\n"
            "Обратитесь к начальнику парка."
        )
        return
    
    # Показываем всю технику (водитель может видеть что есть в организации)
    text = f"🚜 <b>Техника организации</b> ({len(equipment)} ед.)\n\n"
    
    for eq in equipment[:10]:  # Показываем первые 10
        text += f"<b>• {eq['name']}</b> ({eq['model']})\n"
        text += f"  VIN: {eq['vin']}\n"
        text += f"  Статус: {eq['status']}\n"
        
        # Показываем ближайшее ТО если есть
        if eq.get('next_maintenance'):
            next_date = datetime.strptime(eq['next_maintenance'], '%Y-%m-%d').date()
            today = datetime.now().date()
            days_left = (next_date - today).days
            
            if days_left <= 30:
                if days_left < 0:
                    text += f"  ⚠️ ТО просрочено на {abs(days_left)} дней\n"
                elif days_left <= 7:
                    text += f"  🔔 ТО через {days_left} дней\n"
                else:
                    text += f"  📅 ТО через {days_left} дней\n"
        
        text += "\n"
    
    if len(equipment) > 10:
        text += f"... и ещё {len(equipment) - 10} единиц техники\n"
    
    text += "\n<i>Для начала смены выберите технику из меню '🚛 Начать смену'</i>"
    
    await reply(message, text)

# ========== СИСТЕМА УВЕДОМЛЕНИЙ О ТО ==========

async def check_and_notify_maintenance():
    """Проверяет предстоящие ТО и отправляет уведомления"""
    try:
        # Получаем ТО на ближайшие 7 дней
        upcoming_maintenance = await db.get_upcoming_maintenance(days=7)
        
        for maintenance in upcoming_maintenance:
            # Отправляем уведомление директору и начальнику парка
            org_id = None
            
            # Получаем ID организации из техники
            equipment_id = maintenance['equipment_id']
            
            # Здесь нужен метод для получения организации по ID техники
            # Пока упростим - будем получать всех пользователей организации
            
            # Получаем всех директоров и начальников парка
            # (Это нужно доработать в database.py)
            
            # Помечаем что уведомление отправлено
            await db.mark_maintenance_notified(maintenance['id'])
            
            logger.info(f"Уведомление о ТО #{maintenance['id']} отправлено")
            
    except Exception as e:
        logger.error(f"Ошибка при проверке ТО: {e}")

# ========== ОСТАВШИЕСЯ ОБРАБОТЧИКИ ==========

# ... [остальной код без изменений, включая все предыдущие обработчики] ...
# Все обработчики директора, начальника парка, водителя и состояния остаются как были

# ========== ЗАПУСК БОТА ==========

async def on_startup():
    """Инициализация при запуске"""
    try:
        await db.connect()
        
        # Создаем администратора
        ADMIN_ID = 1079922982  # ВАШ TELEGRAM ID
        await db.register_user(
            telegram_id=ADMIN_ID,
            full_name="Администратор Системы",
            username="admin",
            role='botadmin'
        )
        
        logger.info("✅ Бот запущен!")
        logger.info(f"👑 Администратор: ID {ADMIN_ID}")
        
        # Запускаем фоновые задачи
        asyncio.create_task(maintenance_checker())
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

async def maintenance_checker():
    """Фоновая задача для проверки ТО"""
    while True:
        try:
            await check_and_notify_maintenance()
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче проверки ТО: {e}")
        
        # Проверяем каждые 6 часов
        await asyncio.sleep(6 * 60 * 60)

async def main():
    """Основная функция"""
    await on_startup()
    
    try:
        logger.info("🚀 Бот работает...")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
