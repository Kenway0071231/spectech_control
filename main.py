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
from datetime import datetime
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
            [types.KeyboardButton(text="➕ Назначить роль")]
        ],
        
        'director': [
            [types.KeyboardButton(text="👨‍💼 Моя организация")],
            [types.KeyboardButton(text="🚜 Автопарк")],
            [types.KeyboardButton(text="👥 Сотрудники")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить роль")],
            [types.KeyboardButton(text="📊 Отчеты")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")]
        ],
        
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Управление парком")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")]
        ],
        
        'driver': [
            [types.KeyboardButton(text="🚛 Начать смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="✅ Закончить смену")],
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

# ========== ОБРАБОТЧИКИ АДМИНИСТРАТОРА (ВЫШЕ ВСЕХ!) ==========

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
        "• Просмотр статистики"
    )

@dp.message(F.text == "🏢 Все организации")
async def show_all_organizations(message: types.Message):
    """Показывает все организации"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    organizations = await db.get_all_organizations()
    
    if not organizations:
        await reply(message, "🏢 <b>Организаций пока нет</b>\n\nСоздайте первую организацию с помощью команды /createorg")
        return
    
    text = "🏢 <b>Все организации</b>\n\n"
    
    for org in organizations:
        text += f"<b>• {org['name']}</b>\n"
        text += f"  ID: {org['id']}\n"
        text += f"  Директор ID: {org['director_id']}\n"
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
    
    # Статистика
    orgs_with_directors = len([o for o in organizations if o['director_id']])
    
    text = (
        "📊 <b>Статистика системы</b>\n\n"
        f"<b>Организаций:</b> {len(organizations)}\n"
        f"<b>С назначенными директорами:</b> {orgs_with_directors}\n"
        f"<b>Пользователей:</b> {len(users)}\n\n"
        "<b>Распределение по ролям:</b>\n"
    )
    
    # Считаем роли
    roles = {}
    for u in users:
        roles[u['role']] = roles.get(u['role'], 0) + 1
    
    for role, count in roles.items():
        text += f"• {role}: {count} чел.\n"
    
    await reply(message, text)

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

# ========== ОБРАБОТЧИКИ ДИРЕКТОРА ==========

@dp.message(F.text == "👨‍💼 Моя организация")
async def director_org(message: types.Message):
    """Организация директора"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'director':
        await reply(message, "⛔ Доступ только для директора!")
        return
    
    org_id = user.get('organization_id')
    
    if not org_id:
        await reply(
            message,
            "🏢 <b>Создание организации</b>\n\n"
            "У вас ещё нет организации.\n"
            "Создайте её командой:\n"
            "<code>/createorg Название компании</code>\n\n"
            "<b>Пример:</b>\n"
            "<code>/createorg ООО 'Моя компания'</code>"
        )
        return
    
    org = await db.get_organization(org_id)
    users = await db.get_users_by_organization(org_id)
    equipment = await db.get_organization_equipment(org_id)
    
    text = (
        f"🏢 <b>Организация: {org['name']}</b>\n\n"
        f"<b>ID организации:</b> {org_id}\n"
        f"<b>Директор:</b> {user['full_name']}\n"
        f"<b>Создана:</b> {org['created_at'][:10]}\n\n"
        f"<b>Сотрудники:</b> {len(users)} чел.\n"
        f"<b>Техника:</b> {len(equipment)} ед.\n\n"
        "<b>Доступные действия:</b>\n"
        "• Просмотр автопарка\n"
        "• Просмотр сотрудников\n"
        "• Добавление техники\n"
        "• Назначение ролей"
    )
    
    await reply(message, text)

@dp.message(F.text == "🚜 Автопарк")
async def show_equipment(message: types.Message):
    """Показывает технику организации"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    equipment = await db.get_organization_equipment(org_id)
    
    if not equipment:
        await reply(
            message,
            "🚜 <b>Автопарк</b>\n\n"
            "Техники пока нет.\n"
            "Добавьте технику через меню '➕ Добавить технику'."
        )
        return
    
    text = f"🚜 <b>Автопарк ({len(equipment)} ед.)</b>\n\n"
    
    for eq in equipment[:10]:  # Показываем первые 10 единиц
        text += f"<b>• {eq['name']}</b>\n"
        text += f"  Модель: {eq['model']}\n"
        text += f"  VIN: {eq['vin']}\n"
        text += f"  Статус: {eq['status']}\n\n"
    
    if len(equipment) > 10:
        text += f"... и ещё {len(equipment) - 10} единиц"
    
    await reply(message, text)

@dp.message(F.text == "👥 Сотрудники")
async def show_employees(message: types.Message):
    """Показывает сотрудников организации"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    users = await db.get_users_by_organization(org_id)
    
    if not users:
        await reply(
            message,
            "👥 <b>Сотрудники</b>\n\n"
            "Сотрудников пока нет.\n"
            "Назначьте сотрудников через меню '➕ Назначить роль'."
        )
        return
    
    role_names = {
        'director': '👨‍💼 Директор',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    text = f"👥 <b>Сотрудники ({len(users)} чел.)</b>\n\n"
    
    for u in users:
        text += f"{role_names.get(u['role'], '👤')} <b>{u['full_name']}</b>\n"
        if u['username']:
            text += f"@{u['username']} | "
        text += f"ID: {u['telegram_id']}\n\n"
    
    await reply(message, text)

@dp.message(F.text == "➕ Добавить технику")
async def add_equipment_start(message: types.Message, state: FSMContext):
    """Начинает добавление техники"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    await state.update_data(org_id=org_id)
    
    await reply(
        message,
        "🚜 <b>Добавление техники</b>\n\n"
        "Введите название техники:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_equipment_name)

@dp.message(F.text == "📊 Отчеты")
async def show_reports(message: types.Message):
    """Показывает отчеты для директора"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'director':
        await reply(message, "⛔ Доступ только для директора!")
        return
    
    await reply(
        message,
        "📊 <b>Отчеты</b>\n\n"
        "Эта функция в разработке.\n"
        "Скоро здесь будут доступны:\n"
        "• Ежедневные отчеты\n"
        "• Финансовые отчеты\n"
        "• Отчеты по технике\n"
        "• Статистика работы водителей"
    )

@dp.message(F.text == "🔍 Проверить осмотры")
async def check_inspections(message: types.Message):
    """Показывает смены ожидающие проверки"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    pending_shifts = await db.get_pending_inspections(org_id)
    
    if not pending_shifts:
        await reply(
            message,
            "🔍 <b>Проверка осмотров</b>\n\n"
            "Нет смен ожидающих проверки.\n"
            "Все осмотры подтверждены! ✅"
        )
        return
    
    text = f"🔍 <b>Смены ожидающие проверки</b> ({len(pending_shifts)})\n\n"
    
    for shift in pending_shifts[:5]:  # Показываем первые 5
        text += f"🆔 <b>Смена #{shift['id']}</b>\n"
        text += f"🚛 <b>Водитель:</b> {shift['driver_name']}\n"
        text += f"🚜 <b>Техника:</b> {shift['equipment_name']}\n"
        text += f"🕐 <b>Начало:</b> {shift['start_time'][:16]}\n\n"
        
        # Добавляем inline кнопки для проверки
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Подтвердить",
                        callback_data=f"approve_inspection:{shift['id']}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отклонить", 
                        callback_data=f"reject_inspection:{shift['id']}"
                    )
                ]
            ]
        )
        
        await message.answer(text, reply_markup=keyboard)
        text = ""
    
    if len(pending_shifts) > 5:
        await reply(message, f"... и ещё {len(pending_shifts) - 5} смен")

# ========== ОБРАБОТЧИКИ НАЧАЛЬНИКА ПАРКА ==========

@dp.message(F.text == "👷 Управление парком")
async def fleetmanager_panel(message: types.Message):
    """Панель начальника парка"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'fleetmanager':
        await reply(message, "⛔ Доступ только для начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации! Обратитесь к директору.")
        return
    
    org = await db.get_organization(org_id)
    users = await db.get_users_by_organization(org_id)
    equipment = await db.get_organization_equipment(org_id)
    
    drivers = len([u for u in users if u['role'] == 'driver'])
    
    await reply(
        message,
        f"👷 <b>Управление парком</b>\n\n"
        f"<b>Организация:</b> {org['name']}\n"
        f"<b>Водителей:</b> {drivers}\n"
        f"<b>Техники:</b> {len(equipment)} ед.\n\n"
        "<b>Доступные действия:</b>\n"
        "• Просмотр техники\n"
        "• Добавление техники\n"
        "• Просмотр водителей\n"
        "• Назначение водителей"
    )

@dp.message(F.text == "🚜 Техника")
async def show_equipment_fleetmanager(message: types.Message):
    """Показывает технику для начальника парка"""
    await show_equipment(message)  # Используем тот же обработчик

@dp.message(F.text == "👥 Водители")
async def show_drivers(message: types.Message):
    """Показывает водителей для начальника парка"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'fleetmanager':
        await reply(message, "⛔ Доступ только для начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    users = await db.get_users_by_organization(org_id)
    drivers = [u for u in users if u['role'] == 'driver']
    
    if not drivers:
        await reply(
            message,
            "👥 <b>Водители</b>\n\n"
            "Водителей пока нет.\n"
            "Назначьте водителей через меню '➕ Назначить водителя'."
        )
        return
    
    text = f"👥 <b>Водители ({len(drivers)} чел.)</b>\n\n"
    
    for d in drivers:
        text += f"🚛 <b>{d['full_name']}</b>\n"
        if d['username']:
            text += f"@{d['username']} | "
        text += f"ID: {d['telegram_id']}\n\n"
    
    await reply(message, text)

@dp.message(F.text == "➕ Назначить водителя")
async def assign_driver_start(message: types.Message, state: FSMContext):
    """Начинает назначение водителя"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'fleetmanager':
        await reply(message, "⛔ Доступ только для начальника парка!")
        return
    
    await reply(
        message,
        "👤 <b>Назначение водителя</b>\n\n"
        "Введите Telegram ID или @username пользователя:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_username_or_id)

# ========== СИСТЕМА СМЕН ВОДИТЕЛЯ ==========

@dp.message(F.text == "🚛 Начать смену")
async def start_shift_begin(message: types.Message, state: FSMContext):
    """Начинает процесс начала смены"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут начинать смены!")
        return
    
    # Проверяем активную смену
    active_shift = await db.get_active_shift(message.from_user.id)
    if active_shift:
        await reply(
            message,
            f"⚠️ <b>У вас уже есть активная смена!</b>\n\n"
            f"Смена начата: {active_shift['start_time'][:16]}\n"
            f"Техника: {active_shift.get('equipment_name', 'Не указана')}\n\n"
            f"Завершите текущую смену перед началом новой."
        )
        return
    
    # Получаем доступную технику
    equipment = await db.get_equipment_by_driver(message.from_user.id)
    
    if not equipment:
        await reply(
            message,
            "🚛 <b>Начало смены</b>\n\n"
            "❌ Нет доступной техники!\n\n"
            "Обратитесь к начальнику парка для назначения техники."
        )
        return
    
    # Сохраняем список техники в состоянии
    await state.update_data(equipment_list=equipment)
    
    # Создаем клавиатуру с техникой
    keyboard = []
    for eq in equipment:
        keyboard.append([types.KeyboardButton(text=f"🚜 {eq['name']} ({eq['model']})")])
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await reply(
        message,
        "🚛 <b>Начало смены</b>\n\n"
        "Выберите технику для работы:",
        reply_markup=types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    )
    await state.set_state(UserStates.waiting_for_equipment_selection)

@dp.message(UserStates.waiting_for_equipment_selection)
async def process_equipment_selection(message: types.Message, state: FSMContext):
    """Обрабатывает выбор техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Начало смены отменено", reply_markup=get_main_keyboard(user['role']))
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
    
    await reply(
        message,
        f"✅ <b>Выбрана техника:</b> {selected_eq['name']} ({selected_eq['model']})\n\n"
        f"📋 <b>Технический инструктаж</b>\n\n"
        f"Перед началом смены необходимо подтвердить:\n\n"
        f"1. ✅ Знание правил техники безопасности\n"
        f"2. ✅ Проверку состояния техники\n"
        f"3. ✅ Наличие необходимых документов\n"
        f"4. ✅ Знание маршрута (если требуется)\n\n"
        f"<b>Подтверждаете прохождение инструктажа?</b>",
        reply_markup=get_yes_no_keyboard()
    )
    await state.set_state(UserStates.waiting_for_briefing_confirmation)

@dp.message(UserStates.waiting_for_briefing_confirmation)
async def process_briefing_confirmation(message: types.Message, state: FSMContext):
    """Обрабатывает подтверждение инструктажа"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Начало смены отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    if message.text == "❌ Нет":
        await reply(
            message,
            "❌ <b>Инструктаж не подтверждён</b>\n\n"
            "Для начала смены необходимо подтвердить прохождение инструктажа.\n"
            "Обратитесь к начальнику парка."
        )
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
        return
    
    if message.text == "✅ Да":
        data = await state.get_data()
        selected_eq = data.get('selected_equipment')
        
        # Создаем смену
        shift_id = await db.start_shift(
            driver_id=message.from_user.id,
            equipment_id=selected_eq['id'],
            briefing_confirmed=True
        )
        
        await state.update_data(shift_id=shift_id)
        
        await reply(
            message,
            f"✅ <b>Инструктаж подтверждён!</b>\n\n"
            f"🚛 <b>Смена #{shift_id} начата</b>\n"
            f"<b>Техника:</b> {selected_eq['name']} ({selected_eq['model']})\n"
            f"<b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"
            f"📸 <b>Следующий шаг:</b>\n"
            f"Сделайте фото осмотра техники перед началом работы.\n\n"
            f"<b>Что должно быть на фото:</b>\n"
            f"• Общий вид техники\n"
            f"• Состояние шин\n"
            f"• Уровни жидкостей (если видно)\n"
            f"• Салон и приборная панель",
            reply_markup=types.ReplyKeyboardMarkup(
                keyboard=[[types.KeyboardButton(text="📸 Сделать фото")], [types.KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(UserStates.waiting_for_inspection_photo)
        
        # Отправляем уведомление начальнику парка
        await notify_manager_about_shift_start(message.from_user.id, selected_eq['id'], shift_id)
    else:
        await reply(message, "❌ Пожалуйста, выберите 'Да' или 'Нет'")

async def notify_manager_about_shift_start(driver_id, equipment_id, shift_id):
    """Уведомляет начальника парка о начале смены"""
    try:
        # Получаем информацию о водителе
        driver = await db.get_user(driver_id)
        if not driver or not driver.get('organization_id'):
            return
        
        # Получаем начальников парка в организации
        users = await db.get_users_by_organization(driver['organization_id'])
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        # Получаем информацию о технике
        equipment = None
        all_equipment = await db.get_organization_equipment(driver['organization_id'])
        for eq in all_equipment:
            if eq['id'] == equipment_id:
                equipment = eq
                break
        
        if not equipment:
            return
        
        for manager in fleet_managers:
            try:
                await send_to_user(
                    manager['telegram_id'],
                    f"👷 <b>Новая смена начата</b>\n\n"
                    f"🚛 <b>Водитель:</b> {driver['full_name']}\n"
                    f"📞 <b>Контакт:</b> @{driver['username'] if driver.get('username') else 'нет'}\n"
                    f"🚜 <b>Техника:</b> {equipment['name']} ({equipment['model']})\n"
                    f"🆔 <b>ID смены:</b> #{shift_id}\n"
                    f"🕐 <b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"
                    f"Ожидается фото осмотра техники."
                )
            except:
                continue
                
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления: {e}")

@dp.message(UserStates.waiting_for_inspection_photo)
async def process_inspection_photo(message: types.Message, state: FSMContext):
    """Обрабатывает фото осмотра"""
    if message.text == "❌ Отмена":
        await cancel_shift(message, state)
        return
    
    if message.text == "📸 Сделать фото":
        await reply(
            message,
            "📸 <b>Сделайте фото осмотра техники</b>\n\n"
            "Пожалуйста, сделайте фото и отправьте его в этот чат.\n"
            "Фото должно чётко показывать состояние техники.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await reply(message, "❌ Пожалуйста, отправьте фото или нажмите 'Сделать фото'")

@dp.message(F.photo, UserStates.waiting_for_inspection_photo)
async def handle_inspection_photo(message: types.Message, state: FSMContext):
    """Обрабатывает отправленное фото"""
    data = await state.get_data()
    shift_id = data.get('shift_id')
    
    if not shift_id:
        await reply(message, "❌ Ошибка: смена не найдена")
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
        return
    
    # Сохраняем file_id фото
    photo_file_id = message.photo[-1].file_id
    await db.update_shift_photo(shift_id, photo_file_id)
    
    # Отправляем подтверждение
    await reply(
        message,
        "✅ <b>Фото осмотра принято!</b>\n\n"
        "Фото отправлено начальнику парка для проверки.\n"
        "Вы можете приступить к работе.\n\n"
        "⚠️ <b>Важно:</b> Не забудьте провести ежедневные проверки техники."
    )
    
    # Уведомляем начальника парка о новом фото
    await notify_manager_about_new_photo(shift_id, message.from_user.id, photo_file_id)
    
    # Предлагаем пройти ежедневные проверки
    await reply(
        message,
        "🔄 <b>Ежедневные проверки техники</b>\n\n"
        "Рекомендуется проверить:\n"
        "• Уровень масла в двигателе\n"
        "• Уровень охлаждающей жидкости\n"
        "• Давление в шинах\n"
        "• Работу фар и стоп-сигналов\n"
        "• Исправность тормозов\n\n"
        "Хотите отметить выполненные проверки?",
        reply_markup=get_yes_no_keyboard()
    )
    await state.set_state(UserStates.waiting_for_daily_checks)

async def notify_manager_about_new_photo(shift_id, driver_id, photo_file_id):
    """Уведомляет начальника парка о новом фото"""
    try:
        # Получаем информацию о смене
        shift = await db.get_active_shift(driver_id)
        if not shift:
            return
        
        driver = await db.get_user(driver_id)
        if not driver or not driver.get('organization_id'):
            return
        
        # Получаем начальников парка
        users = await db.get_users_by_organization(driver['organization_id'])
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        for manager in fleet_managers:
            try:
                # Отправляем фото с подписью
                await bot.send_photo(
                    chat_id=manager['telegram_id'],
                    photo=photo_file_id,
                    caption=f"👷 <b>Новое фото осмотра</b>\n\n"
                           f"🚛 <b>Водитель:</b> {driver['full_name']}\n"
                           f"🚜 <b>Техника:</b> {shift.get('equipment_name', 'Неизвестно')}\n"
                           f"🆔 <b>ID смены:</b> #{shift_id}\n\n"
                           f"Для подтверждения осмотра используйте команду /check_inspections",
                )
            except:
                continue
                
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления о фото: {e}")

@dp.message(UserStates.waiting_for_daily_checks)
async def process_daily_checks_decision(message: types.Message, state: FSMContext):
    """Обрабатывает решение о ежедневных проверках"""
    if message.text == "❌ Отмена":
        await cancel_shift(message, state)
        return
    
    if message.text == "❌ Нет":
        await reply(
            message,
            "ℹ️ <b>Ежедневные проверки пропущены</b>\n\n"
            "Не забудьте проверить технику перед началом работы.\n"
            "Вы можете завершить смену в любое время через меню."
        )
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
        return
    
    if message.text == "✅ Да":
        # Получаем список проверок
        checks = await db.get_daily_checks()
        if not checks:
            await reply(message, "❌ Список проверок не найден")
            await state.clear()
            user = await db.get_user(message.from_user.id)
            await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
            return
        
        # Сохраняем проверки в состоянии
        await state.update_data(daily_checks=checks, current_check_index=0)
        
        # Начинаем первую проверку
        await send_next_check(message, state)
    else:
        await reply(message, "❌ Пожалуйста, выберите 'Да' или 'Нет'")

async def send_next_check(message: types.Message, state: FSMContext):
    """Отправляет следующую проверку"""
    data = await state.get_data()
    checks = data.get('daily_checks', [])
    current_index = data.get('current_check_index', 0)
    
    if current_index >= len(checks):
        # Все проверки пройдены
        await reply(
            message,
            "✅ <b>Все ежедневные проверки завершены!</b>\n\n"
            "Техника готова к работе.\n"
            "Не забудьте завершить смену после окончания работы.",
            reply_markup=get_main_keyboard('driver')
        )
        await state.clear()
        return
    
    current_check = checks[current_index]
    
    await reply(
        message,
        f"🔍 <b>Проверка {current_index + 1} из {len(checks)}</b>\n\n"
        f"<b>Тип:</b> {current_check['type']}\n"
        f"<b>Элемент:</b> {current_check['item']}\n"
        f"<b>Что проверить:</b> {current_check['check']}\n\n"
        f"<b>Статус элемента:</b>",
        reply_markup=get_check_status_keyboard()
    )

@dp.message(UserStates.waiting_for_daily_checks)
async def process_check_status(message: types.Message, state: FSMContext):
    """Обрабатывает статус проверки"""
    if message.text == "❌ Отмена":
        await cancel_shift(message, state)
        return
    
    if message.text == "⏭️ Пропустить":
        # Пропускаем эту проверку
        data = await state.get_data()
        current_index = data.get('current_check_index', 0)
        await state.update_data(current_check_index=current_index + 1)
        await send_next_check(message, state)
        return
    
    valid_statuses = ["✅ Исправно", "⚠️ Требует внимания", "❌ Неисправно"]
    if message.text not in valid_statuses:
        await reply(message, "❌ Пожалуйста, выберите статус из списка")
        return
    
    data = await state.get_data()
    shift_id = data.get('shift_id')
    checks = data.get('daily_checks', [])
    current_index = data.get('current_check_index', 0)
    
    if current_index < len(checks):
        current_check = checks[current_index]
        
        # Сохраняем проверку в БД
        await db.add_daily_check(
            shift_id=shift_id,
            check_type=current_check['type'],
            item_name=current_check['item'],
            status=message.text,
            notes=None
        )
    
    # Переходим к следующей проверке
    await state.update_data(current_check_index=current_index + 1)
    await send_next_check(message, state)

@dp.message(F.text == "✅ Закончить смену")
async def end_shift_start(message: types.Message, state: FSMContext):
    """Начинает процесс завершения смену"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут завершать смены!")
        return
    
    # Проверяем активную смену
    active_shift = await db.get_active_shift(message.from_user.id)
    if not active_shift:
        await reply(message, "❌ У вас нет активной смены!")
        return
    
    await state.update_data(shift_id=active_shift['id'])
    
    await reply(
        message,
        f"🛑 <b>Завершение смены #{active_shift['id']}</b>\n\n"
        f"<b>Техника:</b> {active_shift.get('equipment_name', 'Неизвестно')}\n"
        f"<b>Начало:</b> {active_shift['start_time'][:16]}\n\n"
        f"Пожалуйста, добавьте заметки о работе за смену:\n"
        f"(можно пропустить, отправив любое сообщение)",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="⏭️ Без заметок")], [types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(UserStates.waiting_for_shift_notes)

@dp.message(UserStates.waiting_for_shift_notes)
async def process_shift_notes(message: types.Message, state: FSMContext):
    """Обрабатывает заметки о смене"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Завершение смены отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    data = await state.get_data()
    shift_id = data.get('shift_id')
    
    notes = None
    if message.text != "⏭️ Без заметок":
        notes = message.text
    
    # Завершаем смену
    success = await db.complete_shift(shift_id, notes)
    
    if success:
        await reply(
            message,
            "✅ <b>Смена завершена успешно!</b>\n\n"
            "Спасибо за работу!\n"
            "Не забудьте сдать технику и заполнить документацию."
        )
        
        # Уведомляем начальника парка
        await notify_manager_about_shift_end(shift_id, message.from_user.id)
    else:
        await reply(message, "❌ Ошибка при завершении смены")
    
    await state.clear()
    await cmd_start(message, state)

async def notify_manager_about_shift_end(shift_id, driver_id):
    """Уведомляет начальника парка о завершении смены"""
    try:
        # Получаем информацию о смене
        driver = await db.get_user(driver_id)
        if not driver or not driver.get('organization_id'):
            return
        
        # Получаем начальников парка
        users = await db.get_users_by_organization(driver['organization_id'])
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        for manager in fleet_managers:
            try:
                await send_to_user(
                    manager['telegram_id'],
                    f"👷 <b>Смена завершена</b>\n\n"
                    f"🚛 <b>Водитель:</b> {driver['full_name']}\n"
                    f"🆔 <b>ID смены:</b> #{shift_id}\n"
                    f"🕐 <b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"
                    f"Техника сдана, смена закрыта."
                )
            except:
                continue
                
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления о завершении смены: {e}")

async def cancel_shift(message: types.Message, state: FSMContext):
    """Отменяет начатую смену"""
    data = await state.get_data()
    shift_id = data.get('shift_id')
    
    if shift_id:
        # Можно отметить смену как отменённую
        await db.complete_shift(shift_id, "Отменена пользователем")
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "❌ Смена отменена", reply_markup=get_main_keyboard(user['role']))

@dp.message(F.text == "📋 Мои смены")
async def my_shifts_history(message: types.Message):
    """Показывает историю смен водителя"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут просматривать свои смены!")
        return
    
    shifts = await db.get_shifts_by_driver(message.from_user.id, limit=5)
    
    if not shifts:
        await reply(
            message,
            "📋 <b>Мои смены</b>\n\n"
            "У вас ещё не было смен.\n"
            "Начните первую смену через меню '🚛 Начать смену'."
        )
        return
    
    text = f"📋 <b>Последние смены</b> ({len(shifts)})\n\n"
    
    for shift in shifts:
        status_emoji = "🟢" if shift['status'] == 'active' else "✅" if shift['status'] == 'completed' else "❌"
        text += f"{status_emoji} <b>Смена #{shift['id']}</b>\n"
        text += f"🚜 {shift.get('equipment_name', 'Техника')}\n"
        text += f"📅 {shift['start_time'][:16]}\n"
        
        if shift['end_time']:
            text += f"🕐 До: {shift['end_time'][:16]}\n"
        
        text += f"📸 Осмотр: {'✅' if shift['inspection_approved'] else '⏳'}\n"
        
        if shift.get('notes'):
            text += f"📝 {shift['notes'][:50]}...\n"
        
        text += "\n"
    
    text += "\n<code>Всего смен: ...</code>"
    
    await reply(message, text)

@dp.message(F.text == "ℹ️ Информация")
async def info(message: types.Message):
    """Показывает информацию"""
    user = await db.get_user(message.from_user.id)
    role_names = {
        'botadmin': '👑 Администратор бота',
        'director': '👨‍💼 Директор компании',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    org_info = ""
    if user.get('organization_id'):
        org = await db.get_organization(user['organization_id'])
        if org:
            org_info = f"<b>Организация:</b> {org['name']}\n"
    
    await reply(
        message,
        f"🤖 <b>ТехКонтроль v1.0</b>\n\n"
        f"<b>Ваша роль:</b> {role_names.get(user['role'], '👤 Пользователь')}\n"
        f"{org_info}"
        f"<b>ID:</b> {message.from_user.id}\n\n"
        "<b>Назначение бота:</b>\n"
        "• Учет и контроль спецтехники\n"
        "• Управление водителями\n"
        "• Отслеживание ТО и ремонтов\n"
        "• Ежедневное обслуживание\n\n"
        "<b>По вопросам:</b>\n"
        "Обращайтесь к администратору вашей организации."
    )

# ========== ОБРАБОТЧИКИ СОСТОЯНИЙ ==========

@dp.message(UserStates.waiting_for_username_or_id)
async def process_username_or_id(message: types.Message, state: FSMContext):
    """Обрабатывает ввод username или ID"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    identifier = message.text.strip()
    
    # Сохраняем в состоянии
    await state.update_data(identifier=identifier)
    
    # Определяем доступные роли для назначения
    user = await db.get_user(message.from_user.id)
    user_role = user['role']
    
    if user_role == 'botadmin':
        roles = ["👑 Администратор", "👨‍💼 Директор", "👷 Начальник парка", "🚛 Водитель"]
    elif user_role == 'director':
        roles = ["👷 Начальник парка", "🚛 Водитель"]
    elif user_role == 'fleetmanager':
        roles = ["🚛 Водитель"]
    else:
        roles = []
    
    if not roles:
        await reply(message, "❌ У вас нет прав для назначения ролей!")
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
        return
    
    keyboard = []
    for role in roles:
        keyboard.append([types.KeyboardButton(text=role)])
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await reply(
        message,
        f"✅ Получено: {identifier}\n\n"
        f"Выберите роль для назначения:",
        reply_markup=types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    )
    await state.set_state(UserStates.waiting_for_role)

@dp.message(UserStates.waiting_for_role)
async def process_role_selection(message: types.Message, state: FSMContext):
    """Обрабатывает выбор роли"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    role_map = {
        "👑 Администратор": "botadmin",
        "👨‍💼 Директор": "director",
        "👷 Начальник парка": "fleetmanager",
        "🚛 Водитель": "driver"
    }
    
    selected_role = role_map.get(message.text)
    if not selected_role:
        await reply(message, "❌ Неверная роль! Выберите из списка.")
        return
    
    data = await state.get_data()
    identifier = data['identifier']
    
    # Определяем ID пользователя
    user_id = None
    
    # Если identifier - число (ID)
    if identifier.isdigit():
        user_id = int(identifier)
    else:
        # Если это username (с @ или без)
        username = identifier.replace('@', '')
        
        # Ищем пользователя в базе по username
        all_users = await db.get_all_users()
        for user in all_users:
            if user.get('username') == username:
                user_id = user['telegram_id']
                break
    
    if not user_id:
        await reply(
            message,
            f"❌ Пользователь '{identifier}' не найден!\n\n"
            f"Попросите пользователя написать боту /start, "
            f"чтобы он зарегистрировался в системе."
        )
        await state.clear()
        return
    
    # Получаем организацию назначающего (если нужно)
    assigner = await db.get_user(message.from_user.id)
    assigner_role = assigner['role']
    org_id = assigner.get('organization_id')
    
    # Проверяем права на назначение этой роли
    can_assign = {
        'botadmin': ['botadmin', 'director', 'fleetmanager', 'driver'],
        'director': ['fleetmanager', 'driver'],
        'fleetmanager': ['driver']
    }
    
    if selected_role not in can_assign.get(assigner_role, []):
        await reply(
            message,
            f"⛔ У вас нет прав назначать роль '{selected_role}'!\n"
            f"Ваша роль: {assigner_role}"
        )
        await state.clear()
        return
    
    # Назначаем роль
    success = await db.update_user_role(user_id, selected_role, org_id)
    
    if success:
        role_names = {
            'botadmin': '👑 Администратора',
            'director': '👨‍💼 Директора',
            'fleetmanager': '👷 Начальника парка',
            'driver': '🚛 Водителя'
        }
        
        await reply(
            message,
            f"✅ <b>Роль назначена успешно!</b>\n\n"
            f"<b>Пользователь:</b> {identifier}\n"
            f"<b>ID:</b> {user_id}\n"
            f"<b>Роль:</b> {role_names.get(selected_role, selected_role)}\n"
            f"{f'<b>Организация:</b> {org_id}' if org_id else ''}"
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f"🎉 <b>Вам назначена новая роль!</b>\n\n"
                f"<b>Роль:</b> {role_names.get(selected_role, selected_role)}\n"
                f"<b>Назначил:</b> {message.from_user.full_name}\n\n"
                f"Напишите /start для обновления меню."
            )
        except:
            pass
    else:
        await reply(message, "❌ Ошибка при назначении роли!")
    
    await state.clear()
    await cmd_start(message, state)

# Обработчики для добавления техники
@dp.message(UserStates.waiting_for_equipment_name)
async def process_equipment_name(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    await state.update_data(name=message.text)
    await reply(message, "✅ Название принято!\n\nТеперь введите модель техники:")
    await state.set_state(UserStates.waiting_for_equipment_model)

@dp.message(UserStates.waiting_for_equipment_model)
async def process_equipment_model(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    await state.update_data(model=message.text)
    await reply(message, "✅ Модель принята!\n\nТеперь введите VIN (уникальный номер):")
    await state.set_state(UserStates.waiting_for_equipment_vin)

@dp.message(UserStates.waiting_for_equipment_vin)
async def process_equipment_vin(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    data = await state.get_data()
    org_id = data['org_id']
    name = data['name']
    model = data['model']
    vin = message.text
    
    try:
        eq_id = await db.add_equipment(name, model, vin, org_id)
        
        await reply(
            message,
            f"✅ <b>Техника добавлена!</b>\n\n"
            f"<b>Название:</b> {name}\n"
            f"<b>Модель:</b> {model}\n"
            f"<b>VIN:</b> {vin}\n"
            f"<b>ID техники:</b> {eq_id}\n\n"
            f"Техника доступна в автопарке организации."
        )
    except Exception as e:
        await reply(
            message,
            f"❌ <b>Ошибка добавления техники!</b>\n\n"
            f"Возможно, техника с таким VIN уже существует."
        )
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))

# ========== КОМАНДЫ ==========

@dp.message(Command("createorg"))
async def create_organization_cmd(message: types.Message):
    """Создает организацию для директора"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'director':
        await reply(message, "⛔ Только директора могут создавать организации!")
        return
    
    if user.get('organization_id'):
        await reply(message, "⚠️ У вас уже есть организация!")
        return
    
    # Получаем название организации из команды
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await reply(
            message,
            "❌ <b>Неверный формат!</b>\n\n"
            "Используйте: <code>/createorg Название организации</code>\n\n"
            "<b>Пример:</b>\n"
            "<code>/createorg ООО 'Моя компания'</code>"
        )
        return
    
    org_name = parts[1]
    org_id = await db.create_organization(org_name, message.from_user.id)
    
    if org_id:
        await reply(
            message,
            f"✅ <b>Организация создана!</b>\n\n"
            f"<b>Название:</b> {org_name}\n"
            f"<b>ID организации:</b> {org_id}\n\n"
            f"Теперь вы можете:\n"
            f"• Добавлять технику\n"
            f"• Назначать сотрудников\n"
            f"• Управлять автопарком"
        )
    else:
        await reply(message, "❌ Ошибка создания организации!")

@dp.message(Command("myrole"))
async def myrole_cmd(message: types.Message):
    """Показывает роль пользователя"""
    user = await db.get_user(message.from_user.id)
    
    if not user:
        await reply(message, "❌ Вы не зарегистрированы!")
        return
    
    role_names = {
        'botadmin': '👑 Администратор бота',
        'director': '👨‍💼 Директор компании',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    org_info = ""
    if user.get('organization_id'):
        org = await db.get_organization(user['organization_id'])
        if org:
            org_info = f"<b>Организация:</b> {org['name']} (ID: {org['id']})\n"
    
    await reply(
        message,
        f"👤 <b>Ваш профиль</b>\n\n"
        f"<b>ID:</b> {user['telegram_id']}\n"
        f"<b>Имя:</b> {user['full_name']}\n"
        f"<b>Роль:</b> {role_names.get(user['role'], user['role'])}\n"
        f"{org_info}"
        f"<b>Зарегистрирован:</b> {user['created_at'][:10]}"
    )

@dp.message(Command("setrole"))
async def setrole_cmd(message: types.Message):
    """Команда для назначения роли (поддержка ID и username)"""
    parts = message.text.split()
    
    if len(parts) < 3:
        await reply(
            message,
            "❌ <b>Неверный формат!</b>\n\n"
            "<b>Использование:</b>\n"
            "<code>/setrole ID_ИЛИ_USERNAME РОЛЬ</code>\n\n"
            "<b>Примеры:</b>\n"
            "<code>/setrole 123456789 director</code>\n"
            "<code>/setrole @username fleetmanager</code>\n"
            "<code>/setrole username driver</code>\n\n"
            "<b>Доступные роли:</b>\n"
            "• botadmin\n"
            "• director\n"
            "• fleetmanager\n"
            "• driver"
        )
        return
    
    identifier = parts[1]
    new_role = parts[2].lower()
    
    # Проверяем существование роли
    valid_roles = ['botadmin', 'director', 'fleetmanager', 'driver']
    if new_role not in valid_roles:
        await reply(
            message,
            f"❌ <b>Неверная роль!</b>\n\n"
            f"Доступные роли: {', '.join(valid_roles)}"
        )
        return
    
    # Ищем пользователя
    user_id = None
    
    # Если identifier - число
    if identifier.isdigit():
        user_id = int(identifier)
    else:
        # Если это username
        username = identifier.replace('@', '')
        
        # Ищем в базе
        all_users = await db.get_all_users()
        for user in all_users:
            if user.get('username') == username:
                user_id = user['telegram_id']
                break
    
    if not user_id:
        await reply(
            message,
            f"❌ <b>Пользователь не найден!</b>\n\n"
            f"Попросите пользователя {identifier} написать боту /start."
        )
        return
    
    # Проверяем права назначающего
    assigner = await db.get_user(message.from_user.id)
    assigner_role = assigner['role']
    
    # Иерархия прав
    can_assign = {
        'botadmin': ['botadmin', 'director', 'fleetmanager', 'driver'],
        'director': ['fleetmanager', 'driver'],
        'fleetmanager': ['driver'],
        'driver': []
    }
    
    if new_role not in can_assign.get(assigner_role, []):
        await reply(
            message,
            f"⛔ <b>У вас нет прав назначать роль '{new_role}'!</b>\n\n"
            f"Ваша роль: {assigner_role}\n"
            f"Вы можете назначать только: {', '.join(can_assign.get(assigner_role, []))}"
        )
        return
    
    # Назначаем организацию если нужно
    org_id = assigner.get('organization_id') if assigner_role in ['director', 'fleetmanager'] else None
    
    # Назначаем роль
    success = await db.update_user_role(user_id, new_role, org_id)
    
    if success:
        role_names = {
            'botadmin': '👑 Администратор бота',
            'director': '👨‍💼 Директор компании',
            'fleetmanager': '👷 Начальник парка',
            'driver': '🚛 Водитель'
        }
        
        await reply(
            message,
            f"✅ <b>Роль назначена!</b>\n\n"
            f"<b>Пользователь:</b> {identifier}\n"
            f"<b>ID:</b> {user_id}\n"
            f"<b>Роль:</b> {role_names.get(new_role, new_role)}\n"
            f"{f'<b>Организация:</b> {org_id}' if org_id else ''}"
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f"🎉 <b>Вам назначена новая роль!</b>\n\n"
                f"<b>Роль:</b> {role_names.get(new_role, new_role)}\n"
                f"<b>Назначил:</b> {message.from_user.full_name}\n\n"
                f"Напишите /start для обновления меню."
            )
        except:
            pass
    else:
        await reply(message, "❌ Ошибка при назначении роли!")

@dp.message(Command("help"))
async def help_cmd(message: types.Message):
    """Показывает справку"""
    await reply(
        message,
        "🤖 <b>ТехКонтроль Бот - Справка</b>\n\n"
        "<b>Основные команды:</b>\n"
        "/start - Главное меню\n"
        "/myrole - Показать мою роль\n"
        "/setrole - Назначить роль (администраторы)\n"
        "/createorg - Создать организацию (директора)\n"
        "/cancel - Отменить текущее действие\n"
        "/help - Эта справка\n\n"
        "<b>Система ролей:</b>\n"
        "• Администратор - полный доступ\n"
        "• Директор - управление организацией\n"
        "• Начальник парка - управление техникой\n"
        "• Водитель - работа со сменами\n\n"
        "<b>Доступные функции:</b>\n"
        "• Учет техники\n"
        "• Начало и завершение смен\n"
        "• Фото осмотра техники\n"
        "• Ежедневные проверки\n"
        "• Назначение ролей\n"
        "• Просмотр статистики\n"
        "• Управление организациями"
    )

# ========== CALLBACK ОБРАБОТЧИКИ ==========

@dp.callback_query(F.data.startswith("approve_inspection:"))
async def approve_inspection_callback(callback: types.CallbackQuery):
    """Подтверждает осмотр"""
    shift_id = int(callback.data.split(":")[1])
    
    success = await db.approve_inspection(shift_id, callback.from_user.id)
    
    if success:
        # Получаем информацию о смене
        try:
            # Нужно добавить метод для получения смены по ID
            # Пока просто отправляем подтверждение
            await callback.message.edit_text(
                f"✅ <b>Осмотр подтверждён!</b>\n\n"
                f"Смена #{shift_id}\n"
                f"Подтвердил: {callback.from_user.full_name}"
            )
        except:
            await callback.message.edit_text(f"✅ Осмотр #{shift_id} подтверждён")
        
        await callback.answer("Осмотр подтверждён!")
    else:
        await callback.answer("❌ Ошибка при подтверждении осмотра", show_alert=True)

@dp.callback_query(F.data.startswith("reject_inspection:"))
async def reject_inspection_callback(callback: types.CallbackQuery):
    """Отклоняет осмотр"""
    shift_id = int(callback.data.split(":")[1])
    
    # Здесь можно добавить логику отклонения
    await callback.message.edit_text(
        f"❌ <b>Осмотр отклонён</b>\n\n"
        f"Смена #{shift_id}\n"
        f"Отклонил: {callback.from_user.full_name}\n\n"
        f"Водитель будет уведомлён о необходимости нового осмотра."
    )
    await callback.answer("Осмотр отклонён")

# ========== ОБРАБОТКА НЕИЗВЕСТНЫХ КОМАНД ==========

@dp.message()
async def handle_unknown(message: types.Message, state: FSMContext):
    """Обрабатывает неизвестные команды"""
    current_state = await state.get_state()
    
    # Если пользователь в состоянии - игнорируем
    if current_state:
        return
    
    # Если это не команда и не кнопка - показываем справку
    if message.text and not message.text.startswith('/'):
        await reply(
            message,
            "🤔 <b>Неизвестная команда</b>\n\n"
            "Используйте меню или команды:\n"
            "/start - главное меню\n"
            "/myrole - моя роль\n"
            "/setrole - назначить роль\n"
            "/createorg - создать организацию\n"
            "/cancel - отменить действие\n"
            "/help - справка"
        )
    elif message.text:
        # Если это текстовая команда, но не обработана
        await reply(message, "❌ Эта команда временно недоступна. Используйте меню или /help.")

# ========== ЗАПУСК БОТА ==========

async def on_startup():
    """Инициализация при запуске"""
    try:
        await db.connect()
        
        # Создаем администратора (ВАЖНО: ЗАМЕНИТЕ ID НА СВОЙ!)
        ADMIN_ID = 1079922982  # <-- ЗАМЕНИТЕ ЭТО НА ВАШ TELEGRAM ID!
        await db.register_user(
            telegram_id=ADMIN_ID,
            full_name="Администратор Системы",
            username="admin",
            role='botadmin'
        )
        
        logger.info("✅ Бот запущен!")
        logger.info(f"👑 Администратор: ID {ADMIN_ID}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

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
