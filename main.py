import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ContentType
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

from database import db, ROLES

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

# Состояния для водителя
class DriverStates(StatesGroup):
    choosing_equipment = State()
    safety_instruction = State()
    pre_inspection = State()
    waiting_for_photos = State()

# Состояния для управления
class ManagementStates(StatesGroup):
    waiting_for_username = State()
    waiting_for_user_role = State()
    waiting_for_org_name = State()
    waiting_for_eq_name = State()
    waiting_for_eq_model = State()
    waiting_for_eq_vin = State()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

async def quick_reply(message: types.Message, text: str, **kwargs):
    """Быстрый ответ"""
    await bot.send_chat_action(message.chat.id, "typing")
    await asyncio.sleep(0.1)
    return await message.answer(text, **kwargs)

def get_main_keyboard(user_role: str, org_id: int = None, has_active_shift: bool = False):
    """Генерирует главное меню по роли"""
    
    base_buttons = {
        'botadmin': [
            [types.KeyboardButton(text="👑 Админ-панель")],
            [types.KeyboardButton(text="🏢 Все организации")],
            [types.KeyboardButton(text="👥 Все пользователи")],
            [types.KeyboardButton(text="➕ Назначить директора")],
            [types.KeyboardButton(text="📊 Статистика")]
        ],
        'director': [
            [types.KeyboardButton(text="👨‍💼 Моя организация")],
            [types.KeyboardButton(text="🚜 Автопарк")],
            [types.KeyboardButton(text="👥 Сотрудники")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить сотрудника")],
            [types.KeyboardButton(text="📊 Отчеты")]
        ],
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Управление парком")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="📋 Активные смены")]
        ],
        'driver': []
    }
    
    # Для водителя меняем меню
    if user_role == 'driver':
        if has_active_shift:
            buttons = [
                [types.KeyboardButton(text="⏹️ Завершить смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="ℹ️ Информация")]
            ]
        else:
            buttons = [
                [types.KeyboardButton(text="🚛 Начать смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="ℹ️ Информация")]
            ]
        base_buttons['driver'] = buttons
    
    # Добавляем общие кнопки
    if user_role in ['director', 'fleetmanager']:
        base_buttons[user_role].append([types.KeyboardButton(text="🔙 Главное меню")])
    
    return types.ReplyKeyboardMarkup(
        keyboard=base_buttons[user_role],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

# ========== КОМАНДА СТАРТ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Главное меню"""
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Регистрируем пользователя
    user = await db.get_user(user_id)
    if not user:
        await db.register_user(
            telegram_id=user_id,
            full_name=f"{message.from_user.first_name} {message.from_user.last_name or ''}",
            username=username,
            role='driver'
        )
        user = await db.get_user(user_id)
    
    user_role = user['role']
    role_name = ROLES.get(user_role, {}).get('name', 'Водитель')
    org_id = user.get('organization_id')
    
    # Проверяем активную смену (для водителей)
    has_active_shift = False
    if user_role == 'driver':
        active_shift = await db.get_active_shift(user_id)
        has_active_shift = bool(active_shift)
    
    welcome_text = {
        'botadmin': "👑 <b>Панель администратора бота</b>",
        'director': f"👨‍💼 <b>Директор компании</b>",
        'fleetmanager': f"👷 <b>Начальник парка</b>",
        'driver': f"👋 <b>Привет, {message.from_user.first_name}!</b>"
    }
    
    await quick_reply(
        message,
        f"{welcome_text.get(user_role, '👋 Привет!')}\n\n"
        f"<b>Роль:</b> {role_name}\n"
        f"<b>ID:</b> {user_id}\n"
        f"{f'<b>Организация:</b> {org_id}' if org_id else ''}\n\n"
        f"Используйте меню для работы:",
        reply_markup=get_main_keyboard(user_role, org_id, has_active_shift)
    )

# ========== МЕНЮ АДМИНИСТРАТОРА БОТА ==========

@dp.message(F.text == "👑 Админ-панель")
async def admin_panel(message: types.Message):
    """Панель администратора"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await quick_reply(message, "⛔ Доступ только для администратора!")
        return
    
    await quick_reply(
        message,
        "👑 <b>Панель администратора</b>\n\n"
        "<b>Управление системой:</b>\n"
        "• Создание организаций\n"
        "• Назначение директоров\n"
        "• Просмотр статистики\n"
        "• Управление пользователями\n\n"
        "Используйте меню ниже:"
    )

@dp.message(F.text == "➕ Назначить директора")
async def assign_director_start(message: types.Message, state: FSMContext):
    """Начинаем назначение директора"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await quick_reply(message, "⛔ Доступ только для администратора!")
        return
    
    await quick_reply(
        message,
        "👨‍💼 <b>Назначение директора</b>\n\n"
        "Введите Telegram username пользователя (например, @username):",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(ManagementStates.waiting_for_username)

@dp.message(ManagementStates.waiting_for_username)
async def process_username_for_director(message: types.Message, state: FSMContext):
    """Обрабатываем username для назначения директора"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    username = message.text.strip().replace('@', '')
    await state.update_data(username=username)
    
    await quick_reply(
        message,
        f"✅ Username получен: @{username}\n\n"
        "Теперь создайте организацию для нового директора.\n"
        "Введите название организации:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(ManagementStates.waiting_for_org_name)

@dp.message(ManagementStates.waiting_for_org_name)
async def process_org_name_for_director(message: types.Message, state: FSMContext):
    """Обрабатываем название организации"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    org_name = message.text.strip()
    data = await state.get_data()
    username = data['username']
    
    await quick_reply(
        message,
        f"🏢 <b>Создание организации</b>\n\n"
        f"Название: {org_name}\n"
        f"Директор: @{username}\n\n"
        f"Для завершения попросите пользователя @{username} "
        f"написать боту /start, а затем используйте команду:\n\n"
        f"<code>/setrole @{username} director</code>\n\n"
        f"После этого создайте организацию командой:\n"
        f"<code>/createorg \"{org_name}\"</code>"
    )
    
    await state.clear()
    await cmd_start(message)

# ========== МЕНЮ ДИРЕКТОРА ==========

@dp.message(F.text == "👨‍💼 Моя организация")
async def director_organization(message: types.Message):
    """Управление организацией директора"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'director':
        await quick_reply(message, "⛔ Доступ только для директора!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(
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
    users = await db.get_organization_users(org_id)
    equipment = await db.get_organization_equipment(org_id)
    
    # Статистика по ролям
    roles_count = {'director': 0, 'fleetmanager': 0, 'driver': 0}
    for u in users:
        roles_count[u['role']] = roles_count.get(u['role'], 0) + 1
    
    text = (
        f"🏢 <b>Организация: {org['name']}</b>\n\n"
        f"<b>ID:</b> {org_id}\n"
        f"<b>Директор:</b> {user['full_name']}\n"
        f"<b>Создана:</b> {org['created_at'][:10]}\n\n"
        f"<b>Сотрудники:</b>\n"
        f"• Директор: {roles_count['director']}\n"
        f"• Начальники парка: {roles_count['fleetmanager']}\n"
        f"• Водители: {roles_count['driver']}\n"
        f"<b>Техника:</b> {len(equipment)} ед.\n\n"
        f"<b>Управление:</b>\n"
        f"1. Добавить технику\n"
        f"2. Назначить сотрудников\n"
        f"3. Просмотреть отчеты"
    )
    
    await quick_reply(message, text)

@dp.message(F.text == "➕ Назначить сотрудника")
async def assign_employee_start(message: types.Message, state: FSMContext):
    """Начинаем назначение сотрудника"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'director':
        await quick_reply(message, "⛔ Доступ только для директора!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(message, "❌ Сначала создайте организацию!")
        return
    
    await state.update_data(org_id=org_id)
    
    await quick_reply(
        message,
        "👥 <b>Назначение сотрудника</b>\n\n"
        "Выберите роль для назначения:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="👷 Начальник парка")],
                [types.KeyboardButton(text="🚛 Водитель")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(ManagementStates.waiting_for_user_role)

@dp.message(ManagementStates.waiting_for_user_role)
async def process_role_for_employee(message: types.Message, state: FSMContext):
    """Обрабатываем выбор роли"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    role_map = {
        "👷 Начальник парка": "fleetmanager",
        "🚛 Водитель": "driver"
    }
    
    if message.text not in role_map:
        await quick_reply(message, "⚠️ Выберите роль из списка!")
        return
    
    target_role = role_map[message.text]
    await state.update_data(target_role=target_role)
    
    await quick_reply(
        message,
        f"✅ Роль выбрана: {message.text}\n\n"
        f"Введите Telegram username сотрудника (например, @username):",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(ManagementStates.waiting_for_username)

@dp.message(ManagementStates.waiting_for_username)
async def process_username_for_employee(message: types.Message, state: FSMContext):
    """Обрабатываем username сотрудника"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    username = message.text.strip().replace('@', '')
    data = await state.get_data()
    org_id = data['org_id']
    target_role = data['target_role']
    
    role_name = "начальника парка" if target_role == 'fleetmanager' else "водителя"
    
    await quick_reply(
        message,
        f"✅ <b>Готово к назначению!</b>\n\n"
        f"Сотрудник: @{username}\n"
        f"Роль: {role_name}\n"
        f"Организация: {org_id}\n\n"
        f"Попросите сотрудника @{username} написать боту /start,\n"
        f"а затем используйте команду:\n\n"
        f"<code>/setrole @{username} {target_role} {org_id}</code>"
    )
    
    await state.clear()
    await cmd_start(message)

@dp.message(F.text == "🚜 Автопарк")
async def show_equipment(message: types.Message):
    """Показывает технику организации"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации!")
        return
    
    equipment = await db.get_organization_equipment(org_id)
    
    if not equipment:
        await quick_reply(
            message,
            "🚜 <b>Автопарк</b>\n\n"
            "Техники пока нет.\n"
            "Добавьте технику через меню."
        )
        return
    
    text = f"🚜 <b>Автопарк ({len(equipment)} ед.)</b>\n\n"
    
    for eq in equipment[:10]:
        status_icon = "🟢" if eq['status'] == 'active' else "🔴"
        text += f"{status_icon} <b>{eq['name']}</b> ({eq['model']})\n"
        text += f"   Статус: {eq['status']}\n\n"
    
    if len(equipment) > 10:
        text += f"... и ещё {len(equipment) - 10} единиц"
    
    await quick_reply(message, text)

@dp.message(F.text == "➕ Добавить технику")
async def add_equipment_start(message: types.Message, state: FSMContext):
    """Начинаем добавление техники"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации!")
        return
    
    await state.update_data(org_id=org_id, user_id=user['telegram_id'])
    
    await quick_reply(
        message,
        "🚜 <b>Добавление техники</b>\n\n"
        "Введите название техники:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(ManagementStates.waiting_for_eq_name)

@dp.message(ManagementStates.waiting_for_eq_name)
async def process_eq_name(message: types.Message, state: FSMContext):
    """Обрабатываем название техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    await state.update_data(eq_name=message.text)
    
    await quick_reply(
        message,
        "✅ Название принято!\n\n"
        "Теперь введите модель техники:"
    )
    await state.set_state(ManagementStates.waiting_for_eq_model)

@dp.message(ManagementStates.waiting_for_eq_model)
async def process_eq_model(message: types.Message, state: FSMContext):
    """Обрабатываем модель техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    await state.update_data(eq_model=message.text)
    
    await quick_reply(
        message,
        "✅ Модель принята!\n\n"
        "Теперь введите VIN (уникальный номер):"
    )
    await state.set_state(ManagementStates.waiting_for_eq_vin)

@dp.message(ManagementStates.waiting_for_eq_vin)
async def process_eq_vin(message: types.Message, state: FSMContext):
    """Обрабатываем VIN и сохраняем технику"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    data = await state.get_data()
    org_id = data['org_id']
    user_id = data['user_id']
    eq_name = data['eq_name']
    eq_model = data['eq_model']
    eq_vin = message.text
    
    # Добавляем технику
    eq_id = await db.add_equipment(eq_name, eq_model, eq_vin, org_id, user_id)
    
    if eq_id:
        await quick_reply(
            message,
            f"✅ <b>Техника добавлена!</b>\n\n"
            f"<b>Название:</b> {eq_name}\n"
            f"<b>Модель:</b> {eq_model}\n"
            f"<b>VIN:</b> {eq_vin}\n"
            f"<b>ID:</b> {eq_id}\n\n"
            f"Техника доступна для использования."
        )
    else:
        await quick_reply(
            message,
            "❌ <b>Ошибка добавления техники</b>\n\n"
            "Возможно, техника с таким VIN уже существует."
        )
    
    await state.clear()
    await cmd_start(message)

# ========== МЕНЮ НАЧАЛЬНИКА ПАРКА ==========

@dp.message(F.text == "👷 Управление парком")
async def fleetmanager_panel(message: types.Message):
    """Панель начальника парка"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'fleetmanager':
        await quick_reply(message, "⛔ Доступ только для начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации! Обратитесь к директору.")
        return
    
    # Получаем статистику
    users = await db.get_organization_users(org_id)
    equipment = await db.get_organization_equipment(org_id)
    
    drivers_count = len([u for u in users if u['role'] == 'driver'])
    active_equipment = len([e for e in equipment if e['status'] == 'active'])
    
    await quick_reply(
        message,
        "👷 <b>Управление парком</b>\n\n"
        f"<b>Водителей:</b> {drivers_count}\n"
        f"<b>Техники:</b> {len(equipment)} ед.\n"
        f"<b>Активной техники:</b> {active_equipment}\n\n"
        "<b>Доступные действия:</b>\n"
        "• Просмотр техники\n"
        "• Добавление техники\n"
        "• Назначение водителей\n"
        "• Просмотр активных смен"
    )

@dp.message(F.text == "👥 Водители")
async def show_drivers(message: types.Message):
    """Показывает водителей организации"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации!")
        return
    
    users = await db.get_organization_users(org_id)
    drivers = [u for u in users if u['role'] == 'driver']
    
    if not drivers:
        await quick_reply(
            message,
            "👥 <b>Водители</b>\n\n"
            "Водителей пока нет.\n"
            "Назначьте водителей через меню."
        )
        return
    
    text = f"👥 <b>Водители ({len(drivers)} чел.)</b>\n\n"
    
    for driver in drivers[:10]:
        text += f"🚛 <b>{driver['full_name']}</b>\n"
        if driver['username']:
            text += f"   @{driver['username']}\n"
        text += f"   ID: {driver['telegram_id']}\n\n"
    
    if len(drivers) > 10:
        text += f"... и ещё {len(drivers) - 10} водителей"
    
    await quick_reply(message, text)

@dp.message(F.text == "➕ Назначить водителя")
async def assign_driver_fleetmanager(message: types.Message, state: FSMContext):
    """Начальник парка назначает водителя"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'fleetmanager':
        await quick_reply(message, "⛔ Доступ только для начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации! Обратитесь к директору.")
        return
    
    await state.update_data(org_id=org_id)
    
    await quick_reply(
        message,
        "🚛 <b>Назначение водителя</b>\n\n"
        "Введите Telegram username водителя (например, @username):",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(ManagementStates.waiting_for_username)

# ========== МЕНЮ ВОДИТЕЛЯ ==========

@dp.message(F.text == "🚛 Начать смену")
async def start_shift_process(message: types.Message, state: FSMContext):
    """Начинает смену"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'driver':
        await quick_reply(message, "⛔ Только водители могут начинать смены!")
        return
    
    # Проверяем активную смену
    active_shift = await db.get_active_shift(user['telegram_id'])
    if active_shift:
        await quick_reply(
            message,
            f"⚠️ <b>У вас уже есть активная смена!</b>\n\n"
            f"Завершите текущую смену перед началом новой."
        )
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации! Обратитесь к начальнику парка.")
        return
    
    equipment = await db.get_organization_equipment(org_id)
    active_equipment = [e for e in equipment if e['status'] == 'active']
    
    if not active_equipment:
        await quick_reply(message, "❌ Нет доступной активной техники.")
        return
    
    keyboard = []
    for eq in active_equipment[:5]:
        keyboard.append([types.KeyboardButton(text=f"🚜 {eq['name']}")])
    
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await quick_reply(
        message,
        "🚛 <b>Выберите технику:</b>\n\n"
        f"Доступно техники: {len(active_equipment)} ед.",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=keyboard,
            resize_keyboard=True
        )
    )
    
    await state.update_data(equipment_list=active_equipment)
    await state.set_state(DriverStates.choosing_equipment)

@dp.message(DriverStates.choosing_equipment)
async def process_equipment_choice(message: types.Message, state: FSMContext):
    """Обрабатывает выбор техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    data = await state.get_data()
    equipment_list = data.get('equipment_list', [])
    
    # Ищем выбранную технику
    selected_eq = None
    search_text = message.text.replace("🚜 ", "").strip()
    
    for eq in equipment_list:
        if search_text in eq['name']:
            selected_eq = eq
            break
    
    if not selected_eq:
        await quick_reply(message, "⚠️ Выберите технику из списка!")
        return
    
    await state.update_data(selected_equipment=selected_eq)
    
    await quick_reply(
        message,
        f"📋 <b>Инструктаж по безопасности</b>\n\n"
        f"<b>Техника:</b> {selected_eq['name']} ({selected_eq['model']})\n\n"
        "Основные правила:\n"
        "1. Проверьте средства пожаротушения\n"
        "2. Убедитесь в исправности ремней\n"
        "3. Проверьте сигналы и огни\n"
        "4. Осмотрите на утечки\n"
        "5. Проверьте давление в шинах\n\n"
        "Подтвердите ознакомление:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✅ Подтверждаю")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    
    await state.set_state(DriverStates.safety_instruction)

@dp.message(DriverStates.safety_instruction)
async def process_safety_instruction(message: types.Message, state: FSMContext):
    """Обрабатывает подтверждение инструктажа"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    if message.text != "✅ Подтверждаю":
        await quick_reply(message, "⚠️ Нажмите '✅ Подтверждаю' для продолжения!")
        return
    
    await quick_reply(
        message,
        "🔍 <b>Предсменный осмотр</b>\n\n"
        "Проверьте основные узлы:\n"
        "• Уровень масла и жидкости\n"
        "• Гидравлические шланги\n"
        "• Работу приборов\n\n"
        "Вы можете добавить фото или продолжить:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="📷 Сделать фото")],
                [types.KeyboardButton(text="⏭️ Без фото")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    
    await state.update_data(inspection_photos=[])
    await state.set_state(DriverStates.pre_inspection)

@dp.message(DriverStates.pre_inspection, F.text == "📷 Сделать фото")
async def request_photos(message: types.Message, state: FSMContext):
    """Запрашивает фото"""
    await quick_reply(
        message,
        "📸 <b>Отправьте фотографии</b>\n\n"
        "Можно отправить несколько фото сразу.\n"
        "После отправки нажмите '✅ Готово'.",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✅ Готово")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(DriverStates.waiting_for_photos)

@dp.message(DriverStates.pre_inspection, F.text == "⏭️ Без фото")
async def skip_photos(message: types.Message, state: FSMContext):
    """Пропускает фото и начинает смену"""
    await complete_shift_start(message, state, photos=[])

async def complete_shift_start(message: types.Message, state: FSMContext, photos=None):
    """Завершает начало смены"""
    data = await state.get_data()
    selected_eq = data.get('selected_equipment')
    
    if not selected_eq:
        await quick_reply(message, "❌ Ошибка: данные не найдены!")
        await state.clear()
        return
    
    # Начинаем смену
    shift_id = await db.start_shift(message.from_user.id, selected_eq['id'])
    
    # Сохраняем фото если есть
    if photos:
        await db.add_inspection(shift_id, photos, f"Осмотр {selected_eq['name']}")
    
    await quick_reply(
        message,
        f"🎉 <b>Смена начата!</b>\n\n"
        f"<b>Техника:</b> {selected_eq['name']}\n"
        f"<b>ID смены:</b> {shift_id}\n"
        f"<b>Время:</b> {message.date.strftime('%H:%M')}\n"
        f"<b>Фото:</b> {len(photos) if photos else 0} шт.\n\n"
        f"Удачной работы! 🚀"
    )
    
    await state.clear()
    await cmd_start(message)

@dp.message(F.text == "⏹️ Завершить смену")
async def end_shift_process(message: types.Message):
    """Завершает смену"""
    active_shift = await db.get_active_shift(message.from_user.id)
    
    if not active_shift:
        await quick_reply(message, "❌ У вас нет активной смены!")
        return
    
    await db.end_shift(active_shift['id'])
    
    await quick_reply(
        message,
        f"✅ <b>Смена завершена!</b>\n\n"
        f"<b>Техника:</b> {active_shift['name']}\n"
        f"<b>ID смены:</b> {active_shift['id']}\n"
        f"<b>Время:</b> {message.date.strftime('%H:%M')}\n\n"
        f"Спасибо за работу! 👷"
    )
    
    await cmd_start(message)

@dp.message(F.text == "📋 Мои смены")
async def show_my_shifts(message: types.Message):
    """Показывает смены пользователя"""
    shifts = await db.get_user_shifts(message.from_user.id, limit=5)
    
    if not shifts:
        await quick_reply(
            message,
            "📋 <b>Мои смены</b>\n\n"
            "У вас ещё не было смен.\n"
            "Начните первую смену!"
        )
        return
    
    text = "📋 <b>Последние смены</b>\n\n"
    
    for shift in shifts:
        status_icon = "🟢" if shift['status'] == 'active' else "✅"
        start_time = shift['start_time'][:16]
        end_time = shift['end_time'][:16] if shift['end_time'] else "в процессе"
        
        text += f"{status_icon} <b>{shift['equipment_name']}</b>\n"
        text += f"   Начало: {start_time}\n"
        text += f"   Окончание: {end_time}\n"
        text += f"   Статус: {shift['status']}\n\n"
    
    await quick_reply(message, text)

# ========== КОМАНДЫ ДЛЯ ОБСЛУЖИВАНИЯ ==========

@dp.message(Command("setrole"))
async def set_role_command(message: types.Message):
    """Команда для назначения роли"""
    try:
        parts = message.text.split()
        
        if len(parts) < 3:
            await quick_reply(
                message,
                "❌ <b>Неверный формат</b>\n\n"
                "<b>Использование:</b>\n"
                "<code>/setrole USERNAME ROLE [ORG_ID]</code>\n\n"
                "<b>Примеры:</b>\n"
                "<code>/setrole @username director</code>\n"
                "<code>/setrole @username driver 1</code>"
            )
            return
        
        username = parts[1].replace('@', '')
        new_role = parts[2].lower()
        org_id = int(parts[3]) if len(parts) > 3 else None
        
        # Находим пользователя по username
        cursor = await db.connection.execute(
            'SELECT telegram_id, full_name FROM users WHERE username = ?',
            (username,)
        )
        user_row = await cursor.fetchone()
        await cursor.close()
        
        if not user_row:
            await quick_reply(
                message,
                f"❌ <b>Пользователь @{username} не найден!</b>\n\n"
                f"Попросите пользователя написать боту /start."
            )
            return
        
        target_id = user_row['telegram_id']
        
        # Меняем роль
        success = await db.update_user_role(target_id, new_role, org_id)
        
        if success:
            role_name = ROLES.get(new_role, {}).get('name', new_role)
            await quick_reply(
                message,
                f"✅ <b>Роль назначена!</b>\n\n"
                f"<b>Пользователь:</b> @{username}\n"
                f"<b>Роль:</b> {role_name}\n"
                f"{f'<b>Организация:</b> {org_id}' if org_id else ''}"
            )
        else:
            await quick_reply(message, "❌ Ошибка назначения роли!")
            
    except Exception as e:
        logger.error(f"Ошибка в setrole: {e}")
        await quick_reply(message, f"❌ Ошибка: {str(e)}")

@dp.message(Command("createorg"))
async def create_org_command(message: types.Message):
    """Создает организацию"""
    try:
        parts = message.text.split(maxsplit=1)
        
        if len(parts) < 2:
            await quick_reply(
                message,
                "❌ <b>Неверный формат</b>\n\n"
                "<b>Использование:</b>\n"
                "<code>/createorg Название организации</code>\n\n"
                "<b>Пример:</b>\n"
                "<code>/createorg ООО 'Моя компания'</code>"
            )
            return
        
        org_name = parts[1]
        user_id = message.from_user.id
        
        # Проверяем, что пользователь - директор
        user = await db.get_user(user_id)
        if user['role'] != 'director':
            await quick_reply(message, "⛔ Только директора могут создавать организации!")
            return
        
        # Проверяем, нет ли уже организации
        if user.get('organization_id'):
            await quick_reply(message, "⚠️ У вас уже есть организация!")
            return
        
        # Создаем организацию
        org_id = await db.create_organization(org_name, user_id)
        
        if org_id:
            await quick_reply(
                message,
                f"✅ <b>Организация создана!</b>\n\n"
                f"<b>Название:</b> {org_name}\n"
                f"<b>ID:</b> {org_id}\n\n"
                f"Теперь вы можете:\n"
                f"• Добавлять технику\n"
                f"• Назначать сотрудников\n"
                f"• Управлять парком"
            )
        else:
            await quick_reply(message, "❌ Ошибка создания организации!")
            
    except Exception as e:
        logger.error(f"Ошибка создания организации: {e}")
        await quick_reply(message, f"❌ Ошибка: {str(e)}")

@dp.message(Command("myrole"))
async def myrole_command(message: types.Message):
    """Показывает роль пользователя"""
    user = await db.get_user(message.from_user.id)
    
    if not user:
        await quick_reply(message, "❌ Вы не зарегистрированы!")
        return
    
    role_name = ROLES.get(user['role'], {}).get('name', user['role'])
    org_info = ""
    
    if user.get('organization_id'):
        org = await db.get_organization(user['organization_id'])
        if org:
            org_info = f"<b>Организация:</b> {org['name']} (ID: {org['id']})\n"
    
    await quick_reply(
        message,
        f"👤 <b>Ваш профиль</b>\n\n"
        f"<b>ID:</b> {user['telegram_id']}\n"
        f"<b>Имя:</b> {user['full_name']}\n"
        f"<b>Роль:</b> {role_name}\n"
        f"{org_info}"
        f"<b>Зарегистрирован:</b> {user['created_at'][:10]}"
    )

# ========== ОБРАБОТКА ОШИБОК ==========

@dp.message(F.text == "🔙 Главное меню")
async def back_to_main(message: types.Message):
    """Возврат в главное меню"""
    await cmd_start(message)

@dp.message(F.text == "ℹ️ Информация")
async def show_info(message: types.Message):
    """Показывает информацию"""
    user = await db.get_user(message.from_user.id)
    role_name = ROLES.get(user['role'], {}).get('name', 'Водитель')
    
    await quick_reply(
        message,
        f"🤖 <b>ТехКонтроль v2.1</b>\n\n"
        f"<b>Ваша роль:</b> {role_name}\n"
        f"<b>Система ролей:</b>\n"
        f"• Администратор бота\n"
        f"• Директор компании\n"
        f"• Начальник парка\n"
        f"• Водитель\n\n"
        f"<b>Функции:</b>\n"
        f"✅ Управление техникой\n"
        f"✅ Назначение сотрудников\n"
        f"✅ Контроль смен\n"
        f"✅ Фото-отчеты\n\n"
        f"По вопросам обращайтесь к администратору."
    )

@dp.message()
async def handle_unknown(message: types.Message):
    """Обработка неизвестных команд"""
    await quick_reply(
        message,
        "🤔 <b>Неизвестная команда</b>\n\n"
        "Используйте меню или команды:\n"
        "/start - главное меню\n"
        "/myrole - моя роль\n"
        "/help - помощь"
    )

# ========== ЗАПУСК ==========

async def on_startup():
    """Запуск бота"""
    try:
        await db.connect()
        await db.add_test_data()
        
        # Создаем администратора (ЗАМЕНИТЕ ID)
        ADMIN_ID = 1079922982  # <-- ВАШ TELEGRAM ID
        await db.register_user(
            ADMIN_ID,
            "Администратор Бота",
            role='botadmin'
        )
        
        logger.info("✅ Бот готов!")
        logger.info(f"👑 Администратор: ID {ADMIN_ID}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

async def main():
    """Основная функция"""
    await on_startup()
    
    try:
        logger.info("🚀 Запуск бота...")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())

