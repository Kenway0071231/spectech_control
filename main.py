import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
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
    waiting_for_org_name = State()

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
            [types.KeyboardButton(text="📊 Отчеты")]
        ],
        
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Управление парком")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить водителя")]
        ],
        
        'driver': [
            [types.KeyboardButton(text="🚛 Начать смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="ℹ️ Информация")]
        ]
    }
    
    return types.ReplyKeyboardMarkup(
        keyboard=keyboards.get(role, keyboards['driver']),
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

def get_cancel_keyboard():
    """Клавиатура с кнопкой отмены"""
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

# ========== КОМАНДА СТАРТ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Главное меню для всех"""
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
    
    await reply(
        message,
        f"🤖 <b>ТехКонтроль Бот</b>\n\n"
        f"<b>Роль:</b> {role_names.get(role, '👤 Пользователь')}\n"
        f"<b>ID:</b> {message.from_user.id}\n"
        f"<b>Имя:</b> {message.from_user.full_name}\n\n"
        f"Выберите действие из меню:",
        reply_markup=get_main_keyboard(role)
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

# ========== ОБРАБОТЧИКИ ВОДИТЕЛЯ ==========

@dp.message(F.text == "🚛 Начать смену")
async def start_shift(message: types.Message):
    """Начинает смену водителя"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут начинать смены!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации! Обратитесь к начальнику парка.")
        return
    
    await reply(
        message,
        "🚛 <b>Начало смены</b>\n\n"
        "Эта функция в разработке.\n"
        "Скоро здесь можно будет:\n"
        "• Выбирать технику\n"
        "• Проходить инструктаж\n"
        "• Делать фото осмотра\n\n"
        "А пока можете просмотреть свои смены."
    )

@dp.message(F.text == "📋 Мои смены")
async def my_shifts(message: types.Message):
    """Показывает смены водителя"""
    await reply(
        message,
        "📋 <b>Мои смены</b>\n\n"
        "История смен в разработке.\n"
        "Скоро здесь появится:\n"
        "• История всех смен\n"
        "• Статистика\n"
        "• Отчеты\n\n"
        "А пока можете начать новую смену!"
    )

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
    await cmd_start(message)

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
async def create_organization_cmd(message: types.Message, state: FSMContext):
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
        "/help - Эта справка\n\n"
        "<b>Система ролей:</b>\n"
        "• Администратор - полный доступ\n"
        "• Директор - управление организацией\n"
        "• Начальник парка - управление техникой\n"
        "• Водитель - работа со сменами\n\n"
        "<b>Доступные функции:</b>\n"
        "• Учет техники\n"
        "• Назначение ролей\n"
        "• Просмотр статистики\n"
        "• Управление организациями"
    )

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
