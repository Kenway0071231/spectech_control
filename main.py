import os
import logging
import asyncio
from typing import Dict, List
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ContentType, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

from database import db, ROLES

# ========== НАСТРОЙКА ==========
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Оптимизация для быстрых ответов
bot = Bot(
    token=os.getenv('BOT_TOKEN'),
    default=DefaultBotProperties(parse_mode="HTML")
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== СОСТОЯНИЯ ДЛЯ РАЗНЫХ РОЛЕЙ ==========

# Состояния для водителя
class DriverStates(StatesGroup):
    choosing_equipment = State()
    safety_instruction = State()
    pre_inspection = State()
    waiting_for_photos = State()

# Состояния для директора/начальника парка
class AdminStates(StatesGroup):
    waiting_for_new_username = State()
    waiting_for_new_role = State()
    waiting_for_equipment_name = State()
    waiting_for_equipment_model = State()
    waiting_for_equipment_vin = State()
    waiting_for_organization_name = State()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

async def typing_action(chat_id: int):
    """Показывает 'печатает...' для быстрого отклика"""
    try:
        await bot.send_chat_action(chat_id, "typing")
        await asyncio.sleep(0.1)
    except:
        pass

async def quick_reply(message: types.Message, text: str, **kwargs):
    """Быстрый ответ пользователю"""
    await typing_action(message.chat.id)
    return await message.answer(text, **kwargs)

def get_role_keyboard(user_role: str, has_active_shift: bool = False) -> types.ReplyKeyboardMarkup:
    """Генерирует клавиатуру в зависимости от роли"""
    
    keyboards = {
        'botadmin': [
            [types.KeyboardButton(text="👑 Панель администратора")],
            [types.KeyboardButton(text="🏢 Организации")],
            [types.KeyboardButton(text="👥 Все пользователи")],
            [types.KeyboardButton(text="➕ Назначить директора")],
            [types.KeyboardButton(text="📊 Статистика")],
            [types.KeyboardButton(text="⚙️ Настройки")]
        ],
        
        'director': [
            [types.KeyboardButton(text="👨‍💼 Панель директора")],
            [types.KeyboardButton(text="🏢 Моя организация")],
            [types.KeyboardButton(text="🚜 Автопарк")],
            [types.KeyboardButton(text="👥 Сотрудники")],
            [types.KeyboardButton(text="➕ Назначить начальника парка")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="📊 Отчеты")]
        ],
        
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Панель начальника парка")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="📋 Активные смены")]
        ],
        
        'driver': []
    }
    
    # Для водителя меняем меню в зависимости от состояния смены
    if user_role == 'driver':
        if has_active_shift:
            buttons = [
                [types.KeyboardButton(text="⏹️ Завершить смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="📸 Мои фото")],
                [types.KeyboardButton(text="ℹ️ Информация")]
            ]
        else:
            buttons = [
                [types.KeyboardButton(text="🚛 Начать смену")],
                [types.KeyboardButton(text="📋 Мои смены")],
                [types.KeyboardButton(text="ℹ️ Информация")]
            ]
        keyboards['driver'] = buttons
    
    return types.ReplyKeyboardMarkup(
        keyboard=keyboards[user_role],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

async def get_user_menu(message: types.Message) -> types.ReplyKeyboardMarkup:
    """Получает меню для текущего пользователя"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    # Проверяем активную смену (только для водителей)
    has_active_shift = False
    if user_role == 'driver':
        active_shift = await db.get_active_shift(user_id)
        has_active_shift = bool(active_shift)
    
    return get_role_keyboard(user_role, has_active_shift)

# ========== КОМАНДА СТАРТ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Главное меню в зависимости от роли"""
    await typing_action(message.chat.id)
    
    user_id = message.from_user.id
    user_info = await db.get_user_info(user_id)
    
    # Регистрируем пользователя, если его нет
    if not user_info:
        await db.register_user(
            user_id,
            f"{message.from_user.first_name} {message.from_user.last_name or ''}",
            role='driver'
        )
        user_info = await db.get_user_info(user_id)
    
    user_role = user_info['role']
    role_name = ROLES.get(user_role, {}).get('name', 'Неизвестно')
    
    welcome_texts = {
        'botadmin': f"👑 <b>Добро пожаловать, Администратор!</b>\n\n",
        'director': f"👨‍💼 <b>Добро пожаловать, Директор!</b>\n\n",
        'fleetmanager': f"👷 <b>Добро пожаловать, Начальник парка!</b>\n\n",
        'driver': f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
    }
    
    welcome = welcome_texts.get(user_role, f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n")
    
    await quick_reply(
        message,
        f"{welcome}"
        f"<b>Роль:</b> {role_name}\n"
        f"<b>ID:</b> {user_id}\n\n"
        f"Используйте меню ниже для работы:",
        reply_markup=await get_user_menu(message)
    )

# ========== МЕНЮ АДМИНИСТРАТОРА БОТА ==========

@dp.message(F.text == "👑 Панель администратора")
async def botadmin_panel(message: types.Message):
    """Панель администратора бота"""
    user_role = await db.get_user_role(message.from_user.id)
    
    if user_role != 'botadmin':
        await quick_reply(message, "⛔ Доступ только для администратора бота!")
        return
    
    # Статистика
    users = await db.get_users_by_role('director')
    organizations = []
    
    await quick_reply(
        message,
        "👑 <b>Панель администратора бота</b>\n\n"
        "<b>Статистика:</b>\n"
        f"• Директоров: {len(users)}\n"
        f"• Организаций: {len(organizations)}\n"
        f"• Всего пользователей: ...\n\n"
        "<b>Доступные действия:</b>\n"
        "1. Назначить директора\n"
        "2. Просмотреть все организации\n"
        "3. Управлять пользователями\n"
        "4. Системные настройки"
    )

@dp.message(F.text == "➕ Назначить директора")
async def assign_director_start(message: types.Message, state: FSMContext):
    """Начало процесса назначения директора"""
    user_role = await db.get_user_role(message.from_user.id)
    
    if user_role != 'botadmin':
        await quick_reply(message, "⛔ Доступ только для администратора бота!")
        return
    
    await quick_reply(
        message,
        "👨‍💼 <b>Назначение директора</b>\n\n"
        "Для назначения директора компании:\n"
        "1. Попросите пользователя написать боту /start\n"
        "2. Получите его Telegram ID\n"
        "3. Используйте команду:\n"
        "<code>/setrole ID director</code>\n\n"
        "<b>Пример:</b>\n"
        "<code>/setrole 123456789 director</code>"
    )

@dp.message(F.text == "👥 Все пользователи")
async def show_all_users(message: types.Message):
    """Показывает всех пользователей"""
    user_role = await db.get_user_role(message.from_user.id)
    
    if user_role != 'botadmin':
        await quick_reply(message, "⛔ Доступ только для администратора бота!")
        return
    
    # Получаем пользователей по ролям
    text = "👥 <b>Все пользователи</b>\n\n"
    
    for role_key, role_info in ROLES.items():
        users = await db.get_users_by_role(role_key)
        if users:
            text += f"<b>{role_info['name']}:</b> {len(users)} чел.\n"
            for user in users[:3]:  # Показываем только первых 3
                text += f"• {user['full_name']} (ID: {user['telegram_id']})\n"
            if len(users) > 3:
                text += f"• ... и ещё {len(users) - 3}\n"
            text += "\n"
    
    await quick_reply(message, text)

# ========== МЕНЮ ДИРЕКТОРА ==========

@dp.message(F.text == "👨‍💼 Панель директора")
async def director_panel(message: types.Message):
    """Панель директора компании"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role != 'director':
        await quick_reply(message, "⛔ Доступ только для директора!")
        return
    
    # Получаем информацию об организации
    org_id = await db.get_user_organization(user_id)
    org_info = await db.get_organization_info(org_id) if org_id else None
    
    # Статистика
    fleetmanagers = await db.get_users_by_role('fleetmanager', org_id)
    drivers = await db.get_users_by_role('driver', org_id)
    equipment = await db.get_equipment_list(org_id)
    shifts = await db.get_organization_shifts(org_id, 5) if org_id else []
    
    org_name = org_info['name'] if org_info else "Организация не создана"
    
    text = (
        f"👨‍💼 <b>Панель директора</b>\n\n"
        f"<b>Организация:</b> {org_name}\n"
        f"<b>ID организации:</b> {org_id or 'нет'}\n\n"
        f"<b>Статистика:</b>\n"
        f"• Начальников парка: {len(fleetmanagers)}\n"
        f"• Водителей: {len(drivers)}\n"
        f"• Техники: {len(equipment)}\n"
        f"• Последние смены: {len(shifts)}\n\n"
        f"<b>Управление:</b>\n"
        f"1. Создать/изменить организацию\n"
        f"2. Назначить начальника парка\n"
        f"3. Назначить водителя\n"
        f"4. Просмотреть автопарк\n"
        f"5. Смотреть отчеты"
    )
    
    await quick_reply(message, text)

@dp.message(F.text == "➕ Назначить начальника парка")
async def assign_fleetmanager_start(message: types.Message, state: FSMContext):
    """Начало назначения начальника парка"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role != 'director':
        await quick_reply(message, "⛔ Доступ только для директора!")
        return
    
    org_id = await db.get_user_organization(user_id)
    if not org_id:
        await quick_reply(message, "❌ Сначала создайте организацию!")
        return
    
    await quick_reply(
        message,
        "👷 <b>Назначение начальника парка</b>\n\n"
        "Для назначения начальника парка:\n"
        "1. Попросите пользователя написать боту /start\n"
        "2. Получите его Telegram ID\n"
        "3. Используйте команду:\n"
        f"<code>/setrole ID fleetmanager {org_id}</code>\n\n"
        "<b>Пример:</b>\n"
        f"<code>/setrole 987654321 fleetmanager {org_id}</code>"
    )

@dp.message(F.text == "➕ Назначить водителя")
async def assign_driver_start(message: types.Message, state: FSMContext):
    """Начало назначения водителя"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = await db.get_user_organization(user_id)
    if not org_id:
        await quick_reply(message, "❌ Сначала создайте организацию!")
        return
    
    role_name = "директора" if user_role == 'director' else "начальника парка"
    
    await quick_reply(
        message,
        f"🚛 <b>Назначение водителя ({role_name})</b>\n\n"
        "Для назначения водителя:\n"
        "1. Попросите пользователя написать боту /start\n"
        "2. Получите его Telegram ID\n"
        "3. Используйте команду:\n"
        f"<code>/setrole ID driver {org_id}</code>\n\n"
        "<b>Пример:</b>\n"
        f"<code>/setrole 555555555 driver {org_id}</code>"
    )

@dp.message(F.text == "🏢 Моя организация")
async def show_organization(message: types.Message):
    """Показывает информацию об организации"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = await db.get_user_organization(user_id)
    
    if not org_id:
        # Предлагаем создать организацию
        if user_role == 'director':
            await quick_reply(
                message,
                "🏢 <b>Создание организации</b>\n\n"
                "У вас ещё нет организации.\n"
                "Чтобы создать организацию, используйте команду:\n"
                "<code>/createorg Название компании</code>\n\n"
                "<b>Пример:</b>\n"
                "<code>/createorg ООО 'СпецТех'</code>"
            )
        else:
            await quick_reply(message, "❌ Вы не привязаны к организации. Обратитесь к директору.")
        return
    
    org_info = await db.get_organization_info(org_id)
    users = await db.get_users_in_organization(org_id)
    equipment = await db.get_equipment_list(org_id)
    
    text = (
        f"🏢 <b>Организация: {org_info['name']}</b>\n\n"
        f"<b>ID:</b> {org_id}\n"
        f"<b>Создана:</b> {org_info['created_at'][:10]}\n\n"
        f"<b>Сотрудники ({len(users)}):</b>\n"
    )
    
    # Группируем по ролям
    roles_count = {}
    for user in users:
        role = user['role']
        roles_count[role] = roles_count.get(role, 0) + 1
    
    for role_key, count in roles_count.items():
        role_name = ROLES.get(role_key, {}).get('name', role_key)
        text += f"• {role_name}: {count} чел.\n"
    
    text += f"\n<b>Техника ({len(equipment)}):</b>\n"
    
    status_count = {}
    for eq in equipment:
        status = eq[3]  # status на позиции 3
        status_count[status] = status_count.get(status, 0) + 1
    
    for status, count in status_count.items():
        text += f"• {status}: {count} ед.\n"
    
    await quick_reply(message, text)

# ========== МЕНЮ НАЧАЛЬНИКА ПАРКА ==========

@dp.message(F.text == "👷 Панель начальника парка")
async def fleetmanager_panel(message: types.Message):
    """Панель начальника парка"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role != 'fleetmanager':
        await quick_reply(message, "⛔ Доступ только для начальника парка!")
        return
    
    org_id = await db.get_user_organization(user_id)
    
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации. Обратитесь к директору.")
        return
    
    # Статистика
    drivers = await db.get_users_by_role('driver', org_id)
    equipment = await db.get_equipment_list(org_id)
    active_shifts = await db.get_organization_shifts(org_id)
    active_shifts = [s for s in active_shifts if s['status'] == 'active']
    
    await quick_reply(
        message,
        "👷 <b>Панель начальника парка</b>\n\n"
        f"<b>Статистика:</b>\n"
        f"• Водителей: {len(drivers)}\n"
        f"• Техники: {len(equipment)}\n"
        f"• Активных смен: {len(active_shifts)}\n\n"
        f"<b>Управление:</b>\n"
        f"1. Просмотреть технику\n"
        f"2. Добавить технику\n"
        f"3. Просмотреть водителей\n"
        f"4. Назначить водителя\n"
        f"5. Активные смены\n"
        f"6. Отчеты по технике"
    )

@dp.message(F.text == "🚜 Техника")
async def show_equipment(message: types.Message):
    """Показывает технику организации"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = await db.get_user_organization(user_id)
    
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации.")
        return
    
    equipment = await db.get_equipment_list(org_id)
    
    if not equipment:
        await quick_reply(
            message,
            "🚜 <b>Техника организации</b>\n\n"
            "Техники пока нет.\n"
            "Добавьте технику через меню."
        )
        return
    
    text = f"🚜 <b>Техника организации ({len(equipment)} ед.)</b>\n\n"
    
    for i, eq in enumerate(equipment[:10], 1):  # Показываем первые 10
        eq_id, name, model, status = eq
        status_icon = "🟢" if status == 'active' else "🔴" if status == 'broken' else "🟡"
        text += f"{status_icon} <b>{name}</b> ({model})\n"
        text += f"   ID: {eq_id} | Статус: {status}\n\n"
    
    if len(equipment) > 10:
        text += f"... и ещё {len(equipment) - 10} единиц техники"
    
    await quick_reply(message, text)

@dp.message(F.text == "➕ Добавить технику")
async def add_equipment_start(message: types.Message, state: FSMContext):
    """Начало добавления техники"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = await db.get_user_organization(user_id)
    
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации.")
        return
    
    await quick_reply(
        message,
        "➕ <b>Добавление техники</b>\n\n"
        "Для добавления техники используйте команду:\n"
        "<code>/addeq Название Модель VIN</code>\n\n"
        "<b>Пример:</b>\n"
        "<code>/addeq Экскаватор CAT-320 CAT123456789</code>\n\n"
        "<i>VIN должен быть уникальным</i>"
    )

# ========== МЕНЮ ВОДИТЕЛЯ (существующий функционал) ==========

@dp.message(F.text == "🚛 Начать смену")
async def start_shift_process(message: types.Message, state: FSMContext):
    """Начинает процесс начала смены"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role != 'driver':
        await quick_reply(message, "⛔ Только водители могут начинать смены!")
        return
    
    org_id = await db.get_user_organization(user_id)
    
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации. Обратитесь к начальнику парка.")
        return
    
    equipment = await db.get_equipment_list(org_id)
    
    if not equipment:
        await quick_reply(message, "❌ В вашей организации нет техники.")
        return
    
    # Фильтруем только активную технику
    active_equipment = [eq for eq in equipment if eq[3] == 'active']
    
    if not active_equipment:
        await quick_reply(message, "❌ Нет доступной активной техники.")
        return
    
    keyboard = []
    for eq in active_equipment[:5]:  # Ограничиваем 5 элементами
        eq_id, name, model, status = eq
        keyboard.append([types.KeyboardButton(text=f"🚜 {name}")])
    
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await quick_reply(
        message,
        "🚛 <b>Выберите технику:</b>\n\n"
        f"Доступно техники: {len(active_equipment)} ед.\n"
        "Нажмите на нужную технику ниже:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=keyboard,
            resize_keyboard=True
        )
    )
    
    await state.update_data(equipment_list=active_equipment, org_id=org_id)
    await state.set_state(DriverStates.choosing_equipment)

# [Остальные обработчики для водителя остаются аналогичными предыдущей версии,
#  но с учетом organization_id]

# ========== КОМАНДЫ ДЛЯ УПРАВЛЕНИЯ РОЛЯМИ ==========

@dp.message(Command("setrole"))
async def set_role_command(message: types.Message):
    """Команда для установки роли пользователя"""
    try:
        parts = message.text.split()
        
        if len(parts) < 3:
            await quick_reply(
                message,
                "❌ <b>Неверный формат команды</b>\n\n"
                "<b>Использование:</b>\n"
                "<code>/setrole USER_ID ROLE [ORG_ID]</code>\n\n"
                "<b>Примеры:</b>\n"
                "<code>/setrole 123456789 director</code>\n"
                "<code>/setrole 987654321 driver 1</code>"
            )
            return
        
        target_id = int(parts[1])
        new_role = parts[2].lower()
        org_id = int(parts[3]) if len(parts) > 3 else None
        
        # Проверяем существование роли
        if new_role not in ROLES:
            await quick_reply(
                message,
                f"❌ <b>Неизвестная роль:</b> {new_role}\n\n"
                f"<b>Доступные роли:</b>\n"
                f"{', '.join(ROLES.keys())}"
            )
            return
        
        # Меняем роль
        success = await db.change_user_role(
            telegram_id=target_id,
            new_role=new_role,
            changed_by=message.from_user.id,
            organization_id=org_id
        )
        
        if success:
            role_name = ROLES[new_role]['name']
            await quick_reply(
                message,
                f"✅ <b>Роль успешно изменена!</b>\n\n"
                f"<b>Пользователь:</b> {target_id}\n"
                f"<b>Новая роль:</b> {role_name}\n"
                f"<b>Организация:</b> {org_id or 'не указана'}"
            )
            
            # Уведомляем пользователя
            try:
                await bot.send_message(
                    target_id,
                    f"🎉 <b>Ваша роль изменена!</b>\n\n"
                    f"Вам назначена роль: <b>{role_name}</b>\n"
                    f"Назначил: {message.from_user.full_name}\n\n"
                    f"Перезапустите бота командой /start"
                )
            except:
                pass
        else:
            await quick_reply(
                message,
                f"❌ <b>Не удалось изменить роль</b>\n\n"
                f"Возможные причины:\n"
                f"1. Нет прав на назначение этой роли\n"
                f"2. Пользователь не найден\n"
                f"3. Ошибка базы данных"
            )
            
    except ValueError:
        await quick_reply(message, "❌ Неверный формат ID. ID должен быть числом.")
    except Exception as e:
        logger.error(f"Ошибка в setrole: {e}")
        await quick_reply(message, f"❌ Ошибка: {str(e)}")

@dp.message(Command("createorg"))
async def create_organization_command(message: types.Message):
    """Создает организацию"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role != 'director':
        await quick_reply(message, "⛔ Только директора могут создавать организации!")
        return
    
    parts = message.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await quick_reply(
            message,
            "❌ <b>Неверный формат команды</b>\n\n"
            "<b>Использование:</b>\n"
            "<code>/createorg Название организации</code>\n\n"
            "<b>Пример:</b>\n"
            "<code>/createorg ООО 'СпецТех Север'</code>"
        )
        return
    
    org_name = parts[1]
    
    # Проверяем, нет ли уже организации у директора
    existing_org = await db.get_user_organization(user_id)
    if existing_org:
        await quick_reply(
            message,
            f"⚠️ <b>У вас уже есть организация!</b>\n\n"
            f"Используйте команду /myorg для просмотра\n"
            f"или обратитесь к администратору для изменения."
        )
        return
    
    # Создаем организацию
    org_id = await db.create_organization(org_name, user_id)
    
    await quick_reply(
        message,
        f"✅ <b>Организация создана!</b>\n\n"
        f"<b>Название:</b> {org_name}\n"
        f"<b>ID организации:</b> {org_id}\n"
        f"<b>Директор:</b> {message.from_user.full_name}\n\n"
        f"Теперь вы можете:\n"
        f"1. Назначать начальников парка\n"
        f"2. Назначать водителей\n"
        f"3. Добавлять технику\n\n"
        f"Используйте меню директора для управления."
    )

@dp.message(Command("addeq"))
async def add_equipment_command(message: types.Message):
    """Добавляет технику"""
    user_id = message.from_user.id
    user_role = await db.get_user_role(user_id)
    
    if user_role not in ['director', 'fleetmanager']:
        await quick_reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = await db.get_user_organization(user_id)
    
    if not org_id:
        await quick_reply(message, "❌ Вы не привязаны к организации.")
        return
    
    parts = message.text.split(maxsplit=3)
    
    if len(parts) < 4:
        await quick_reply(
            message,
            "❌ <b>Неверный формат команды</b>\n\n"
            "<b>Использование:</b>\n"
            "<code>/addeq Название Модель VIN</code>\n\n"
            "<b>Пример:</b>\n"
            "<code>/addeq Экскаватор CAT-320 CAT123456789</code>"
        )
        return
    
    name = parts[1]
    model = parts[2]
    vin = parts[3]
    
    try:
        eq_id = await db.add_equipment(name, model, vin, org_id, user_id)
        
        await quick_reply(
            message,
            f"✅ <b>Техника добавлена!</b>\n\n"
            f"<b>Название:</b> {name}\n"
            f"<b>Модель:</b> {model}\n"
            f"<b>VIN:</b> {vin}\n"
            f"<b>ID техники:</b> {eq_id}\n"
            f"<b>Организация:</b> {org_id}\n\n"
            f"Техника доступна для начала смены."
        )
    except Exception as e:
        logger.error(f"Ошибка добавления техники: {e}")
        await quick_reply(
            message,
            f"❌ <b>Ошибка добавления техники</b>\n\n"
            f"Возможные причины:\n"
            f"1. VIN уже существует\n"
            f"2. Ошибка базы данных\n"
            f"3. {str(e)}"
        )

@dp.message(Command("myrole"))
async def show_my_role(message: types.Message):
    """Показывает текущую роль пользователя"""
    user_id = message.from_user.id
    user_info = await db.get_user_info(user_id)
    
    if not user_info:
        await quick_reply(message, "❌ Вы не зарегистрированы в системе.")
        return
    
    role_key = user_info['role']
    role_info = ROLES.get(role_key, {})
    
    text = (
        f"👤 <b>Ваш профиль</b>\n\n"
        f"<b>ID:</b> {user_id}\n"
        f"<b>Имя:</b> {user_info['full_name']}\n"
        f"<b>Роль:</b> {role_info.get('name', 'Неизвестно')}\n"
        f"<b>Уровень доступа:</b> {role_info.get('level', 0)}/100\n"
    )
    
    if user_info['organization_id']:
        org_info = await db.get_organization_info(user_info['organization_id'])
        if org_info:
            text += f"<b>Организация:</b> {org_info['name']}\n"
    
    if user_info['assigned_by']:
        assigner_info = await db.get_user_info(user_info['assigned_by'])
        if assigner_info:
            text += f"<b>Назначил:</b> {assigner_info['full_name']}\n"
    
    text += f"\n<b>Дата регистрации:</b> {user_info['created_at'][:10]}"
    
    await quick_reply(message, text)

# ========== ОБРАБОТКА ОШИБОК ==========

@dp.message()
async def handle_other_messages(message: types.Message):
    """Обработка всех остальных сообщений"""
    user_role = await db.get_user_role(message.from_user.id)
    
    help_texts = {
        'botadmin': (
            "👑 <b>Команды администратора:</b>\n"
            "/setrole ID РОЛЬ [ORG] - назначить роль\n"
            "/myrole - показать свою роль\n"
            "/createorg НАЗВАНИЕ - создать организацию\n"
            "/addeq НАЗВ МОДЕЛЬ VIN - добавить технику\n\n"
            "<b>Примеры:</b>\n"
            "<code>/setrole 123456789 director</code>\n"
            "<code>/createorg ООО 'СпецТех'</code>"
        ),
        'director': (
            "👨‍💼 <b>Команды директора:</b>\n"
            "/setrole ID РОЛЬ [ORG] - назначить роль\n"
            "/myrole - показать свою роль\n"
            "/createorg НАЗВАНИЕ - создать организацию\n"
            "/addeq НАЗВ МОДЕЛЬ VIN - добавить технику\n\n"
            "<b>Примеры:</b>\n"
            "<code>/setrole 987654321 fleetmanager 1</code>\n"
            "<code>/addeq Экскаватор CAT-320 CAT123</code>"
        ),
        'fleetmanager': (
            "👷 <b>Команды начальника парка:</b>\n"
            "/setrole ID driver [ORG] - назначить водителя\n"
            "/myrole - показать свою роль\n"
            "/addeq НАЗВ МОДЕЛЬ VIN - добавить технику\n\n"
            "<b>Примеры:</b>\n"
            "<code>/setrole 555555555 driver 1</code>\n"
            "<code>/addeq Бульдозер Komatsu KOM123</code>"
        ),
        'driver': (
            "🚛 <b>Команды водителя:</b>\n"
            "/myrole - показать свою роль\n"
            "/start - главное меню\n\n"
            "Используйте кнопки меню для работы."
        )
    }
    
    help_text = help_texts.get(user_role, 
        "🤖 <b>Общие команды:</b>\n"
        "/start - главное меню\n"
        "/myrole - показать свою роль\n"
        "/help - помощь"
    )
    
    await quick_reply(
        message,
        f"🤔 <b>Используйте меню или команды</b>\n\n{help_text}"
    )

# ========== ЗАПУСК БОТА ==========

async def on_startup():
    """Запуск бота с созданием администратора"""
    try:
        await db.connect()
        await db.add_test_data()
        
        # Создаем администратора бота (ЗАМЕНИТЕ ID НА СВОЙ)
        ADMIN_ID = 1079922982  # <-- ВАШ TELEGRAM ID
        await db.register_user(
            ADMIN_ID,
            "Администратор Бота",
            role='botadmin'
        )
        
        logger.info("✅ Бот готов к работе!")
        logger.info(f"👑 Администратор создан: ID {ADMIN_ID}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

async def main():
    """Основная функция"""
    await on_startup()
    
    try:
        logger.info("🚀 Запускаю бота с системой ролей...")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
