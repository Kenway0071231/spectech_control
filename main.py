import os
import logging
import asyncio
import json
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
    
    # Для управления организацией
    waiting_for_org_name = State()
    waiting_for_edit_org_name = State()
    waiting_for_driver_stats_days = State()
    
    # Для отчетов
    waiting_for_report_type = State()
    waiting_for_report_period = State()
    
    # Для поиска
    waiting_for_search_query = State()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

async def log_user_action(user_id, action_type, details=""):
    """Логирует действие пользователя"""
    try:
        await db.log_action(user_id, action_type, details)
    except Exception as e:
        logger.error(f"Ошибка логирования действия: {e}")

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

def get_main_keyboard(role, has_organization=False):
    """Генерирует клавиатуру в зависимости от роли"""
    
    keyboards = {
        'botadmin': [
            [types.KeyboardButton(text="👑 Админ-панель")],
            [types.KeyboardButton(text="🏢 Все организации")],
            [types.KeyboardButton(text="👥 Все пользователи")],
            [types.KeyboardButton(text="📊 Статистика")],
            [types.KeyboardButton(text="➕ Назначить роль")],
            [types.KeyboardButton(text="🔔 Отправить уведомление")],
            [types.KeyboardButton(text="📋 Журнал действий")],
            [types.KeyboardButton(text="🔍 Поиск")]
        ],
        
        'director': [
            [types.KeyboardButton(text="👨‍💼 Моя организация")],
            [types.KeyboardButton(text="🚜 Автопарк")],
            [types.KeyboardButton(text="👥 Сотрудники")],
            [types.KeyboardButton(text="📈 Статистика организации")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Добавить ТО")],
            [types.KeyboardButton(text="➕ Назначить роль")],
            [types.KeyboardButton(text="📊 Отчеты")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")],
            [types.KeyboardButton(text="📅 Ближайшие ТО")],
            [types.KeyboardButton(text="⚙️ Настройки организации")],
            [types.KeyboardButton(text="🔍 Поиск")]
        ],
        
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Управление парком")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="📊 Статистика водителей")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Добавить ТО")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")],
            [types.KeyboardButton(text="📅 Ближайшие ТО")],
            [types.KeyboardButton(text="🔍 Поиск")]
        ],
        
        'driver': [
            [types.KeyboardButton(text="🚛 Начать смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="📊 Моя статистика")],
            [types.KeyboardButton(text="✅ Закончить смену")],
            [types.KeyboardButton(text="🚜 Моя техника")],
            [types.KeyboardButton(text="ℹ️ Информация")]
        ]
    }
    
    # Для директора без организации показываем упрощенное меню
    if role == 'director' and not has_organization:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="🏢 Создать организацию")],
                [types.KeyboardButton(text="ℹ️ Информация")]
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие..."
        )
    
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

def get_period_keyboard():
    """Клавиатура для выбора периода"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="📅 За сегодня")],
            [types.KeyboardButton(text="📅 За неделю")],
            [types.KeyboardButton(text="📅 За месяц")],
            [types.KeyboardButton(text="📅 За 3 месяца")],
            [types.KeyboardButton(text="📅 За год")],
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
        await log_user_action(message.from_user.id, "registration", f"New user: {message.from_user.full_name}")
    
    role = user['role']
    has_organization = bool(user.get('organization_id'))
    
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
                reply_markup=get_main_keyboard(role, has_organization)
            )
            return
    
    welcome_text = f"🤖 <b>ТехКонтроль Бот</b>\n\n"
    
    if role == 'director' and not has_organization:
        welcome_text += f"<b>Роль:</b> {role_names.get(role, '👤 Пользователь')}\n"
        welcome_text += "<b>Статус:</b> У вас ещё нет организации\n\n"
        welcome_text += "📌 <b>Для начала работы создайте организацию:</b>"
    else:
        welcome_text += f"<b>Роль:</b> {role_names.get(role, '👤 Пользователь')}\n"
        welcome_text += f"<b>ID:</b> {message.from_user.id}\n"
        welcome_text += f"<b>Имя:</b> {message.from_user.full_name}\n\n"
        welcome_text += f"Выберите действие из меню:"
    
    await reply(
        message,
        welcome_text,
        reply_markup=get_main_keyboard(role, has_organization)
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
        reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
    )

# ========== КОМАНДА ПОМОЩЬ ==========
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
        "<b>Основные функции:</b>\n"
        "• Создание и управление организациями\n"
        "• Учет техники и ТО\n"
        "• Начало и завершение смен\n"
        "• Фото осмотра техники\n"
        "• Статистика и отчеты\n"
        "• Журнал действий"
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
        "• Отправка уведомлений\n"
        "• Просмотр журнала действий"
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
        text += f"• {role}: {count} чел.\n"
    
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
        await reply(message, "❌ Отправка уведомления отменена", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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

@dp.message(F.text == "📋 Журнал действий")
async def action_logs(message: types.Message):
    """Показывает журнал действий"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    # Получаем последние 20 действий
    actions = await db.get_recent_actions(limit=20)
    
    if not actions:
        await reply(message, "📋 <b>Журнал действий пуст</b>")
        return
    
    text = "📋 <b>Журнал действий (последние 20)</b>\n\n"
    
    for action in actions:
        time = datetime.strptime(action['created_at'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
        user_name = action['full_name']
        role = action['role']
        
        role_emoji = {
            'botadmin': '👑',
            'director': '👨‍💼',
            'fleetmanager': '👷',
            'driver': '🚛'
        }.get(role, '👤')
        
        text += f"<b>{time}</b> {role_emoji} {user_name}\n"
        text += f"   {action['action_type']}\n"
        if action['details']:
            text += f"   <i>{action['details'][:50]}</i>\n"
        text += "\n"
    
    await reply(message, text)

@dp.message(F.text == "🔍 Поиск")
async def search_start(message: types.Message, state: FSMContext):
    """Начинает поиск"""
    user = await db.get_user(message.from_user.id)
    
    # Проверяем права
    if user['role'] == 'driver':
        await reply(message, "⛔ У водителей нет доступа к поиску!")
        return
    
    await reply(
        message,
        "🔍 <b>Поиск по системе</b>\n\n"
        "Введите запрос для поиска:\n"
        "(можно искать пользователей, технику, организации)",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_search_query)

@dp.message(UserStates.waiting_for_search_query)
async def process_search_query(message: types.Message, state: FSMContext):
    """Обрабатывает поисковый запрос"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Поиск отменен", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    search_query = message.text.strip()
    
    if len(search_query) < 2:
        await reply(message, "❌ Слишком короткий запрос!")
        return
    
    user = await db.get_user(message.from_user.id)
    results_text = f"🔍 <b>Результаты поиска:</b> '{search_query}'\n\n"
    
    # Поиск пользователей
    all_users = await db.get_all_users()
    user_results = []
    
    for u in all_users:
        if (search_query.lower() in u['full_name'].lower() or 
            (u['username'] and search_query.lower() in u['username'].lower()) or
            search_query == str(u['telegram_id'])):
            user_results.append(u)
    
    if user_results:
        results_text += f"👥 <b>Пользователи ({len(user_results)}):</b>\n"
        for u in user_results[:5]:  # Показываем первые 5
            role_names = {
                'botadmin': '👑 Админ',
                'director': '👨‍💼 Директор',
                'fleetmanager': '👷 Нач. парка',
                'driver': '🚛 Водитель'
            }
            results_text += f"• {role_names.get(u['role'], u['role'])} {u['full_name']}"
            if u['username']:
                results_text += f" (@{u['username']})"
            results_text += f" (ID: {u['telegram_id']})\n"
        
        if len(user_results) > 5:
            results_text += f"... и ещё {len(user_results) - 5}\n"
        results_text += "\n"
    
    # Поиск организаций (только для администратора)
    if user['role'] == 'botadmin':
        all_orgs = await db.get_all_organizations()
        org_results = [o for o in all_orgs if search_query.lower() in o['name'].lower()]
        
        if org_results:
            results_text += f"🏢 <b>Организации ({len(org_results)}):</b>\n"
            for o in org_results[:3]:
                results_text += f"• {o['name']} (ID: {o['id']})\n"
            
            if len(org_results) > 3:
                results_text += f"... и ещё {len(org_results) - 3}\n"
            results_text += "\n"
    
    # Поиск техники (для директора и начальника парка)
    if user['role'] in ['director', 'fleetmanager'] and user.get('organization_id'):
        equipment = await db.get_organization_equipment(user['organization_id'])
        eq_results = []
        
        for eq in equipment:
            if (search_query.lower() in eq['name'].lower() or 
                search_query.lower() in eq['model'].lower() or
                search_query.lower() in eq['vin'].lower()):
                eq_results.append(eq)
        
        if eq_results:
            results_text += f"🚜 <b>Техника ({len(eq_results)}):</b>\n"
            for eq in eq_results[:3]:
                results_text += f"• {eq['name']} ({eq['model']}) - VIN: {eq['vin'][:8]}...\n"
            
            if len(eq_results) > 3:
                results_text += f"... и ещё {len(eq_results) - 3}\n"
    
    if results_text == f"🔍 <b>Результаты поиска:</b> '{search_query}'\n\n":
        results_text += "😕 Ничего не найдено."
    
    await reply(message, results_text)
    await state.clear()

# ========== УПРАВЛЕНИЕ ОРГАНИЗАЦИЕЙ ДЛЯ ДИРЕКТОРА ==========

@dp.message(F.text == "🏢 Создать организацию")
async def create_organization_start(message: types.Message, state: FSMContext):
    """Начинает создание организации"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'director':
        await reply(message, "⛔ Только директора могут создавать организации!")
        return
    
    if user.get('organization_id'):
        await reply(message, "⚠️ У вас уже есть организация!")
        return
    
    await reply(
        message,
        "🏢 <b>Создание организации</b>\n\n"
        "Введите название вашей организации:\n\n"
        "<b>Примеры:</b>\n"
        "• ООО 'Моя Компания'\n"
        "• ИП Иванов\n"
        "• Строительная компания 'Проект'",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_org_name)

@dp.message(UserStates.waiting_for_org_name)
async def process_org_name(message: types.Message, state: FSMContext):
    """Обрабатывает ввод названия организации"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message, state)
        return
    
    org_name = message.text.strip()
    
    if len(org_name) < 2:
        await reply(message, "❌ Название организации слишком короткое!")
        return
    
    # Создаем организацию
    org_id, error = await db.create_organization_for_director(message.from_user.id, org_name)
    
    if error:
        await reply(message, f"❌ {error}")
        await state.clear()
        await cmd_start(message, state)
        return
    
    if org_id:
        await log_user_action(message.from_user.id, "organization_created", f"Organization: {org_name}")
        
        await reply(
            message,
            f"✅ <b>Организация создана успешно!</b>\n\n"
            f"<b>Название:</b> {org_name}\n"
            f"<b>ID организации:</b> {org_id}\n\n"
            f"<b>Теперь вы можете:</b>\n"
            "• Добавлять технику\n"
            "• Назначать сотрудников\n"
            "• Управлять автопарком\n"
            "• Создавать ТО\n"
            "• Просматривать статистику"
        )
        
        # Обновляем пользователя чтобы получить актуальные данные
        user = await db.get_user(message.from_user.id)
        await state.clear()
        await cmd_start(message, state)
    else:
        await reply(message, "❌ Ошибка при создании организации!")

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
            "Создайте её через меню '🏢 Создать организацию'."
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
        "• Назначение ролей\n"
        "• Статистика и отчеты"
    )
    
    await reply(message, text)

@dp.message(F.text == "⚙️ Настройки организации")
async def organization_settings(message: types.Message):
    """Настройки организации для директора"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'director':
        await reply(message, "⛔ Доступ только для директора!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(
            message,
            "🏢 <b>У вас нет организации</b>\n\n"
            "Создайте организацию через меню."
        )
        return
    
    org = await db.get_organization(org_id)
    if not org:
        await reply(message, "❌ Организация не найдена!")
        return
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить название", callback_data=f"edit_org_name:{org_id}")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data=f"org_stats:{org_id}")],
            [InlineKeyboardButton(text="📋 Журнал действий", callback_data=f"org_logs:{org_id}")],
        ]
    )
    
    await reply(
        message,
        f"⚙️ <b>Настройки организации</b>\n\n"
        f"<b>Название:</b> {org['name']}\n"
        f"<b>ID:</b> {org_id}\n"
        f"<b>Создана:</b> {org['created_at'][:10]}\n\n"
        f"Выберите действие:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("edit_org_name:"))
async def edit_org_name_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обработка запроса на изменение названия организации"""
    org_id = int(callback.data.split(":")[1])
    
    # Проверяем права
    user = await db.get_user(callback.from_user.id)
    if user['role'] != 'director' or user.get('organization_id') != org_id:
        await callback.answer("⛔ У вас нет прав для этого действия!", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"✏️ <b>Изменение названия организации</b>\n\n"
        f"Введите новое название:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel_edit:{org_id}")]
            ]
        )
    )
    
    await state.update_data(org_id=org_id)
    await state.set_state(UserStates.waiting_for_edit_org_name)
    await callback.answer()

@dp.message(UserStates.waiting_for_edit_org_name)
async def process_edit_org_name(message: types.Message, state: FSMContext):
    """Обрабатывает новое название организации"""
    data = await state.get_data()
    org_id = data.get('org_id')
    
    if not org_id:
        await state.clear()
        await cmd_start(message, state)
        return
    
    new_name = message.text.strip()
    
    if len(new_name) < 2:
        await reply(message, "❌ Название организации слишком короткое!")
        return
    
    # Обновляем название
    success = await db.update_organization_name(org_id, new_name)
    
    if success:
        await log_user_action(message.from_user.id, "organization_renamed", f"New name: {new_name}")
        
        await reply(
            message,
            f"✅ <b>Название организации изменено!</b>\n\n"
            f"<b>Новое название:</b> {new_name}"
        )
    else:
        await reply(message, "❌ Ошибка при изменении названия!")
    
    await state.clear()
    await cmd_start(message, state)

@dp.callback_query(F.data.startswith("cancel_edit:"))
async def cancel_edit_callback(callback: types.CallbackQuery, state: FSMContext):
    """Отмена редактирования"""
    await state.clear()
    await callback.message.edit_text("❌ Изменение названия отменено.")
    await callback.answer()

@dp.callback_query(F.data.startswith("org_stats:"))
async def org_stats_callback(callback: types.CallbackQuery):
    """Показывает статистику организации"""
    org_id = int(callback.data.split(":")[1])
    
    org = await db.get_organization(org_id)
    if not org:
        await callback.answer("❌ Организация не найдена!", show_alert=True)
        return
    
    # Получаем статистику
    stats = await db.get_organization_stats(org_id)
    
    text = f"📊 <b>Статистика организации</b>\n\n"
    text += f"<b>Название:</b> {org['name']}\n\n"
    
    # Сотрудники
    if 'roles' in stats:
        text += "<b>👥 Сотрудники:</b>\n"
        role_names = {
            'director': '👨‍💼 Директор',
            'fleetmanager': '👷 Начальник парка',
            'driver': '🚛 Водитель'
        }
        for role, count in stats['roles'].items():
            text += f"  {role_names.get(role, role)}: {count} чел.\n"
        text += f"  <b>Всего:</b> {sum(stats['roles'].values())} чел.\n\n"
    
    # Техника
    if 'equipment' in stats:
        text += "<b>🚜 Техника:</b>\n"
        for status, count in stats['equipment'].items():
            status_name = {
                'active': '✅ Активная',
                'maintenance': '🔧 На ТО',
                'repair': '🔨 В ремонте',
                'inactive': '❌ Неактивная'
            }.get(status, status)
            text += f"  {status_name}: {count} ед.\n"
        text += f"  <b>Всего:</b> {sum(stats['equipment'].values())} ед.\n\n"
    
    # Активные смены
    text += f"<b>🔄 Активные смены:</b> {stats.get('active_shifts', 0)}\n\n"
    
    # Предстоящие ТО
    text += f"<b>📅 ТО на неделю:</b> {stats.get('weekly_maintenance', 0)}\n\n"
    
    # Последние действия
    recent_actions = await db.get_recent_actions(org_id, limit=5)
    if recent_actions:
        text += "<b>📋 Последние действия:</b>\n"
        for action in recent_actions[:3]:
            time = datetime.strptime(action['created_at'], "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
            user_name = action['full_name'].split()[0]
            text += f"  {time} {user_name}: {action['action_type']}\n"
    
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(F.data.startswith("org_logs:"))
async def org_logs_callback(callback: types.CallbackQuery):
    """Показывает журнал действий организации"""
    org_id = int(callback.data.split(":")[1])
    
    org = await db.get_organization(org_id)
    if not org:
        await callback.answer("❌ Организация не найдена!", show_alert=True)
        return
    
    actions = await db.get_recent_actions(org_id, limit=10)
    
    if not actions:
        text = f"📋 <b>Журнал действий организации</b>\n\n"
        text += f"<b>Название:</b> {org['name']}\n\n"
        text += "Действий пока нет."
    else:
        text = f"📋 <b>Журнал действий организации</b>\n\n"
        text += f"<b>Название:</b> {org['name']}\n\n"
        
        for action in actions:
            time = datetime.strptime(action['created_at'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
            user_name = action['full_name']
            text += f"<b>{time}</b> {user_name}\n"
            text += f"   {action['action_type']}\n"
            if action['details']:
                text += f"   <i>{action['details'][:40]}...</i>\n"
            text += "\n"
    
    await callback.message.edit_text(text)
    await callback.answer()

# ========== ОБРАБОТЧИКИ ДИРЕКТОРА ==========

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

@dp.message(UserStates.waiting_for_equipment_selection)
async def process_maintenance_equipment_selection(message: types.Message, state: FSMContext):
    """Обрабатывает выбор техники для ТО"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "❌ Добавление ТО отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
    await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))

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
        await reply(message, "❌ Начало смены отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "❌ Начало смены отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    if message.text == "✅ Да":
        # Получаем список проверок
        checks = await db.get_daily_checks()
        if not checks:
            await reply(message, "❌ Список проверок не найден")
            await state.clear()
            user = await db.get_user(message.from_user.id)
            await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
            reply_markup=get_main_keyboard('driver', True)
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
    """Начинает процесс завершения смены"""
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
        await reply(message, "❌ Завершение смены отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
    await reply(message, "❌ Смена отменена", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))

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

@dp.message(F.text == "📊 Моя статистика")
async def my_statistics(message: types.Message):
    """Статистика для водителя"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут просматривать свою статистику!")
        return
    
    # Статистика за 30 дней
    stats_30 = await db.get_driver_stats(message.from_user.id, 30)
    # Статистика за 7 дней
    stats_7 = await db.get_driver_stats(message.from_user.id, 7)
    
    text = f"📊 <b>Ваша статистика</b>\n\n"
    
    text += "<b>📅 За последние 7 дней:</b>\n"
    text += f"• Смен: {stats_7.get('shifts_count', 0)}\n"
    text += f"• Средняя продолжительность: {stats_7.get('avg_shift_hours', 0)}ч\n"
    text += f"• Разной техники: {stats_7.get('equipment_used', 0)} ед.\n\n"
    
    text += "<b>📅 За последние 30 дней:</b>\n"
    text += f"• Смен: {stats_30.get('shifts_count', 0)}\n"
    text += f"• Средняя продолжительность: {stats_30.get('avg_shift_hours', 0)}ч\n"
    text += f"• Разной техники: {stats_30.get('equipment_used', 0)} ед.\n\n"
    
    # Получаем последние смены
    shifts = await db.get_shifts_by_driver(message.from_user.id, 3)
    if shifts:
        text += "<b>📋 Последние смены:</b>\n"
        for shift in shifts:
            date = datetime.strptime(shift['start_time'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m")
            status = "🟢" if shift['status'] == 'active' else "✅"
            text += f"{status} {date}: {shift.get('equipment_name', 'Техника')}"
            if shift.get('end_time'):
                end = datetime.strptime(shift['end_time'], "%Y-%m-%d %H:%M:%S")
                start = datetime.strptime(shift['start_time'], "%Y-%m-%d %H:%M:%S")
                hours = round((end - start).total_seconds() / 3600, 1)
                text += f" ({hours}ч)"
            text += "\n"
    
    await reply(message, text)

# ========== УЛУЧШЕННАЯ СТАТИСТИКА ==========

@dp.message(F.text == "📈 Статистика организации")
async def organization_statistics(message: types.Message):
    """Расширенная статистика организации"""
    user = await db.get_user(message.from_user.id)
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    org = await db.get_organization(org_id)
    stats = await db.get_organization_stats(org_id)
    
    # Получаем водителей для статистики
    users = await db.get_users_by_organization(org_id)
    drivers = [u for u in users if u['role'] == 'driver']
    
    text = f"📈 <b>Расширенная статистика</b>\n\n"
    text += f"<b>Организация:</b> {org['name']}\n"
    text += f"<b>Период:</b> последние 30 дней\n\n"
    
    # Основная статистика
    text += f"<b>Основные показатели:</b>\n"
    text += f"• Сотрудников: {sum(stats.get('roles', {}).values())} чел.\n"
    text += f"• Техники: {sum(stats.get('equipment', {}).values())} ед.\n"
    text += f"• Активных смен: {stats.get('active_shifts', 0)}\n"
    text += f"• ТО на неделю: {stats.get('weekly_maintenance', 0)}\n\n"
    
    # Статистика по водителям
    if drivers:
        text += f"<b>Топ водителей (по сменам):</b>\n"
        
        driver_stats = []
        for driver in drivers[:5]:  # Берем первых 5 водителей
            stats_driver = await db.get_driver_stats(driver['telegram_id'], 30)
            driver_stats.append({
                'name': driver['full_name'].split()[0],
                'shifts': stats_driver.get('shifts_count', 0),
                'hours': stats_driver.get('avg_shift_hours', 0),
                'equipment': stats_driver.get('equipment_used', 0)
            })
        
        # Сортируем по количеству смен
        driver_stats.sort(key=lambda x: x['shifts'], reverse=True)
        
        for i, driver in enumerate(driver_stats, 1):
            text += f"{i}. {driver['name']}: {driver['shifts']} смен, {driver['hours']}ч/смена\n"
    
    # Кнопки для детальной статистики
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Детальная статистика", callback_data=f"detail_stats:{org_id}")],
            [InlineKeyboardButton(text="👥 Статистика водителей", callback_data=f"drivers_stats:{org_id}")],
            [InlineKeyboardButton(text="🚜 Статистика техники", callback_data=f"equipment_stats:{org_id}")]
        ]
    )
    
    await reply(message, text, reply_markup=keyboard)

@dp.message(F.text == "📊 Статистика водителей")
async def drivers_statistics(message: types.Message, state: FSMContext):
    """Статистика по водителям"""
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
        "📊 <b>Статистика водителей</b>\n\n"
        "Выберите период для статистики:",
        reply_markup=get_period_keyboard()
    )
    await state.set_state(UserStates.waiting_for_driver_stats_days)

@dp.message(UserStates.waiting_for_driver_stats_days)
async def process_driver_stats_period(message: types.Message, state: FSMContext):
    """Обрабатывает выбор периода для статистики"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message, state)
        return
    
    period_map = {
        "📅 За сегодня": 1,
        "📅 За неделю": 7,
        "📅 За месяц": 30,
        "📅 За 3 месяца": 90,
        "📅 За год": 365
    }
    
    if message.text not in period_map:
        await reply(message, "❌ Пожалуйста, выберите период из списка!")
        return
    
    days = period_map[message.text]
    data = await state.get_data()
    org_id = data.get('org_id')
    
    # Получаем водителей организации
    users = await db.get_users_by_organization(org_id)
    drivers = [u for u in users if u['role'] == 'driver']
    
    if not drivers:
        await reply(message, "❌ В организации нет водителей!")
        await state.clear()
        return
    
    text = f"📊 <b>Статистика водителей за {days} дней</b>\n\n"
    
    driver_stats_list = []
    for driver in drivers:
        stats = await db.get_driver_stats(driver['telegram_id'], days)
        driver_stats_list.append({
            'name': driver['full_name'],
            'shifts': stats.get('shifts_count', 0),
            'avg_hours': stats.get('avg_shift_hours', 0),
            'equipment': stats.get('equipment_used', 0)
        })
    
    # Сортируем по количеству смен
    driver_stats_list.sort(key=lambda x: x['shifts'], reverse=True)
    
    for i, driver in enumerate(driver_stats_list, 1):
        if driver['shifts'] > 0:
            text += f"<b>{i}. {driver['name']}</b>\n"
            text += f"   Смен: {driver['shifts']}\n"
            text += f"   Средняя смена: {driver['avg_hours']}ч\n"
            text += f"   Техники использовано: {driver['equipment']} ед.\n\n"
    
    if all(d['shifts'] == 0 for d in driver_stats_list):
        text += "За выбранный период смен не было.\n"
    
    await reply(message, text)
    await state.clear()

# ========== СИСТЕМА ОТЧЕТОВ ==========

@dp.message(F.text == "📊 Отчеты")
async def reports_menu(message: types.Message, state: FSMContext):
    """Меню отчетов"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'director':
        await reply(message, "⛔ Доступ только для директора!")
        return
    
    await reply(
        message,
        "📊 <b>Система отчетов</b>\n\n"
        "Выберите тип отчета:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="📈 По сменам")],
                [types.KeyboardButton(text="🚜 По технике")],
                [types.KeyboardButton(text="👥 По сотрудникам")],
                [types.KeyboardButton(text="🔧 По ТО")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(UserStates.waiting_for_report_type)

@dp.message(UserStates.waiting_for_report_type)
async def process_report_type(message: types.Message, state: FSMContext):
    """Обрабатывает выбор типа отчета"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message, state)
        return
    
    report_types = ["📈 По сменам", "🚜 По технике", "👥 По сотрудникам", "🔧 По ТО"]
    
    if message.text not in report_types:
        await reply(message, "❌ Пожалуйста, выберите тип отчета из списка!")
        return
    
    await state.update_data(report_type=message.text)
    
    await reply(
        message,
        f"📊 <b>Отчет: {message.text}</b>\n\n"
        f"Выберите период для отчета:",
        reply_markup=get_period_keyboard()
    )
    await state.set_state(UserStates.waiting_for_report_period)

@dp.message(UserStates.waiting_for_report_period)
async def process_report_period(message: types.Message, state: FSMContext):
    """Генерирует отчет"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message, state)
        return
    
    period_map = {
        "📅 За сегодня": 1,
        "📅 За неделю": 7,
        "📅 За месяц": 30,
        "📅 За 3 месяца": 90,
        "📅 За год": 365
    }
    
    if message.text not in period_map:
        await reply(message, "❌ Пожалуйста, выберите период из списка!")
        return
    
    days = period_map[message.text]
    data = await state.get_data()
    report_type = data.get('report_type')
    
    user = await db.get_user(message.from_user.id)
    org_id = user.get('organization_id')
    
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        await state.clear()
        return
    
    org = await db.get_organization(org_id)
    
    text = f"📊 <b>Отчет: {report_type}</b>\n"
    text += f"<b>Период:</b> {message.text}\n"
    text += f"<b>Организация:</b> {org['name']}\n\n"
    
    # Генерация отчета в зависимости от типа
    if report_type == "📈 По сменам":
        # Здесь можно добавить логику для отчета по сменам
        text += "Отчет по сменам в разработке...\n"
        text += "Скоро здесь будет:\n"
        text += "• Количество смен\n"
        text += "• Общее время работы\n"
        text += "• Средняя продолжительность смены\n"
        text += "• Распределение по дням недели\n"
    
    elif report_type == "🚜 По технике":
        text += "Отчет по технике в разработке...\n"
        text += "Скоро здесь будет:\n"
        text += "• Загрузка техники\n"
        text += "• Время простоя\n"
        text += "• Частота поломок\n"
        text += "• Затраты на обслуживание\n"
    
    elif report_type == "👥 По сотрудникам":
        # Получаем статистику по сотрудникам
        users = await db.get_users_by_organization(org_id)
        
        text += f"<b>Сотрудников всего:</b> {len(users)} чел.\n\n"
        
        # Группируем по ролям
        roles_count = {}
        for u in users:
            roles_count[u['role']] = roles_count.get(u['role'], 0) + 1
        
        role_names = {
            'director': '👨‍💼 Директор',
            'fleetmanager': '👷 Начальник парка',
            'driver': '🚛 Водитель'
        }
        
        text += "<b>Распределение по ролям:</b>\n"
        for role, count in roles_count.items():
            text += f"• {role_names.get(role, role)}: {count} чел.\n"
        
        # Статистика по водителям
        drivers = [u for u in users if u['role'] == 'driver']
        if drivers:
            text += f"\n<b>Статистика водителей ({len(drivers)} чел.):</b>\n"
            
            total_shifts = 0
            total_hours = 0
            
            for driver in drivers[:5]:  # Берем первых 5 для примера
                stats = await db.get_driver_stats(driver['telegram_id'], days)
                shifts = stats.get('shifts_count', 0)
                hours = stats.get('avg_shift_hours', 0)
                
                total_shifts += shifts
                total_hours += hours * shifts if shifts > 0 else 0
                
                text += f"• {driver['full_name']}: {shifts} смен"
                if shifts > 0:
                    text += f", {hours}ч/смена"
                text += "\n"
            
            if len(drivers) > 5:
                text += f"... и ещё {len(drivers) - 5} водителей\n"
            
            if len(drivers) > 0:
                avg_shifts = total_shifts / len(drivers)
                avg_hours = total_hours / total_shifts if total_shifts > 0 else 0
                text += f"\n<b>Средние показатели:</b>\n"
                text += f"• Смен на водителя: {avg_shifts:.1f}\n"
                text += f"• Средняя смена: {avg_hours:.1f}ч\n"
    
    elif report_type == "🔧 По ТО":
        # Получаем активные ТО
        maintenance_list = await db.get_active_maintenance(org_id)
        
        text += f"<b>Предстоящих ТО:</b> {len(maintenance_list)} ед.\n\n"
        
        if maintenance_list:
            # Сортируем по дате
            maintenance_list.sort(key=lambda x: x['scheduled_date'])
            
            today = datetime.now().date()
            
            text += "<b>Ближайшие ТО:</b>\n"
            for maint in maintenance_list[:5]:
                scheduled_date = datetime.strptime(maint['scheduled_date'], "%Y-%m-%d").date()
                days_left = (scheduled_date - today).days
                
                status = "🔴" if days_left < 0 else "🟡" if days_left == 0 else "🟢"
                days_text = f"просрочено на {abs(days_left)} дней" if days_left < 0 else f"через {days_left} дней"
                
                text += f"{status} {maint['equipment_name']} ({maint['type']})\n"
                text += f"   📅 {scheduled_date.strftime('%d.%m.%Y')} ({days_text})\n"
        
        # Получаем историю ТО за период
        # Здесь нужен дополнительный метод в БД
    
    text += "\n<i>Отчет сгенерирован автоматически. Для детальных отчетов обратитесь к администратору.</i>"
    
    await reply(message, text)
    await state.clear()

# ========== ОБРАБОТЧИКИ СОСТОЯНИЙ ==========

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

@dp.message(UserStates.waiting_for_username_or_id)
async def process_username_or_id(message: types.Message, state: FSMContext):
    """Обрабатывает ввод username или ID"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    await state.update_data(name=message.text)
    await reply(message, "✅ Название принято!\n\nТеперь введите модель техники:")
    await state.set_state(UserStates.waiting_for_equipment_model)

@dp.message(UserStates.waiting_for_equipment_model)
async def process_equipment_model(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    await state.update_data(model=message.text)
    await reply(message, "✅ Модель принята!\n\nТеперь введите VIN (уникальный номер):")
    await state.set_state(UserStates.waiting_for_equipment_vin)

@dp.message(UserStates.waiting_for_equipment_vin)
async def process_equipment_vin(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Отменено", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
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
    await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))

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

# ========== ИНФОРМАЦИЯ ==========

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
        f"🤖 <b>ТехКонтроль v2.0</b>\n\n"
        f"<b>Ваша роль:</b> {role_names.get(user['role'], '👤 Пользователь')}\n"
        f"{org_info}"
        f"<b>ID:</b> {message.from_user.id}\n\n"
        "<b>Назначение бота:</b>\n"
        "• Учет и контроль спецтехники\n"
        "• Управление водителями\n"
        "• Отслеживание ТО и ремонтов\n"
        "• Ежедневное обслуживание\n\n"
        "<b>Основные функции:</b>\n"
        "• Создание и управление организациями\n"
        "• Учет техники\n"
        "• Система смен водителей\n"
        "• Техническое обслуживание\n"
        "• Статистика и отчеты\n\n"
        "<b>По вопросам:</b>\n"
        "Обращайтесь к администратору вашей организации."
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

async def check_and_notify_maintenance():
    """Проверяет предстоящие ТО и отправляет уведомления"""
    try:
        # Получаем ТО на ближайшие 7 дней
        upcoming_maintenance = await db.get_upcoming_maintenance(days=7)
        
        for maintenance in upcoming_maintenance:
            # Здесь будет логика отправки уведомлений
            # Пока просто логируем
            logger.info(f"Найдено ТО: {maintenance}")
            
            # Помечаем как уведомленное
            await db.mark_maintenance_notified(maintenance['id'])
            
    except Exception as e:
        logger.error(f"Ошибка при проверке ТО: {e}")

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
