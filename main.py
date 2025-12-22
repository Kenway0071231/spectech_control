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
    
    # Для управления организацией
    waiting_for_org_name = State()
    waiting_for_edit_org_name = State()
    waiting_for_driver_stats_days = State()
    
    # Для отчетов
    waiting_for_report_type = State()
    waiting_for_report_period = State()

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
            [types.KeyboardButton(text="📋 Журнал действий")]
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
            [types.KeyboardButton(text="⚙️ Настройки организации")]
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
            [types.KeyboardButton(text="📅 Ближайшие ТО")]
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
            [InlineKeyboardButton(text="👥 Управление доступом", callback_data=f"manage_access:{org_id}")]
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
        f"Текущее название: {callback.message.text.split('Название: ')[1].split('\\n')[0]}\n\n"
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

# ========== ЖУРНАЛ ДЕЙСТВИЙ ==========

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

# ========== ОСТАВШИЕСЯ ОБРАБОТЧИКИ (без изменений) ==========

# ... [все остальные обработчики из предыдущего кода остаются без изменений] ...

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
