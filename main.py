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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime, timedelta
import aioschedule
from dotenv import load_dotenv
import aiohttp
from typing import Optional, Dict, List

from database import db

# ========== НАСТРОЙКА ==========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Настройки ИИ
AI_ENABLED = os.getenv('AI_ENABLED', 'False').lower() == 'true'
YANDEX_API_KEY = os.getenv('YANDEX_API_KEY', '')
YANDEX_FOLDER_ID = os.getenv('YANDEX_FOLDER_ID', '')
YANDEX_GPT_MODEL = os.getenv('YANDEX_GPT_MODEL', 'yandexgpt-lite')

# Инициализация бота
bot = Bot(
    token=os.getenv('BOT_TOKEN'),
    default=DefaultBotProperties(parse_mode="HTML")
)

# Инициализация диспетчера
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== СОСТОЯНИЯ ==========
class UserStates(StatesGroup):
    # Основные состояния
    waiting_for_ai_question = State()
    waiting_for_ai_followup = State()
    
    # Для админа
    waiting_for_user_id_to_assign = State()
    waiting_for_role_to_assign = State()
    waiting_for_org_to_assign = State()
    
    # Для начала смены
    waiting_for_equipment_selection = State()
    waiting_for_start_odometer = State()
    waiting_for_briefing_confirmation = State()
    waiting_for_inspection_photo = State()
    
    # Для учета топлива
    waiting_for_fuel_equipment = State()
    waiting_for_fuel_amount = State()
    waiting_for_fuel_type = State()
    waiting_for_fuel_cost = State()
    waiting_for_fuel_odometer = State()
    waiting_for_fuel_photo = State()
    waiting_for_fuel_notes = State()
    
    # Для завершения смены
    waiting_for_end_odometer = State()
    waiting_for_shift_notes = State()

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
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

async def ask_yandex_gpt(question: str, context: str = "", user_id: int = None) -> str:
    """Взаимодействие с Yandex GPT"""
    try:
        if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
            return "⚠️ Yandex GPT не настроен. Обратитесь к администратору."
        
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        
        headers = {
            "Authorization": f"Api-Key {YANDEX_API_KEY}",
            "x-folder-id": YANDEX_FOLDER_ID,
            "Content-Type": "application/json"
        }
        
        system_prompt = "Ты — профессиональный помощник по обслуживанию и эксплуатации спецтехники."
        
        data = {
            "modelUri": f"gpt://{YANDEX_FOLDER_ID}/{YANDEX_GPT_MODEL}",
            "completionOptions": {
                "stream": False,
                "temperature": 0.3,
                "maxTokens": 1500
            },
            "messages": [
                {
                    "role": "system",
                    "text": system_prompt
                },
                {
                    "role": "user",
                    "text": f"{context}\n\nВопрос: {question}"
                }
            ]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data, timeout=30) as response:
                if response.status == 200:
                    result = await response.json()
                    answer = result['result']['alternatives'][0]['message']['text']
                    
                    if user_id:
                        user = await db.get_user(user_id)
                        if user and user.get('organization_id'):
                            await db.add_ai_context(
                                organization_id=user['organization_id'],
                                context_type="assistance",
                                equipment_model="",
                                question=question,
                                answer=answer,
                                source="yandex_gpt"
                            )
                    
                    return answer
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка Yandex GPT: {error_text}")
                    return f"⚠️ Ошибка при обращении к ИИ. Статус: {response.status}"
                    
    except asyncio.TimeoutError:
        logger.error("Таймаут при обращении к Yandex GPT")
        return "⚠️ Превышено время ожидания ответа от ИИ. Попробуйте еще раз."
    except Exception as e:
        logger.error(f"Ошибка Yandex GPT: {e}")
        return "⚠️ Произошла ошибка при обработке запроса. Попробуйте еще раз или обратитесь к специалисту."

async def ask_ai_assistant(question: str, context: str = "", user_id: int = None) -> str:
    """Взаимодействие с ИИ для помощи по технике"""
    if not AI_ENABLED:
        return "🤖 Функция ИИ-помощника временно недоступна. Обратитесь к начальнику парка."
    
    try:
        if YANDEX_API_KEY and YANDEX_FOLDER_ID:
            return await ask_yandex_gpt(question, context, user_id)
        
        # Локальная база знаний
        answers = {
            "масло": "✅ **Проверка масла в двигателе:**\n\n1. Заглушить двигатель и подождать 5-10 минут\n2. Вынуть масляный щуп, протереть\n3. Вставить обратно и вынуть\n4. Уровень между MIN и MAX\n5. Цвет: золотистый или светло-коричневый - норма",
            "тормоза": "✅ **Проверка тормозов:**\n\n1. Проверить уровень тормозной жидкости\n2. Проверить износ колодок (мин. 3 мм)\n3. Проверить состояние дисков\n4. Прокачать систему при необходимости",
            "шины": "✅ **Проверка шин:**\n\nДавление:\n- Передние: 8-9 бар\n- Задние: 6-7 бар\n\nПротектор: мин. 3 мм",
            "топливо": "✅ **Правила заправки:**\n\n1. Использовать только ДТ\n2. Заправляться на проверенных АЗС\n3. Сохранять чеки\n4. Не заправляться 'под горлышко'",
        }
        
        question_lower = question.lower()
        for key, answer in answers.items():
            if key in question_lower:
                return answer
        
        return "🤖 Для точного ответа обратитесь к руководству по эксплуатации или к начальнику парка."
        
    except Exception as e:
        logger.error(f"Ошибка ИИ ассистента: {e}")
        return "⚠️ Произошла ошибка при обработке запроса."

def get_main_keyboard(role, has_organization=False):
    """Генерирует клавиатуру в зависимости от роли"""
    
    if role == 'unassigned':
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="ℹ️ Информация о боте")],
                [types.KeyboardButton(text="🤖 ИИ Помощник")],
                [types.KeyboardButton(text="📞 Контакты")],
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие..."
        )
    
    if role == 'botadmin':
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="👥 Все пользователи")],
                [types.KeyboardButton(text="🏢 Все организации")],
                [types.KeyboardButton(text="➕ Назначить роль")],
                [types.KeyboardButton(text="📊 Статистика")],
                [types.KeyboardButton(text="🤖 ИИ Помощник")],
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие..."
        )
    
    if role == 'director':
        if not has_organization:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="🏢 Создать организацию")],
                    [types.KeyboardButton(text="ℹ️ Информация о боте")],
                    [types.KeyboardButton(text="🤖 ИИ Помощник")],
                ],
                resize_keyboard=True
            )
        else:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="🏢 Моя организация")],
                    [types.KeyboardButton(text="🚜 Автопарк")],
                    [types.KeyboardButton(text="👥 Сотрудники")],
                    [types.KeyboardButton(text="➕ Добавить технику")],
                    [types.KeyboardButton(text="➕ Назначить сотрудника")],
                    [types.KeyboardButton(text="📊 Статистика")],
                    [types.KeyboardButton(text="⛽ Учет топлива")],
                    [types.KeyboardButton(text="🤖 ИИ Помощник")],
                ],
                resize_keyboard=True
            )
    
    if role == 'fleetmanager':
        if not has_organization:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="ℹ️ Информация о боте")],
                    [types.KeyboardButton(text="🤖 ИИ Помощник")],
                ],
                resize_keyboard=True
            )
        else:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="🚜 Управление парком")],
                    [types.KeyboardButton(text="👷 Водители")],
                    [types.KeyboardButton(text="🔍 Проверить осмотры")],
                    [types.KeyboardButton(text="📅 Ближайшие ТО")],
                    [types.KeyboardButton(text="⛽ Учет топлива")],
                    [types.KeyboardButton(text="📦 Заказы запчастей")],
                    [types.KeyboardButton(text="🤖 ИИ Помощник")],
                ],
                resize_keyboard=True
            )
    
    if role == 'driver':
        if not has_organization:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="ℹ️ Информация о боте")],
                    [types.KeyboardButton(text="🤖 ИИ Помощник")],
                ],
                resize_keyboard=True
            )
        else:
            active_shift = asyncio.run(db.get_active_shift(user_id))
            if active_shift:
                return types.ReplyKeyboardMarkup(
                    keyboard=[
                        [types.KeyboardButton(text="✅ Закончить смену")],
                        [types.KeyboardButton(text="📋 Моя смена")],
                        [types.KeyboardButton(text="⛽ Заправить технику")],
                        [types.KeyboardButton(text="🤖 ИИ Помощник")],
                    ],
                    resize_keyboard=True
                )
            else:
                return types.ReplyKeyboardMarkup(
                    keyboard=[
                        [types.KeyboardButton(text="🚛 Начать смену")],
                        [types.KeyboardButton(text="📋 Мои смены")],
                        [types.KeyboardButton(text="🚜 Моя техника")],
                        [types.KeyboardButton(text="⛽ Учет топлива")],
                        [types.KeyboardButton(text="🤖 ИИ Помощник")],
                    ],
                    resize_keyboard=True
                )
    
    # По умолчанию для всех остальных
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="ℹ️ Информация о боте")],
            [types.KeyboardButton(text="🤖 ИИ Помощник")],
        ],
        resize_keyboard=True
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

def get_fuel_type_keyboard():
    """Клавиатура для выбора типа топлива"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="⛽ Дизель ДТ")],
            [types.KeyboardButton(text="⛽ Бензин АИ-92")],
            [types.KeyboardButton(text="⛽ Бензин АИ-95")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

# ========== КОМАНДА СТАРТ (ПОЛНОСТЬЮ ПЕРЕРАБОТАНА) ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Главное меню для всех"""
    await state.clear()
    
    user = await db.get_user(message.from_user.id)
    if not user:
        await db.register_user(
            telegram_id=message.from_user.id,
            full_name=message.from_user.full_name,
            username=message.from_user.username
        )
        user = await db.get_user(message.from_user.id)

    if not user:
        await reply(message, "❌ Ошибка регистрации. Попробуйте еще раз.")
        return
    
    role = user['role']
    has_organization = bool(user.get('organization_id'))
    
    # Для не назначенных пользователей
    if role == 'unassigned':
        welcome_text = (
            f"👋 <b>Добро пожаловать в ТехКонтроль!</b>\n\n"
            f"<b>Ваш ID:</b> <code>{message.from_user.id}</code>\n"
            f"<b>Ваше имя:</b> {message.from_user.full_name}\n\n"
            "📋 <b>Информация для получения доступа:</b>\n\n"
            "1. Отправьте ваш ID вышестоящему сотруднику\n"
            "2. Администратор назначит вам роль\n"
            "3. После назначения вы получите доступ к функциям\n\n"
            "👥 <b>Возможные роли:</b>\n"
            "• 🚛 Водитель - работа со сменами\n"
            "• 👷 Начальник парка - управление техникой\n"
            "• 👨‍💼 Директор - управление организацией\n\n"
            "📞 Для ускорения процесса обратитесь к администратору."
        )
        
        await reply(message, welcome_text, reply_markup=get_main_keyboard(role, has_organization))
        return
    
    # Для назначенных ролей
    role_names = {
        'botadmin': '👑 Администратор бота',
        'director': '👨‍💼 Директор компании',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    welcome_text = f"🤖 <b>ТехКонтроль</b>\n\n"
    welcome_text += f"<b>Роль:</b> {role_names.get(role, 'Пользователь')}\n"
    welcome_text += f"<b>ID:</b> <code>{message.from_user.id}</code>\n"
    welcome_text += f"<b>Имя:</b> {message.from_user.full_name}\n"
    
    if has_organization:
        org = await db.get_organization(user['organization_id'])
        if org:
            welcome_text += f"<b>Организация:</b> {org['name']}\n"
    
    # Особые случаи
    if role == 'driver' and has_organization:
        active_shift = await db.get_active_shift(message.from_user.id)
        if active_shift:
            welcome_text += f"\n⚠️ <b>У вас активная смена!</b>\n"
            welcome_text += f"Техника: {active_shift.get('equipment_name', 'Не указана')}\n"
            welcome_text += f"Начало: {active_shift['start_time'][:16]}"
    
    elif role == 'director' and not has_organization:
        welcome_text += "\n\n📌 <b>Для начала работы создайте организацию</b>"
    
    elif role in ['fleetmanager', 'driver'] and not has_organization:
        welcome_text += "\n\n⏳ <b>Ожидайте назначения в организацию</b>\n"
        welcome_text += "Для ускорения отправьте ваш ID директору"
    
    await reply(message, welcome_text, reply_markup=get_main_keyboard(role, has_organization))

# ========== ИИ ПОМОЩНИК ==========
@dp.message(F.text == "🤖 ИИ Помощник")
async def ai_assistant_start(message: types.Message, state: FSMContext):
    """Начинает диалог с ИИ помощником"""
    await reply(
        message,
        "🤖 <b>ИИ Помощник по обслуживанию техники</b>\n\n"
        "Задайте вопрос о:\n"
        "• Обслуживании техники\n"
        "• Проверках и осмотрах\n"
        "• Ремонте и устранении неисправностей\n"
        "• Расходе топлива\n"
        "• ТО и техническому обслуживанию\n\n"
        "<i>Примеры вопросов:</i>\n"
        "• Как проверить масло в двигателе?\n"
        "• Какое давление в шинах должно быть?\n"
        "• Как часто нужно делать ТО?\n\n"
        "Введите ваш вопрос:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_ai_question)

@dp.message(UserStates.waiting_for_ai_question)
async def process_ai_question(message: types.Message, state: FSMContext):
    """Обрабатывает вопрос к ИИ"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Диалог с ИИ отменен", 
                   reply_markup=get_main_keyboard(role, has_org))
        return
    
    question = message.text.strip()
    
    if len(question) < 3:
        await reply(message, "❌ Вопрос слишком короткий. Уточните, пожалуйста.")
        return
    
    await reply(message, "🤖 <b>ИИ думает...</b>\n\nПожалуйста, подождите...")
    
    # Получаем контекст
    user = await db.get_user(message.from_user.id)
    context = ""
    
    if user and user.get('organization_id'):
        if user['role'] == 'driver':
            equipment = await db.get_equipment_by_driver(message.from_user.id)
            if equipment:
                context = "Техника водителя:\n"
                for eq in equipment[:2]:
                    context += f"- {eq['name']} ({eq['model']})\n"
    
    # Получаем ответ от ИИ
    answer = await ask_ai_assistant(question, context, message.from_user.id)
    
    await reply(
        message,
        f"❓ <b>Ваш вопрос:</b>\n{question}\n\n"
        f"🤖 <b>Ответ ИИ-помощника:</b>\n{answer}\n\n"
        f"<i>Если ответ не помог, обратитесь к начальнику парка</i>"
    )
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Возврат в главное меню", 
               reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))

# ========== ИНФОРМАЦИЯ О БОТЕ ==========
@dp.message(F.text == "ℹ️ Информация о боте")
async def bot_info(message: types.Message):
    """Показывает информацию о боте"""
    info_text = (
        "🤖 <b>ТехКонтроль - система управления спецтехникой</b>\n\n"
        "🔧 <b>Основные возможности:</b>\n"
        "• Учет и контроль спецтехники\n"
        "• Управление сменами водителей\n"
        "• Контроль ТО и обслуживания\n"
        "• Учет топлива и аналитика\n"
        "• ИИ-помощник по обслуживанию\n\n"
        "👥 <b>Роли в системе:</b>\n"
        "• 🚛 Водитель - работа со сменами\n"
        "• 👷 Начальник парка - управление техникой\n"
        "• 👨‍💼 Директор - управление организацией\n"
        "• 👑 Администратор - управление системой\n\n"
        "📞 <b>Поддержка:</b>\n"
        "Для получения доступа отправьте ваш ID вышестоящему сотруднику.\n"
        "ID можно узнать через команду /start\n\n"
        "🚀 <b>Разработка:</b>\n"
        "Бот постоянно улучшается. Следите за обновлениями!"
    )
    
    await reply(message, info_text)

# ========== КОНТАКТЫ ==========
@dp.message(F.text == "📞 Контакты")
async def contacts(message: types.Message):
    """Показывает контакты"""
    contacts_text = (
        "📞 <b>Контакты</b>\n\n"
        "<b>Техническая поддержка:</b>\n"
        "• По вопросам работы бота\n"
        "• По проблемам с доступом\n"
        "• По предложениям по улучшению\n\n"
        "<b>Администратор системы:</b>\n"
        "• Для назначения ролей\n"
        "• Для создания организаций\n"
        "• Для решения сложных вопросов\n\n"
        "<i>Для связи используйте Telegram</i>"
    )
    
    await reply(message, contacts_text)

# ========== АДМИН ФУНКЦИИ ==========
@dp.message(F.text == "👥 Все пользователи")
async def all_users(message: types.Message):
    """Показывает всех пользователей (админ)"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    users = await db.get_all_users_simple()
    
    if not users:
        await reply(message, "📭 Пользователей пока нет.")
        return
    
    text = "👥 <b>Все пользователи</b>\n\n"
    
    for u in users[:15]:  # Ограничим 15 пользователями
        role_emoji = {
            'botadmin': '👑',
            'director': '👨‍💼',
            'fleetmanager': '👷',
            'driver': '🚛',
            'unassigned': '❓'
        }.get(u['role'], '❓')
        
        text += f"{role_emoji} <b>{u['full_name']}</b>\n"
        text += f"ID: <code>{u['telegram_id']}</code>\n"
        text += f"Роль: {u['role']}\n"
        if u.get('organization_id'):
            text += f"Организация ID: {u['organization_id']}\n"
        text += "\n"
    
    if len(users) > 15:
        text += f"<i>... и еще {len(users) - 15} пользователей</i>"
    
    await reply(message, text)

@dp.message(F.text == "🏢 Все организации")
async def all_organizations(message: types.Message):
    """Показывает все организации (админ)"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    organizations = await db.get_all_organizations_simple()
    
    if not organizations:
        await reply(message, "🏢 Организаций пока нет.")
        return
    
    text = "🏢 <b>Все организации</b>\n\n"
    
    for org in organizations:
        text += f"<b>ID:</b> {org['id']}\n"
        text += f"<b>Название:</b> {org['name']}\n"
        if org.get('director_id'):
            text += f"<b>Директор ID:</b> {org['director_id']}\n"
        text += "\n"
    
    await reply(message, text)

@dp.message(F.text == "➕ Назначить роль")
async def assign_role_start(message: types.Message, state: FSMContext):
    """Начинает процесс назначения роли (админ)"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    await reply(
        message,
        "➕ <b>Назначение роли пользователю</b>\n\n"
        "Введите ID пользователя, которому хотите назначить роль:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_user_id_to_assign)

@dp.message(UserStates.waiting_for_user_id_to_assign)
async def process_user_id_for_role(message: types.Message, state: FSMContext):
    """Обрабатывает ID пользователя для назначения роли"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Назначение роли отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    try:
        user_id = int(message.text)
        user_to_assign = await db.get_user(user_id)
        
        if not user_to_assign:
            await reply(message, "❌ Пользователь с таким ID не найден!")
            return
        
        await state.update_data(user_id_to_assign=user_id)
        
        keyboard = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="👑 Администратор")],
                [types.KeyboardButton(text="👨‍💼 Директор")],
                [types.KeyboardButton(text="👷 Начальник парка")],
                [types.KeyboardButton(text="🚛 Водитель")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
        
        await reply(
            message,
            f"✅ <b>Пользователь найден:</b> {user_to_assign['full_name']}\n\n"
            f"Выберите роль для назначения:",
            reply_markup=keyboard
        )
        await state.set_state(UserStates.waiting_for_role_to_assign)
        
    except ValueError:
        await reply(message, "❌ Введите числовой ID пользователя!")

@dp.message(UserStates.waiting_for_role_to_assign)
async def process_role_to_assign(message: types.Message, state: FSMContext):
    """Обрабатывает выбор роли для назначения"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Назначение роли отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    role_map = {
        "👑 Администратор": "botadmin",
        "👨‍💼 Директор": "director",
        "👷 Начальник парка": "fleetmanager",
        "🚛 Водитель": "driver"
    }
    
    if message.text not in role_map:
        await reply(message, "❌ Выберите роль из списка!")
        return
    
    selected_role = role_map[message.text]
    data = await state.get_data()
    user_id_to_assign = data.get('user_id_to_assign')
    
    if selected_role == 'director':
        # Для директора нужно выбрать организацию или создать новую
        organizations = await db.get_all_organizations_simple()
        
        if organizations:
            keyboard = []
            for org in organizations[:5]:
                keyboard.append([types.KeyboardButton(text=f"🏢 {org['name']} (ID: {org['id']})")])
            keyboard.append([types.KeyboardButton(text="🏢 Создать новую организацию")])
            keyboard.append([types.KeyboardButton(text="❌ Отмена")])
            
            await reply(
                message,
                "🏢 <b>Выберите организацию для директора:</b>\n\n"
                "Или создайте новую организацию:",
                reply_markup=types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
            )
            await state.update_data(selected_role=selected_role)
            await state.set_state(UserStates.waiting_for_org_to_assign)
        else:
            # Нет организаций - предлагаем создать новую
            await reply(
                message,
                "🏢 <b>Организаций пока нет</b>\n\n"
                "Создать новую организацию для директора?",
                reply_markup=get_yes_no_keyboard()
            )
            await state.update_data(selected_role=selected_role, create_new_org=True)
            await state.set_state(UserStates.waiting_for_org_to_assign)
    else:
        # Для других ролей просто назначаем
        success = await db.assign_role_to_user(user_id_to_assign, selected_role)
        
        if success:
            user_to_assign = await db.get_user(user_id_to_assign)
            await reply(
                message,
                f"✅ <b>Роль назначена успешно!</b>\n\n"
                f"<b>Пользователь:</b> {user_to_assign['full_name']}\n"
                f"<b>Роль:</b> {message.text}\n"
                f"<b>ID:</b> {user_id_to_assign}\n\n"
                f"Пользователь получит уведомление."
            )
            
            # Уведомляем пользователя
            await send_to_user(
                user_id_to_assign,
                f"✅ <b>Вам назначена роль!</b>\n\n"
                f"<b>Роль:</b> {message.text}\n"
                f"<b>Назначил:</b> {message.from_user.full_name}\n\n"
                f"Перезапустите бота командой /start для обновления меню."
            )
        else:
            await reply(message, "❌ Ошибка при назначении роли!")
        
        await state.clear()
        await reply(message, "Возврат в главное меню", 
                   reply_markup=get_main_keyboard('botadmin', True))

@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: types.Message):
    """Показывает статистику (админ)"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    users = await db.get_all_users_simple()
    organizations = await db.get_all_organizations_simple()
    
    # Считаем пользователей по ролям
    roles_count = {'unassigned': 0}
    for u in users:
        roles_count[u['role']] = roles_count.get(u['role'], 0) + 1
    
    text = "📊 <b>Статистика системы</b>\n\n"
    text += f"<b>Всего пользователей:</b> {len(users)}\n"
    text += f"<b>Всего организаций:</b> {len(organizations)}\n\n"
    
    text += "<b>Пользователи по ролям:</b>\n"
    for role, count in sorted(roles_count.items()):
        role_name = {
            'botadmin': '👑 Администраторы',
            'director': '👨‍💼 Директоры',
            'fleetmanager': '👷 Начальники парка',
            'driver': '🚛 Водители',
            'unassigned': '❓ Не назначенные'
        }.get(role, role)
        text += f"• {role_name}: {count}\n"
    
    # Статистика по активности
    active_shifts = 0
    for u in users:
        if u['role'] == 'driver':
            shift = await db.get_active_shift(u['telegram_id'])
            if shift:
                active_shifts += 1
    
    text += f"\n<b>Активных смен:</b> {active_shifts}\n"
    
    await reply(message, text)

# ========== ФУНКЦИИ ДИРЕКТОРА ==========
@dp.message(F.text == "🏢 Создать организацию")
async def create_organization_start(message: types.Message, state: FSMContext):
    """Создание организации (директор)"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'director':
        await reply(message, "⛔ Только директор может создавать организацию!")
        return
    
    if user.get('organization_id'):
        await reply(message, "❌ У вас уже есть организация!")
        return
    
    await reply(
        message,
        "🏢 <b>Создание новой организации</b>\n\n"
        "Введите название вашей организации:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_ai_question)  # Временно используем это состояние

@dp.message(F.text == "🏢 Моя организация")
async def my_organization(message: types.Message):
    """Информация об организации (директор)"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'director' or not user.get('organization_id'):
        await reply(message, "⛔ Доступ только для директора с организацией!")
        return
    
    org = await db.get_organization(user['organization_id'])
    if not org:
        await reply(message, "❌ Организация не найдена!")
        return
    
    # Получаем сотрудников
    employees = await db.get_users_by_organization(org['id'])
    # Получаем технику
    equipment = await db.get_organization_equipment(org['id'])
    
    text = f"🏢 <b>Моя организация</b>\n\n"
    text += f"<b>Название:</b> {org['name']}\n"
    text += f"<b>ID организации:</b> {org['id']}\n"
    text += f"<b>Дата создания:</b> {org['created_at'][:10]}\n\n"
    
    if employees:
        text += f"<b>👥 Сотрудники ({len(employees)}):</b>\n"
        for emp in employees:
            if emp['role'] == 'director':
                continue
            role_emoji = {
                'fleetmanager': '👷',
                'driver': '🚛'
            }.get(emp['role'], '👤')
            text += f"• {role_emoji} {emp['full_name']} ({emp['role']})\n"
    
    if equipment:
        text += f"\n<b>🚜 Техника ({len(equipment)}):</b>\n"
        for eq in equipment[:5]:  # Показываем первые 5
            status_emoji = {
                'active': '✅',
                'maintenance': '🔧',
                'repair': '🛠️',
                'inactive': '❌'
            }.get(eq['status'], '❓')
            text += f"• {status_emoji} {eq['name']} ({eq['model']})\n"
        if len(equipment) > 5:
            text += f"<i>... и еще {len(equipment) - 5} единиц техники</i>\n"
    
    await reply(message, text)

@dp.message(F.text == "🚜 Автопарк")
async def fleet_list(message: types.Message):
    """Список техники в организации"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager'] or not user.get('organization_id'):
        await reply(message, "⛔ Доступ только для директора и начальника парка!")
        return
    
    equipment = await db.get_organization_equipment(user['organization_id'])
    
    if not equipment:
        await reply(message, "🚜 В вашей организации пока нет техники.")
        return
    
    text = "🚜 <b>Автопарк</b>\n\n"
    
    for eq in equipment:
        status_emoji = {
            'active': '✅',
            'maintenance': '🔧',
            'repair': '🛠️',
            'inactive': '❌'
        }.get(eq['status'], '❓')
        
        text += f"{status_emoji} <b>{eq['name']}</b> ({eq['model']})\n"
        text += f"VIN: {eq['vin']}\n"
        text += f"Статус: {eq['status']}\n"
        
        if eq.get('odometer'):
            text += f"Пробег: {eq['odometer']} км\n"
        
        if eq.get('current_fuel_level') is not None and eq.get('fuel_capacity'):
            percentage = round((eq['current_fuel_level'] / eq['fuel_capacity']) * 100, 1) if eq['fuel_capacity'] > 0 else 0
            fuel_emoji = '🟢' if percentage > 50 else '🟡' if percentage > 20 else '🔴'
            text += f"{fuel_emoji} Топливо: {eq['current_fuel_level']} л ({percentage}%)\n"
        
        if eq.get('next_maintenance'):
            text += f"📅 Следующее ТО: {eq['next_maintenance'][:10]}\n"
        
        text += "\n"
    
    await reply(message, text)

# ========== ФУНКЦИИ ВОДИТЕЛЯ ==========
@dp.message(F.text == "🚛 Начать смену")
async def start_shift_begin(message: types.Message, state: FSMContext):
    """Начало смены (водитель)"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут начинать смены!")
        return
    
    if not user.get('organization_id'):
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    active_shift = await db.get_active_shift(message.from_user.id)
    if active_shift:
        await reply(
            message,
            f"⚠️ <b>У вас уже есть активная смена!</b>\n\n"
            f"Техника: {active_shift.get('equipment_name', 'Не указана')}\n"
            f"Начало: {active_shift['start_time'][:16]}\n\n"
            f"Завершите текущую смену перед началом новой."
        )
        return
    
    equipment = await db.get_equipment_by_driver(message.from_user.id)
    
    if not equipment:
        await reply(
            message,
            "🚛 <b>Начало смены</b>\n\n"
            "❌ Нет доступной техники!\n\n"
            "Обратитесь к начальнику парка для назначения техники."
        )
        return
    
    await state.update_data(equipment_list=equipment)
    
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
    """Обрабатывает выбор техники для смены"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Начало смены отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    data = await state.get_data()
    equipment_list = data.get('equipment_list', [])
    
    selected_eq = None
    for eq in equipment_list:
        if f"🚜 {eq['name']} ({eq['model']})" == message.text:
            selected_eq = eq
            break
    
    if not selected_eq:
        await reply(message, "❌ Пожалуйста, выберите технику из списка")
        return
    
    await state.update_data(selected_equipment=selected_eq)
    
    await reply(
        message,
        f"✅ <b>Техника:</b> {selected_eq['name']} ({selected_eq['model']})\n\n"
        f"Введите начальные показания одометра (пробег в км):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_start_odometer)

@dp.message(UserStates.waiting_for_start_odometer)
async def process_start_odometer(message: types.Message, state: FSMContext):
    """Обрабатывает начальный одометр"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Начало смены отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    try:
        start_odometer = int(message.text)
        if start_odometer < 0 or start_odometer > 1000000:
            await reply(message, "❌ Некорректные показания! Введите от 0 до 1,000,000 км")
            return
    except ValueError:
        await reply(message, "❌ Введите целое число! Например: 12500")
        return
    
    data = await state.get_data()
    selected_eq = data.get('selected_equipment')
    
    # Начинаем смену
    shift_id = await db.start_shift(
        driver_id=message.from_user.id,
        equipment_id=selected_eq['id'],
        briefing_confirmed=False,
        start_odometer=start_odometer
    )
    
    if shift_id:
        await reply(
            message,
            f"✅ <b>Смена начата!</b>\n\n"
            f"<b>Техника:</b> {selected_eq['name']} ({selected_eq['model']})\n"
            f"<b>Начальный пробег:</b> {start_odometer} км\n"
            f"<b>ID смены:</b> #{shift_id}\n"
            f"<b>Время начала:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"
            f"📋 <b>Не забудьте провести предрейсовый осмотр!</b>\n\n"
            f"Подтверждаете, что провели осмотр техники?",
            reply_markup=get_yes_no_keyboard()
        )
        await state.update_data(shift_id=shift_id)
        await state.set_state(UserStates.waiting_for_briefing_confirmation)
    else:
        await reply(message, "❌ Ошибка при начале смены!")
        await state.clear()

# ========== СИСТЕМА НАПОМИНАНИЙ ==========
async def check_and_send_notifications():
    """Проверяет и отправляет напоминания"""
    try:
        organizations = await db.get_all_organizations()
        
        for org in organizations:
            org_id = org['id']
            
            # Проверяем предстоящие ТО
            upcoming_maintenance = await db.get_upcoming_maintenance(org_id, 7)
            
            for maintenance in upcoming_maintenance:
                equipment_name = maintenance.get('equipment_name', 'Неизвестная техника')
                
                # Получаем пользователей организации
                users = await db.get_users_by_organization(org_id)
                
                # Отправляем уведомления директору и начальнику парка
                for user in users:
                    if user['role'] in ['director', 'fleetmanager']:
                        try:
                            await send_to_user(
                                user['telegram_id'],
                                f"🔔 <b>Напоминание о ТО</b>\n\n"
                                f"🚜 <b>Техника:</b> {equipment_name}\n"
                                f"📅 <b>Следующее ТО:</b> скоро\n\n"
                                f"⚠️ Запланируйте обслуживание!"
                            )
                        except:
                            continue
            
            # Проверяем низкий уровень топлива
            low_fuel_equipment = await db.get_low_fuel_equipment(org_id, 20.0)
            
            for eq in low_fuel_equipment:
                fuel_percentage = eq.get('fuel_percentage', 0)
                
                if fuel_percentage < 20:
                    for user in users:
                        if user['role'] in ['director', 'fleetmanager']:
                            try:
                                await send_to_user(
                                    user['telegram_id'],
                                    f"⚠️ <b>Низкий уровень топлива</b>\n\n"
                                    f"🚜 <b>Техника:</b> {eq['name']} ({eq['model']})\n"
                                    f"⛽ <b>Уровень:</b> {eq.get('current_fuel_level', 0)} л ({fuel_percentage}%)\n\n"
                                    f"Требуется заправка!"
                                )
                            except:
                                continue
    
    except Exception as e:
        logger.error(f"Ошибка в системе напоминаний: {e}")

# ========== ПЛАНИРОВЩИК ==========
async def scheduler():
    """Планировщик задач"""
    aioschedule.every().hour.do(check_and_send_notifications)
    
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(60)

# ========== ЗАПУСК БОТА ==========
async def on_startup():
    """Инициализация при запуске"""
    try:
        await db.connect()
        
        # Создаем администратора
        ADMIN_ID = int(os.getenv('ADMIN_ID', 1079922982))
        await db.register_user(
            telegram_id=ADMIN_ID,
            full_name="Администратор Системы",
            username="admin",
            role='botadmin'
        )
        
        # Запускаем планировщик в фоне
        asyncio.create_task(scheduler())
        
        logger.info("✅ Бот запущен!")
        logger.info(f"👑 Администратор: ID {ADMIN_ID}")
        logger.info(f"🤖 ИИ помощник: {'ВКЛ' if AI_ENABLED else 'ВЫКЛ'}")
        
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
