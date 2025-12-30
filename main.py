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
import openai
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
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
HUGGINGFACE_API_KEY = os.getenv('HUGGINGFACE_API_KEY', '')
 
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
    # Существующие состояния
    waiting_for_username_or_id = State()
    waiting_for_role = State()
    waiting_for_equipment_name = State()
    waiting_for_equipment_model = State()
    waiting_for_equipment_vin = State()
    waiting_for_equipment_selection = State()
    waiting_for_briefing_confirmation = State()
    waiting_for_inspection_photo = State()
    waiting_for_daily_checks = State()
    waiting_for_shift_notes = State()
    waiting_for_maintenance_type = State()
    waiting_for_maintenance_date = State()
    waiting_for_maintenance_description = State()
    waiting_for_notification_text = State()
    waiting_for_org_name = State()
    waiting_for_edit_org_name = State()
    waiting_for_driver_stats_days = State()
    waiting_for_report_type = State()
    waiting_for_report_period = State()
    waiting_for_search_query = State()
    waiting_for_equipment_edit_choice = State()
    waiting_for_equipment_edit_value = State()
    
    # НОВЫЕ состояния для ИИ помощи
    waiting_for_ai_question = State()
    waiting_for_ai_followup = State()
    
    # НОВЫЕ состояния для учета топлива
    waiting_for_fuel_equipment = State()
    waiting_for_fuel_amount = State()
    waiting_for_fuel_cost = State()
    waiting_for_fuel_odometer = State()
    waiting_for_fuel_photo = State()
    waiting_for_fuel_notes = State()
    
    # НОВЫЕ состояния для запчастей
    waiting_for_part_name = State()
    waiting_for_part_details = State()
    waiting_for_part_quantity = State()
    waiting_for_part_supplier = State()
    
    # НОВЫЕ состояния для заказов
    waiting_for_order_type = State()
    waiting_for_order_details = State()
    waiting_for_order_quantity = State()
    waiting_for_order_urgency = State()
    
    # НОВЫЕ состояния для инструкций
    waiting_for_instruction_search = State()
    waiting_for_instruction_type = State()
    
    # НОВЫЕ состояния для расширенного ТО
    waiting_for_maintenance_schedule_type = State()
    waiting_for_maintenance_interval = State()
    waiting_for_maintenance_parts = State()
    
    # НОВЫЕ состояния для аналитики
    waiting_for_analytics_period = State()
 
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
 
async def ask_ai_assistant(question: str, context: str = "", user_id: int = None) -> str:
    """Взаимодействие с ИИ для помощи по технике"""
    if not AI_ENABLED:
        return "🤖 Функция ИИ-помощника временно недоступна. Обратитесь к начальнику парка."
    
    try:
        # Сначала проверяем локальную базу знаний
        if user_id:
            user = await db.get_user(user_id)
            if user and user.get('organization_id'):
                ai_contexts = await db.get_ai_context(
                    organization_id=user['organization_id'],
                    limit=5
                )
                if ai_contexts:
                    # Добавляем контекст из базы
                    context += "\n\nКонтекст из базы знаний:\n"
                    for ctx in ai_contexts:
                        context += f"Вопрос: {ctx['question']}\nОтвет: {ctx['answer']}\n\n"
        
        # Если есть OpenAI API ключ
        if OPENAI_API_KEY:
            openai.api_key = OPENAI_API_KEY
            try:
                response = await openai.ChatCompletion.acreate(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "Ты помощник по обслуживанию спецтехники. Отвечай профессионально и подробно."},
                        {"role": "user", "content": f"{context}\n\nВопрос: {question}"}
                    ],
                    temperature=0.7,
                    max_tokens=500
                )
                answer = response.choices[0].message.content
                
                # Сохраняем в базу знаний
                if user_id:
                    user = await db.get_user(user_id)
                    if user and user.get('organization_id'):
                        await db.add_ai_context(
                            organization_id=user['organization_id'],
                            context_type="assistance",
                            equipment_model="",
                            question=question,
                            answer=answer,
                            source="ai"
                        )
                
                return answer
            except Exception as e:
                logger.error(f"Ошибка OpenAI: {e}")
        
        # Запасной вариант: Hugging Face
        if HUGGINGFACE_API_KEY:
            try:
                API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-large"
                headers = {"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"}
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        API_URL,
                        headers=headers,
                        json={"inputs": f"Вопрос о спецтехнике: {question}. Ответь подробно."}
                    ) as response:
                        result = await response.json()
                        if isinstance(result, list) and len(result) > 0:
                            answer = result[0].get('generated_text', 'Извините, не могу ответить.')
                            return answer
            except Exception as e:
                logger.error(f"Ошибка Hugging Face: {e}")
        
        # Запасные ответы
        answers = {
            "масло": "✅ Проверка масла:\n1. Заглушить двигатель и подождать 5 минут\n2. Вынуть щуп, протереть его\n3. Вставить щуп обратно и вынуть\n4. Уровень должен быть между метками MIN и MAX\n5. Цвет масла должен быть золотистым или светло-коричневым\n\n⚠️ Если масло черное или уровень низкий - требуется замена!",
            "тормоза": "✅ Проверка тормозов:\n1. Проверить уровень тормозной жидкости\n2. Проверить износ тормозных колодок (мин. толщина 3мм)\n3. Проверить состояние тормозных дисков\n4. Прокачать тормозную систему при необходимости\n\n🚨 При скрипе или вибрации - обратиться к механику!",
            "шины": "✅ Проверка шин:\n1. Давление: передние 8-9 бар, задние 6-7 бар\n2. Протектор: мин. глубина 3мм\n3. Внешний вид: нет порезов, гвоздей\n4. Балансировка: нет вибрации на скорости\n\n📅 Рекомендуется проверять давление еженедельно!",
            "топливо": "✅ Заправка топлива:\n1. Использовать только дизельное топливо ДТ\n2. Заправляться только на проверенных АЗС\n3. Проверять чек и наличие акцизных марок\n4. Не заправляться 'под завязку'\n\n⛽ Норма расхода: 25-35л/100км в зависимости от нагрузки",
        }
        
        for key, answer in answers.items():
            if key in question.lower():
                return answer
        
        return "🤖 Для точного ответа обратитесь к руководству по эксплуатации техники или к начальнику парка. Вы также можете уточнить вопрос."
        
    except Exception as e:
        logger.error(f"Ошибка ИИ ассистента: {e}")
        return "⚠️ Произошла ошибка при обработке запроса. Попробуйте еще раз или обратитесь к специалисту."
 
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
            [types.KeyboardButton(text="🔍 Поиск")],
            [types.KeyboardButton(text="🤖 ИИ Помощник")]
        ],
        
        'director': [
            [types.KeyboardButton(text="👨‍💼 Моя организация")],
            [types.KeyboardButton(text="🚜 Автопарк")],
            [types.KeyboardButton(text="✏️ Редактировать технику")],
            [types.KeyboardButton(text="👥 Сотрудники")],
            [types.KeyboardButton(text="📈 Статистика организации")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Добавить ТО")],
            [types.KeyboardButton(text="➕ Назначить роль")],
            [types.KeyboardButton(text="📊 Отчеты")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")],
            [types.KeyboardButton(text="📅 Ближайшие ТО")],
            [types.KeyboardButton(text="⚙️ Настройки организации")],
            [types.KeyboardButton(text="🔍 Поиск")],
            [types.KeyboardButton(text="⛽ Учет топлива")],
            [types.KeyboardButton(text="🔧 Запчасти")],
            [types.KeyboardButton(text="📦 Заказы")],
            [types.KeyboardButton(text="🤖 ИИ Помощник")],
            [types.KeyboardButton(text="📈 Аналитика")]
        ],
        
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Управление парком")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="✏️ Редактировать технику")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="📊 Статистика водителей")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Добавить ТО")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")],
            [types.KeyboardButton(text="📅 Ближайшие ТО")],
            [types.KeyboardButton(text="🔍 Поиск")],
            [types.KeyboardButton(text="⛽ Учет топлива")],
            [types.KeyboardButton(text="🔧 Запчасти")],
            [types.KeyboardButton(text="📦 Заказы")],
            [types.KeyboardButton(text="📋 Инструкции")],
            [types.KeyboardButton(text="🤖 ИИ Помощник")]
        ],
        
        'driver': [
            [types.KeyboardButton(text="🚛 Начать смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="📊 Моя статистика")],
            [types.KeyboardButton(text="✅ Закончить смену")],
            [types.KeyboardButton(text="🚜 Моя техника")],
            [types.KeyboardButton(text="⛽ Заправить технику")],
            [types.KeyboardButton(text="🔧 Заказать запчасть")],
            [types.KeyboardButton(text="📋 Инструкции")],
            [types.KeyboardButton(text="🤖 ИИ Помощник")],
            [types.KeyboardButton(text="ℹ️ Информация")]
        ]
    }
    
    # Для директора без организации
    if role == 'director' and not has_organization:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="🏢 Создать организацию")],
                [types.KeyboardButton(text="ℹ️ Информация")],
                [types.KeyboardButton(text="🤖 ИИ Помощник")]
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие..."
        )
    
    # Для ролей без организации
    if role in ['fleetmanager', 'driver'] and not has_organization:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="ℹ️ Информация")],
                [types.KeyboardButton(text="🤖 ИИ Помощник")]
            ],
            resize_keyboard=True,
            input_field_placeholder="Ожидайте назначения..."
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
 
def get_fuel_type_keyboard():
    """Клавиатура для выбора типа топлива"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="⛽ Дизель ДТ")],
            [types.KeyboardButton(text="⛽ Бензин АИ-92")],
            [types.KeyboardButton(text="⛽ Бензин АИ-95")],
            [types.KeyboardButton(text="⚡ Электричество")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )
 
def get_order_type_keyboard():
    """Клавиатура для выбора типа заказа"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🔧 Запчасть")],
            [types.KeyboardButton(text="⛽ Топливо")],
            [types.KeyboardButton(text="🛠️ Услуга")],
            [types.KeyboardButton(text="📋 Другое")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )
 
def get_urgency_keyboard():
    """Клавиатура для выбора срочности"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🚨 Срочно (сегодня)")],
            [types.KeyboardButton(text="⚠️ Средняя (1-3 дня)")],
            [types.KeyboardButton(text="📅 Не срочно (неделя)")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )
 
# ========== КОМАНДА СТАРТ ==========
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
        welcome_text = (
            "👋 <b>Добро пожаловать в ТехКонтроль 2.0!</b>\n\n"
            "Я — умный бот для учета и контроля спецтехники с ИИ-помощником.\n\n"
            f"<b>Ваш ID:</b> <code>{message.from_user.id}</code>\n"
            f"<b>Ваше имя:</b> {message.from_user.full_name}\n\n"
            "🚀 <b>Новые возможности:</b>\n"
            "• 🤖 ИИ-помощник по обслуживанию\n"
            "• ⛽ Учет топлива и аналитика расхода\n"
            "• 🔧 Управление запчастями\n"
            "• 📦 Система заказов\n"
            "• 📋 Интерактивные инструкции\n\n"
            "Для получения доступа обратитесь к администратору."
        )
        
        keyboard = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="ℹ️ О боте")],
                [types.KeyboardButton(text="🤖 ИИ Помощник")],
                [types.KeyboardButton(text="📞 Контакты")],
                [types.KeyboardButton(text="🆘 Помощь")]
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие..."
        )
        await reply(message, welcome_text, reply_markup=keyboard)
        return
    
    role = user['role']
    has_organization = bool(user.get('organization_id'))
    role_names = {
        'botadmin': '👑 Администратор бота',
        'director': '👨‍💼 Директор компании',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    if role == 'driver':
        active_shift = await db.get_active_shift(message.from_user.id)
        if active_shift:
            await reply(
                message,
                f"🚛 <b>У вас активная смена!</b>\n\n"
                f"<b>Техника:</b> {active_shift.get('equipment_name', 'Не указана')}\n"
                f"<b>Начало:</b> {active_shift['start_time'][:16]}\n"
                f"<b>Пробег:</b> {active_shift.get('odometer', 'Не указан')} км\n"
                f"<b>Статус осмотра:</b> {'✅ Подтверждён' if active_shift['inspection_approved'] else '⏳ Ожидает проверки'}\n\n"
                f"Вы можете завершить смену через меню.",
                reply_markup=get_main_keyboard(role, has_organization)
            )
            return
    
    welcome_text = f"🤖 <b>ТехКонтроль 2.0</b>\n\n"
    
    if role == 'director' and not has_organization:
        welcome_text += f"<b>Роль:</b> {role_names.get(role, '👤 Пользователь')}\n"
        welcome_text += "<b>Статус:</b> У вас ещё нет организации\n\n"
        welcome_text += "📌 <b>Для начала работы создайте организации:</b>"
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
 
# ========== ИИ ПОМОЩНИК ==========
 
@dp.message(F.text == "🤖 ИИ Помощник")
async def ai_assistant_start(message: types.Message, state: FSMContext):
    """Начинает диалог с ИИ помощником"""
    user = await db.get_user(message.from_user.id)
    
    if not user:
        await reply(message, "❌ Сначала зарегистрируйтесь через /start")
        return
    
    await reply(
        message,
        "🤖 <b>ИИ Помощник по обслуживанию техники</b>\n\n"
        "Задайте вопрос о:\n"
        "• Обслуживании техники\n"
        "• Проверках и осмотрах\n"
        "• Ремонте и устранении неисправностей\n"
        "• Расходе топлива\n"
        "• ТО и техническому обслуживанию\n"
        "• Работе с техникой\n\n"
        "<i>Примеры вопросов:</i>\n"
        "• Как проверить масло в двигателе?\n"
        "• Какое давление в шинах должно быть?\n"
        "• Как часто нужно делать ТО?\n"
        "• Почему техника не заводится?\n\n"
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
        await reply(message, "❌ Диалог с ИИ отменен", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    question = message.text.strip()
    
    if len(question) < 3:
        await reply(message, "❌ Вопрос слишком короткий. Уточните, пожалуйста.")
        return
    
    await reply(message, "🤖 <b>ИИ думает...</b>\n\nПожалуйста, подождите...")
    
    # Получаем контекст пользователя
    user = await db.get_user(message.from_user.id)
    context = ""
    
    if user and user.get('organization_id'):
        # Получаем технику пользователя для контекста
        if user['role'] == 'driver':
            equipment = await db.get_equipment_by_driver(message.from_user.id)
            if equipment:
                context = "Техника водителя:\n"
                for eq in equipment[:3]:
                    context += f"- {eq['name']} ({eq['model']})\n"
    
    # Получаем ответ от ИИ
    answer = await ask_ai_assistant(question, context, message.from_user.id)
    
    # Создаем клавиатуру для уточнения
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❓ Уточнить вопрос", callback_data="ai_clarify")],
            [InlineKeyboardButton(text="✅ Ответ помог", callback_data="ai_helpful")],
            [InlineKeyboardButton(text="❌ Ответ не помог", callback_data="ai_not_helpful")],
        ]
    )
    
    await reply(
        message,
        f"❓ <b>Ваш вопрос:</b>\n{question}\n\n"
        f"🤖 <b>Ответ ИИ-помощника:</b>\n{answer}\n\n"
        f"<i>Этот ответ был полезен?</i>",
        reply_markup=keyboard
    )
    
    await state.update_data(last_question=question, last_answer=answer)
    await state.set_state(UserStates.waiting_for_ai_followup)
 
@dp.callback_query(F.data == "ai_clarify", UserStates.waiting_for_ai_followup)
async def ai_clarify_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает запрос на уточнение"""
    await callback.message.edit_reply_markup(reply_markup=None)
    
    await reply(
        callback.message,
        "❓ <b>Уточните ваш вопрос:</b>\n\n"
        "Опишите более подробно или задайте уточняющий вопрос:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_ai_question)
    await callback.answer()
 
@dp.callback_query(F.data == "ai_helpful", UserStates.waiting_for_ai_followup)
async def ai_helpful_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает положительную оценку ответа"""
    data = await state.get_data()
    question = data.get('last_question', '')
    answer = data.get('last_answer', '')
    
    user = await db.get_user(callback.from_user.id)
    if user and user.get('organization_id'):
        # Сохраняем как полезный ответ
        await db.add_ai_context(
            organization_id=user['organization_id'],
            context_type="helpful_answer",
            equipment_model="",
            question=question,
            answer=answer,
            source="user_feedback"
        )
    
    await callback.message.edit_text(
        f"{callback.message.text}\n\n"
        f"✅ <b>Спасибо за обратную связь!</b>\n"
        f"Это поможет улучшить ИИ-помощника.",
        reply_markup=None
    )
    
    await state.clear()
    await callback.answer("Спасибо за оценку!")
 
@dp.callback_query(F.data == "ai_not_helpful", UserStates.waiting_for_ai_followup)
async def ai_not_helpful_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает отрицательную оценку ответа"""
    await callback.message.edit_text(
        f"{callback.message.text}\n\n"
        f"⚠️ <b>Извините, что ответ не помог.</b>\n"
        f"Рекомендуем обратиться к начальнику парка или техническому специалисту.",
        reply_markup=None
    )
    
    await state.clear()
    await callback.answer("Извините за неудобства!")
 
# ========== УЧЕТ ТОПЛИВА ==========
 
@dp.message(F.text == "⛽ Учет топлива")
async def fuel_menu(message: types.Message):
    """Меню учета топлива"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager', 'driver']:
        await reply(message, "⛔ Нет доступа к учету топлива!")
        return
    
    org_id = user.get('organization_id')
    if not org_id and user['role'] != 'botadmin':
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⛽ Добавить заправку", callback_data="add_fuel")],
            [InlineKeyboardButton(text="📊 Статистика расхода", callback_data="fuel_stats")],
            [InlineKeyboardButton(text="📋 История заправок", callback_data="fuel_history")],
            [InlineKeyboardButton(text="⚠️ Низкий уровень", callback_data="low_fuel")],
        ]
    )
    
    await reply(
        message,
        "⛽ <b>Учет топлива</b>\n\n"
        "Управление заправками и мониторинг расхода топлива.",
        reply_markup=keyboard
    )
 
@dp.callback_query(F.data == "add_fuel")
async def add_fuel_callback(callback: types.CallbackQuery, state: FSMContext):
    """Начинает добавление заправки"""
    user = await db.get_user(callback.from_user.id)
    
    if user['role'] not in ['driver', 'fleetmanager']:
        await callback.answer("⛔ Только водители и начальники парка могут добавлять заправки!", show_alert=True)
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await callback.answer("❌ Вы не привязаны к организации!", show_alert=True)
        return
    
    # Получаем технику
    if user['role'] == 'driver':
        equipment = await db.get_equipment_by_driver(callback.from_user.id)
    else:
        equipment = await db.get_organization_equipment(org_id)
    
    if not equipment:
        await callback.message.edit_text(
            "🚜 <b>Нет доступной техники</b>\n\n"
            "Сначала добавьте технику в организацию."
        )
        await callback.answer()
        return
    
    # Создаем клавиатуру с техникой
    keyboard = []
    for eq in equipment[:10]:
        keyboard.append([types.KeyboardButton(text=f"🚜 {eq['name']} ({eq['model']})")])
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await state.update_data(equipment_list=equipment, org_id=org_id)
    
    await callback.message.edit_text(
        "⛽ <b>Добавление заправки</b>\n\n"
        "Выберите технику:",
        reply_markup=None
    )
    
    await reply(
        callback.message,
        "Выберите технику для заправки:",
        reply_markup=types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    )
    await state.set_state(UserStates.waiting_for_fuel_equipment)
    await callback.answer()
 
@dp.message(UserStates.waiting_for_fuel_equipment)
async def process_fuel_equipment(message: types.Message, state: FSMContext):
    """Обрабатывает выбор техники для заправки"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление заправки отменено", 
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
        f"Введите количество топлива в литрах:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_fuel_amount)
 
@dp.message(UserStates.waiting_for_fuel_amount)
async def process_fuel_amount(message: types.Message, state: FSMContext):
    """Обрабатывает количество топлива"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    try:
        fuel_amount = float(message.text.replace(',', '.'))
        if fuel_amount <= 0 or fuel_amount > 1000:
            await reply(message, "❌ Некорректное количество! Введите от 0.1 до 1000 литров")
            return
    except ValueError:
        await reply(message, "❌ Введите число! Например: 50.5")
        return
    
    await state.update_data(fuel_amount=fuel_amount)
    
    await reply(
        message,
        f"✅ <b>Количество:</b> {fuel_amount} л\n\n"
        f"Выберите тип топлива:",
        reply_markup=get_fuel_type_keyboard()
    )
    await state.set_state(UserStates.waiting_for_fuel_cost)
 
@dp.message(UserStates.waiting_for_fuel_cost)
async def process_fuel_type(message: types.Message, state: FSMContext):
    """Обрабатывает тип топлива и стоимость"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    valid_fuels = ["⛽ Дизель ДТ", "⛽ Бензин АИ-92", "⛽ Бензин АИ-95", "⚡ Электричество"]
    if message.text not in valid_fuels:
        await reply(message, "❌ Выберите тип топлива из списка")
        return
    
    fuel_type = message.text.replace('⛽ ', '').replace('⚡ ', '')
    
    await state.update_data(fuel_type=fuel_type)
    
    await reply(
        message,
        f"✅ <b>Тип топлива:</b> {fuel_type}\n\n"
        f"Введите цену за литр (руб.):\n"
        f"<i>Например: 55.30</i>",
        reply_markup=get_cancel_keyboard()
    )
 
@dp.message(UserStates.waiting_for_fuel_cost)
async def process_fuel_cost(message: types.Message, state: FSMContext):
    """Обрабатывает стоимость топлива"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    try:
        cost_per_liter = float(message.text.replace(',', '.'))
        if cost_per_liter <= 0 or cost_per_liter > 200:
            await reply(message, "❌ Некорректная цена! Введите от 1 до 200 руб.")
            return
    except ValueError:
        await reply(message, "❌ Введите число! Например: 55.30")
        return
    
    data = await state.get_data()
    fuel_amount = data.get('fuel_amount', 0)
    total_cost = round(fuel_amount * cost_per_liter, 2)
    
    await state.update_data(cost_per_liter=cost_per_liter, total_cost=total_cost)
    
    await reply(
        message,
        f"✅ <b>Цена за литр:</b> {cost_per_liter} руб.\n"
        f"✅ <b>Общая стоимость:</b> {total_cost} руб.\n\n"
        f"Введите показания одометра (км):\n"
        f"<i>Необязательно, можно пропустить</i>",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="⏭️ Пропустить")], [types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(UserStates.waiting_for_fuel_odometer)
 
@dp.message(UserStates.waiting_for_fuel_odometer)
async def process_fuel_odometer(message: types.Message, state: FSMContext):
    """Обрабатывает показания одометра"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    odometer_reading = None
    if message.text != "⏭️ Пропустить":
        try:
            odometer_reading = int(message.text)
            if odometer_reading < 0 or odometer_reading > 1000000:
                await reply(message, "❌ Некорректные показания! Введите от 0 до 1,000,000 км")
                return
        except ValueError:
            await reply(message, "❌ Введите целое число! Например: 12500")
            return
    
    await state.update_data(odometer_reading=odometer_reading)
    
    await reply(
        message,
        "📸 <b>Прикрепите фото чека</b> (необязательно):\n\n"
        "Отправьте фото или нажмите 'Пропустить':",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="📸 Сделать фото")], 
                     [types.KeyboardButton(text="⏭️ Пропустить")],
                     [types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(UserStates.waiting_for_fuel_photo)
 
@dp.message(UserStates.waiting_for_fuel_photo)
async def process_fuel_photo_prompt(message: types.Message, state: FSMContext):
    """Обрабатывает запрос на фото"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    if message.text == "⏭️ Пропустить":
        await state.update_data(receipt_photo=None)
        await reply(
            message,
            "📝 <b>Добавьте заметки</b> (необязательно):\n\n"
            "Например: 'Заправка на АЗС Лукойл, смена 2'",
            reply_markup=types.ReplyKeyboardMarkup(
                keyboard=[[types.KeyboardButton(text="⏭️ Без заметок")], 
                         [types.KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(UserStates.waiting_for_fuel_notes)
        return
    
    if message.text == "📸 Сделать фото":
        await reply(
            message,
            "📸 <b>Сделайте фото чека и отправьте его</b>\n\n"
            "Убедитесь, что на фото видно:\n"
            "• Название АЗС\n"
            "• Дата и время\n"
            "• Количество топлива\n"
            "• Сумма",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await reply(message, "❌ Пожалуйста, отправьте фото или выберите действие")
 
@dp.message(F.photo, UserStates.waiting_for_fuel_photo)
async def handle_fuel_photo(message: types.Message, state: FSMContext):
    """Обрабатывает фото чека"""
    photo_file_id = message.photo[-1].file_id
    await state.update_data(receipt_photo=photo_file_id)
    
    await reply(
        message,
        "✅ <b>Фото чека принято!</b>\n\n"
        "📝 <b>Добавьте заметки</b> (необязательно):\n\n"
        "Например: 'Заправка на АЗС Лукойл, смена 2'",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="⏭️ Без заметок")], 
                     [types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(UserStates.waiting_for_fuel_notes)
 
@dp.message(UserStates.waiting_for_fuel_notes)
async def process_fuel_notes(message: types.Message, state: FSMContext):
    """Обрабатывает заметки о заправке"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    notes = None
    if message.text != "⏭️ Без заметок":
        notes = message.text
    
    data = await state.get_data()
    selected_eq = data.get('selected_equipment')
    fuel_amount = data.get('fuel_amount')
    fuel_type = data.get('fuel_type')
    cost_per_liter = data.get('cost_per_liter')
    total_cost = data.get('total_cost')
    odometer_reading = data.get('odometer_reading')
    receipt_photo = data.get('receipt_photo')
    
    # Добавляем запись о заправке
    fuel_log_id = await db.add_fuel_log(
        equipment_id=selected_eq['id'],
        driver_id=message.from_user.id,
        fuel_amount=fuel_amount,
        fuel_type=fuel_type,
        cost_per_liter=cost_per_liter,
        total_cost=total_cost,
        odometer_reading=odometer_reading,
        receipt_photo=receipt_photo,
        notes=notes
    )
    
    if fuel_log_id:
        # Получаем обновленные данные о технике
        equipment = await db.get_equipment_by_id(selected_eq['id'])
        
        response_text = f"✅ <b>Заправка добавлена успешно!</b>\n\n"
        response_text += f"<b>Техника:</b> {selected_eq['name']} ({selected_eq['model']})\n"
        response_text += f"<b>Топливо:</b> {fuel_amount} л ({fuel_type})\n"
        response_text += f"<b>Стоимость:</b> {total_cost} руб. ({cost_per_liter} руб./л)\n"
        
        if equipment and equipment.get('fuel_capacity'):
            fuel_percentage = round((equipment['current_fuel_level'] / equipment['fuel_capacity']) * 100, 1)
            response_text += f"<b>Текущий уровень:</b> {equipment['current_fuel_level']} л ({fuel_percentage}%)\n"
        
        if odometer_reading:
            response_text += f"<b>Одометр:</b> {odometer_reading} км\n"
        
        if notes:
            response_text += f"<b>Заметки:</b> {notes}\n"
        
        response_text += f"\n<code>ID заправки: #{fuel_log_id}</code>"
        
        # Отправляем уведомление начальнику парка
        await notify_manager_about_fueling(
            message.from_user.id, 
            selected_eq['id'], 
            fuel_amount, 
            total_cost,
            fuel_log_id
        )
        
    else:
        response_text = "❌ <b>Ошибка при добавлении заправки!</b>\n\nПопробуйте еще раз."
    
    await reply(message, response_text)
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Возврат в главное меню", 
               reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
 
async def notify_manager_about_fueling(driver_id, equipment_id, fuel_amount, total_cost, fuel_log_id):
    """Уведомляет начальника парка о заправке"""
    try:
        driver = await db.get_user(driver_id)
        if not driver or not driver.get('organization_id'):
            return
        
        users = await db.get_users_by_organization(driver['organization_id'])
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        equipment = await db.get_equipment_by_id(equipment_id)
        if not equipment:
            return
        
        for manager in fleet_managers:
            try:
                await send_to_user(
                    manager['telegram_id'],
                    f"⛽ <b>Новая заправка</b>\n\n"
                    f"🚛 <b>Водитель:</b> {driver['full_name']}\n"
                    f"🚜 <b>Техника:</b> {equipment['name']} ({equipment['model']})\n"
                    f"⛽ <b>Топливо:</b> {fuel_amount} л\n"
                    f"💰 <b>Стоимость:</b> {total_cost} руб.\n"
                    f"🆔 <b>ID:</b> #{fuel_log_id}\n"
                    f"🕐 <b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}"
                )
            except:
                continue
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления о заправке: {e}")
 
@dp.callback_query(F.data == "fuel_stats")
async def fuel_stats_callback(callback: types.CallbackQuery):
    """Показывает статистику расхода топлива"""
    user = await db.get_user(callback.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await callback.answer("⛔ Доступ только для директора и начальника парка!", show_alert=True)
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await callback.answer("❌ Вы не привязаны к организации!", show_alert=True)
        return
    
    equipment = await db.get_organization_equipment(org_id)
    
    if not equipment:
        await callback.message.edit_text(
            "🚜 <b>Нет техники</b>\n\n"
            "Сначала добавьте технику в организацию."
        )
        await callback.answer()
        return
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🚜 {eq['name'][:20]}", callback_data=f"fuel_stats_eq:{eq['id']}")]
            for eq in equipment[:10]
        ]
    )
    
    await callback.message.edit_text(
        "📊 <b>Статистика расхода топлива</b>\n\n"
        "Выберите технику для просмотра статистики:",
        reply_markup=keyboard
    )
    await callback.answer()
 
@dp.callback_query(F.data.startswith("fuel_stats_eq:"))
async def fuel_stats_equipment_callback(callback: types.CallbackQuery):
    """Показывает статистику по конкретной технике"""
    equipment_id = int(callback.data.split(":")[1])
    
    equipment = await db.get_equipment_by_id(equipment_id)
    if not equipment:
        await callback.answer("❌ Техника не найдена!", show_alert=True)
        return
    
    # Получаем статистику за 30 дней
    stats = await db.get_fuel_statistics(equipment_id, 30)
    
    text = f"📊 <b>Статистика расхода</b>\n\n"
    text += f"🚜 <b>Техника:</b> {equipment['name']} ({equipment['model']})\n\n"
    
    if stats.get('total_fuel'):
        text += f"<b>📅 За 30 дней:</b>\n"
        text += f"• Всего топлива: {stats['total_fuel']} л\n"
        text += f"• Общая стоимость: {stats.get('total_cost', 0)} руб.\n"
        text += f"• Средняя цена: {stats.get('avg_price', 0)} руб./л\n"
        
        if stats.get('avg_consumption'):
            text += f"• Средний расход: {stats['avg_consumption']} л/100км\n"
            text += f"• Пройдено: {stats.get('km_traveled', 0)} км\n"
        
        text += f"\n<b>Текущий уровень:</b> {equipment.get('current_fuel_level', 0)} л"
        
        if equipment.get('fuel_capacity'):
            percentage = round((equipment['current_fuel_level'] / equipment['fuel_capacity']) * 100, 1)
            text += f" ({percentage}%)\n"
            if percentage < 20:
                text += f"⚠️ <b>Низкий уровень топлива!</b>\n"
        text += f"\n<b>Одометр:</b> {equipment.get('odometer', 0)} км\n"
    else:
        text += "📊 <b>Нет данных о заправках за последние 30 дней</b>"
    
    await callback.message.edit_text(text)
    await callback.answer()
 
@dp.callback_query(F.data == "fuel_history")
async def fuel_history_callback(callback: types.CallbackQuery):
    """Показывает историю заправок"""
    user = await db.get_user(callback.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await callback.answer("⛔ Доступ только для директора и начальника парка!", show_alert=True)
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await callback.answer("❌ Вы не привязаны к организации!", show_alert=True)
        return
    
    # Получаем последние 10 заправок
    fuel_logs = await db.get_fuel_logs(days=30)
    org_equipment = await db.get_organization_equipment(org_id)
    org_equipment_ids = [eq['id'] for eq in org_equipment]
    
    org_fuel_logs = [log for log in fuel_logs if log['equipment_id'] in org_equipment_ids]
    
    if not org_fuel_logs:
        await callback.message.edit_text(
            "📋 <b>История заправок</b>\n\n"
            "За последние 30 дней заправок не было."
        )
        await callback.answer()
        return
    
    text = "📋 <b>История заправок (последние 10)</b>\n\n"
    
    for log in org_fuel_logs[:10]:
        date = datetime.strptime(log['fueling_date'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
        text += f"<b>{date}</b>\n"
        text += f"🚜 {log.get('equipment_name', 'Техника')}\n"
        text += f"⛽ {log['fuel_amount']} л ({log['fuel_type']})\n"
        text += f"💰 {log.get('total_cost', 0)} руб.\n"
        
        if log.get('driver_name'):
            text += f"👤 {log['driver_name']}\n"
        
        text += f"🆔 #{log['id']}\n\n"
    
    await callback.message.edit_text(text)
    await callback.answer()
 
@dp.callback_query(F.data == "low_fuel")
async def low_fuel_callback(callback: types.CallbackQuery):
    """Показывает технику с низким уровнем топлива"""
    user = await db.get_user(callback.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await callback.answer("⛔ Доступ только для директора и начальника парка!", show_alert=True)
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await callback.answer("❌ Вы не привязаны к организации!", show_alert=True)
        return
    
    low_fuel_equipment = await db.get_low_fuel_equipment(org_id, 20.0)
    
    if not low_fuel_equipment:
        await callback.message.edit_text(
            "⚠️ <b>Низкий уровень топлива</b>\n\n"
            "✅ Вся техника имеет достаточный уровень топлива (>20%)."
        )
        await callback.answer()
        return
    
    text = "⚠️ <b>Техника с низким уровнем топлива</b>\n\n"
    
    for eq in low_fuel_equipment:
        fuel_percentage = eq.get('fuel_percentage', 0)
        text += f"🚜 <b>{eq['name']}</b> ({eq['model']})\n"
        text += f"⛽ Уровень: {eq.get('current_fuel_level', 0)} л ({fuel_percentage}%)\n"
        
        if fuel_percentage < 10:
            text += "🚨 <b>Требуется срочная заправка!</b>\n"
        elif fuel_percentage < 20:
            text += "⚠️ <b>Требуется заправка в ближайшее время</b>\n"
        
        text += "\n"
    
    await callback.message.edit_text(text)
    await callback.answer()
 
# ========== ЗАПЧАСТИ ==========
 
@dp.message(F.text == "🔧 Запчасти")
async def spare_parts_menu(message: types.Message):
    """Меню управления запчастями"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Нет доступа к управлению запчастями!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить запчасть", callback_data="add_spare_part")],
            [InlineKeyboardButton(text="📋 Список запчастей", callback_data="list_spare_parts")],
            [InlineKeyboardButton(text="⚠️ Низкий запас", callback_data="low_stock_parts")],
            [InlineKeyboardButton(text="📦 Заказать запчасть", callback_data="order_part")],
        ]
    )
    
    await reply(
        message,
        "🔧 <b>Управление запчастями</b>\n\n"
        "Склад запчастей и управление запасами.",
        reply_markup=keyboard
    )
 
@dp.message(F.text == "🔧 Заказать запчасть")
async def order_part_driver(message: types.Message, state: FSMContext):
    """Начинает процесс заказа запчасти для водителя"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут заказывать запчасти!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    await state.update_data(org_id=org_id, requested_by=message.from_user.id)
    
    await reply(
        message,
        "🔧 <b>Заказ запчасти</b>\n\n"
        "Введите название запчасти, которую нужно заказать:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_part_name)
 
@dp.message(UserStates.waiting_for_part_name)
async def process_part_name(message: types.Message, state: FSMContext):
    """Обрабатывает название запчасти"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Заказ отменен", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    part_name = message.text.strip()
    if len(part_name) < 2:
        await reply(message, "❌ Название слишком короткое!")
        return
    
    await state.update_data(part_name=part_name)
    
    await reply(
        message,
        f"✅ <b>Запчасть:</b> {part_name}\n\n"
        f"Введите описание или номер детали (необязательно):",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="⏭️ Без описания")], [types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(UserStates.waiting_for_part_details)
 
@dp.message(UserStates.waiting_for_part_details)
async def process_part_details(message: types.Message, state: FSMContext):
    """Обрабатывает описание запчасти"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Заказ отменен", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    description = None
    if message.text != "⏭️ Без описания":
        description = message.text
    
    await state.update_data(description=description)
    
    await reply(
        message,
        "🔢 <b>Введите количество:</b>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_part_quantity)
 
@dp.message(UserStates.waiting_for_part_quantity)
async def process_part_quantity(message: types.Message, state: FSMContext):
    """Обрабатывает количество запчастей"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Заказ отменен", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    try:
        quantity = int(message.text)
        if quantity <= 0 or quantity > 1000:
            await reply(message, "❌ Некорректное количество! Введите от 1 до 1000")
            return
    except ValueError:
        await reply(message, "❌ Введите целое число!")
        return
    
    await state.update_data(quantity=quantity)
    
    await reply(
        message,
        f"✅ <b>Количество:</b> {quantity} шт.\n\n"
        f"Выберите срочность заказа:",
        reply_markup=get_urgency_keyboard()
    )
    await state.set_state(UserStates.waiting_for_order_urgency)
 
@dp.message(UserStates.waiting_for_order_urgency)
async def process_order_urgency(message: types.Message, state: FSMContext):
    """Обрабатывает срочность заказа"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Заказ отменен", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    urgency_map = {
        "🚨 Срочно (сегодня)": True,
        "⚠️ Средняя (1-3 дня)": False,
        "📅 Не срочно (неделя)": False
    }
    
    if message.text not in urgency_map:
        await reply(message, "❌ Выберите срочность из списка")
        return
    
    urgent = urgency_map[message.text]
    
    data = await state.get_data()
    org_id = data.get('org_id')
    part_name = data.get('part_name')
    description = data.get('description')
    quantity = data.get('quantity')
    requested_by = data.get('requested_by')
    
    # Создаем заказ
    order_id = await db.create_order(
        organization_id=org_id,
        order_type='parts',
        part_name=part_name,
        quantity=quantity,
        urgent=urgent,
        requested_by=requested_by,
        notes=description
    )
    
    if order_id:
        await reply(
            message,
            f"✅ <b>Заказ создан успешно!</b>\n\n"
            f"<b>Запчасть:</b> {part_name}\n"
            f"<b>Количество:</b> {quantity} шт.\n"
            f"<b>Срочность:</b> {'🚨 Срочно' if urgent else '📅 Обычная'}\n"
            f"<b>ID заказа:</b> #{order_id}\n\n"
            f"Заказ отправлен начальнику парка на утверждение."
        )
        
        # Уведомляем начальника парка
        await notify_manager_about_order(order_id, org_id, message.from_user.id, part_name, quantity, urgent)
    else:
        await reply(message, "❌ Ошибка при создании заказа!")
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Возврат в главное меню", 
               reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
 
async def notify_manager_about_order(order_id, org_id, requester_id, part_name, quantity, urgent):
    """Уведомляет начальника парка о новом заказе"""
    try:
        requester = await db.get_user(requester_id)
        users = await db.get_users_by_organization(org_id)
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        urgency_text = "🚨 СРОЧНО" if urgent else "📅 Обычный"
        
        for manager in fleet_managers:
            try:
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Утвердить", callback_data=f"approve_order:{order_id}"),
                         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_order:{order_id}")]
                    ]
                )
                
                await send_to_user(
                    manager['telegram_id'],
                    f"📦 <b>Новый заказ запчастей</b>\n\n"
                    f"👤 <b>Заказал:</b> {requester['full_name']}\n"
                    f"🔧 <b>Запчасть:</b> {part_name}\n"
                    f"🔢 <b>Количество:</b> {quantity} шт.\n"
                    f"⏱️ <b>Срочность:</b> {urgency_text}\n"
                    f"🆔 <b>ID заказа:</b> #{order_id}\n\n"
                    f"Утвердить заказ?",
                    reply_markup=keyboard
                )
            except:
                continue
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления о заказе: {e}")
 
@dp.callback_query(F.data.startswith("approve_order:"))
async def approve_order_callback(callback: types.CallbackQuery):
    """Утверждает заказ"""
    order_id = int(callback.data.split(":")[1])
    
    success = await db.update_order_status(order_id, 'approved', callback.from_user.id)
    
    if success:
        await callback.message.edit_text(
            f"✅ <b>Заказ утвержден!</b>\n\n"
            f"Заказ #{order_id}\n"
            f"Утвердил: {callback.from_user.full_name}"
        )
        
        # Уведомляем заказчика
        order = await db.get_orders_by_id(order_id)
        if order and order.get('requested_by'):
            await send_to_user(
                order['requested_by'],
                f"✅ <b>Ваш заказ утвержден!</b>\n\n"
                f"Заказ #{order_id} был утвержден начальником парка.\n"
                f"Запчасть будет заказана в ближайшее время."
            )
    else:
        await callback.answer("❌ Ошибка при утверждении заказа", show_alert=True)
    
    await callback.answer()
 
@dp.callback_query(F.data.startswith("reject_order:"))
async def reject_order_callback(callback: types.CallbackQuery):
    """Отклоняет заказ"""
    order_id = int(callback.data.split(":")[1])
    
    success = await db.update_order_status(order_id, 'rejected', callback.from_user.id)
    
    if success:
        await callback.message.edit_text(
            f"❌ <b>Заказ отклонен</b>\n\n"
            f"Заказ #{order_id}\n"
            f"Отклонил: {callback.from_user.full_name}"
        )
        
        # Уведомляем заказчика
        order = await db.get_orders_by_id(order_id)
        if order and order.get('requested_by'):
            await send_to_user(
                order['requested_by'],
                f"❌ <b>Ваш заказ отклонен</b>\n\n"
                f"Заказ #{order_id} был отклонен начальником парка.\n"
                f"Для уточнения причин обратитесь к начальнику парка."
            )
    else:
        await callback.answer("❌ Ошибка при отклонении заказа", show_alert=True)
    
    await callback.answer()
 
# ========== ИНСТРУКЦИИ ==========
 
@dp.message(F.text == "📋 Инструкции")
async def instructions_menu(message: types.Message):
    """Меню инструкций"""
    user = await db.get_user(message.from_user.id)
    
    if not user:
        await reply(message, "❌ Сначала зарегистрируйтесь через /start")
        return
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔧 Обслуживание", callback_data="instructions:maintenance")],
            [InlineKeyboardButton(text="⛽ Заправка", callback_data="instructions:fueling")],
            [InlineKeyboardButton(text="🔩 Шприцевание", callback_data="instructions:greasing")],
            [InlineKeyboardButton(text="🔍 Осмотр", callback_data="instructions:inspection")],
            [InlineKeyboardButton(text="🔎 Поиск по модели", callback_data="instructions:search")],
        ]
    )
    
    await reply(
        message,
        "📋 <b>Инструкции по обслуживанию</b>\n\n"
        "Выберите тип инструкции:",
        reply_markup=keyboard
    )
 
@dp.callback_query(F.data.startswith("instructions:"))
async def instructions_callback(callback: types.CallbackQuery):
    """Показывает инструкции"""
    instruction_type = callback.data.split(":")[1]
    
    if instruction_type == "search":
        await callback.message.edit_text(
            "🔎 <b>Поиск инструкций по модели</b>\n\n"
            "Введите модель техники:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="instructions_cancel")]
                ]
            )
        )
        # Здесь можно реализовать поиск, но для простоты покажем пример
        await callback.answer()
        return
    
    # Примеры инструкций
    instructions = {
        "maintenance": (
            "🔧 <b>Обслуживание техники</b>\n\n"
            "1. <b>Ежедневное обслуживание:</b>\n"
            "• Проверка уровней жидкостей\n"
            "• Проверка давления в шинах\n"
            "• Проверка работы фар и сигналов\n"
            "• Осмотр на утечки\n\n"
            "2. <b>Еженедельное обслуживание:</b>\n"
            "• Очистка фильтров\n"
            "• Проверка аккумулятора\n"
            "• Смазка шарниров\n\n"
            "3. <b>Ежемесячное обслуживание:</b>\n"
            "• Замена масла (если требуется)\n"
            "• Проверка тормозной системы\n"
            "• Диагностика электросистемы"
        ),
        "fueling": (
            "⛽ <b>Правила заправки</b>\n\n"
            "1. <b>Подготовка:</b>\n"
            "• Заглушить двигатель\n"
            "• Выключить зажигание\n"
            "• Не курить вблизи\n"
            "• Использовать только разрешенное топливо\n\n"
            "2. <b>Процесс заправки:</b>\n"
            "• Проверить чистоту горловины\n"
            "• Использовать чистую тару/пистолет\n"
            "• Не переливать (оставить 5% объема)\n"
            "• Плотно закрыть крышку\n\n"
            "3. <b>После заправки:</b>\n"
            "• Проверить на утечки\n"
            "• Записать данные в журнал\n"
            "• Прикрепить чек"
        ),
        "greasing": (
            "🔩 <b>Шприцевание (смазка)</b>\n\n"
            "1. <b>Что смазывать:</b>\n"
            "• Шаровые опоры\n"
            "• ШРУСы\n"
            "• Карданные шарниры\n"
            "• Тросы управления\n"
            "• Подшипники\n\n"
            "2. <b>Интервалы смазки:</b>\n"
            "• Каждые 100 часов работы\n"
            "• Или раз в месяц\n"
            "• После работы в пыльных условиях\n\n"
            "3. <b>Типы смазок:</b>\n"
            "• Литиевая смазка - для большинства узлов\n"
            "• Медная смазка - для высоких температур\n"
            "• Силиконовая смазка - для резиновых деталей"
        ),
        "inspection": (
            "🔍 <b>Предрейсовый осмотр</b>\n\n"
            "1. <b>Внешний осмотр:</b>\n"
            "• Шины (давление, износ, повреждения)\n"
            "• Кузов (отсутствие повреждений)\n"
            "• Стекло (чистота, трещины)\n"
            "• Зеркала (чистота, регулировка)\n\n"
            "2. <b>Проверка жидкостей:</b>\n"
            "• Масло двигателя\n"
            "• Охлаждающая жидкость\n"
            "• Тормозная жидкость\n"
            "• Жидкость ГУР\n"
            "• Омыватель стекла\n\n"
            "3. <b>Проверка оборудования:</b>\n"
            "• Фары и сигналы\n"
            "• Звуковой сигнал\n"
            "• Стеклоочистители\n"
            "• Система отопления/кондиционирования"
        ),
    }
    
    instruction_text = instructions.get(instruction_type, "Инструкция не найдена.")
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Все инструкции", callback_data="instructions_menu")],
            [InlineKeyboardButton(text="🤖 Спросить ИИ", callback_data="ai_from_instruction")],
        ]
    )
    
    await callback.message.edit_text(
        instruction_text,
        reply_markup=keyboard
    )
    await callback.answer()
 
@dp.callback_query(F.data == "ai_from_instruction")
async def ai_from_instruction_callback(callback: types.CallbackQuery, state: FSMContext):
    """Переход от инструкции к ИИ"""
    await callback.message.edit_reply_markup(reply_markup=None)
    
    await reply(
        callback.message,
        "🤖 <b>ИИ Помощник</b>\n\n"
        "Задайте уточняющий вопрос по инструкции:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_ai_question)
    await callback.answer()
 
# ========== АНАЛИТИКА ==========
 
@dp.message(F.text == "📈 Аналитика")
async def analytics_menu(message: types.Message, state: FSMContext):
    """Меню аналитики"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора и начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    await reply(
        message,
        "📈 <b>Аналитика организации</b>\n\n"
        "Выберите период для анализа:",
        reply_markup=get_period_keyboard()
    )
    await state.update_data(org_id=org_id)
    await state.set_state(UserStates.waiting_for_analytics_period)
 
@dp.message(UserStates.waiting_for_analytics_period)
async def process_analytics_period(message: types.Message, state: FSMContext):
    """Обрабатывает выбор периода для аналитики"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Аналитика отменена", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    period_map = {
        "📅 За сегодня": 1,
        "📅 За неделю": 7,
        "📅 За месяц": 30,
        "📅 За 3 месяца": 90,
        "📅 За год": 365
    }
    
    if message.text not in period_map:
        await reply(message, "❌ Выберите период из списка")
        return
    
    days = period_map[message.text]
    data = await state.get_data()
    org_id = data.get('org_id')
    
    # Получаем аналитику
    analytics = await db.get_organization_analytics(org_id, days)
    
    text = f"📈 <b>Аналитика за {days} дней</b>\n\n"
    
    if analytics.get('shifts'):
        shifts = analytics['shifts']
        text += "<b>📊 Смены:</b>\n"
        text += f"• Всего: {shifts.get('total_shifts', 0)}\n"
        text += f"• Завершено: {shifts.get('completed_shifts', 0)}\n"
        text += f"• Средняя продолжительность: {shifts.get('avg_shift_hours', 0)} ч\n\n"
    
    if analytics.get('fuel'):
        fuel = analytics['fuel']
        text += "<b>⛽ Топливо:</b>\n"
        text += f"• Всего заправлено: {fuel.get('total_fuel', 0)} л\n"
        text += f"• Общая стоимость: {fuel.get('total_fuel_cost', 0)} руб.\n"
        text += f"• Средняя цена: {fuel.get('avg_fuel_price', 0)} руб./л\n\n"
    
    if analytics.get('maintenance'):
        maintenance = analytics['maintenance']
        text += "<b>🔧 Техобслуживание:</b>\n"
        text += f"• Всего ТО: {maintenance.get('total_maintenance', 0)}\n"
        text += f"• Выполнено: {maintenance.get('completed_maintenance', 0)}\n"
        text += f"• Общая стоимость: {maintenance.get('total_maintenance_cost', 0)} руб.\n\n"
    
    if analytics.get('equipment_by_status'):
        equipment = analytics['equipment_by_status']
        text += "<b>🚜 Техника по статусам:</b>\n"
        status_names = {
            'active': '✅ Активная',
            'maintenance': '🔧 На ТО',
            'repair': '🔨 В ремонте',
            'inactive': '❌ Неактивная'
        }
        for status, count in equipment.items():
            text += f"• {status_names.get(status, status)}: {count} ед.\n"
    
    # Получаем топ-3 водителей по количеству смен
    users = await db.get_users_by_organization(org_id)
    drivers = [u for u in users if u['role'] == 'driver']
    
    driver_stats = []
    for driver in drivers[:5]:  # Берем первых 5 для анализа
        stats = await db.get_driver_stats(driver['telegram_id'], days)
        if stats.get('shifts_count', 0) > 0:
            driver_stats.append({
                'name': driver['full_name'],
                'shifts': stats['shifts_count'],
                'hours': stats.get('avg_shift_hours', 0)
            })
    
    if driver_stats:
        driver_stats.sort(key=lambda x: x['shifts'], reverse=True)
        text += f"\n<b>👥 Топ-{min(3, len(driver_stats))} водителя:</b>\n"
        for i, driver in enumerate(driver_stats[:3]):
            text += f"{i+1}. {driver['name']}: {driver['shifts']} смен, {driver['hours']} ч/смену\n"
    
    await reply(message, text)
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Возврат в главное меню", 
               reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
 
# ========== ЗАКАЗЫ ==========
 
@dp.message(F.text == "📦 Заказы")
async def orders_menu(message: types.Message):
    """Меню управления заказами"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Нет доступа к управлению заказами!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Все заказы", callback_data="list_orders")],
            [InlineKeyboardButton(text="⏳ Ожидающие", callback_data="pending_orders")],
            [InlineKeyboardButton(text="✅ Утвержденные", callback_data="approved_orders")],
            [InlineKeyboardButton(text="📦 Заказанные", callback_data="ordered_orders")],
        ]
    )
    
    await reply(
        message,
        "📦 <b>Управление заказами</b>\n\n"
        "Просмотр и управление заказами запчастей и топлива.",
        reply_markup=keyboard
    )
 
@dp.callback_query(F.data == "list_orders")
async def list_orders_callback(callback: types.CallbackQuery):
    """Показывает все заказы"""
    user = await db.get_user(callback.from_user.id)
    org_id = user.get('organization_id')
    
    orders = await db.get_orders(org_id)
    
    if not orders:
        await callback.message.edit_text(
            "📦 <b>Заказы</b>\n\n"
            "Заказов пока нет."
        )
        await callback.answer()
        return
    
    text = "📦 <b>Все заказы</b>\n\n"
    
    for order in orders[:10]:
        status_emoji = {
            'pending': '⏳',
            'approved': '✅',
            'ordered': '📦',
            'delivered': '🚚',
            'cancelled': '❌'
        }.get(order['status'], '❓')
        
        text += f"{status_emoji} <b>Заказ #{order['id']}</b>\n"
        text += f"Тип: {order['order_type']}\n"
        
        if order.get('part_name'):
            text += f"Запчасть: {order['part_name']}\n"
        if order.get('equipment_name'):
            text += f"Техника: {order['equipment_name']}\n"
        
        text += f"Количество: {order['quantity']}\n"
        text += f"Статус: {order['status']}\n"
        
        if order.get('requested_by_name'):
            text += f"Заказал: {order['requested_by_name']}\n"
        
        text += f"Дата: {order['created_at'][:10]}\n\n"
    
    if len(orders) > 10:
        text += f"... и ещё {len(orders) - 10} заказов"
    
    await callback.message.edit_text(text)
    await callback.answer()
 
# ========== ОБНОВЛЕНИЕ СМЕНЫ С ОДОМЕТРОМ ==========
 
@dp.message(F.text == "🚛 Начать смену")
async def start_shift_begin(message: types.Message, state: FSMContext):
    """Начинает процесс начала смены с учетом одометра"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут начинать смены!")
        return
    
    active_shift = await db.get_active_shift(message.from_user.id)
    if active_shift:
        await reply(
            message,
            f"⚠️ <b>У вас уже есть активная смена!</b>\n\n"
            f"Смена начата: {active_shift['start_time'][:16]}\n"
            f"Техника: {active_shift.get('equipment_name', 'Не указана')}\n"
            f"Пробег: {active_shift.get('odometer', 'Не указан')} км\n\n"
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
async def process_equipment_selection_with_odometer(message: types.Message, state: FSMContext):
    """Обрабатывает выбор техники с запросом одометра"""
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
        f"✅ <b>Выбрана техника:</b> {selected_eq['name']} ({selected_eq['model']})\n\n"
        f"Введите показания одометра (пробег в км):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_briefing_confirmation)
 
@dp.message(UserStates.waiting_for_briefing_confirmation)
async def process_odometer_and_briefing(message: types.Message, state: FSMContext):
    """Обрабатывает одометр и инструктаж"""
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
    
    await state.update_data(start_odometer=start_odometer)
    
    data = await state.get_data()
    selected_eq = data.get('selected_equipment')
    
    await reply(
        message,
        f"✅ <b>Пробег:</b> {start_odometer} км\n\n"
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
 
# Обновите также функцию завершения смены для учета конечного одометра
@dp.message(F.text == "✅ Закончить смену")
async def end_shift_with_odometer(message: types.Message, state: FSMContext):
    """Завершает смену с учетом одометра"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут завершать смены!")
        return
    
    active_shift = await db.get_active_shift(message.from_user.id)
    if not active_shift:
        await reply(message, "❌ У вас нет активной смены!")
        return
    
    await state.update_data(shift_id=active_shift['id'])
    
    await reply(
        message,
        f"🛑 <b>Завершение смены #{active_shift['id']}</b>\n\n"
        f"<b>Техника:</b> {active_shift.get('equipment_name', 'Неизвестно')}\n"
        f"<b>Начало:</b> {active_shift['start_time'][:16]}\n"
        f"<b>Начальный пробег:</b> {active_shift.get('start_odometer', 'Не указан')} км\n\n"
        f"Введите конечные показания одометра (км):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_shift_notes)
 
# ========== СИСТЕМА НАПОМИНАНИЙ ==========
 
async def check_and_send_notifications():
    """Проверяет и отправляет напоминания"""
    try:
        organizations = await db.get_all_organizations()
        
        for org in organizations:
            org_id = org['id']
            
            # Проверяем предстоящие ТО
            upcoming_maintenance = await db.get_upcoming_maintenance(org_id, 7)  # На 7 дней вперед
            
            for maintenance in upcoming_maintenance:
                equipment_name = maintenance.get('equipment_name', 'Неизвестная техника')
                maintenance_type = maintenance.get('maintenance_type', 'ТО')
                
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
                                f"🔧 <b>Тип ТО:</b> {maintenance_type}\n"
                                f"📅 <b>Следующее ТО:</b> "
                                f"{'через ' + str(maintenance.get('days_left', 0)) + ' дней' if maintenance.get('days_left') else 'скоро'}\n"
                                f"📏 <b>Пробег:</b> {maintenance.get('odometer', 0)} км\n"
                                f"🎯 <b>Цель:</b> {maintenance.get('next_due_km', 0)} км / {maintenance.get('next_due_date', 'не указано')}\n\n"
                                f"⚠️ Запланируйте обслуживание заранее!"
                            )
                        except:
                            continue
            
            # Проверяем низкий уровень топлива
            low_fuel_equipment = await db.get_low_fuel_equipment(org_id, 15.0)
            
            for eq in low_fuel_equipment:
                fuel_percentage = eq.get('fuel_percentage', 0)
                
                if fuel_percentage < 15:
                    for user in users:
                        if user['role'] in ['director', 'fleetmanager']:
                            try:
                                await send_to_user(
                                    user['telegram_id'],
                                    f"⚠️ <b>Низкий уровень топлива</b>\n\n"
                                    f"🚜 <b>Техника:</b> {eq['name']} ({eq['model']})\n"
                                    f"⛽ <b>Уровень:</b> {eq.get('current_fuel_level', 0)} л ({fuel_percentage}%)\n"
                                    f"📏 <b>Одометр:</b> {eq.get('odometer', 0)} км\n\n"
                                    f"🚨 Требуется заправка!"
                                )
                            except:
                                continue
    
    except Exception as e:
        logger.error(f"Ошибка в системе напоминаний: {e}")
 
# ========== ПЛАНИРОВЩИК ==========
 
async def scheduler():
    """Планировщик задач"""
    # Проверяем напоминания каждый час
    aioschedule.every().hour.do(check_and_send_notifications)
    
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(60)  # Проверяем каждую минуту
 
# ========== ЗАПУСК БОТА ==========
 
async def on_startup():
    """Инициализация при запуске"""
    try:
        await db.connect()
        
        # Создаем администратора
        ADMIN_ID = 1079922982  # Ваш Telegram ID
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
