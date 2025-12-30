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

# НОВЫЕ НАСТРОЙКИ YANDEX GPT
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
    waiting_for_fuel_type = State()  # ДОБАВЛЕНО: отдельное состояние для типа топлива
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
        
        system_prompt = (
            "Ты — профессиональный помощник по обслуживанию и эксплуатации спецтехники. "
            "Твоя задача — давать точные, профессиональные и подробные ответы на вопросы "
            "по техническому обслуживанию, ремонту, эксплуатации спецтехники, а также по "
            "технике безопасности. Отвечай на русском языке, используй техническую терминологию. "
            "Если не уверен в ответе, скажи об этом и посоветуй обратиться к специалисту."
        )
        
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
                    "text": f"{context}\n\nВопрос пользователя: {question}"
                }
            ]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data, timeout=30) as response:
                if response.status == 200:
                    result = await response.json()
                    answer = result['result']['alternatives'][0]['message']['text']
                    
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
        
        # Приоритет 1: Yandex GPT
        if YANDEX_API_KEY and YANDEX_FOLDER_ID:
            logger.info("Использую Yandex GPT")
            return await ask_yandex_gpt(question, context, user_id)
        
        # Приоритет 2: OpenAI
        elif OPENAI_API_KEY:
            try:
                openai.api_key = OPENAI_API_KEY
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
                            source="openai"
                        )
                
                return answer
            except Exception as e:
                logger.error(f"Ошибка OpenAI: {e}")
        
        # Приоритет 3: Hugging Face
        elif HUGGINGFACE_API_KEY:
            try:
                API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-large"
                headers = {"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"}
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        API_URL,
                        headers=headers,
                        json={"inputs": f"Вопрос о спецтехнике: {question}. Ответь подробно."},
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            if isinstance(result, list) and len(result) > 0:
                                answer = result[0].get('generated_text', 'Извините, не могу ответить.')
                                return answer
                        else:
                            return "⚠️ Ошибка при обращении к ИИ"
            except Exception as e:
                logger.error(f"Ошибка Hugging Face: {e}")
        
        # Запасные ответы (локальная база)
        answers = {
            "масло": "✅ **Проверка масла в двигателе:**\n\n1. **Заглушить двигатель** и подождать 5-10 минут для стекания масла\n2. **Вынуть масляный щуп**, протереть его чистой тряпкой\n3. **Вставить щуп обратно** до упора и снова вынуть\n4. **Уровень должен быть** между метками MIN и MAX\n5. **Цвет масла:** золотистый или светло-коричневый - нормально; черный, молочный или с металлической стружкой - требуется замена\n\n⚠️ **Важно:** Если уровень ниже MIN, долейте масло той же марки. Если выше MAX, слейте излишек.",
            "тормоза": "✅ **Проверка тормозной системы:**\n\n1. **Тормозная жидкость:** уровень должен быть между MIN и MAX\n2. **Цвет жидкости:** прозрачный или светло-желтый - нормально; темный или мутный - требуется замена\n3. **Тормозные колодки:** минимальная толщина 3 мм\n4. **Тормозные диски:** без глубоких борозд и трещин\n5. **Педаль тормоза:** должна быть упругой, не проваливаться\n\n🚨 **Тревожные признаки:** скрип, вибрация при торможении, увеличенный тормозной путь",
            "шины": "✅ **Проверка шин:**\n\n**Давление (стандартные значения):**\n- Передние: 8-9 бар\n- Заредние: 6-7 бар\n- Запасное: 8 бар\n\n**Протектор:**\n- Минимальная глубина: 3 мм\n- Летние шины: 1.6 мм (по закону)\n- Зимние шины: 4 мм (рекомендуется)\n\n**Внешний вид:**\n- Нет порезов, гвоздей, трещин\n- Равномерный износ\n- Правильная балансировка (нет вибрации)\n\n📅 **Рекомендуется проверять давление еженедельно!**",
            "топливо": "✅ **Правила заправки дизельной техники:**\n\n1. **Тип топлива:** только дизельное ДТ\n2. **Качество:** заправляйтесь только на проверенных АЗС (Лукойл, Газпром, Роснефть)\n3. **Зимнее топливо:** при температуре ниже -5°C используйте зимнюю солярку\n4. **Объем:** не заправляйтесь 'под горлышко', оставляйте 5-10% объема\n5. **Чек:** всегда берите и сохраняйте чек\n\n⛽ **Нормы расхода (примерные):**\n- Экскаватор: 12-18 л/час\n- Погрузчик: 8-12 л/час\n- Каток: 6-10 л/час\n- Самосвал: 25-35 л/100км",
            "аккумулятор": "✅ **Проверка аккумулятора:**\n\n1. **Напряжение:** 12.6-12.8В - норма; ниже 12.4В - требуется зарядка\n2. **Клеммы:** чистые, без окисления, хорошо затянуты\n3. **Крепление:** аккумулятор должен быть надежно закреплен\n4. **Уровень электролита:** выше пластин на 10-15 мм\n5. **Плотность электролита:** 1.27-1.29 г/см³\n\n⚠️ **Зимой:** держите заряд не ниже 75%",
            "фильтры": "✅ **Замена фильтров:**\n\n**Воздушный фильтр:**\n- Замена каждые 500 часов или при загрязнении\n- Признаки загрязнения: черный дым, потеря мощности\n\n**Топливный фильтр:**\n- Замена каждые 1000 часов\n- Признаки загрязнения: трудный запуск, рывки при работе\n\n**Масляный фильтр:**\n- Замена при каждой замене масла\n- Обычно каждые 250-500 часов",
            "смазка": "✅ **Шприцевание (смазка) техники:**\n\n**Что смазывать:**\n1. Шарниры рычагов\n2. ШРУСы\n3. Карданные валы\n4. Тросы управления\n5. Шкворни\n6. Подшипники\n\n**Интервалы:**\n- Каждые 50 часов работы\n- После работы в пыльных условиях\n- Перед длительным хранением\n\n**Типы смазок:**\n- Литиевая (основная)\n- Медная (высокие температуры)\n- Силиконовая (резиновые детали)",
            "гидравлика": "✅ **Проверка гидравлической системы:**\n\n1. **Уровень жидкости:** между MIN и MAX\n2. **Цвет:** прозрачный или светло-желтый\n3. **Температура:** 50-80°C - норма; выше 90°C - перегрев\n4. **Давление:** по манометру на панели\n5. **Утечки:** проверьте шланги и соединения\n\n⚠️ **Признаки проблем:** медленная работа, шум, перегрев",
        }
        
        question_lower = question.lower()
        for key, answer in answers.items():
            if key in question_lower:
                return answer
        
        # Если вопрос не найден в локальной базе
        return ("🤖 **ИИ-помощник:**\n\n"
                "К сожалению, я не нашел точного ответа на ваш вопрос в своей базе знаний.\n\n"
                "**Рекомендую:**\n"
                "1. Обратиться к руководству по эксплуатации техники\n"
                "2. Проконсультироваться с начальником парка\n"
                "3. Вызвать технического специалиста\n\n"
                "**Вы можете уточнить вопрос или задать его по-другому.**")
        
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
    
    # Проверяем, есть ли клавиатура для роли
    keyboard_list = keyboards.get(role, keyboards['driver'])
    
    return types.ReplyKeyboardMarkup(
        keyboard=keyboard_list,
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
        "• Почему техника не заводится?\n"
        "• Как шприцевать экскаватор?\n\n"
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

# ========== СОЗДАНИЕ ОРГАНИЗАЦИИ (важная функция для MVP) ==========
@dp.message(F.text == "🏢 Создать организацию")
async def create_organization_start(message: types.Message, state: FSMContext):
    """Начинает процесс создания организации"""
    user = await db.get_user(message.from_user.id)
    
    if not user:
        await reply(message, "❌ Сначала зарегистрируйтесь через /start")
        return
    
    if user['role'] != 'director':
        await reply(message, "❌ Только директор может создавать организацию!")
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
    await state.set_state(UserStates.waiting_for_org_name)

@dp.message(UserStates.waiting_for_org_name)
async def process_org_name(message: types.Message, state: FSMContext):
    """Обрабатывает название организации"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Создание организации отменено", 
                   reply_markup=get_main_keyboard(user['role'], False))
        return
    
    org_name = message.text.strip()
    
    if len(org_name) < 2:
        await reply(message, "❌ Название слишком короткое!")
        return
    
    # Создаем организацию
    org_id, error = await db.create_organization_for_director(
        director_id=message.from_user.id,
        org_name=org_name
    )
    
    if error:
        await reply(message, f"❌ Ошибка: {error}")
        await state.clear()
        return
    
    await reply(
        message,
        f"✅ <b>Организация создана успешно!</b>\n\n"
        f"<b>Название:</b> {org_name}\n"
        f"<b>ID организации:</b> {org_id}\n\n"
        f"Теперь вы можете добавлять технику и сотрудников."
    )
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Главное меню", 
               reply_markup=get_main_keyboard(user['role'], True))

# ========== УЧЕТ ТОПЛИВА (исправленная версия) ==========

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
    
    if user['role'] == 'driver':
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⛽ Добавить заправку", callback_data="add_fuel")],
                [InlineKeyboardButton(text="📋 История моих заправок", callback_data="my_fuel_history")],
            ]
        )
    else:
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
    
    if user['role'] not in ['driver', 'fleetmanager', 'director']:
        await callback.answer("⛔ Только водители и руководители могут добавлять заправки!", show_alert=True)
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
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(role, has_org))
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
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(role, has_org))
        return
    
    try:
        fuel_amount = float(message.text.replace(',', '.'))
        if fuel_amount <= 0 or fuel_amount > 5000:
            await reply(message, "❌ Некорректное количество! Введите от 0.1 до 5000 литров")
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
    await state.set_state(UserStates.waiting_for_fuel_type)

@dp.message(UserStates.waiting_for_fuel_type)
async def process_fuel_type(message: types.Message, state: FSMContext):
    """Обрабатывает тип топлива"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(role, has_org))
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
    await state.set_state(UserStates.waiting_for_fuel_cost)

@dp.message(UserStates.waiting_for_fuel_cost)
async def process_fuel_cost(message: types.Message, state: FSMContext):
    """Обрабатывает стоимость топлива"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(role, has_org))
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
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(role, has_org))
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
        "Отправьте фото чека или нажмите 'Пропустить':",
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
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(role, has_org))
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
        role = user['role'] if user else 'unassigned'
        has_org = user.get('organization_id') if user else False
        await reply(message, "❌ Добавление заправки отменено", 
                   reply_markup=get_main_keyboard(role, has_org))
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

# ========== ДОБАВЛЕНИЕ ТЕХНИКИ (важная функция для MVP) ==========
@dp.message(F.text == "➕ Добавить технику")
async def add_equipment_start(message: types.Message, state: FSMContext):
    """Начинает процесс добавления техники"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Только директор и начальник парка могут добавлять технику!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    await reply(
        message,
        "🚜 <b>Добавление новой техники</b>\n\n"
        "Введите название техники (например: 'Экскаватор Volvo'):",
        reply_markup=get_cancel_keyboard()
    )
    await state.update_data(org_id=org_id)
    await state.set_state(UserStates.waiting_for_equipment_name)

@dp.message(UserStates.waiting_for_equipment_name)
async def process_equipment_name(message: types.Message, state: FSMContext):
    """Обрабатывает название техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление техники отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    equipment_name = message.text.strip()
    if len(equipment_name) < 2:
        await reply(message, "❌ Название слишком короткое!")
        return
    
    await state.update_data(equipment_name=equipment_name)
    
    await reply(
        message,
        f"✅ <b>Название:</b> {equipment_name}\n\n"
        f"Введите модель техники (например: 'EC210D'):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_equipment_model)

@dp.message(UserStates.waiting_for_equipment_model)
async def process_equipment_model(message: types.Message, state: FSMContext):
    """Обрабатывает модель техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление техники отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    model = message.text.strip()
    if len(model) < 1:
        await reply(message, "❌ Модель не может быть пустой!")
        return
    
    await state.update_data(model=model)
    
    await reply(
        message,
        f"✅ <b>Модель:</b> {model}\n\n"
        f"Введите VIN номер техники (17 символов):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_equipment_vin)

@dp.message(UserStates.waiting_for_equipment_vin)
async def process_equipment_vin(message: types.Message, state: FSMContext):
    """Обрабатывает VIN номер"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Добавление техники отменено", 
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    vin = message.text.strip().upper()
    if len(vin) != 17:
        await reply(message, "❌ VIN номер должен содержать 17 символов!")
        return
    
    data = await state.get_data()
    org_id = data.get('org_id')
    equipment_name = data.get('equipment_name')
    model = data.get('model')
    
    # Добавляем технику в базу
    equipment_id = await db.add_equipment(
        name=equipment_name,
        model=model,
        vin=vin,
        org_id=org_id,
        fuel_type='diesel'
    )
    
    if equipment_id:
        await reply(
            message,
            f"✅ <b>Техника добавлена успешно!</b>\n\n"
            f"<b>Название:</b> {equipment_name}\n"
            f"<b>Модель:</b> {model}\n"
            f"<b>VIN:</b> {vin}\n"
            f"<b>ID техники:</b> {equipment_id}\n\n"
            f"Теперь вы можете назначить технику водителю."
        )
    else:
        await reply(message, "❌ Ошибка при добавлении техники! Возможно, VIN уже существует.")
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "Главное меню", 
               reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))

# ========== ИНФОРМАЦИЯ О БОТЕ ==========
@dp.message(F.text == "ℹ️ Информация")
async def info_menu(message: types.Message):
    """Показывает информацию о боте"""
    info_text = (
        "🤖 <b>ТехКонтроль 2.0 - Система управления спецтехникой</b>\n\n"
        "🔧 <b>Основные функции:</b>\n"
        "• Учет и контроль спецтехники\n"
        "• Управление сменами водителей\n"
        "• Контроль ТО и технического обслуживания\n"
        "• Учет топлива и аналитика расхода\n"
        "• Управление запчастями\n"
        "• Система заказов и напоминаний\n"
        "• ИИ-помощник по обслуживанию\n\n"
        "👥 <b>Роли пользователей:</b>\n"
        "• <b>Директор</b> - полный контроль организации\n"
        "• <b>Начальник парка</b> - управление техникой и водителями\n"
        "• <b>Водитель</b> - работа со сменами и отчетность\n\n"
        "🚀 <b>Ближайшие обновления:</b>\n"
        "• Интеграция с системами контроля топлива\n"
        "• Автоматические напоминания о ТО\n"
        "• Расширенная аналитика\n"
        "• Мобильное приложение\n\n"
        "📞 <b>Поддержка:</b> @your_support_contact"
    )
    
    await reply(message, info_text)

# ========== МОЯ СТАТИСТИКА (для водителя) ==========
@dp.message(F.text == "📊 Моя статистика")
async def my_statistics(message: types.Message):
    """Показывает статистику водителя"""
    user = await db.get_user(message.from_user.id)
    
    if not user or user['role'] != 'driver':
        await reply(message, "❌ Эта функция доступна только водителям!")
        return
    
    # Получаем статистику за 30 дней
    stats = await db.get_driver_stats(message.from_user.id, 30)
    
    text = "📊 <b>Ваша статистика за 30 дней</b>\n\n"
    
    if stats.get('shifts_count', 0) > 0:
        text += f"<b>Количество смен:</b> {stats['shifts_count']}\n"
        text += f"<b>Средняя продолжительность смены:</b> {stats.get('avg_shift_hours', 0):.1f} ч\n"
        text += f"<b>Разных машин за рулем:</b> {stats.get('equipment_used', 0)}\n"
        
        # Получаем последние смены
        shifts = await db.get_shifts_by_driver(message.from_user.id, 5)
        if shifts:
            text += "\n<b>Последние смены:</b>\n"
            for shift in shifts:
                start_time = datetime.strptime(shift['start_time'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
                end_time = datetime.strptime(shift['end_time'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M") if shift['end_time'] else "в процессе"
                text += f"• {start_time} - {end_time}: {shift.get('equipment_name', 'Неизвестно')}\n"
    else:
        text += "📭 <b>У вас пока нет завершенных смен</b>\n\nНачните свою первую смену!"
    
    await reply(message, text)

# ========== МОЯ ТЕХНИКА (для водителя) ==========
@dp.message(F.text == "🚜 Моя техника")
async def my_equipment(message: types.Message):
    """Показывает технику водителя"""
    user = await db.get_user(message.from_user.id)
    
    if not user or user['role'] != 'driver':
        await reply(message, "❌ Эта функция доступна только водителям!")
        return
    
    equipment = await db.get_equipment_by_driver(message.from_user.id)
    
    if not equipment:
        await reply(
            message,
            "🚜 <b>Ваша техника</b>\n\n"
            "❌ Вам еще не назначена техника.\n\n"
            "Обратитесь к начальнику парка для назначения техники."
        )
        return
    
    text = "🚜 <b>Ваша техника</b>\n\n"
    
    for eq in equipment:
        text += f"<b>{eq['name']}</b> ({eq['model']})\n"
        text += f"VIN: {eq['vin']}\n"
        text += f"Статус: {eq['status']}\n"
        
        if eq.get('odometer'):
            text += f"Пробег: {eq['odometer']} км\n"
        
        if eq.get('fuel_capacity') and eq.get('current_fuel_level') is not None:
            percentage = round((eq['current_fuel_level'] / eq['fuel_capacity']) * 100, 1)
            text += f"Топливо: {eq['current_fuel_level']} л ({percentage}%)\n"
        
        if eq.get('next_maintenance'):
            next_maint = datetime.strptime(eq['next_maintenance'], "%Y-%m-%d").strftime("%d.%m.%Y")
            text += f"Следующее ТО: {next_maint}\n"
        
        text += "\n"
    
    await reply(message, text)

# ========== НАЧАЛО СМЕНЫ (исправленная версия) ==========
@dp.message(F.text == "🚛 Начать смену")
async def start_shift_begin(message: types.Message, state: FSMContext):
    """Начинает процесс начала смены"""
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
    """Обрабатывает выбор техники"""
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
        f"Введите начальные показания одометра (пробег в км):",
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
            f"📋 <b>Технический инструктаж</b>\n\n"
            f"Перед началом работы необходимо:\n\n"
            f"1. ✅ Проверить уровни жидкостей (масло, охлаждающая, тормозная)\n"
            f"2. ✅ Проверить давление в шинах\n"
            f"3. ✅ Проверить работу фар и сигналов\n"
            f"4. ✅ Убедиться в исправности тормозов\n"
            f"5. ✅ Проверить наличие документов\n\n"
            f"<b>Подтверждаете, что провели осмотр?</b>",
            reply_markup=get_yes_no_keyboard()
        )
        await state.update_data(shift_id=shift_id, start_odometer=start_odometer)
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
                maintenance_type = maintenance.get('maintenance_type', 'ТО')
                
                # Получаем пользователей организации
                users = await db.get_users_by_organization(org_id)
                
                # Отправляем уведомления директору и начальнику парка
                for user in users:
                    if user['role'] in ['director', 'fleetmanager']:
                        try:
                            days_left = maintenance.get('days_left', 0)
                            if days_left == 0:
                                days_text = "сегодня"
                            elif days_left == 1:
                                days_text = "завтра"
                            else:
                                days_text = f"через {days_left} дней"
                            
                            await send_to_user(
                                user['telegram_id'],
                                f"🔔 <b>Напоминание о ТО</b>\n\n"
                                f"🚜 <b>Техника:</b> {equipment_name}\n"
                                f"🔧 <b>Тип ТО:</b> {maintenance_type}\n"
                                f"📅 <b>Следующее ТО:</b> {days_text}\n"
                                f"📏 <b>Текущий пробег:</b> {maintenance.get('odometer', 0)} км\n"
                                f"🎯 <b>Целевой пробег:</b> {maintenance.get('next_due_km', 0)} км\n\n"
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
                                urgency = "🚨 СРОЧНО!" if fuel_percentage < 10 else "⚠️ Внимание"
                                await send_to_user(
                                    user['telegram_id'],
                                    f"{urgency} <b>Низкий уровень топлива</b>\n\n"
                                    f"🚜 <b>Техника:</b> {eq['name']} ({eq['model']})\n"
                                    f"⛽ <b>Уровень:</b> {eq.get('current_fuel_level', 0)} л ({fuel_percentage}%)\n"
                                    f"📏 <b>Одометр:</b> {eq.get('odometer', 0)} км\n\n"
                                    f"Требуется заправка!"
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
        logger.info(f"🔌 Yandex GPT: {'ВКЛ' if YANDEX_API_KEY and YANDEX_FOLDER_ID else 'ВЫКЛ'}")
        
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
