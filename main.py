import os
import logging
import asyncio
import json
import base64
import re
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from io import BytesIO

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import aioschedule

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
YANDEX_GPT_FOLDER_ID = os.getenv('YC_FOLDER_ID', '')  # Для Yandex GPT
VISION_FOLDER_ID = os.getenv('VISION_FOLDER_ID', '')  # Для Yandex Vision
YANDEX_GPT_MODEL = os.getenv('YANDEX_GPT_MODEL', 'yandexgpt-lite')

# Настройки Object Storage (если нужно в будущем)
YC_BUCKET_NAME = os.getenv('YC_BUCKET_NAME', '')
YC_ACCESS_KEY_ID = os.getenv('YC_ACCESS_KEY_ID', '')
YC_SECRET_KEY = os.getenv('YC_SECRET_KEY', '')
INPUT_FOLDER = os.getenv('INPUT_FOLDER', 'input')
RESULT_FOLDER = os.getenv('RESULT_FOLDER', 'result')

# Инициализация бота
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не найден в .env файле!")
    exit(1)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)

# Инициализация диспетчера
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== КЛАСС ДЛЯ АНАЛИЗА ИЗОБРАЖЕНИЙ ==========
class YandexVisionAnalyzer:
    def __init__(self):
        self.api_key = YANDEX_API_KEY
        self.folder_id = VISION_FOLDER_ID  # Используем VISION_FOLDER_ID
        
    async def analyze_document(self, image_bytes: bytes) -> Dict[str, Any]:
        """Анализирует документ (СТС/ПТС) с помощью Yandex Vision"""
        try:
            if not self.api_key or not self.folder_id:
                return {"error": "API ключ или Folder ID не настроены"}
            
            # Кодируем изображение в base64
            image_base64 = base64.b64encode(image_bytes).decode('utf-8')
            
            url = "https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze"
            
            headers = {
                "Authorization": f"Api-Key {self.api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "folderId": self.folder_id,
                "analyzeSpecs": [{
                    "content": image_base64,
                    "features": [{
                        "type": "TEXT_DETECTION",
                        "textDetectionConfig": {
                            "languageCodes": ["ru", "en"]
                        }
                    }]
                }]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=data, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        return self._extract_text_from_result(result)
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка Vision API: {response.status} - {error_text}")
                        return {"error": f"Ошибка API: {response.status}"}
                        
        except Exception as e:
            logger.error(f"Ошибка анализа документа: {e}")
            return {"error": str(e)}
    
    def _extract_text_from_result(self, result: Dict) -> Dict:
        """Извлекает текст из результата Vision API"""
        try:
            extracted_text = ""
            
            # Извлекаем текст из сложной структуры ответа
            for result_item in result.get('results', []):
                for analysis_result in result_item.get('results', []):
                    text_detection = analysis_result.get('textDetection', {})
                    pages = text_detection.get('pages', [])
                    
                    for page in pages:
                        blocks = page.get('blocks', [])
                        for block in blocks:
                            lines = block.get('lines', [])
                            for line in lines:
                                words = line.get('words', [])
                                line_text = ' '.join([word.get('text', '') for word in words])
                                extracted_text += line_text + '\n'
            
            if not extracted_text:
                return {"error": "Не удалось извлечь текст из изображения"}
            
            return {
                "success": True,
                "extracted_text": extracted_text.strip(),
                "raw_result": result
            }
            
        except Exception as e:
            logger.error(f"Ошибка извлечения текста: {e}")
            return {"error": f"Ошибка обработки: {e}"}
    
    def _parse_document_text(self, text: str) -> Dict[str, str]:
        """Парсит текст документа и извлекает ключевые поля"""
        info = {}
        
        # Простые поиски по ключевым словам (без сложных regex)
        text_upper = text.upper()
        
        # Ищем VIN (обычно 17 символов)
        words = text.split()
        for word in words:
            if len(word) == 17 and word.isalnum():
                info['vin'] = word
                break
        
        # Ищем госномер (русские буквы + цифры)
        for word in words:
            if any(cyr in word for cyr in 'АВЕКМНОРСТУХ') and any(char.isdigit() for char in word):
                info['registration_number'] = word
                break
        
        # Ищем год (4 цифры)
        for word in words:
            if word.isdigit() and len(word) == 4 and 1900 <= int(word) <= 2024:
                info['year'] = word
                break
        
        return info

# Создаем глобальный экземпляр анализатора
vision_analyzer = YandexVisionAnalyzer()

# ========== СОСТОЯНИЯ ==========
class UserStates(StatesGroup):
    # Основные состояния
    waiting_for_ai_question = State()
    
    # Для админа
    waiting_for_user_id_to_assign = State()
    waiting_for_role_to_assign = State()
    waiting_for_org_to_assign = State()
    
    # Для регистрации техники
    waiting_for_document_photo = State()
    waiting_for_document_analysis = State()
    waiting_for_motohours = State()
    waiting_for_last_service = State()
    waiting_for_equipment_type = State()
    waiting_for_equipment_name = State()

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
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

async def ask_yandex_gpt(question: str, context: str = "", user_id: int = None) -> str:
    """Взаимодействие с Yandex GPT"""
    try:
        if not YANDEX_API_KEY or not YANDEX_GPT_FOLDER_ID:
            return "⚠️ Yandex GPT не настроен. Обратитесь к администратору."
        
        # ИСПРАВЛЕННЫЙ URL (без многоточия)
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        
        headers = {
            "Authorization": f"Api-Key {YANDEX_API_KEY}",
            "x-folder-id": YANDEX_GPT_FOLDER_ID,
            "Content-Type": "application/json"
        }
        
        system_prompt = "Ты — профессиональный помощник по обслуживанию и эксплуатации спецтехники."
        
        data = {
            "modelUri": f"gpt://{YANDEX_GPT_FOLDER_ID}/{YANDEX_GPT_MODEL}",
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
                    return answer
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка Yandex GPT: {error_text}")
                    return f"⚠️ Ошибка при обращении к ИИ. Статус: {response.status}"
                    
    except Exception as e:
        logger.error(f"Ошибка Yandex GPT: {e}")
        return "⚠️ Произошла ошибка при обработке запроса."

async def analyze_document_with_ai(image_bytes: bytes, user_id: int = None) -> Dict:
    """Анализирует документ с помощью Vision API и GPT"""
    try:
        # 1. Анализируем изображение с помощью Vision API
        vision_result = await vision_analyzer.analyze_document(image_bytes)
        
        if "error" in vision_result:
            return {"error": vision_result["error"]}
        
        extracted_text = vision_result.get("extracted_text", "")
        
        if not extracted_text:
            return {"error": "Не удалось распознать текст на изображении"}
        
        # 2. Отправляем текст в GPT для структурирования
        prompt = f"""
        Извлеки информацию о технике из следующего текста документа (СТС/ПТС):
        
        {extracted_text[:2000]}
        
        Верни ответ в формате JSON со следующими полями:
        - model: модель техники
        - brand: марка/производитель
        - vin: VIN номер
        - registration_number: регистрационный номер (госномер)
        - year: год выпуска
        - category: категория техники
        - engine_power: мощность двигателя
        
        Если какое-то поле не найдено, укажи null.
        """
        
        gpt_response = await ask_yandex_gpt(prompt, "", user_id)
        
        # Извлекаем JSON из ответа GPT
        try:
            # Ищем JSON в ответе
            json_match = re.search(r'\{.*\}', gpt_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                ai_analysis = json.loads(json_str)
            else:
                # Если не нашли JSON, создаем базовую структуру
                ai_analysis = {
                    "model": "Не распознано",
                    "brand": "Не распознано",
                    "vin": "Не распознано",
                    "registration_number": "Не распознано",
                    "year": "Не распознано",
                    "category": "Не распознано",
                    "engine_power": "Не распознано"
                }
        except:
            ai_analysis = {
                "model": "Не распознано",
                "brand": "Не распознано",
                "vin": "Не распознано",
                "registration_number": "Не распознано",
                "year": "Не распознано",
                "category": "Не распознано",
                "engine_power": "Не распознано"
            }
        
        return {
            "success": True,
            "extracted_text": extracted_text,
            "ai_analysis": ai_analysis,
            "vision_result": vision_result
        }
        
    except Exception as e:
        logger.error(f"Ошибка анализа документа с ИИ: {e}")
        return {"error": str(e)}

async def ask_ai_assistant(question: str, context: str = "", user_id: int = None) -> str:
    """Взаимодействие с ИИ для помощи по технике"""
    if not AI_ENABLED:
        return "🤖 Функция ИИ-помощника временно недоступна. Обратитесь к начальнику парка."
    
    try:
        # Проверяем, имеет ли пользователь доступ к ИИ
        if user_id:
            user = await db.get_user(user_id)
            allowed_roles = ['botadmin', 'director', 'fleetmanager', 'driver']
            if user and user['role'] not in allowed_roles:
                return "⛔ Доступ к ИИ-помощнику только для назначенных пользователей."
        
        if YANDEX_API_KEY and YANDEX_GPT_FOLDER_ID:
            return await ask_yandex_gpt(question, context, user_id)
        
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
                [types.KeyboardButton(text="📞 Контакты")],
            ],
            resize_keyboard=True
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
            resize_keyboard=True
        )
    
    if role == 'director':
        if not has_organization:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="🏢 Создать организацию")],
                    [types.KeyboardButton(text="ℹ️ Информация о боте")],
                    [types.KeyboardButton(text="📞 Контакты")],
                ],
                resize_keyboard=True
            )
        else:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="🏢 Моя организация")],
                    [types.KeyboardButton(text="🚜 Автопарк")],
                    [types.KeyboardButton(text="👥 Сотрудники")],
                    [types.KeyboardButton(text="📷 Зарегистрировать технику")],
                    [types.KeyboardButton(text="📊 Статистика")],
                    [types.KeyboardButton(text="🤖 ИИ Помощник")],
                ],
                resize_keyboard=True
            )
    
    if role == 'fleetmanager':
        if not has_organization:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="ℹ️ Информация о боте")],
                    [types.KeyboardButton(text="📞 Контакты")],
                ],
                resize_keyboard=True
            )
        else:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="🚜 Управление парком")],
                    [types.KeyboardButton(text="🔍 Проверить осмотры")],
                    [types.KeyboardButton(text="📅 Ближайшие ТО")],
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
                    [types.KeyboardButton(text="📞 Контакты")],
                ],
                resize_keyboard=True
            )
        else:
            return types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="🚛 Начать смену")],
                    [types.KeyboardButton(text="📋 Мои смены")],
                    [types.KeyboardButton(text="🚜 Моя техника")],
                    [types.KeyboardButton(text="🤖 ИИ Помощник")],
                ],
                resize_keyboard=True
            )
    
    # По умолчанию
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="ℹ️ Информация о боте")],
            [types.KeyboardButton(text="📞 Контакты")],
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
    if role == 'director' and not has_organization:
        welcome_text += "\n\n📌 <b>Для начала работы создайте организацию</b>"
    
    elif role in ['fleetmanager', 'driver'] and not has_organization:
        welcome_text += "\n\n⏳ <b>Ожидайте назначения в организацию</b>\n"
        welcome_text += "Для ускорения отправьте ваш ID директору"
    
    await reply(message, welcome_text, reply_markup=get_main_keyboard(role, has_organization))

# ========== ИИ ПОМОЩНИК ==========
@dp.message(F.text == "🤖 ИИ Помощник")
async def ai_assistant_start(message: types.Message, state: FSMContext):
    """Начинает диалог с ИИ помощником"""
    user = await db.get_user(message.from_user.id)
    
    # Проверяем роль
    allowed_roles = ['botadmin', 'director', 'fleetmanager', 'driver']
    if user['role'] not in allowed_roles:
        await reply(message, "⛔ Доступ к ИИ-помощнику только для назначенных пользователей.")
        return
    
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
        "• ИИ-помощник по обслуживанию\n"
        "• 📷 Анализ документов (СТС/ПТС)\n"
        "• 🔍 Контроль ежедневных осмотров\n\n"
        "👥 <b>Роли в системе:</b>\n"
        "• 🚛 Водитель - работа со сменами\n"
        "• 👷 Начальник парка - управление техникой\n"
        "• 👨‍💼 Директор - управление организацией\n"
        "• 👑 Администратор - управление системой\n\n"
        "📞 <b>Техническая поддержка:</b> @Sekynds\n\n"
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
        "<b>Техническая поддержка:</b> @Sekynds\n"
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

# ========== РЕГИСТРАЦИЯ ТЕХНИКИ С ИИ ==========
@dp.message(F.text == "📷 Зарегистрировать технику")
async def register_equipment_with_photo(message: types.Message, state: FSMContext):
    """Начинает регистрацию техники с помощью фото"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Только руководители могут регистрировать технику!")
        return
    
    if not user.get('organization_id'):
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    await reply(
        message,
        "🚜 <b>Регистрация новой техники</b>\n\n"
        "📸 <b>Шаг 1 из 5:</b> Отправьте фото СТС или ПТС\n\n"
        "ИИ автоматически считает все данные:\n"
        "• VIN номер\n• Модель\n• Госномер\n• Год выпуска\n\n"
        "<i>Сфотографируйте документ и отправьте фото</i>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_document_photo)

@dp.message(UserStates.waiting_for_document_photo, F.photo)
async def process_document_photo(message: types.Message, state: FSMContext):
    """Обрабатывает фото документа"""
    try:
        await reply(message, "🔍 <b>Анализирую документ...</b>\n\nИИ обрабатывает изображение...")
        
        # Скачиваем фото
        photo = message.photo[-1]
        file = await bot.get_file(photo.file_id)
        photo_bytes = await bot.download_file(file.file_path)
        
        # Читаем байты
        image_data = await photo_bytes.read()
        
        # Анализируем документ
        analysis = await analyze_document_with_ai(image_data, message.from_user.id)
        
        if "error" in analysis:
            await reply(
                message,
                f"❌ <b>Ошибка анализа:</b> {analysis['error']}\n\n"
                "Попробуйте еще раз или введите данные вручную.",
                reply_markup=get_cancel_keyboard()
            )
            return
        
        # Сохраняем результат
        await state.update_data(
            document_photo_id=photo.file_id,
            document_analysis=analysis
        )
        
        # Показываем результат
        info_text = "✅ <b>ИИ распознал данные:</b>\n\n"
        
        if "ai_analysis" in analysis:
            data = analysis["ai_analysis"]
            info_text += f"🚜 <b>Модель:</b> {data.get('model', 'Не распознано')}\n"
            info_text += f"🏷️ <b>Марка:</b> {data.get('brand', 'Не распознано')}\n"
            info_text += f"🔢 <b>VIN:</b> {data.get('vin', 'Не распознано')}\n"
            info_text += f"🚗 <b>Госномер:</b> {data.get('registration_number', 'Не распознано')}\n"
            info_text += f"📅 <b>Год:</b> {data.get('year', 'Не распознано')}\n"
            if data.get('category'):
                info_text += f"🏗️ <b>Тип:</b> {data.get('category', 'Не распознано')}\n"
        
        info_text += "\n<b>Продолжить регистрацию этой техники?</b>"
        
        keyboard = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✅ Да, продолжить")],
                [types.KeyboardButton(text="🔄 Нет, отправить другое фото")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
        
        await reply(message, info_text, reply_markup=keyboard)
        await state.set_state(UserStates.waiting_for_document_analysis)
        
    except Exception as e:
        logger.error(f"Ошибка обработки фото документа: {e}")
        await reply(message, "❌ Ошибка при обработке фото. Попробуйте еще раз.")

@dp.message(UserStates.waiting_for_document_analysis)
async def process_document_confirmation(message: types.Message, state: FSMContext):
    """Обрабатывает подтверждение данных документа"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    if message.text == "🔄 Нет, отправить другое фото":
        await reply(
            message,
            "🔄 <b>Отправьте новое фото документа</b>\n\n"
            "Убедитесь, что:\n"
            "1. Фото четкое\n"
            "2. Весь документ в кадре\n"
            "3. Хорошее освещение",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_document_photo)
        return
    
    if message.text == "✅ Да, продолжить":
        # Запрашиваем дополнительные данные
        await reply(
            message,
            "📊 <b>Шаг 2 из 5:</b> Дополнительная информация\n\n"
            "Введите текущие моточасы техники:\n"
            "<i>Например: 1250</i>",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_motohours)

@dp.message(UserStates.waiting_for_motohours)
async def process_motohours(message: types.Message, state: FSMContext):
    """Обрабатывает ввод моточасов"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    try:
        motohours = int(message.text)
        await state.update_data(motohours=motohours)
        
        await reply(
            message,
            "🛠️ <b>Шаг 3 из 5:</b> Последнее ТО\n\n"
            "Введите, что делалось на последнем ТО:\n"
            "<i>Например: Замена масла, фильтров 01.12.2023</i>",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_last_service)
        
    except ValueError:
        await reply(message, "❌ Введите число! Например: 1250")

@dp.message(UserStates.waiting_for_last_service)
async def process_last_service(message: types.Message, state: FSMContext):
    """Обрабатывает ввод данных о последнем ТО"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    await state.update_data(last_service=message.text)
    
    # Запрашиваем тип техники
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🚜 Экскаватор")],
            [types.KeyboardButton(text="🚚 Погрузчик")],
            [types.KeyboardButton(text="🏗️ Бульдозер")],
            [types.KeyboardButton(text="🚛 Самосвал")],
            [types.KeyboardButton(text="🚒 Кран")],
            [types.KeyboardButton(text="🔄 Другое")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )
    
    await reply(
        message,
        "🏗️ <b>Шаг 4 из 5:</b> Тип техники\n\n"
        "Выберите тип техники:",
        reply_markup=keyboard
    )
    await state.set_state(UserStates.waiting_for_equipment_type)

@dp.message(UserStates.waiting_for_equipment_type)
async def process_equipment_type(message: types.Message, state: FSMContext):
    """Обрабатывает выбор типа техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    equipment_type = message.text.replace("🚜", "").replace("🚚", "").replace("🏗️", "").replace("🚛", "").replace("🚒", "").replace("🔄", "").strip()
    
    await state.update_data(equipment_type=equipment_type)
    
    # Запрашиваем имя/название техники
    await reply(
        message,
        "🏷️ <b>Шаг 5 из 5:</b> Название техники\n\n"
        "Введите имя для техники (для удобства):\n"
        "<i>Например: Экскаватор №1, Волга-2023, Синий кран</i>",
        reply_markup=get_cancel_keyboard()
    )
    
    await state.set_state(UserStates.waiting_for_equipment_name)

@dp.message(UserStates.waiting_for_equipment_name)
async def finalize_equipment_registration(message: types.Message, state: FSMContext):
    """Завершает регистрацию техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    equipment_name = message.text
    data = await state.get_data()
    user = await db.get_user(message.from_user.id)
    
    # Собираем все данные
    document_data = data.get('document_analysis', {}).get('ai_analysis', {})
    
    # Если нет данных от ИИ, используем ручной ввод
    model = document_data.get('model', 'Неизвестная модель')
    if model == 'Не распознано' or model == 'null':
        model = f"Техника {equipment_name}"
    
    vin = document_data.get('vin', 'Неизвестно')
    if vin == 'Не распознано' or vin == 'null':
        vin = f"TEMP_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    reg_number = document_data.get('registration_number', 'Без номера')
    if reg_number == 'Не распознано' or reg_number == 'null':
        reg_number = 'Без номера'
    
    # Добавляем технику в базу
    equipment_id = await db.add_equipment(
        name=equipment_name,
        model=model,
        vin=vin,
        org_id=user['organization_id'],
        registration_number=reg_number,
        fuel_type='diesel',
        fuel_capacity=300
    )
    
    if equipment_id:
        # Добавляем моточасы
        await db.update_equipment(equipment_id, odometer=data.get('motohours', 0))
        
        # Сохраняем информацию для ИИ
        ai_context = f"""
        Новая техника зарегистрирована:
        - Название: {equipment_name}
        - Модель: {model}
        - Тип: {data.get('equipment_type', 'Не указано')}
        - Моточасы: {data.get('motohours', 0)}
        - Последнее ТО: {data.get('last_service', 'Не указано')}
        - VIN: {vin}
        """
        
        await db.add_ai_context(
            organization_id=user['organization_id'],
            context_type="equipment_registration",
            equipment_model=model,
            question="Регистрация новой техники",
            answer=ai_context,
            source="bot_auto"
        )
        
        await reply(
            message,
            f"✅ <b>Техника успешно зарегистрирована!</b>\n\n"
            f"🏷️ <b>Название:</b> {equipment_name}\n"
            f"🚜 <b>Модель:</b> {model}\n"
            f"🔢 <b>VIN:</b> {vin}\n"
            f"📊 <b>Моточасы:</b> {data.get('motohours', 0)}\n"
            f"🛠️ <b>Последнее ТО:</b> {data.get('last_service', 'Не указано')}\n\n"
            f"Техника добавлена в ваш автопарк. ID: {equipment_id}",
            reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
        )
        
    else:
        await reply(
            message,
            "❌ Ошибка при сохранении техники в базу.",
            reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
        )
    
    await state.clear()

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
    
    for u in users[:15]:
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
    user_to_assign = await db.get_user(user_id_to_assign)
    user_to_assign_name = user_to_assign['full_name'] if user_to_assign else f"ID {user_id_to_assign}"
    
    if selected_role == 'director':
        # Для директора сразу создаем организацию
        await reply(
            message,
            f"👨‍💼 <b>Назначение директора</b>\n\n"
            f"Пользователь: {user_to_assign_name}\n"
            f"ID: {user_id_to_assign}\n\n"
            f"Введите название организации для этого директора:",
            reply_markup=get_cancel_keyboard()
        )
        await state.update_data(
            selected_role=selected_role,
            user_id_to_assign=user_id_to_assign,
            user_to_assign_name=user_to_assign_name
        )
        await state.set_state(UserStates.waiting_for_org_to_assign)
    else:
        # Для других ролей просто назначаем
        success = await db.assign_role_to_user(user_id_to_assign, selected_role)
        
        if success:
            await reply(
                message,
                f"✅ <b>Роль назначена успешно!</b>\n\n"
                f"<b>Пользователь:</b> {user_to_assign_name}\n"
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

@dp.message(UserStates.waiting_for_org_to_assign)
async def process_org_for_director(message: types.Message, state: FSMContext):
    """Обрабатывает создание организации для директора"""
    if message.text == "❌ Отмена":
        await state.clear()
        await reply(message, "❌ Назначение отменено", 
                   reply_markup=get_main_keyboard('botadmin', True))
        return
    
    org_name = message.text.strip()
    data = await state.get_data()
    user_id_to_assign = data.get('user_id_to_assign')
    selected_role = data.get('selected_role')
    user_to_assign_name = data.get('user_to_assign_name', f"ID {user_id_to_assign}")
    
    # Создаем организацию
    org_id, error = await db.create_organization_for_director(user_id_to_assign, org_name)
    
    if error:
        await reply(message, f"❌ Ошибка: {error}")
        return
    
    # Назначаем роль
    success = await db.assign_role_to_user(user_id_to_assign, selected_role, org_id)
    
    if success:
        await reply(
            message,
            f"✅ <b>Директор назначен успешно!</b>\n\n"
            f"<b>Пользователь:</b> {user_to_assign_name}\n"
            f"<b>Роль:</b> 👨‍💼 Директор\n"
            f"<b>Организация:</b> {org_name}\n"
            f"<b>ID организации:</b> {org_id}\n\n"
            f"Пользователь получил доступ к управлению организацией."
        )
        
        # Уведомляем пользователя
        await send_to_user(
            user_id_to_assign,
            f"✅ <b>Вам назначена роль Директора!</b>\n\n"
            f"<b>Организация:</b> {org_name}\n"
            f"<b>ID организации:</b> {org_id}\n\n"
            f"Используйте команду /start для начала работы."
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
    
    await reply(message, text)

# ========== ФУНКЦИИ ДИРЕКТОРА ==========
@dp.message(F.text == "🏢 Создать организацию")
async def create_organization_start(message: types.Message):
    """Создание организации (директор)"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'director':
        await reply(message, "⛔ Только директор может создавать организацию!")
        return
    
    if user.get('organization_id'):
        await reply(message, "❌ У вас уже есть организация!")
        return
    
    # Создаем организацию с именем по умолчанию
    org_name = f"Организация {message.from_user.full_name}"
    org_id, error = await db.create_organization_for_director(message.from_user.id, org_name)
    
    if error:
        await reply(message, f"❌ Ошибка: {error}")
        return
    
    await reply(
        message,
        f"✅ <b>Организация создана успешно!</b>\n\n"
        f"<b>Название:</b> {org_name}\n"
        f"<b>ID организации:</b> {org_id}\n\n"
        f"Теперь вы можете управлять своей организацией.",
        reply_markup=get_main_keyboard('director', True)
    )

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
        for eq in equipment[:5]:
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

@dp.message(F.text == "👥 Сотрудники")
async def show_employees(message: types.Message):
    """Показывает сотрудники организации"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager'] or not user.get('organization_id'):
        await reply(message, "⛔ Доступ только для руководителей с организацией!")
        return
    
    employees = await db.get_users_by_organization(user['organization_id'])
    
    if not employees:
        await reply(message, "👥 В вашей организации пока нет сотрудников.")
        return
    
    text = "👥 <b>Сотрудники организации</b>\n\n"
    
    for emp in employees:
        role_emoji = {
            'director': '👨‍💼',
            'fleetmanager': '👷',
            'driver': '🚛'
        }.get(emp['role'], '👤')
        
        text += f"{role_emoji} <b>{emp['full_name']}</b>\n"
        text += f"ID: <code>{emp['telegram_id']}</code>\n"
        text += f"Роль: {emp['role']}\n"
        if emp.get('phone_number'):
            text += f"📞: {emp['phone_number']}\n"
        text += "\n"
    
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
        
        text += "\n"
    
    await reply(message, text)

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
