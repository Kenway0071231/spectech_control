import os
import logging
import asyncio
import json
import base64
import re
import aiohttp
import aiocron
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from enum import Enum

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv

from database import db
from prompts import get_prompt, PROMPTS

# ========== НАСТРОЙКА ==========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ИИ МОДУЛЕЙ ==========
class AIModule(Enum):
    DOCUMENT_ANALYSIS = "document_analysis"
    REGISTRATION = "registration"
    SERVICE = "service"
    SHIFT = "shift"
    SPARE_PARTS = "spare_parts"

AI_CONFIG = {
    AIModule.DOCUMENT_ANALYSIS: {
        'enabled': os.getenv('DOCUMENT_ANALYSIS_ENABLED', 'True').lower() == 'true',
        'function_url': os.getenv('DOCUMENT_ANALYSIS_FUNCTION_URL', ''),
        'timeout': int(os.getenv('CF_TIMEOUT', 60)),
        'max_retries': int(os.getenv('CF_MAX_RETRIES', 3))
    },
    AIModule.REGISTRATION: {
        'enabled': os.getenv('AI_ENABLED', 'True').lower() == 'true',
        'api_key': os.getenv('YANDEX_API_KEY', ''),
        'model': os.getenv('REGISTRATION_GPT_MODEL', 'yandexgpt'),
        'folder_id': os.getenv('YC_FOLDER_ID', ''),
        'url': "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    },
    AIModule.SERVICE: {
        'enabled': os.getenv('AI_ENABLED', 'True').lower() == 'true',
        'api_key': os.getenv('YANDEX_API_KEY', ''),
        'model': os.getenv('YANDEX_GPT_MODEL', 'yandexgpt-lite'),
        'folder_id': os.getenv('YC_FOLDER_ID', ''),
        'url': "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    },
    AIModule.SHIFT: {
        'enabled': os.getenv('AI_ENABLED', 'True').lower() == 'true',
        'api_key': os.getenv('YANDEX_API_KEY', ''),
        'model': os.getenv('YANDEX_GPT_MODEL', 'yandexgpt-lite'),
        'folder_id': os.getenv('YC_FOLDER_ID', ''),
        'url': "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    },
    AIModule.SPARE_PARTS: {
        'enabled': os.getenv('AI_ENABLED', 'True').lower() == 'true',
        'api_key': os.getenv('YANDEX_API_KEY', ''),
        'model': os.getenv('YANDEX_GPT_MODEL', 'yandexgpt-lite'),
        'folder_id': os.getenv('YC_FOLDER_ID', ''),
        'url': "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    }
}

VISION_API_KEY = os.getenv('VISION_API_KEY', '')
VISION_FOLDER_ID = os.getenv('VISION_FOLDER_ID', '')
VISION_ENABLED = os.getenv('VISION_API_ENABLED', 'True').lower() == 'true'

# ========== ИНИЦИАЛИЗАЦИЯ БОТА ==========
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не найден в .env файле!")
    exit(1)

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== КЛАСС ДЛЯ АНАЛИЗА ДОКУМЕНТОВ СТС/ПТС ==========
class DocumentAnalyzer:
    """Класс для анализа документов СТС/ПТС через Yandex Cloud Function"""
    
    def __init__(self):
        self.function_url = AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['function_url']
        self.enabled = AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['enabled']
        self.timeout = AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['timeout']
        self.max_retries = AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['max_retries']
        
    async def analyze_document(self, image_bytes: bytes, document_type: str = "СТС") -> Dict[str, Any]:
        """
        Анализирует документ СТС/ПТС через Yandex Cloud Function
        """
        if not self.enabled:
            return {"error": "Функция анализа документов отключена", "success": False}
        
        if not self.function_url:
            return {"error": "URL функции анализа документов не настроен", "success": False}
        
        # Кодируем изображение в base64
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        
        # Формируем промпт
        prompt = get_prompt("document_analysis")
        prompt = prompt.replace("СТС/ПТС/ПСМ", document_type)
        
        # Формируем запрос
        payload = {
            "image": image_base64,
            "prompt": prompt,
            "document_type": document_type,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Отправка документа {document_type} в функцию анализа...")
        
        # Пытаемся отправить запрос с повторными попытками
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    timeout = aiohttp.ClientTimeout(total=self.timeout)
                    
                    async with session.post(
                        self.function_url, 
                        json=payload, 
                        timeout=timeout,
                        headers={'Content-Type': 'application/json'}
                    ) as response:
                        
                        if response.status == 200:
                            result_data = await response.json()
                            logger.info(f"Получен ответ (попытка {attempt + 1})")
                            return self._process_response(result_data, document_type)
                            
                        elif response.status == 429:
                            logger.warning(f"Слишком много запросов. Попытка {attempt + 1}")
                            if attempt < self.max_retries - 1:
                                wait_time = 2 ** attempt
                                await asyncio.sleep(wait_time)
                                continue
                            
                        else:
                            error_text = await response.text()
                            logger.error(f"Ошибка функции: {response.status}")
                            return {
                                "error": f"Ошибка API: {response.status}",
                                "status_code": response.status,
                                "success": False
                            }
                            
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут (попытка {attempt + 1})")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return {"error": "Таймаут при обработке документа", "success": False}
                
            except aiohttp.ClientError as e:
                logger.error(f"Ошибка соединения: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return {"error": f"Ошибка соединения: {str(e)}", "success": False}
                
            except Exception as e:
                logger.error(f"Неожиданная ошибка: {e}")
                return {"error": f"Неожиданная ошибка: {str(e)}", "success": False}
        
        return {"error": "Превышено количество попыток", "success": False}
    
    def _process_response(self, result_data: Dict, document_type: str) -> Dict[str, Any]:
        """Обрабатывает ответ от Cloud Function"""
        try:
            # Извлекаем текст ответа
            if "result" in result_data:
                result_text = result_data["result"]
            elif "text" in result_data:
                result_text = result_data["text"]
            elif "message" in result_data:
                result_text = result_data["message"]
            else:
                result_text = str(result_data)
            
            # Извлекаем JSON из ответа
            json_data = self._extract_json_from_response(result_text)
            
            if json_data:
                # Валидируем и очищаем данные
                validated_data = self._validate_and_clean_data(json_data)
                validated_data["document_type"] = document_type
                validated_data["success"] = True
                validated_data["analysis_timestamp"] = datetime.now().isoformat()
                
                # Рассчитываем качество анализа
                quality_score = self._calculate_quality_score(validated_data)
                validated_data["analysis_quality"] = quality_score["quality"]
                validated_data["quality_score"] = quality_score["score"]
                validated_data["missing_fields"] = quality_score["missing_fields"]
                
                logger.info(f"Анализ завершен: {quality_score['quality']} качество")
                
                return validated_data
            else:
                return {
                    "success": False,
                    "error": "Не удалось извлечь структурированные данные",
                    "extracted_text": result_text[:500],
                    "suggestion": "Попробуйте сделать более четкое фото"
                }
                
        except Exception as e:
            logger.error(f"Ошибка обработки ответа: {e}")
            return {
                "success": False,
                "error": f"Ошибка обработки: {str(e)}"
            }
    
    def _extract_json_from_response(self, response_text: str) -> Optional[Dict]:
        """Извлекает JSON из ответа функции"""
        try:
            # Ищем JSON в ответе
            json_patterns = [
                r'```json\s*(.*?)\s*```',
                r'```\s*(.*?)\s*```',
                r'(\{.*?\})',
            ]
            
            json_str = None
            for pattern in json_patterns:
                match = re.search(pattern, response_text, re.DOTALL)
                if match:
                    json_str = match.group(1) if len(match.groups()) > 0 else match.group(0)
                    break
            
            if not json_str:
                # Ищем начало и конец JSON
                start = response_text.find('{')
                end = response_text.rfind('}')
                if start != -1 and end != -1 and end > start:
                    json_str = response_text[start:end+1]
            
            if json_str:
                # Очищаем строку
                json_str = json_str.strip()
                json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', json_str)
                
                data = json.loads(json_str)
                return data
                
        except json.JSONDecodeError:
            logger.warning("Ошибка декодирования JSON")
            return None
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении JSON: {e}")
            
        return None
    
    def _validate_and_clean_data(self, data: Dict) -> Dict:
        """Валидирует и очищает данные из JSON"""
        cleaned = {}
        
        # Список ожидаемых полей
        expected_fields = [
            "document_type", "vin", "registration_number", "model", "brand",
            "year", "category", "engine_power", "engine_volume", "color",
            "weight", "max_weight", "owner", "passport_number", "registration_date",
            "engine_number", "chassis_number", "body_number", "environmental_class",
            "extracted_text"
        ]
        
        for field in expected_fields:
            value = data.get(field)
            
            if value is None or value == "null" or value == "":
                cleaned[field] = None
                continue
            
            # Очистка и валидация
            if isinstance(value, str):
                value = value.strip()
                value = re.sub(r'\s+', ' ', value)
                
                # Специальная обработка
                if field == "vin":
                    vin_match = re.search(r'[A-HJ-NPR-Z0-9]{17}', value.upper())
                    if vin_match:
                        value = vin_match.group(0)
                    else:
                        value = None
                        
                elif field == "registration_number":
                    value = value.upper()
                    value = re.sub(r'[^А-Я0-9]', '', value)
                    
                elif field == "year":
                    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', value)
                    if year_match:
                        value = int(year_match.group(0))
                    else:
                        value = None
                        
                elif field == "engine_power":
                    power_match = re.search(r'(\d+)\s*(л\.с\.|лс|кВт|сил|hp)', value, re.IGNORECASE)
                    if power_match:
                        value = int(power_match.group(1))
                    else:
                        num_match = re.search(r'\b(\d{2,4})\b', value)
                        if num_match:
                            value = int(num_match.group(1))
                        else:
                            value = None
                            
                elif field == "color":
                    colors = ["белый", "черный", "красный", "синий", "зеленый", 
                             "желтый", "серый", "коричневый", "оранжевый", "фиолетовый"]
                    for color in colors:
                        if color in value.lower():
                            value = color.capitalize()
                            break
                
                elif field in ["weight", "max_weight", "engine_volume"]:
                    num_match = re.search(r'\b(\d+)\b', value)
                    if num_match:
                        value = int(num_match.group(0))
                    else:
                        value = None
            
            cleaned[field] = value
        
        return cleaned
    
    def _calculate_quality_score(self, data: Dict) -> Dict[str, Any]:
        """Рассчитывает качество распознавания"""
        critical_fields = ["vin", "model", "brand"]
        important_fields = ["registration_number", "year", "engine_power", "category"]
        additional_fields = ["color", "weight", "owner", "registration_date"]
        
        missing_fields = []
        score = 0
        
        # Проверяем критические поля
        for field in critical_fields:
            if data.get(field):
                score += 13.33
            else:
                missing_fields.append(field)
        
        # Проверяем важные поля
        for field in important_fields:
            if data.get(field):
                score += 8.75
            else:
                missing_fields.append(field)
        
        # Проверяем дополнительные поля
        for field in additional_fields:
            if data.get(field):
                score += 6.25
        
        # Определяем качество
        if score >= 80:
            quality = "high"
        elif score >= 50:
            quality = "medium"
        else:
            quality = "low"
        
        return {
            "quality": quality,
            "score": round(score, 2),
            "missing_fields": missing_fields
        }

# ========== КЛАСС ДЛЯ YANDEX VISION ==========
class YandexVisionAnalyzer:
    def __init__(self):
        self.api_key = VISION_API_KEY
        self.folder_id = VISION_FOLDER_ID
        
    async def analyze_document_text(self, image_bytes: bytes) -> Dict[str, Any]:
        """Анализирует текст документа через Yandex Vision API"""
        try:
            if not VISION_ENABLED or not self.api_key or not self.folder_id:
                return {"error": "Yandex Vision API не настроен", "success": False}
            
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
                        return self._extract_text_from_vision_result(result)
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка Vision API: {response.status}")
                        return {
                            "error": f"Ошибка API: {response.status}",
                            "success": False
                        }
                        
        except asyncio.TimeoutError:
            logger.error("Таймаут Vision API")
            return {"error": "Таймаут при анализе", "success": False}
        except Exception as e:
            logger.error(f"Ошибка анализа документа через Vision: {e}")
            return {"error": str(e), "success": False}
    
    def _extract_text_from_vision_result(self, result: Dict) -> Dict:
        """Извлекает текст из результата Vision API"""
        try:
            extracted_text = ""
            blocks_info = []
            
            for result_item in result.get('results', []):
                for analysis_result in result_item.get('results', []):
                    text_detection = analysis_result.get('textDetection', {})
                    pages = text_detection.get('pages', [])
                    
                    for page in pages:
                        blocks = page.get('blocks', [])
                        for block in blocks:
                            lines = block.get('lines', [])
                            block_text = ""
                            
                            for line in lines:
                                words = line.get('words', [])
                                line_text = ' '.join([word.get('text', '') for word in words])
                                block_text += line_text + '\n'
                            
                            if block_text.strip():
                                blocks_info.append({
                                    "text": block_text.strip(),
                                    "confidence": block.get('confidence', 0)
                                })
                                extracted_text += block_text + '\n\n'
            
            if not extracted_text.strip():
                return {
                    "success": False,
                    "error": "Не удалось извлечь текст из документа"
                }
            
            return {
                "success": True,
                "extracted_text": extracted_text.strip(),
                "text_blocks": blocks_info,
                "total_blocks": len(blocks_info),
                "average_confidence": sum(b["confidence"] for b in blocks_info) / len(blocks_info) if blocks_info else 0
            }
            
        except Exception as e:
            logger.error(f"Ошибка извлечения текста: {e}")
            return {
                "success": False,
                "error": f"Ошибка обработки: {e}"
            }

# ========== СОЗДАЕМ ЭКЗЕМПЛЯРЫ ==========
document_analyzer = DocumentAnalyzer()
vision_analyzer = YandexVisionAnalyzer()

# ========== СОСТОЯНИЯ ==========
class UserStates(StatesGroup):
    # Основные состояния
    waiting_for_document_type = State()
    waiting_for_document_photo = State()
    waiting_for_document_analysis = State()
    waiting_for_registration_confirmation = State()
    waiting_for_equipment_name = State()
    waiting_for_motohours = State()
    waiting_for_last_service = State()
    
    # Состояния для назначения ролей
    waiting_for_role_user_id = State()
    waiting_for_role_type = State()
    waiting_for_role_organization = State()
    
    # Состояния для ИИ помощников
    waiting_for_service_issue = State()
    waiting_for_shift_details = State()
    waiting_for_spare_parts = State()

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

def get_main_keyboard(role, has_organization=False):
    """Генерирует клавиатуру в зависимости от роли"""
    
    if role == 'unassigned':
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="ℹ️ Информация о боте")],
                [KeyboardButton(text="📞 Контакты")],
            ],
            resize_keyboard=True
        )
    
    if role == 'botadmin':
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="👥 Все пользователи")],
                [KeyboardButton(text="🏢 Все организации")],
                [KeyboardButton(text="➕ Назначить роль")],
                [KeyboardButton(text="📊 Статистика")],
                [KeyboardButton(text="⚙️ Настройки ИИ")],
            ],
            resize_keyboard=True
        )
    
    if role == 'director':
        if not has_organization:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🏢 Создать организацию")],
                    [KeyboardButton(text="ℹ️ Информация о боте")],
                    [KeyboardButton(text="📞 Контакты")],
                ],
                resize_keyboard=True
            )
        else:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🏢 Моя организация")],
                    [KeyboardButton(text="🚜 Автопарк")],
                    [KeyboardButton(text="👥 Сотрудники")],
                    [KeyboardButton(text="📷 Зарегистрировать технику")],
                    [KeyboardButton(text="📊 Статистика")],
                    [KeyboardButton(text="🔧 Сервисный помощник")],
                ],
                resize_keyboard=True
            )
    
    if role == 'fleetmanager':
        if not has_organization:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="ℹ️ Информация о боте")],
                    [KeyboardButton(text="📞 Контакты")],
                ],
                resize_keyboard=True
            )
        else:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🚜 Управление парком")],
                    [KeyboardButton(text="🔍 Проверить осмотры")],
                    [KeyboardButton(text="📅 Ближайшие ТО")],
                    [KeyboardButton(text="📷 Зарегистрировать технику")],
                    [KeyboardButton(text="🔧 Сервисный помощник")],
                    [KeyboardButton(text="📦 Заказы запчастей")],
                ],
                resize_keyboard=True
            )
    
    if role == 'driver':
        if not has_organization:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="ℹ️ Информация о боте")],
                    [KeyboardButton(text="📞 Контакты")],
                ],
                resize_keyboard=True
            )
        else:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🚛 Начать смену")],
                    [KeyboardButton(text="📋 Ежедневный отчет")],
                    [KeyboardButton(text="🚜 Моя техника")],
                    [KeyboardButton(text="🔧 Сервисный помощник")],
                    [KeyboardButton(text="📊 Моя статистика")],
                ],
                resize_keyboard=True
            )
    
    # По умолчанию
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="ℹ️ Информация о боте")],
            [KeyboardButton(text="📞 Контакты")],
        ],
        resize_keyboard=True
    )

def get_cancel_keyboard():
    """Клавиатура с кнопкой отмена"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def get_document_type_keyboard():
    """Клавиатура для выбора типа документа"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📄 СТС (Свидетельство о регистрации)")],
            [KeyboardButton(text="📋 ПТС (Паспорт транспортного средства)")],
            [KeyboardButton(text="🏭 ПСМ (Паспорт самоходной машины)")],
            [KeyboardButton(text="📃 Другой документ")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def get_confirmation_keyboard():
    """Клавиатура для подтверждения данных"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Все верно, продолжить")],
            [KeyboardButton(text="✏️ Внести правки")],
            [KeyboardButton(text="🔄 Загрузить другой документ")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def get_role_type_keyboard():
    """Клавиатура для выбора роли"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👑 Администратор")],
            [KeyboardButton(text="👨‍💼 Директор")],
            [KeyboardButton(text="👷 Начальник парка")],
            [KeyboardButton(text="🚛 Водитель")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def get_ai_assistant_keyboard():
    """Клавиатура для выбора ИИ помощника"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔧 Сервисный помощник")],
            [KeyboardButton(text="🚛 Помощник по сменам")],
            [KeyboardButton(text="📦 Помощник по запчастям")],
            [KeyboardButton(text="📄 Анализ документов")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

# ========== КОМАНДА СТАРТ ==========
@dp.message(CommandStart())
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
            "📋 <b>Для получения доступа:</b>\n"
            "1. Отправьте ваш ID вышестоящему сотруднику\n"
            "2. Администратор назначит вам роль\n"
            "3. После назначения вы получите доступ к функциям\n\n"
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

# ========== РЕГИСТРАЦИЯ ТЕХНИКИ С АНАЛИЗОМ ДОКУМЕНТОВ ==========
@dp.message(F.text == "📷 Зарегистрировать технику")
async def start_equipment_registration(message: types.Message, state: FSMContext):
    """Начинает регистрацию техники с анализом документов"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Только руководители могут регистрировать технику!")
        return
    
    if not user.get('organization_id'):
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    if not AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['enabled']:
        await reply(message, "⚠️ Функция анализа документов временно отключена")
        return
    
    await reply(
        message,
        "🚜 <b>Регистрация новой техники с анализом документов</b>\n\n"
        "📄 <b>Система автоматически извлечет данные из документов:</b>\n"
        "• VIN номер\n• Модель и марка\n• Госномер\n• Год выпуска\n• Мощность двигателя\n• Цвет и другие данные\n\n"
        "📸 <b>Выберите тип документа:</b>",
        reply_markup=get_document_type_keyboard()
    )
    await state.set_state(UserStates.waiting_for_document_type)

@dp.message(UserStates.waiting_for_document_type)
async def select_document_type(message: types.Message, state: FSMContext):
    """Обрабатывает выбор типа документа"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    document_type_map = {
        "📄 СТС (Свидетельство о регистрации)": "СТС",
        "📋 ПТС (Паспорт транспортного средства)": "ПТС",
        "🏭 ПСМ (Паспорт самоходной машины)": "ПСМ",
        "📃 Другой документ": "Другой документ"
    }
    
    if message.text not in document_type_map:
        await reply(message, "❌ Выберите тип документа из списка", reply_markup=get_document_type_keyboard())
        return
    
    document_type = document_type_map[message.text]
    
    await state.update_data(document_type=document_type)
    
    await reply(
        message,
        f"📸 <b>Загрузите фото документа ({document_type})</b>\n\n"
        "<i>Советы для лучшего распознавания:</i>\n"
        "1. Расположите документ ровно в кадре\n"
        "2. Убедитесь в хорошем освещении\n"
        "3. Весь документ должен быть виден\n"
        "4. Избегайте бликов и теней\n"
        "5. Текст должен быть четким\n\n"
        "<b>Отправьте фото документа:</b>",
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
        image_data = await photo_bytes.read()
        
        # Получаем тип документа
        data = await state.get_data()
        document_type = data.get('document_type', 'СТС')
        
        # Анализируем документ
        analysis_result = await document_analyzer.analyze_document(image_data, document_type)
        
        if not analysis_result.get("success", False):
            error_msg = analysis_result.get("error", "Неизвестная ошибка")
            await reply(
                message,
                f"❌ <b>Ошибка анализа документа:</b> {error_msg}\n\n"
                "Попробуйте:\n"
                "1. Сделать более четкое фото\n"
                "2. Улучшить освещение\n"
                "3. Отправить другой документ",
                reply_markup=get_cancel_keyboard()
            )
            return
        
        # Сохраняем результат анализа
        await state.update_data(
            analysis_result=analysis_result,
            document_photo_id=photo.file_id
        )
        
        # Формируем сообщение с результатами
        info_text = "✅ <b>Документ успешно проанализирован!</b>\n\n"
        
        # Качество анализа
        quality = analysis_result.get("analysis_quality", "unknown")
        quality_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(quality, "⚪")
        
        info_text += f"<b>Качество анализа:</b> {quality_emoji} {quality.upper()}\n\n"
        
        # Основные поля
        fields = [
            ("📄 Тип документа", analysis_result.get("document_type", "СТС")),
            ("🔢 VIN номер", analysis_result.get("vin")),
            ("🚗 Госномер", analysis_result.get("registration_number")),
            ("🏷️ Марка", analysis_result.get("brand")),
            ("🚜 Модель", analysis_result.get("model")),
            ("📅 Год выпуска", analysis_result.get("year")),
            ("⚡ Мощность", f"{analysis_result.get('engine_power')} л.с." if analysis_result.get('engine_power') else None),
            ("🎨 Цвет", analysis_result.get("color")),
            ("🏗️ Тип техники", analysis_result.get("category")),
        ]
        
        for label, value in fields:
            if value:
                info_text += f"<b>{label}:</b> {value}\n"
        
        info_text += "\n<b>Все данные верны?</b>"
        
        await reply(message, info_text, reply_markup=get_confirmation_keyboard())
        await state.set_state(UserStates.waiting_for_document_analysis)
        
    except Exception as e:
        logger.error(f"Ошибка обработки фото документа: {e}")
        await reply(
            message,
            "❌ Ошибка при обработке фото. Попробуйте еще раз.",
            reply_markup=get_cancel_keyboard()
        )

@dp.message(UserStates.waiting_for_document_analysis)
async def process_document_analysis_confirmation(message: types.Message, state: FSMContext):
    """Обрабатывает подтверждение данных документа"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    if message.text == "🔄 Загрузить другой документ":
        await reply(
            message,
            "📸 <b>Отправьте новое фото документа</b>",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_document_photo)
        return
    
    if message.text == "✏️ Внести правки":
        await reply(
            message,
            "✏️ <b>Введите исправления:</b>\n\n"
            "<i>Формат:</i>\n"
            "VIN: X9F12345678901234\n"
            "Модель: Камаз-6520\n"
            "Год: 2022\n\n"
            "<b>Введите исправления:</b>",
            reply_markup=get_cancel_keyboard()
        )
        # Здесь можно добавить обработку исправлений
        return
    
    if message.text == "✅ Все верно, продолжить":
        data = await state.get_data()
        analysis_result = data.get('analysis_result', {})
        
        # Предлагаем название
        brand = analysis_result.get('brand', 'Техника')
        model = analysis_result.get('model', '')
        name = f"{brand} {model}" if brand and model else brand
        
        await reply(
            message,
            f"🏷️ <b>Предлагаемое название:</b> {name}\n\n"
            "Вы можете оставить это название или ввести свое:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text=f"✅ Оставить: {name[:30]}")],
                    [KeyboardButton(text="✏️ Ввести другое название")],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
        )
        await state.set_state(UserStates.waiting_for_equipment_name)

@dp.message(UserStates.waiting_for_equipment_name)
async def process_equipment_name(message: types.Message, state: FSMContext):
    """Обрабатывает ввод названия техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    equipment_name = message.text
    
    if equipment_name.startswith("✅ Оставить: "):
        equipment_name = equipment_name.replace("✅ Оставить: ", "")
    
    await state.update_data(equipment_name=equipment_name)
    
    await reply(
        message,
        "⏱️ <b>Введите текущие моточасы техники:</b>\n\n"
        "<i>Пример:</i> 1250",
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
        numbers = re.findall(r'\d+', message.text)
        if numbers:
            motohours = int(numbers[0])
        else:
            motohours = int(message.text)
        
        await state.update_data(motohours=motohours)
        
        await reply(
            message,
            "🛠️ <b>Введите информацию о последнем ТО:</b>\n\n"
            "<i>Пример:</i>\n"
            "Замена масла и фильтров 01.12.2023",
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
    
    last_service = message.text
    
    # Получаем все данные
    data = await state.get_data()
    user = await db.get_user(message.from_user.id)
    analysis_result = data.get('analysis_result', {})
    equipment_name = data.get('equipment_name')
    motohours = data.get('motohours', 0)
    
    # Формируем данные для регистрации
    vin = analysis_result.get('vin')
    if not vin:
        vin = f"TEMP_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Добавляем технику в базу
    equipment_id = await db.add_equipment(
        name=equipment_name,
        model=analysis_result.get('model', 'Неизвестно'),
        vin=vin,
        org_id=user['organization_id'],
        registration_number=analysis_result.get('registration_number', 'Без номера'),
        fuel_type='diesel',
        fuel_capacity=300
    )
    
    if equipment_id:
        # Обновляем дополнительные данные
        update_data = {'odometer': motohours}
        
        if analysis_result.get('year'):
            update_data['year'] = analysis_result['year']
        if analysis_result.get('color'):
            update_data['color'] = analysis_result['color']
        if analysis_result.get('engine_power'):
            update_data['engine_power'] = analysis_result['engine_power']
        
        await db.update_equipment(equipment_id, **update_data)
        
        # Сохраняем информацию о последнем ТО
        await db.add_maintenance(
            equipment_id=equipment_id,
            type="Регистрация",
            scheduled_date=datetime.now().strftime('%Y-%m-%d'),
            description=f"Регистрация техники. Последнее ТО: {last_service}"
        )
        
        # Сохраняем анализ документа
        await db.save_document_analysis({
            "equipment_id": equipment_id,
            "document_type": data.get('document_type', 'СТС'),
            "analysis_data": analysis_result,
            "analysis_quality": analysis_result.get('analysis_quality', 'unknown'),
            "motohours": motohours,
            "last_service": last_service,
            "registration_date": datetime.now().strftime('%Y-%m-%d')
        })
        
        # Отправляем сообщение об успехе
        success_text = f"✅ <b>Техника успешно зарегистрирована!</b>\n\n"
        success_text += f"<b>ID техники:</b> {equipment_id}\n"
        success_text += f"<b>Название:</b> {equipment_name}\n"
        success_text += f"<b>Модель:</b> {analysis_result.get('model', 'Неизвестно')}\n"
        success_text += f"<b>VIN:</b> {vin}\n"
        success_text += f"<b>Госномер:</b> {analysis_result.get('registration_number', 'Без номера')}\n"
        
        if analysis_result.get('year'):
            success_text += f"<b>Год выпуска:</b> {analysis_result['year']}\n"
        
        success_text += f"<b>Моточасы:</b> {motohours}\n"
        success_text += "\n🚜 <b>Техника добавлена в ваш автопарк!</b>"
        
        await reply(message, success_text)
        
        # Очищаем состояние
        await state.clear()
        await reply(
            message,
            "Возврат в главное меню",
            reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
        )
        
    else:
        await reply(
            message,
            "❌ Ошибка при сохранении техники.",
            reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
        )
        await state.clear()

# ========== НАЗНАЧЕНИЕ РОЛЕЙ (АДМИНИСТРАТОР) ==========
@dp.message(F.text == "➕ Назначить роль")
async def assign_role_start(message: types.Message, state: FSMContext):
    """Начинает процесс назначения роли"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    await reply(
        message,
        "👤 <b>Назначение роли пользователю</b>\n\n"
        "Введите Telegram ID пользователя:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_role_user_id)

@dp.message(UserStates.waiting_for_role_user_id)
async def process_role_user_id(message: types.Message, state: FSMContext):
    """Обрабатывает ввод ID пользователя"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Назначение роли отменено",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    try:
        user_id = int(message.text)
        target_user = await db.get_user(user_id)
        
        if not target_user:
            await reply(message, f"❌ Пользователь с ID {user_id} не найден.")
            return
        
        await state.update_data(role_user_id=user_id, target_user_name=target_user['full_name'])
        
        await reply(
            message,
            f"👤 <b>Пользователь:</b> {target_user['full_name']}\n"
            f"<b>Текущая роль:</b> {target_user['role']}\n\n"
            "Выберите новую роль:",
            reply_markup=get_role_type_keyboard()
        )
        await state.set_state(UserStates.waiting_for_role_type)
        
    except ValueError:
        await reply(message, "❌ Введите числовой ID пользователя!")

@dp.message(UserStates.waiting_for_role_type)
async def process_role_type(message: types.Message, state: FSMContext):
    """Обрабатывает выбор типа роли"""
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
        await reply(message, "❌ Выберите роль из списка", reply_markup=get_role_type_keyboard())
        return
    
    selected_role = role_map[message.text]
    data = await state.get_data()
    user_id = data.get('role_user_id')
    target_user_name = data.get('target_user_name')
    
    await state.update_data(selected_role=selected_role)
    
    # Если назначаем директора, спрашиваем об организации
    if selected_role == 'director':
        await reply(
            message,
            "🏢 <b>Создание организации для директора</b>\n\n"
            "Введите название организации:",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_role_organization)
    else:
        # Для других ролей просто назначаем
        success = await db.update_user_role(user_id, selected_role)
        
        if success:
            await reply(
                message,
                f"✅ <b>Роль успешно назначена!</b>\n\n"
                f"👤 Пользователь: {target_user_name}\n"
                f"🎭 Новая роль: {message.text}\n"
                f"🆔 ID: {user_id}",
                reply_markup=get_main_keyboard('botadmin', False)
            )
        else:
            await reply(
                message,
                "❌ Ошибка при назначении роли",
                reply_markup=get_main_keyboard('botadmin', False)
            )
        
        await state.clear()

@dp.message(UserStates.waiting_for_role_organization)
async def process_role_organization(message: types.Message, state: FSMContext):
    """Обрабатывает ввод названия организации для директора"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Назначение роли отменено",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    org_name = message.text
    data = await state.get_data()
    user_id = data.get('role_user_id')
    target_user_name = data.get('target_user_name')
    selected_role = data.get('selected_role', 'director')
    
    # Создаем организацию и назначаем директора
    org_id, error = await db.create_organization_for_director(user_id, org_name)
    
    if org_id:
        await reply(
            message,
            f"✅ <b>Организация создана и роль назначена!</b>\n\n"
            f"👤 Пользователь: {target_user_name}\n"
            f"🎭 Роль: Директор\n"
            f"🏢 Организация: {org_name}\n"
            f"🆔 ID организации: {org_id}",
            reply_markup=get_main_keyboard('botadmin', False)
        )
    else:
        await reply(
            message,
            f"❌ Ошибка: {error}",
            reply_markup=get_main_keyboard('botadmin', False)
        )
    
    await state.clear()

# ========== СТАТИСТИКА ==========
@dp.message(F.text == "📊 Статистика")
async def show_statistics(message: types.Message):
    """Показывает статистику"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['botadmin', 'director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для руководителей!")
        return
    
    if user['role'] == 'botadmin':
        # Статистика для администратора
        users = await db.get_all_users_simple()
        organizations = await db.get_all_organizations_simple()
        
        stats_text = "📊 <b>Общая статистика системы</b>\n\n"
        stats_text += f"👥 <b>Пользователей:</b> {len(users)}\n"
        stats_text += f"🏢 <b>Организаций:</b> {len(organizations)}\n"
        
        # Распределение по ролям
        roles_count = {}
        for u in users:
            roles_count[u['role']] = roles_count.get(u['role'], 0) + 1
        
        stats_text += "\n<b>Распределение по ролям:</b>\n"
        role_names = {
            'botadmin': '👑 Администраторы',
            'director': '👨‍💼 Директоры',
            'fleetmanager': '👷 Начальники парка',
            'driver': '🚛 Водители',
            'unassigned': '❓ Не назначенные'
        }
        
        for role, count in roles_count.items():
            stats_text += f"• {role_names.get(role, role)}: {count}\n"
    
    else:
        # Статистика для организации
        org_id = user.get('organization_id')
        if not org_id:
            await reply(message, "❌ Вы не привязаны к организации!")
            return
        
        org = await db.get_organization(org_id)
        if not org:
            await reply(message, "❌ Организация не найдена!")
            return
        
        # Здесь можно добавить получение статистики по организации
        stats_text = f"📊 <b>Статистика организации</b>\n\n"
        stats_text += f"🏢 <b>Организация:</b> {org['name']}\n"
        stats_text += "\n<b>Функции статистики в разработке...</b>\n"
        stats_text += "В ближайшее время здесь будет:\n"
        stats_text += "• Количество техники\n• Статусы ТО\n• Статистика смен\n• Анализ эффективности"
    
    await reply(message, stats_text)

@dp.message(F.text == "📊 Моя статистика")
async def show_my_statistics(message: types.Message):
    """Показывает персональную статистику для водителя"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Доступ только для водителей!")
        return
    
    stats_text = "📊 <b>Ваша персональная статистика</b>\n\n"
    stats_text += f"👤 <b>Водитель:</b> {user['full_name']}\n"
    stats_text += f"🆔 <b>ID:</b> {user['telegram_id']}\n\n"
    stats_text += "Статистика в разработке...\n"
    stats_text += "Скоро здесь будет:\n"
    stats_text += "• Отработанные смены\n• Пройденные километры\n• Расход топлива\n• Рейтинг безопасности"
    
    await reply(message, stats_text)

# ========== НАСТРОЙКИ ИИ ==========
@dp.message(F.text == "⚙️ Настройки ИИ")
async def show_ai_settings(message: types.Message):
    """Показывает настройки ИИ модулей"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    settings_text = "⚙️ <b>Настройки ИИ модулей</b>\n\n"
    
    for module_name, config in AI_CONFIG.items():
        if module_name == AIModule.DOCUMENT_ANALYSIS:
            status = "✅ ВКЛ" if config['enabled'] else "❌ ВЫКЛ"
            has_url = "✅ Настроен" if config.get('function_url') else "❌ Не настроен"
            
            settings_text += f"<b>📄 Анализ документов (Cloud Function):</b>\n"
            settings_text += f"• Статус: {status}\n"
            settings_text += f"• URL: {has_url}\n"
            if config.get('function_url'):
                settings_text += f"• Таймаут: {config.get('timeout', 60)}с\n"
                settings_text += f"• Повторные попытки: {config.get('max_retries', 3)}\n"
        else:
            status = "✅ ВКЛ" if config['enabled'] else "❌ ВЫКЛ"
            has_key = "✅ Настроен" if config.get('api_key') else "❌ Не настроен"
            
            settings_text += f"<b>{module_name.value}:</b>\n"
            settings_text += f"• Статус: {status}\n"
            settings_text += f"• API ключ: {has_key}\n"
            if config.get('model'):
                settings_text += f"• Модель: {config.get('model')}\n"
    
    settings_text += f"\n<b>👁️ Vision API:</b> {'✅ ВКЛ' if VISION_ENABLED else '❌ ВЫКЛ'}\n"
    settings_text += f"<b>API ключ Vision:</b> {'✅ Настроен' if VISION_API_KEY else '❌ Не настроен'}\n"
    
    settings_text += f"\n<b>📝 Всего промптов:</b> {len(PROMPTS)}\n"
    
    await reply(message, settings_text)

# ========== ИИ ПОМОЩНИКИ ==========
@dp.message(F.text == "🔧 Сервисный помощник")
async def service_assistant_start(message: types.Message, state: FSMContext):
    """Запускает сервисного ИИ помощника"""
    user = await db.get_user(message.from_user.id)
    
    if not AI_CONFIG[AIModule.SERVICE]['enabled']:
        await reply(message, "⚠️ Сервисный помощник временно отключен")
        return
    
    await reply(
        message,
        "🔧 <b>Сервисный ИИ помощник</b>\n\n"
        "Опишите проблему с техникой, и я помогу:\n"
        "• Диагностировать неисправность\n"
        "• Предложить решение\n"
        "• Подобрать запчасти\n"
        "• Рассчитать стоимость ремонта\n\n"
        "<b>Опишите проблему:</b>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_service_issue)

@dp.message(UserStates.waiting_for_service_issue)
async def process_service_issue(message: types.Message, state: FSMContext):
    """Обрабатывает описание проблемы для сервисного помощника"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Помощник отменен",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    issue = message.text
    
    await reply(message, "🤖 <b>ИИ анализирует проблему...</b>")
    
    # Здесь должен быть вызов ИИ для анализа проблемы
    # Пока заглушка
    await asyncio.sleep(2)
    
    response_text = (
        "✅ <b>Анализ завершен</b>\n\n"
        f"<b>Проблема:</b> {issue[:100]}...\n\n"
        "<b>Рекомендации:</b>\n"
        "1. Проверьте уровень масла\n"
        "2. Осмотрите фильтры\n"
        "3. Проверьте работу гидравлики\n\n"
        "<b>Предполагаемая стоимость ремонта:</b> 15,000 - 25,000 руб.\n"
        "<b>Время ремонта:</b> 1-2 рабочих дня"
    )
    
    await reply(message, response_text)
    await state.clear()
    
    user = await db.get_user(message.from_user.id)
    await reply(
        message,
        "Возврат в главное меню",
        reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
    )

@dp.message(F.text == "🚛 Помощник по сменам")
async def shift_assistant_start(message: types.Message, state: FSMContext):
    """Запускает ИИ помощника по сменам"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Доступ только для водителей!")
        return
    
    if not AI_CONFIG[AIModule.SHIFT]['enabled']:
        await reply(message, "⚠️ Помощник по сменам временно отключен")
        return
    
    await reply(
        message,
        "🚛 <b>ИИ помощник по сменам</b>\n\n"
        "Расскажите о вашей смене, и я помогу:\n"
        "• Составить отчет\n"
        "• Рассчитать нормы\n"
        "• Дать рекомендации\n"
        "• Предупредить о нарушениях\n\n"
        "<b>Опишите вашу смену:</b>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_shift_details)

@dp.message(F.text == "📦 Помощник по запчастям")
async def spare_parts_assistant_start(message: types.Message, state: FSMContext):
    """Запускает ИИ помощника по запчастям"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['fleetmanager', 'director']:
        await reply(message, "⛔ Доступ только для руководителей!")
        return
    
    if not AI_CONFIG[AIModule.SPARE_PARTS]['enabled']:
        await reply(message, "⚠️ Помощник по запчастям временно отключен")
        return
    
    await reply(
        message,
        "📦 <b>ИИ помощник по запчастям</b>\n\n"
        "Опишите что нужно, и я помогу:\n"
        "• Подобрать аналоги\n"
        "• Найти поставщиков\n"
        "• Сравнить цены\n"
        "• Рассчитать сроки\n\n"
        "<b>Что вам нужно?</b>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_spare_parts)

# ========== КОМАНДА ДЛЯ РУЧНОГО АНАЛИЗА ДОКУМЕНТА ==========
@dp.message(Command("analyze_document"))
async def cmd_analyze_document(message: types.Message, state: FSMContext):
    """Команда для ручного анализа документа"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Только руководители могут анализировать документы!")
        return
    
    await reply(
        message,
        "🔍 <b>Анализ документа СТС/ПТС</b>\n\n"
        "Отправьте фото документа для анализа.\n\n"
        "<b>Отправьте фото:</b>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_document_photo)

# ========== КОМАНДА ДЛЯ ПРОВЕРКИ СТАТУСА CLOUD FUNCTION ==========
@dp.message(Command("check_cf_status"))
async def cmd_check_cf_status(message: types.Message):
    """Проверяет статус Cloud Function"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    config = AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]
    
    status_text = "🔧 <b>Статус Cloud Function</b>\n\n"
    status_text += f"<b>Включена:</b> {'✅ Да' if config['enabled'] else '❌ Нет'}\n"
    status_text += f"<b>URL:</b> {config['function_url']}\n"
    
    # Пробуем отправить тестовый запрос
    if config['function_url']:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(config['function_url'], timeout=10) as response:
                    status_text += f"<b>HTTP статус:</b> {response.status}\n"
                    if response.status == 200:
                        status_text += "🟢 <b>Функция доступна</b>\n"
                    else:
                        status_text += f"🔴 <b>Проблема: {response.status}</b>\n"
        except Exception as e:
            status_text += f"🔴 <b>Ошибка подключения:</b> {str(e)}\n"
    
    await reply(message, status_text)

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

# ========== ФУНКЦИИ ДИРЕКТОРА ==========
@dp.message(F.text == "🏢 Моя организация")
async def my_organization(message: types.Message):
    """Показывает информацию об организации директора"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'director':
        await reply(message, "⛔ Доступ только для директора!")
        return
    
    if not user.get('organization_id'):
        await reply(message, "❌ У вас нет организации!")
        return
    
    org = await db.get_organization(user['organization_id'])
    if not org:
        await reply(message, "❌ Организация не найдена!")
        return
    
    org_text = f"🏢 <b>Моя организация</b>\n\n"
    org_text += f"<b>Название:</b> {org['name']}\n"
    org_text += f"<b>ID:</b> {org['id']}\n"
    if org.get('director_id'):
        org_text += f"<b>Директор ID:</b> {org['director_id']}\n"
    if org.get('address'):
        org_text += f"<b>Адрес:</b> {org['address']}\n"
    if org.get('contact_phone'):
        org_text += f"<b>Телефон:</b> {org['contact_phone']}\n"
    
    org_text += f"\n<b>Дата создания:</b> {org.get('created_at', 'Неизвестно')}"
    
    await reply(message, org_text)

@dp.message(F.text == "🏢 Создать организацию")
async def create_organization(message: types.Message):
    """Создает организацию для директора"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'director':
        await reply(message, "⛔ Доступ только для директора!")
        return
    
    if user.get('organization_id'):
        await reply(message, "❌ У вас уже есть организация!")
        return
    
    await reply(
        message,
        "🏢 <b>Создание организации</b>\n\n"
        "Введите название вашей организации:",
        reply_markup=get_cancel_keyboard()
    )
    # Здесь нужно добавить состояние для создания организации
    # Пока просто сообщение
    await reply(message, "Функция создания организации в разработке...")

# ========== ЗАПУСК БОТА ==========
async def on_startup():
    """Инициализация при запуске"""
    try:
        await db.connect()
        
        # Создаем администратора если нет
        ADMIN_ID = int(os.getenv('ADMIN_ID', 1079922982))
        existing_admin = await db.get_user(ADMIN_ID)
        
        if not existing_admin:
            await db.register_user(
                telegram_id=ADMIN_ID,
                full_name="Администратор Системы",
                username="admin",
                role='botadmin'
            )
            logger.info(f"✅ Администратор создан: ID {ADMIN_ID}")
        
        logger.info("🚀 Бот запущен!")
        logger.info(f"🤖 Анализ документов: {'✅ ВКЛ' if AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['enabled'] else '❌ ВЫКЛ'}")
        logger.info(f"👑 Администратор: ID {ADMIN_ID}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

async def main():
    """Основная функция"""
    await on_startup()
    
    try:
        logger.info("🤖 Бот работает...")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
