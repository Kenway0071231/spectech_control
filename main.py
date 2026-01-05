import os
import logging
import asyncio
import json
import base64
import re
import aiohttp
import aiocron
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from io import BytesIO
from enum import Enum

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile
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

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)

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
        
        Args:
            image_bytes: Байты изображения документа
            document_type: Тип документа (СТС, ПТС, ПСМ)
            
        Returns:
            Dict с результатами анализа
        """
        if not self.enabled:
            return {"error": "Функция анализа документов отключена", "success": False}
        
        if not self.function_url:
            return {"error": "URL функции анализа документов не настроен", "success": False}
        
        # Кодируем изображение в base64
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        
        # Формируем промпт с учетом типа документа
        prompt = get_prompt("document_analysis")
        prompt = prompt.replace("СТС/ПТС/ПСМ/Другое", document_type)
        
        # Формируем запрос к функции
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
                            logger.info(f"Получен ответ от функции анализа документов (попытка {attempt + 1})")
                            
                            # Обрабатываем ответ
                            return self._process_response(result_data, document_type)
                            
                        elif response.status == 429:
                            logger.warning(f"Слишком много запросов. Попытка {attempt + 1} из {self.max_retries}")
                            if attempt < self.max_retries - 1:
                                wait_time = 2 ** attempt  # Экспоненциальная задержка
                                await asyncio.sleep(wait_time)
                                continue
                            
                        else:
                            error_text = await response.text()
                            logger.error(f"Ошибка функции анализа: {response.status} - {error_text[:200]}")
                            return {
                                "error": f"Ошибка API: {response.status}",
                                "status_code": response.status,
                                "success": False
                            }
                            
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут при анализе документа (попытка {attempt + 1})")
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
                
                # Логируем результат
                logger.info(f"Анализ завершен: {quality_score['quality']} качество, найдено {20 - len(quality_score['missing_fields'])}/20 полей")
                
                return validated_data
            else:
                # Если не удалось распознать JSON, пробуем классифицировать текст
                classified_type = self._classify_document_from_text(result_text)
                return {
                    "success": False,
                    "error": "Не удалось извлечь структурированные данные",
                    "extracted_text": result_text[:1000],
                    "classified_type": classified_type,
                    "suggestion": "Попробуйте сделать более четкое фото или использовать другой документ"
                }
                
        except Exception as e:
            logger.error(f"Ошибка обработки ответа: {e}")
            return {
                "success": False,
                "error": f"Ошибка обработки: {str(e)}",
                "raw_response": str(result_data)[:500]
            }
    
    def _extract_json_from_response(self, response_text: str) -> Optional[Dict]:
        """Извлекает JSON из ответа функции"""
        try:
            # Ищем JSON в ответе
            json_patterns = [
                r'```json\s*(.*?)\s*```',  # JSON в markdown
                r'```\s*(.*?)\s*```',      # Любой код в markdown
                r'(\{.*?\})',               # Просто JSON
            ]
            
            json_str = None
            for pattern in json_patterns:
                match = re.search(pattern, response_text, re.DOTALL)
                if match:
                    json_str = match.group(1) if len(match.groups()) > 0 else match.group(0)
                    break
            
            # Если не нашли по паттернам, пробуем весь текст как JSON
            if not json_str:
                # Ищем начало и конец JSON
                start = response_text.find('{')
                end = response_text.rfind('}')
                if start != -1 and end != -1 and end > start:
                    json_str = response_text[start:end+1]
            
            if json_str:
                # Очищаем строку от лишних символов
                json_str = json_str.strip()
                # Заменяем нестандартные кавычки
                json_str = json_str.replace('"', '"').replace('"', '"')
                # Удаляем управляющие символы
                json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', json_str)
                
                data = json.loads(json_str)
                return data
                
        except json.JSONDecodeError as e:
            logger.warning(f"Ошибка декодирования JSON: {e}")
            # Пытаемся исправить распространенные ошибки
            try:
                json_str = self._fix_json_errors(json_str)
                if json_str:
                    data = json.loads(json_str)
                    return data
            except:
                pass
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении JSON: {e}")
            
        return None
    
    def _fix_json_errors(self, json_str: str) -> Optional[str]:
        """Пытается исправить распространенные ошибки в JSON"""
        try:
            # Заменяем одинарные кавычки на двойные
            json_str = re.sub(r"(?<!\\)'", '"', json_str)
            # Исправляем незакрытые кавычки
            json_str = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', json_str)
            # Исправляем trailing commas
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            return json_str
        except:
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
            
            # Очистка и валидация для каждого поля
            if isinstance(value, str):
                value = value.strip()
                
                # Убираем лишние символы
                value = re.sub(r'\s+', ' ', value)
                
                # Специальная обработка для разных полей
                if field == "vin":
                    # Ищем VIN в тексте
                    vin_match = re.search(r'[A-HJ-NPR-Z0-9]{17}', value.upper())
                    if vin_match:
                        value = vin_match.group(0)
                    else:
                        value = None
                        
                elif field == "registration_number":
                    # Стандартизируем госномер
                    value = value.upper()
                    # Удаляем лишние пробелы и символы
                    value = re.sub(r'[^А-Я0-9]', '', value)
                    
                elif field == "year":
                    # Извлекаем год
                    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', value)
                    if year_match:
                        value = int(year_match.group(0))
                    else:
                        value = None
                        
                elif field == "engine_power":
                    # Извлекаем мощность
                    power_match = re.search(r'(\d+)\s*(л\.с\.|лс|кВт|сил|hp)', value, re.IGNORECASE)
                    if power_match:
                        value = int(power_match.group(1))
                    else:
                        # Пробуем найти просто число
                        num_match = re.search(r'\b(\d{2,4})\b', value)
                        if num_match:
                            value = int(num_match.group(1))
                        else:
                            value = None
                            
                elif field == "color":
                    # Стандартизируем цвет
                    colors = ["белый", "черный", "красный", "синий", "зеленый", 
                             "желтый", "серый", "коричневый", "оранжевый", "фиолетовый"]
                    for color in colors:
                        if color in value.lower():
                            value = color.capitalize()
                            break
                
                elif field in ["weight", "max_weight", "engine_volume"]:
                    # Извлекаем числа
                    num_match = re.search(r'\b(\d+)\b', value)
                    if num_match:
                        value = int(num_match.group(0))
                    else:
                        value = None
            
            cleaned[field] = value
        
        return cleaned
    
    def _calculate_quality_score(self, data: Dict) -> Dict[str, Any]:
        """Рассчитывает качество распознавания"""
        # Критически важные поля
        critical_fields = ["vin", "model", "brand"]
        
        # Важные поля
        important_fields = ["registration_number", "year", "engine_power", "category"]
        
        # Дополнительные поля
        additional_fields = ["color", "weight", "owner", "registration_date"]
        
        missing_fields = []
        score = 0
        max_score = 100
        
        # Проверяем критические поля (40% от оценки)
        for field in critical_fields:
            if data.get(field):
                score += 13.33  # 40/3
            else:
                missing_fields.append(field)
        
        # Проверяем важные поля (35% от оценки)
        for field in important_fields:
            if data.get(field):
                score += 8.75  # 35/4
            else:
                missing_fields.append(field)
        
        # Проверяем дополнительные поля (25% от оценки)
        for field in additional_fields:
            if data.get(field):
                score += 6.25  # 25/4
        
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
    
    def _classify_document_from_text(self, text: str) -> str:
        """Классифицирует документ по тексту"""
        text_lower = text.lower()
        
        if "свидетельство о регистрации" in text_lower and "гибдд" in text_lower:
            return "СТС"
        elif "паспорт транспортного средства" in text_lower:
            return "ПТС"
        elif "паспорт самоходной машины" in text_lower:
            return "ПСМ"
        elif "свидетельство о регистрации самоходной машины" in text_lower:
            return "СТСМ"
        else:
            return "НЕИЗВЕСТНО"

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
                            "languageCodes": ["ru", "en"],
                            "model": "page"  # Лучшее качество для документов
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
                        logger.error(f"Ошибка Vision API: {response.status} - {error_text[:200]}")
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
                    "error": "Не удалось извлечь текст из документа",
                    "raw_result": result
                }
            
            # Анализируем структуру текста
            structure = self._analyze_text_structure(extracted_text)
            
            return {
                "success": True,
                "extracted_text": extracted_text.strip(),
                "text_blocks": blocks_info,
                "structure": structure,
                "total_blocks": len(blocks_info),
                "average_confidence": sum(b["confidence"] for b in blocks_info) / len(blocks_info) if blocks_info else 0
            }
            
        except Exception as e:
            logger.error(f"Ошибка извлечения текста: {e}")
            return {
                "success": False,
                "error": f"Ошибка обработки: {e}",
                "raw_result": result
            }
    
    def _analyze_text_structure(self, text: str) -> Dict[str, Any]:
        """Анализирует структуру текста документа"""
        lines = text.split('\n')
        
        # Ищем ключевые разделы
        sections = {
            "personal_data": any(word in text.lower() for word in ["фио", "собственник", "владелец"]),
            "vehicle_data": any(word in text.lower() for word in ["марка", "модель", "vin", "год"]),
            "registration_data": any(word in text.lower() for word in ["регистрация", "выдан", "дата"]),
            "technical_data": any(word in text.lower() for word in ["мощность", "объем", "масса", "цвет"])
        }
        
        # Подсчитываем статистику
        word_count = len(text.split())
        line_count = len(lines)
        avg_line_length = sum(len(line) for line in lines) / line_count if line_count > 0 else 0
        
        return {
            "sections_found": sum(sections.values()),
            "sections": sections,
            "word_count": word_count,
            "line_count": line_count,
            "avg_line_length": round(avg_line_length, 2)
        }

# ========== КЛАСС ДЛЯ ИИ РЕГИСТРАЦИИ ==========
class RegistrationAI:
    """ИИ для регистрации техники с использованием анализа документов"""
    
    def __init__(self):
        self.document_analyzer = DocumentAnalyzer()
        self.vision_analyzer = YandexVisionAnalyzer()
        self.config = AI_CONFIG[AIModule.REGISTRATION]
        
    async def register_equipment_from_document(self, image_bytes: bytes, document_type: str = "СТС") -> Dict[str, Any]:
        """
        Регистрирует технику на основе анализа документа
        
        Args:
            image_bytes: Байты изображения документа
            document_type: Тип документа
            
        Returns:
            Dict с данными для регистрации
        """
        try:
            logger.info(f"Начало регистрации техники из документа типа {document_type}")
            
            # 1. Анализируем документ через Cloud Function
            document_analysis = await self.document_analyzer.analyze_document(image_bytes, document_type)
            
            # Если Cloud Function не сработала, пробуем Vision API
            if not document_analysis.get("success", False):
                logger.warning("Cloud Function не сработала, пробуем Vision API")
                return await self._fallback_registration(image_bytes, document_type)
            
            # 2. Проверяем качество анализа
            quality = document_analysis.get("analysis_quality", "low")
            
            if quality == "low":
                logger.warning("Низкое качество распознавания, требуются дополнительные проверки")
                # Пробуем улучшить данные через Vision
                vision_result = await self.vision_analyzer.analyze_document_text(image_bytes)
                if vision_result.get("success"):
                    document_analysis = self._enhance_with_vision(document_analysis, vision_result)
            
            # 3. Формируем данные для регистрации
            registration_data = self._format_registration_data(document_analysis)
            
            # 4. Получаем рекомендации от GPT если включено
            if self.config['enabled'] and self.config['api_key']:
                recommendations = await self._get_gpt_recommendations(document_analysis)
                registration_data["ai_recommendations"] = recommendations
            
            # 5. Добавляем метаданные
            registration_data["document_analysis"] = document_analysis
            registration_data["success"] = True
            registration_data["registration_method"] = "cloud_function"
            
            logger.info(f"Регистрация успешно обработана: {registration_data.get('vin', 'без VIN')}")
            
            return registration_data
            
        except Exception as e:
            logger.error(f"Ошибка регистрации техники: {e}")
            return {
                "error": str(e),
                "success": False,
                "registration_method": "failed"
            }
    
    async def _fallback_registration(self, image_bytes: bytes, document_type: str) -> Dict[str, Any]:
        """Запасной метод регистрации через Vision API"""
        try:
            logger.info("Используем запасной метод регистрации через Vision API")
            
            # 1. Получаем текст через Vision API
            vision_result = await self.vision_analyzer.analyze_document_text(image_bytes)
            
            if not vision_result.get("success"):
                return {
                    "error": vision_result.get("error", "Неизвестная ошибка Vision API"),
                    "success": False,
                    "registration_method": "vision_failed"
                }
            
            extracted_text = vision_result.get("extracted_text", "")
            
            # 2. Парсим текст вручную
            manual_data = self._parse_document_text_manually(extracted_text, document_type)
            
            # 3. Получаем помощь от GPT если доступно
            if self.config['enabled'] and self.config['api_key']:
                gpt_analysis = await self._analyze_with_gpt(extracted_text, document_type)
                if gpt_analysis and gpt_analysis.get("success"):
                    manual_data.update(gpt_analysis.get("ai_analysis", {}))
            
            # 4. Форматируем данные
            registration_data = self._format_registration_data(manual_data)
            registration_data["extracted_text"] = extracted_text[:1000] + "..." if len(extracted_text) > 1000 else extracted_text
            registration_data["vision_result"] = {
                "total_blocks": vision_result.get("total_blocks", 0),
                "average_confidence": vision_result.get("average_confidence", 0),
                "structure": vision_result.get("structure", {})
            }
            registration_data["success"] = True
            registration_data["registration_method"] = "vision_api"
            registration_data["requires_manual_check"] = True
            
            return registration_data
            
        except Exception as e:
            logger.error(f"Ошибка запасного метода регистрации: {e}")
            return {
                "error": str(e),
                "success": False,
                "registration_method": "fallback_failed"
            }
    
    def _enhance_with_vision(self, document_data: Dict, vision_result: Dict) -> Dict:
        """Улучшает данные документа с помощью Vision API"""
        try:
            enhanced = document_data.copy()
            extracted_text = vision_result.get("extracted_text", "")
            
            # Если VIN не найден, пробуем найти в тексте Vision
            if not enhanced.get("vin") or enhanced.get("vin") == "null":
                vin_match = re.search(r'[A-HJ-NPR-Z0-9]{17}', extracted_text.upper())
                if vin_match:
                    enhanced["vin"] = vin_match.group(0)
            
            # Если госномер не найден
            if not enhanced.get("registration_number") or enhanced.get("registration_number") == "null":
                # Паттерны для российских номеров
                patterns = [
                    r'[АВЕКМНОРСТУХ]{1}\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}',
                    r'[АВЕКМНОРСТУХ]{2}\d{3}\d{2,3}',
                    r'\d{4}[АВЕКМНОРСТУХ]{2}\d{2,3}',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, extracted_text)
                    if match:
                        enhanced["registration_number"] = match.group(0)
                        break
            
            # Обновляем извлеченный текст
            enhanced["extracted_text"] = extracted_text[:2000] + "..." if len(extracted_text) > 2000 else extracted_text
            
            return enhanced
            
        except Exception as e:
            logger.error(f"Ошибка улучшения данных Vision: {e}")
            return document_data
    
    def _parse_document_text_manually(self, text: str, document_type: str) -> Dict[str, Any]:
        """Ручной парсинг текста документа"""
        data = {
            "document_type": document_type,
            "vin": None,
            "registration_number": None,
            "model": "Неизвестно",
            "brand": "Неизвестно",
            "year": None,
            "category": "Спецтехника",
            "engine_power": None,
            "color": "Неизвестно",
            "extracted_text": text[:1000] + "..." if len(text) > 1000 else text
        }
        
        text_upper = text.upper()
        lines = text.split('\n')
        
        # Поиск VIN
        for line in lines:
            vin_match = re.search(r'[A-HJ-NPR-Z0-9]{17}', line.upper())
            if vin_match:
                data["vin"] = vin_match.group(0)
                break
        
        # Поиск госномера
        for line in lines:
            patterns = [
                r'[АВЕКМНОРСТУХ]{1}\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}',
                r'[АВЕКМНОРСТУХ]{2}\d{3}\d{2,3}',
                r'\d{4}[АВЕКМНОРСТУХ]{2}\d{2,3}',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    data["registration_number"] = match.group(0)
                    break
            if data["registration_number"]:
                break
        
        # Поиск года
        for line in lines:
            year_match = re.search(r'\b(19\d{2}|20\d{2})\b', line)
            if year_match:
                year = int(year_match.group(0))
                if 1950 <= year <= datetime.now().year + 1:
                    data["year"] = year
                    break
        
        # Поиск марки и модели
        common_brands = {
            "КАМАЗ": ["КАМАЗ", "KAMAZ"],
            "МАЗ": ["МАЗ", "MAZ"],
            "ЗИЛ": ["ЗИЛ", "ZIL"],
            "ГАЗ": ["ГАЗ", "GAZ"],
            "УРАЛ": ["УРАЛ", "URAL"],
            "БЕЛАЗ": ["БЕЛАЗ", "BELAZ"],
            "HITACHI": ["HITACHI"],
            "CAT": ["CAT", "CATERPILLAR"],
            "KOMATSU": ["KOMATSU"],
            "VOLVO": ["VOLVO"],
            "LIEBHERR": ["LIEBHERR"],
            "JCB": ["JCB"],
            "HYUNDAI": ["HYUNDAI"],
            "DOOSAN": ["DOOSAN"],
            "XCMG": ["XCMG"],
            "ZOOMLION": ["ZOOMLION"]
        }
        
        for brand, keywords in common_brands.items():
            for keyword in keywords:
                if keyword in text_upper:
                    data["brand"] = brand
                    # Пытаемся найти модель
                    idx = text_upper.find(keyword)
                    if idx != -1:
                        rest = text_upper[idx + len(keyword):idx + 100]
                        model_match = re.search(r'[A-Z0-9\-]{2,20}', rest)
                        if model_match:
                            data["model"] = f"{brand} {model_match.group(0)}"
                    break
            if data["brand"] != "Неизвестно":
                break
        
        # Поиск мощности
        for line in lines:
            power_match = re.search(r'(\d+)\s*(л\.с\.|лс|кВт|сил|hp)', line, re.IGNORECASE)
            if power_match:
                data["engine_power"] = int(power_match.group(1))
                break
        
        # Поиск цвета
        colors = ["белый", "черный", "красный", "синий", "зеленый", "желтый", 
                 "серый", "коричневый", "оранжевый", "фиолетовый"]
        
        for line in lines:
            line_lower = line.lower()
            for color in colors:
                if color in line_lower:
                    data["color"] = color.capitalize()
                    break
            if data["color"] != "Неизвестно":
                break
        
        return data
    
    async def _analyze_with_gpt(self, extracted_text: str, document_type: str) -> Dict[str, Any]:
        """Анализирует текст документа с помощью GPT"""
        try:
            if not self.config['api_key'] or not self.config['folder_id']:
                return None
            
            url = self.config['url']
            
            headers = {
                "Authorization": f"Api-Key {self.config['api_key']}",
                "x-folder-id": self.config['folder_id'],
                "Content-Type": "application/json"
            }
            
            # Обрезаем текст если слишком длинный
            if len(extracted_text) > 3000:
                extracted_text = extracted_text[:3000] + "... [текст обрезан]"
            
            prompt = get_prompt("vision_analysis")
            
            data = {
                "modelUri": f"gpt://{self.config['folder_id']}/{self.config['model']}",
                "completionOptions": {
                    "stream": False,
                    "temperature": 0.1,
                    "maxTokens": 1000
                },
                "messages": [
                    {
                        "role": "system",
                        "text": "Ты - эксперт по автомобильным документам. Извлекай только факты. Возвращай JSON."
                    },
                    {
                        "role": "user",
                        "text": f"{prompt}\n\nТекст документа ({document_type}):\n{extracted_text}"
                    }
                ]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=data, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        answer = result['result']['alternatives'][0]['message']['text']
                        
                        # Извлекаем JSON
                        try:
                            json_match = re.search(r'\{.*\}', answer, re.DOTALL)
                            if json_match:
                                json_str = json_match.group(0)
                                ai_analysis = json.loads(json_str)
                                return {"success": True, "ai_analysis": ai_analysis}
                        except:
                            pass
                    
            return None
            
        except Exception as e:
            logger.error(f"Ошибка GPT анализа: {e}")
            return None
    
    async def _get_gpt_recommendations(self, document_data: Dict) -> Dict[str, Any]:
        """Получает рекомендации от GPT"""
        try:
            missing_fields = []
            for field, value in document_data.items():
                if value is None or value == "null" or value == "Неизвестно":
                    missing_fields.append(field)
            
            if not missing_fields and document_data.get("analysis_quality") == "high":
                return {
                    "status": "excellent",
                    "message": "Все поля заполнены корректно. Техника готова к регистрации.",
                    "next_steps": ["Подтвердите регистрацию в системе"]
                }
            
            url = self.config['url']
            
            headers = {
                "Authorization": f"Api-Key {self.config['api_key']}",
                "x-folder-id": self.config['folder_id'],
                "Content-Type": "application/json"
            }
            
            prompt = get_prompt("registration", document_data=json.dumps(document_data, ensure_ascii=False, indent=2))
            
            data = {
                "modelUri": f"gpt://{self.config['folder_id']}/{self.config['model']}",
                "completionOptions": {
                    "stream": False,
                    "temperature": 0.3,
                    "maxTokens": 800
                },
                "messages": [
                    {
                        "role": "system",
                        "text": "Ты - помощник по регистрации спецтехники. Дай практические рекомендации в формате JSON."
                    },
                    {
                        "role": "user",
                        "text": prompt
                    }
                ]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=data, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        answer = result['result']['alternatives'][0]['message']['text']
                        
                        # Пытаемся извлечь JSON
                        try:
                            json_match = re.search(r'\{.*\}', answer, re.DOTALL)
                            if json_match:
                                json_str = json_match.group(0)
                                return json.loads(json_str)
                        except:
                            pass
                        
                        # Если не JSON, возвращаем как текст
                        return {
                            "status": "recommendations",
                            "message": answer[:500],
                            "next_steps": ["Проверьте данные вручную", "Заполните недостающие поля"]
                        }
                    
            return {
                "status": "unknown",
                "message": "Не удалось получить рекомендации",
                "next_steps": ["Проверьте данные вручную"]
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения рекомендаций: {e}")
            return {
                "status": "error",
                "message": f"Ошибка: {str(e)}",
                "next_steps": ["Проверьте данные вручную"]
            }
    
    def _format_registration_data(self, analysis_data: Dict) -> Dict[str, Any]:
        """Форматирует данные для регистрации"""
        # Генерируем имя для техники
        brand = analysis_data.get('brand', 'Техника')
        model = analysis_data.get('model', '')
        year = analysis_data.get('year')
        
        if brand and model and brand not in model:
            name = f"{brand} {model}"
        elif model and model != "Неизвестно":
            name = model
        else:
            name = brand
        
        if year:
            name = f"{name} ({year})"
        
        # Формируем VIN или генерируем временный
        vin = analysis_data.get('vin')
        if not vin or vin == "null":
            vin = f"TEMP_{datetime.now().strftime('%Y%m%d%H%M%S')}_{analysis_data.get('document_type', 'DOC')}"
        
        # Определяем категорию
        category = analysis_data.get('category', 'Спецтехника')
        model_lower = str(analysis_data.get('model', '')).lower()
        
        if any(word in model_lower for word in ['экскаватор', 'excavator']):
            category = 'Экскаватор'
        elif any(word in model_lower for word in ['погрузчик', 'loader']):
            category = 'Погрузчик'
        elif any(word in model_lower for word in ['бульдозер', 'bulldozer']):
            category = 'Бульдозер'
        elif any(word in model_lower for word in ['кран', 'crane']):
            category = 'Кран'
        elif any(word in model_lower for word in ['самосвал', 'dumper']):
            category = 'Самосвал'
        
        return {
            "name": name.strip(),
            "model": analysis_data.get('model', 'Неизвестно'),
            "brand": analysis_data.get('brand', 'Неизвестно'),
            "vin": vin,
            "registration_number": analysis_data.get('registration_number', 'Без номера'),
            "year": analysis_data.get('year'),
            "category": category,
            "engine_power": analysis_data.get('engine_power'),
            "color": analysis_data.get('color', 'Неизвестно'),
            "weight": analysis_data.get('weight'),
            "max_weight": analysis_data.get('max_weight'),
            "notes": f"Зарегистрировано через анализ {analysis_data.get('document_type', 'документа')}. "
                    f"Качество анализа: {analysis_data.get('analysis_quality', 'неизвестно')}",
            "document_type": analysis_data.get('document_type', 'Неизвестно'),
            "analysis_quality": analysis_data.get('analysis_quality', 'unknown'),
            "missing_fields": analysis_data.get('missing_fields', [])
        }

# ========== СОЗДАЕМ ЭКЗЕМПЛЯРЫ ==========
document_analyzer = DocumentAnalyzer()
vision_analyzer = YandexVisionAnalyzer()
registration_ai = RegistrationAI()

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
    
    # Дополнительные
    waiting_for_additional_info = State()
    waiting_for_manual_correction = State()
    waiting_for_field_correction = State()

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
                [types.KeyboardButton(text="⚙️ Настройки ИИ")],
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
                    [types.KeyboardButton(text="🔧 Сервисный помощник")],
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
                    [types.KeyboardButton(text="📷 Зарегистрировать технику")],
                    [types.KeyboardButton(text="🔧 Сервисный помощник")],
                    [types.KeyboardButton(text="📦 Заказы запчастей")],
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
                    [types.KeyboardButton(text="📋 Ежедневный отчет")],
                    [types.KeyboardButton(text="🚜 Моя техника")],
                    [types.KeyboardButton(text="🔧 Сервисный помощник")],
                    [types.KeyboardButton(text="📊 Моя статистика")],
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

def get_document_type_keyboard():
    """Клавиатура для выбора типа документа"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="📄 СТС (Свидетельство о регистрации)")],
            [types.KeyboardButton(text="📋 ПТС (Паспорт транспортного средства)")],
            [types.KeyboardButton(text="🏭 ПСМ (Паспорт самоходной машины)")],
            [types.KeyboardButton(text="📃 Другой документ")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def get_confirmation_keyboard():
    """Клавиатура для подтверждения данных"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="✅ Все верно, продолжить")],
            [types.KeyboardButton(text="✏️ Внести правки")],
            [types.KeyboardButton(text="🔄 Загрузить другой документ")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def get_correction_keyboard():
    """Клавиатура для корректировки данных"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="✅ Подтвердить все данные")],
            [types.KeyboardButton(text="🔧 Исправить VIN")],
            [types.KeyboardButton(text="🚗 Исправить госномер")],
            [types.KeyboardButton(text="🏷️ Исправить модель/марку")],
            [types.KeyboardButton(text="📅 Исправить год")],
            [types.KeyboardButton(text="🔄 Загрузить новый документ")],
            [types.KeyboardButton(text="❌ Отмена")]
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
        
        # Сохраняем ID фото
        await state.update_data(document_photo_id=photo.file_id, image_size=len(image_data))
        
        # Анализируем документ
        registration_result = await registration_ai.register_equipment_from_document(image_data, document_type)
        
        if not registration_result.get("success", False):
            error_msg = registration_result.get("error", "Неизвестная ошибка")
            registration_method = registration_result.get("registration_method", "")
            
            error_text = f"❌ <b>Ошибка анализа документа:</b> {error_msg}\n\n"
            
            if registration_method == "cloud_function":
                error_text += (
                    "📡 <b>Проблема с Cloud Function</b>\n"
                    "1. Проверьте доступность функции\n"
                    "2. Убедитесь в правильности URL\n"
                    "3. Попробуйте позже или используйте другой документ\n\n"
                    "🔄 <b>Попробовать через Vision API:</b>"
                )
                
                # Предлагаем использовать Vision API
                keyboard = types.ReplyKeyboardMarkup(
                    keyboard=[
                        [types.KeyboardButton(text="🔄 Использовать Vision API")],
                        [types.KeyboardButton(text="📤 Загрузить другой документ")],
                        [types.KeyboardButton(text="❌ Отмена")]
                    ],
                    resize_keyboard=True
                )
                
                await reply(message, error_text, reply_markup=keyboard)
                await state.update_data(cloud_function_failed=True)
                return
            
            elif registration_method == "vision_failed":
                error_text += (
                    "👁️ <b>Не удалось распознать текст</b>\n"
                    "Попробуйте:\n"
                    "1. Сделать более четкое фото\n"
                    "2. Улучшить освещение\n"
                    "3. Отправить другой документ\n\n"
                    "Или введите данные вручную."
                )
            else:
                error_text += "Попробуйте другой документ или введите данные вручную."
            
            await reply(message, error_text, reply_markup=get_cancel_keyboard())
            return
        
        # Сохраняем результат анализа
        await state.update_data(
            registration_result=registration_result,
            document_analysis=registration_result.get("document_analysis", {})
        )
        
        # Формируем сообщение с результатами
        result_data = registration_result
        
        info_text = "✅ <b>Документ успешно проанализирован!</b>\n\n"
        
        # Статус анализа
        quality = result_data.get("analysis_quality", "unknown")
        quality_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(quality, "⚪")
        
        info_text += f"<b>Качество анализа:</b> {quality_emoji} {quality.upper()}\n"
        info_text += f"<b>Метод:</b> {result_data.get('registration_method', 'неизвестно')}\n\n"
        
        # Основные поля
        fields = [
            ("📄 Тип документа", result_data.get("document_type", "СТС")),
            ("🔢 VIN номер", result_data.get("vin")),
            ("🚗 Госномер", result_data.get("registration_number")),
            ("🏷️ Марка", result_data.get("brand")),
            ("🚜 Модель", result_data.get("model")),
            ("📅 Год выпуска", result_data.get("year")),
            ("⚡ Мощность", f"{result_data.get('engine_power')} л.с." if result_data.get('engine_power') else None),
            ("🎨 Цвет", result_data.get("color")),
            ("🏗️ Тип техники", result_data.get("category")),
        ]
        
        for label, value in fields:
            if value:
                info_text += f"<b>{label}:</b> {value}\n"
        
        # Отсутствующие поля
        missing_fields = result_data.get("missing_fields", [])
        if missing_fields:
            info_text += f"\n⚠️ <b>Отсутствуют:</b> {', '.join(missing_fields)}\n"
        
        # Рекомендации ИИ
        if result_data.get("ai_recommendations"):
            rec = result_data["ai_recommendations"]
            if isinstance(rec, dict):
                status = rec.get("status", "")
                message = rec.get("message", "")
                if message:
                    info_text += f"\n<b>Рекомендации ИИ ({status}):</b>\n{message[:200]}"
            else:
                info_text += f"\n<b>Рекомендации ИИ:</b>\n{str(rec)[:200]}"
        
        info_text += "\n\n<b>Все данные верны?</b>"
        
        if quality == "low":
            await reply(message, info_text, reply_markup=get_correction_keyboard())
        else:
            await reply(message, info_text, reply_markup=get_confirmation_keyboard())
        
        await state.set_state(UserStates.waiting_for_document_analysis)
        
    except Exception as e:
        logger.error(f"Ошибка обработки фото документа: {e}")
        await reply(
            message,
            "❌ Ошибка при обработке фото. Попробуйте еще раз.\n\n"
            "<i>Убедитесь, что фото четкое и документ полностью виден.</i>",
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
            "📸 <b>Отправьте новое фото документа</b>\n\n"
            "Убедитесь, что:\n"
            "1. Фото четкое\n"
            "2. Весь документ в кадре\n"
            "3. Хорошее освещение\n"
            "4. Нет бликов и теней",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_document_photo)
        return
    
    if message.text == "✏️ Внести правки" or message.text.startswith("🔧 Исправить"):
        data = await state.get_data()
        registration_result = data.get('registration_result', {})
        
        if message.text == "✏️ Внести правки":
            await reply(
                message,
                "✏️ <b>Введите исправления в формате:</b>\n\n"
                "<code>Поле: Значение</code>\n\n"
                "<i>Пример:</i>\n"
                "VIN: X9F12345678901234\n"
                "Модель: Экскаватор Hitachi ZX200\n"
                "Год: 2022\n"
                "Цвет: Желтый\n\n"
                "<b>Введите исправления:</b>",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(UserStates.waiting_for_manual_correction)
            
        elif message.text == "🔧 Исправить VIN":
            await reply(
                message,
                f"🔧 <b>Текущий VIN:</b> {registration_result.get('vin', 'не указан')}\n\n"
                "Введите правильный VIN (17 символов):",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(UserStates.waiting_for_field_correction)
            await state.update_data(correcting_field="vin")
            
        elif message.text == "🚗 Исправить госномер":
            await reply(
                message,
                f"🚗 <b>Текущий госномер:</b> {registration_result.get('registration_number', 'не указан')}\n\n"
                "Введите правильный госномер:",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(UserStates.waiting_for_field_correction)
            await state.update_data(correcting_field="registration_number")
            
        elif message.text == "🏷️ Исправить модель/марку":
            await reply(
                message,
                f"🏷️ <b>Текущие данные:</b>\n"
                f"Марка: {registration_result.get('brand', 'не указана')}\n"
                f"Модель: {registration_result.get('model', 'не указана')}\n\n"
                "Введите исправленные данные в формате:\n"
                "<code>Марка: Камаз\nМодель: 6520</code>",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(UserStates.waiting_for_field_correction)
            await state.update_data(correcting_field="brand_model")
            
        elif message.text == "📅 Исправить год":
            await reply(
                message,
                f"📅 <b>Текущий год:</b> {registration_result.get('year', 'не указан')}\n\n"
                "Введите правильный год выпуска (4 цифры):",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(UserStates.waiting_for_field_correction)
            await state.update_data(correcting_field="year")
        
        return
    
    if message.text == "🔄 Использовать Vision API":
        # Пробуем использовать Vision API как запасной вариант
        await reply(message, "👁️ <b>Использую Vision API для анализа...</b>")
        
        data = await state.get_data()
        image_data = None
        
        # Пытаемся получить фото из состояния или запросить новое
        if data.get('document_photo_id'):
            try:
                file = await bot.get_file(data['document_photo_id'])
                photo_bytes = await bot.download_file(file.file_path)
                image_data = await photo_bytes.read()
            except:
                pass
        
        if not image_data:
            await reply(
                message,
                "📸 <b>Отправьте фото документа для анализа через Vision API</b>",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(UserStates.waiting_for_document_photo)
            return
        
        # Анализируем через Vision API
        vision_result = await vision_analyzer.analyze_document_text(image_data)
        
        if vision_result.get("success"):
            # Парсим результат
            document_type = data.get('document_type', 'СТС')
            manual_data = registration_ai._parse_document_text_manually(
                vision_result.get("extracted_text", ""),
                document_type
            )
            
            # Форматируем данные
            registration_data = registration_ai._format_registration_data(manual_data)
            registration_data["success"] = True
            registration_data["registration_method"] = "vision_api_fallback"
            registration_data["requires_manual_check"] = True
            
            await state.update_data(
                registration_result=registration_data,
                document_analysis={"extracted_text": vision_result.get("extracted_text", "")[:1000]}
            )
            
            info_text = "👁️ <b>Анализ через Vision API завершен</b>\n\n"
            info_text += f"<b>Качество:</b> Найдено {vision_result.get('total_blocks', 0)} блоков текста\n\n"
            
            if registration_data.get("vin"):
                info_text += f"<b>VIN:</b> {registration_data['vin']}\n"
            if registration_data.get("registration_number"):
                info_text += f"<b>Госномер:</b> {registration_data['registration_number']}\n"
            if registration_data.get("model"):
                info_text += f"<b>Модель:</b> {registration_data['model']}\n"
            if registration_data.get("brand"):
                info_text += f"<b>Марка:</b> {registration_data['brand']}\n"
            
            info_text += "\n⚠️ <b>Требуется ручная проверка данных</b>"
            
            await reply(message, info_text, reply_markup=get_correction_keyboard())
        else:
            await reply(
                message,
                f"❌ <b>Vision API не смог проанализировать документ:</b>\n"
                f"{vision_result.get('error', 'Неизвестная ошибка')}",
                reply_markup=get_cancel_keyboard()
            )
        
        return
    
    if message.text == "✅ Все верно, продолжить" or message.text == "✅ Подтвердить все данные":
        # Переходим к следующему шагу
        data = await state.get_data()
        registration_result = data.get('registration_result', {})
        
        # Проверяем, есть ли имя для техники
        if registration_result.get('name'):
            await reply(
                message,
                f"🏷️ <b>Предлагаемое название техники:</b>\n{registration_result['name']}\n\n"
                "Вы можете оставить это название или ввести свое:",
                reply_markup=types.ReplyKeyboardMarkup(
                    keyboard=[
                        [types.KeyboardButton(text=f"✅ Оставить: {registration_result['name'][:30]}")],
                        [types.KeyboardButton(text="✏️ Ввести другое название")],
                        [types.KeyboardButton(text="❌ Отмена")]
                    ],
                    resize_keyboard=True
                )
            )
        else:
            await reply(
                message,
                "🏷️ <b>Введите название для техники:</b>\n\n"
                "<i>Примеры:</i>\n"
                "• Экскаватор №1\n• КАМАЗ-6520\n• Погрузчик Volvo\n• Синий кран",
                reply_markup=get_cancel_keyboard()
            )
        
        await state.set_state(UserStates.waiting_for_equipment_name)

@dp.message(UserStates.waiting_for_manual_correction)
async def process_manual_correction(message: types.Message, state: FSMContext):
    """Обрабатывает ручные правки данных"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    corrections = message.text
    data = await state.get_data()
    registration_result = data.get('registration_result', {}).copy()
    
    # Парсим правки
    lines = corrections.split('\n')
    for line in lines:
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().lower()
            value = value.strip()
            
            # Сопоставляем ключи
            field_map = {
                'vin': 'vin',
                'госномер': 'registration_number',
                'номер': 'registration_number',
                'марка': 'brand',
                'модель': 'model',
                'год': 'year',
                'цвет': 'color',
                'мощность': 'engine_power',
                'категория': 'category',
                'тип': 'category'
            }
            
            for ru_key, en_key in field_map.items():
                if ru_key in key:
                    # Обработка специальных случаев
                    if en_key == 'year' and value.isdigit():
                        value = int(value)
                    elif en_key == 'engine_power':
                        # Извлекаем число из строки
                        num_match = re.search(r'\d+', value)
                        if num_match:
                            value = int(num_match.group())
                    
                    registration_result[en_key] = value
                    break
    
    await state.update_data(registration_result=registration_result)
    
    # Показываем обновленные данные
    info_text = "✅ <b>Данные обновлены!</b>\n\n"
    
    fields = [
        ("🔢 VIN номер", registration_result.get("vin")),
        ("🚗 Госномер", registration_result.get("registration_number")),
        ("🏷️ Марка", registration_result.get("brand")),
        ("🚜 Модель", registration_result.get("model")),
        ("📅 Год выпуска", registration_result.get("year")),
        ("🎨 Цвет", registration_result.get("color")),
    ]
    
    for label, value in fields:
        if value:
            info_text += f"<b>{label}:</b> {value}\n"
    
    info_text += "\n<b>Продолжить с этими данными?</b>"
    
    await reply(message, info_text, reply_markup=get_confirmation_keyboard())
    await state.set_state(UserStates.waiting_for_document_analysis)

@dp.message(UserStates.waiting_for_field_correction)
async def process_field_correction(message: types.Message, state: FSMContext):
    """Обрабатывает исправление конкретного поля"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    data = await state.get_data()
    registration_result = data.get('registration_result', {}).copy()
    correcting_field = data.get('correcting_field')
    
    if correcting_field == 'vin':
        # Проверяем VIN
        vin = message.text.strip().upper()
        if len(vin) == 17 and re.match(r'^[A-HJ-NPR-Z0-9]{17}$', vin):
            registration_result['vin'] = vin
        else:
            await reply(message, "❌ Неверный формат VIN. Должно быть 17 символов (буквы и цифры)")
            return
    
    elif correcting_field == 'registration_number':
        registration_result['registration_number'] = message.text.strip().upper()
    
    elif correcting_field == 'brand_model':
        text = message.text.strip()
        lines = text.split('\n')
        for line in lines:
            if 'марка:' in line.lower():
                registration_result['brand'] = line.split(':', 1)[1].strip()
            elif 'модель:' in line.lower():
                registration_result['model'] = line.split(':', 1)[1].strip()
    
    elif correcting_field == 'year':
        year = message.text.strip()
        if year.isdigit() and len(year) == 4:
            year_int = int(year)
            if 1950 <= year_int <= datetime.now().year + 1:
                registration_result['year'] = year_int
            else:
                await reply(message, f"❌ Год должен быть между 1950 и {datetime.now().year + 1}")
                return
        else:
            await reply(message, "❌ Введите 4 цифры года")
            return
    
    await state.update_data(registration_result=registration_result)
    
    # Показываем обновленные данные
    await reply(
        message,
        f"✅ <b>Поле исправлено!</b>\n\n"
        f"Продолжайте исправлять другие поля или подтвердите все данные.",
        reply_markup=get_correction_keyboard()
    )
    await state.set_state(UserStates.waiting_for_document_analysis)

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
    
    # Если пользователь выбрал "Оставить предложенное название"
    if equipment_name.startswith("✅ Оставить: "):
        equipment_name = equipment_name.replace("✅ Оставить: ", "")
    
    await state.update_data(equipment_name=equipment_name)
    
    await reply(
        message,
        "⏱️ <b>Введите текущие моточасы техники:</b>\n\n"
        "<i>Примеры:</i>\n"
        "• 1250 моточасов\n• 2500\n• 500 (новый)\n\n"
        "<b>Введите число:</b>",
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
        # Извлекаем число из текста
        numbers = re.findall(r'\d+', message.text)
        if numbers:
            motohours = int(numbers[0])
        else:
            motohours = int(message.text)
        
        if motohours < 0 or motohours > 100000:
            await reply(message, "❌ Введите разумное количество моточасов (0-100000)")
            return
        
        await state.update_data(motohours=motohours)
        
        await reply(
            message,
            "🛠️ <b>Введите информацию о последнем ТО:</b>\n\n"
            "<i>Примеры:</i>\n"
            "• Замена масла и фильтров 01.12.2023\n"
            "• Полное ТО 1500 моточасов\n"
            "• Новый, ТО не проводилось\n"
            "• Ремонт гидравлики в ноябре\n\n"
            "<b>Опишите последнее обслуживание:</b>",
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
    registration_result = data.get('registration_result', {})
    equipment_name = data.get('equipment_name')
    motohours = data.get('motohours', 0)
    
    # Формируем данные для регистрации
    vin = registration_result.get('vin')
    if not vin or vin.startswith('TEMP_'):
        # Генерируем временный VIN
        vin = f"TEMP_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Добавляем технику в базу
    equipment_id = await db.add_equipment(
        name=equipment_name,
        model=registration_result.get('model', 'Неизвестная модель'),
        vin=vin,
        org_id=user['organization_id'],
        registration_number=registration_result.get('registration_number', 'Без номера'),
        fuel_type='diesel',
        fuel_capacity=300
    )
    
    if equipment_id:
        # Обновляем дополнительные данные
        update_data = {'odometer': motohours}
        
        if registration_result.get('year'):
            update_data['year'] = registration_result['year']
        if registration_result.get('color'):
            update_data['color'] = registration_result['color']
        if registration_result.get('engine_power'):
            update_data['engine_power'] = registration_result['engine_power']
        if registration_result.get('category'):
            update_data['category'] = registration_result['category']
        
        await db.update_equipment(equipment_id, **update_data)
        
        # Сохраняем информацию о последнем ТО
        await db.add_maintenance(
            equipment_id=equipment_id,
            type="Регистрация",
            scheduled_date=datetime.now().strftime('%Y-%m-%d'),
            description=f"Регистрация техники. Последнее ТО: {last_service}"
        )
        
        # Сохраняем анализ документа
        document_analysis = data.get('document_analysis', {})
        if document_analysis:
            await db.save_document_analysis({
                "equipment_id": equipment_id,
                "document_type": data.get('document_type', 'СТС'),
                "analysis_data": registration_result,
                "analysis_quality": registration_result.get('analysis_quality', 'unknown'),
                "motohours": motohours,
                "last_service": last_service,
                "registration_date": datetime.now().strftime('%Y-%m-%d')
            })
        
        # Отправляем сообщение об успехе
        success_text = f"✅ <b>Техника успешно зарегистрирована!</b>\n\n"
        success_text += f"<b>ID техники:</b> {equipment_id}\n"
        success_text += f"<b>Название:</b> {equipment_name}\n"
        success_text += f"<b>Модель:</b> {registration_result.get('model', 'Неизвестно')}\n"
        success_text += f"<b>Марка:</b> {registration_result.get('brand', 'Неизвестно')}\n"
        success_text += f"<b>VIN:</b> {vin}\n"
        success_text += f"<b>Госномер:</b> {registration_result.get('registration_number', 'Без номера')}\n"
        
        if registration_result.get('year'):
            success_text += f"<b>Год выпуска:</b> {registration_result['year']}\n"
        
        success_text += f"<b>Моточасы:</b> {motohours}\n"
        success_text += f"<b>Последнее ТО:</b> {last_service}\n\n"
        
        quality = registration_result.get('analysis_quality', 'unknown')
        if quality == 'high':
            success_text += "🟢 <b>Высокое качество анализа</b> - данные проверены\n"
        elif quality == 'medium':
            success_text += "🟡 <b>Среднее качество</b> - рекомендуется проверить\n"
        elif quality == 'low':
            success_text += "🔴 <b>Низкое качество</b> - требуется проверка данных\n"
        
        success_text += "\n🚜 <b>Техника добавлена в ваш автопарк!</b>"
        
        await reply(message, success_text)
        
        # Очищаем состояние и возвращаем в главное меню
        await state.clear()
        await reply(
            message,
            "Возврат в главное меню",
            reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
        )
        
    else:
        await reply(
            message,
            "❌ Ошибка при сохранении техники в базу данных.",
            reply_markup=get_main_keyboard(user['role'], user.get('organization_id'))
        )
        await state.clear()

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
        "Отправьте фото документа, и я извлеку из него всю информацию.\n\n"
        "<b>Поддерживаемые документы:</b>\n"
        "• СТС (Свидетельство о регистрации)\n"
        "• ПТС (Паспорт транспортного средства)\n"
        "• ПСМ (Паспорт самоходной машины)\n\n"
        "<b>Отправьте фото документа:</b>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_document_photo)
    await state.update_data(manual_analysis=True)

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
    status_text += f"<b>URL:</b> <code>{config['function_url']}</code>\n"
    status_text += f"<b>Таймаут:</b> {config['timeout']} сек\n"
    status_text += f"<b>Повторные попытки:</b> {config['max_retries']}\n\n"
    
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

@dp.message(F.text == "⚙️ Настройки ИИ")
async def ai_settings(message: types.Message):
    """Показывает настройки ИИ (админ)"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    text = "⚙️ <b>Настройки ИИ-модулей</b>\n\n"
    
    for module_name, config in AI_CONFIG.items():
        if module_name == AIModule.DOCUMENT_ANALYSIS:
            status = "✅ ВКЛ" if config['enabled'] else "❌ ВЫКЛ"
            has_url = "✅" if config.get('function_url') else "❌"
            
            text += f"<b>📄 Анализ документов (Cloud Function):</b>\n"
            text += f"Статус: {status}\n"
            text += f"URL: {has_url}\n"
            text += f"Таймаут: {config.get('timeout', 60)}с\n"
            text += f"Повторы: {config.get('max_retries', 3)}\n\n"
        else:
            status = "✅ ВКЛ" if config['enabled'] else "❌ ВЫКЛ"
            has_key = "✅" if config.get('api_key') else "❌"
            
            text += f"<b>{module_name.value}:</b>\n"
            text += f"Статус: {status}\n"
            text += f"API ключ: {has_key}\n\n"
    
    text += f"<b>Vision API:</b> {'✅ ВКЛ' if VISION_ENABLED else '❌ ВЫКЛ'}\n"
    text += f"<b>API ключ Vision:</b> {'✅' if VISION_API_KEY else '❌'}\n"
    text += f"<b>Folder ID Vision:</b> {'✅' if VISION_FOLDER_ID else '❌'}\n\n"
    
    text += f"<b>Всего промптов:</b> {len(PROMPTS)}\n"
    text += f"<b>Доступные промпты:</b> {', '.join(PROMPTS.keys())}\n\n"
    
    text += "<i>Для изменения настроек отредактируйте .env файл</i>"
    
    await reply(message, text)

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
        
        # Проверяем настройки ИИ
        logger.info("🚀 Бот запущен!")
        logger.info(f"🤖 Анализ документов: {'✅ ВКЛ' if AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['enabled'] else '❌ ВЫКЛ'}")
        logger.info(f"👑 Администратор: ID {ADMIN_ID}")
        logger.info(f"📝 Загружено промптов: {len(PROMPTS)}")
        
        # Проверяем конфигурацию Cloud Function
        cf_config = AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]
        if cf_config['enabled'] and cf_config['function_url']:
            logger.info(f"🔗 Cloud Function URL: {cf_config['function_url']}")
        else:
            logger.warning("⚠️ Cloud Function не настроена или отключена")
        
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
