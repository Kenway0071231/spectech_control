import os
import logging
import asyncio
import json
import base64
import re
import aiohttp
from datetime import datetime
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
        prompt = prompt.replace("СТС/ПТС/ПСМ/Другое", document_type)
        
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
                logger.warning("Низкое качество распознавания")
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
            
            logger.info(f"Регистрация успешно обработана")
            
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
            logger.info("Используем запасной метод через Vision API")
            
            # 1. Получаем текст через Vision API
            vision_result = await self.vision_analyzer.analyze_document_text(image_bytes)
            
            if not vision_result.get("success"):
                return {
                    "error": vision_result.get("error", "Неизвестная ошибка"),
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
            registration_data["success"] = True
            registration_data["registration_method"] = "vision_api"
            registration_data["requires_manual_check"] = True
            
            return registration_data
            
        except Exception as e:
            logger.error(f"Ошибка запасного метода: {e}")
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
            "DOOSAN": ["DOOSAN"]
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
                    "message": "Все поля заполнены корректно. Техника готова к регистрации."
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
                        "text": "Ты - помощник по регистрации спецтехники. Дай практические рекомендации."
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
                            "message": answer[:500]
                        }
                    
            return {
                "status": "unknown",
                "message": "Не удалось получить рекомендации"
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения рекомендаций: {e}")
            return {
                "status": "error",
                "message": f"Ошибка: {str(e)}"
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
        
        return {
            "name": name.strip(),
            "model": analysis_data.get('model', 'Неизвестно'),
            "brand": analysis_data.get('brand', 'Неизвестно'),
            "vin": vin,
            "registration_number": analysis_data.get('registration_number', 'Без номера'),
            "year": analysis_data.get('year'),
            "category": analysis_data.get('category', 'Спецтехника'),
            "engine_power": analysis_data.get('engine_power'),
            "color": analysis_data.get('color', 'Неизвестно'),
            "notes": f"Зарегистрировано через анализ {analysis_data.get('document_type', 'документа')}",
            "document_type": analysis_data.get('document_type', 'Неизвестно'),
            "analysis_quality": analysis_data.get('analysis_quality', 'unknown')
        }

# ========== СОЗДАЕМ ЭКЗЕМПЛЯРЫ ==========
document_analyzer = DocumentAnalyzer()
vision_analyzer = YandexVisionAnalyzer()
registration_ai = RegistrationAI()

# ========== СОСТОЯНИЯ ==========
class UserStates(StatesGroup):
    waiting_for_document_type = State()
    waiting_for_document_photo = State()
    waiting_for_document_analysis = State()
    waiting_for_registration_confirmation = State()
    waiting_for_equipment_name = State()
    waiting_for_motohours = State()
    waiting_for_last_service = State()
    waiting_for_manual_correction = State()

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
                ],
                resize_keyboard=True
            )
        else:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🚜 Автопарк")],
                    [KeyboardButton(text="👥 Сотрудники")],
                    [KeyboardButton(text="📷 Зарегистрировать технику")],
                    [KeyboardButton(text="📊 Статистика")],
                ],
                resize_keyboard=True
            )
    
    if role == 'fleetmanager':
        if not has_organization:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="ℹ️ Информация о боте")],
                ],
                resize_keyboard=True
            )
        else:
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🚜 Управление парком")],
                    [KeyboardButton(text="📷 Зарегистрировать технику")],
                    [KeyboardButton(text="📊 Статистика")],
                ],
                resize_keyboard=True
            )
    
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="ℹ️ Информация о боте")],
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
            [KeyboardButton(text="📄 СТС")],
            [KeyboardButton(text="📋 ПТС")],
            [KeyboardButton(text="🏭 ПСМ")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def get_confirmation_keyboard():
    """Клавиатура для подтверждения данных"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Все верно")],
            [KeyboardButton(text="✏️ Исправить")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

# ========== КОМАНДА СТАРТ ==========
@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    """Главное меню"""
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
        await reply(message, "❌ Ошибка регистрации")
        return
    
    role = user['role']
    has_organization = bool(user.get('organization_id'))
    
    if role == 'unassigned':
        welcome_text = (
            f"👋 <b>Добро пожаловать в ТехКонтроль!</b>\n\n"
            f"<b>Ваш ID:</b> <code>{message.from_user.id}</code>\n"
            f"<b>Ваше имя:</b> {message.from_user.full_name}\n\n"
            "📋 <b>Для получения доступа:</b>\n"
            "1. Отправьте ваш ID вышестоящему сотруднику\n"
            "2. Администратор назначит вам роль\n"
            "3. После назначения вы получите доступ к функциям"
        )
        
        await reply(message, welcome_text, reply_markup=get_main_keyboard(role, has_organization))
        return
    
    role_names = {
        'botadmin': '👑 Администратор',
        'director': '👨‍💼 Директор',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    welcome_text = f"🤖 <b>ТехКонтроль</b>\n\n"
    welcome_text += f"<b>Роль:</b> {role_names.get(role, 'Пользователь')}\n"
    welcome_text += f"<b>ID:</b> <code>{message.from_user.id}</code>\n"
    
    if has_organization:
        org = await db.get_organization(user['organization_id'])
        if org:
            welcome_text += f"<b>Организация:</b> {org['name']}\n"
    
    await reply(message, welcome_text, reply_markup=get_main_keyboard(role, has_organization))

# ========== РЕГИСТРАЦИЯ ТЕХНИКИ ==========
@dp.message(F.text == "📷 Зарегистрировать технику")
async def start_equipment_registration(message: types.Message, state: FSMContext):
    """Начинает регистрацию техники"""
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
        "🚜 <b>Регистрация новой техники</b>\n\n"
        "📄 <b>Система автоматически извлечет данные из документов:</b>\n"
        "• VIN номер\n• Модель и марка\n• Госномер\n• Год выпуска\n\n"
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
        "📄 СТС": "СТС",
        "📋 ПТС": "ПТС",
        "🏭 ПСМ": "ПСМ"
    }
    
    if message.text not in document_type_map:
        await reply(message, "❌ Выберите тип документа из списка", reply_markup=get_document_type_keyboard())
        return
    
    document_type = document_type_map[message.text]
    await state.update_data(document_type=document_type)
    
    await reply(
        message,
        f"📸 <b>Загрузите фото документа ({document_type})</b>\n\n"
        "<i>Советы:</i>\n"
        "1. Расположите документ ровно\n"
        "2. Убедитесь в хорошем освещении\n"
        "3. Весь документ должен быть виден\n\n"
        "<b>Отправьте фото документа:</b>",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_document_photo)

@dp.message(UserStates.waiting_for_document_photo, F.photo)
async def process_document_photo(message: types.Message, state: FSMContext):
    """Обрабатывает фото документа"""
    try:
        await reply(message, "🔍 <b>Анализирую документ...</b>")
        
        # Скачиваем фото
        photo = message.photo[-1]
        file = await bot.get_file(photo.file_id)
        photo_bytes = await bot.download_file(file.file_path)
        image_data = await photo_bytes.read()
        
        # Получаем тип документа
        data = await state.get_data()
        document_type = data.get('document_type', 'СТС')
        
        # Анализируем документ
        registration_result = await registration_ai.register_equipment_from_document(image_data, document_type)
        
        if not registration_result.get("success", False):
            error_msg = registration_result.get("error", "Неизвестная ошибка")
            await reply(
                message,
                f"❌ <b>Ошибка анализа:</b> {error_msg}\n\n"
                "Попробуйте:\n"
                "1. Сделать более четкое фото\n"
                "2. Улучшить освещение\n"
                "3. Отправить другой документ",
                reply_markup=get_cancel_keyboard()
            )
            return
        
        # Сохраняем результат
        await state.update_data(registration_result=registration_result)
        
        # Формируем сообщение с результатами
        result_data = registration_result
        
        info_text = "✅ <b>Документ проанализирован!</b>\n\n"
        
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
        ]
        
        for label, value in fields:
            if value:
                info_text += f"<b>{label}:</b> {value}\n"
        
        # Качество анализа
        quality = result_data.get("analysis_quality", "unknown")
        quality_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(quality, "⚪")
        info_text += f"\n<b>Качество анализа:</b> {quality_emoji} {quality.upper()}\n"
        
        info_text += "\n<b>Все данные верны?</b>"
        
        await reply(message, info_text, reply_markup=get_confirmation_keyboard())
        await state.set_state(UserStates.waiting_for_document_analysis)
        
    except Exception as e:
        logger.error(f"Ошибка обработки фото: {e}")
        await reply(
            message,
            "❌ Ошибка при обработке фото. Попробуйте еще раз.",
            reply_markup=get_cancel_keyboard()
        )

@dp.message(UserStates.waiting_for_document_analysis)
async def process_document_analysis_confirmation(message: types.Message, state: FSMContext):
    """Обрабатывает подтверждение данных"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    if message.text == "✏️ Исправить":
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
        await state.set_state(UserStates.waiting_for_manual_correction)
        return
    
    if message.text == "✅ Все верно":
        data = await state.get_data()
        registration_result = data.get('registration_result', {})
        
        if registration_result.get('name'):
            await reply(
                message,
                f"🏷️ <b>Предлагаемое название:</b> {registration_result['name']}\n\n"
                "Вы можете оставить это название или ввести свое:",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text=f"✅ Оставить: {registration_result['name'][:30]}")],
                        [KeyboardButton(text="✏️ Ввести другое")],
                        [KeyboardButton(text="❌ Отмена")]
                    ],
                    resize_keyboard=True
                )
            )
        else:
            await reply(
                message,
                "🏷️ <b>Введите название для техники:</b>\n\n"
                "<i>Примеры:</i>\n"
                "• Экскаватор №1\n• КАМАЗ-6520\n• Погрузчик Volvo",
                reply_markup=get_cancel_keyboard()
            )
        
        await state.set_state(UserStates.waiting_for_equipment_name)

@dp.message(UserStates.waiting_for_manual_correction)
async def process_manual_correction(message: types.Message, state: FSMContext):
    """Обрабатывает ручные правки"""
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
                'марка': 'brand',
                'модель': 'model',
                'год': 'year',
                'цвет': 'color',
                'мощность': 'engine_power'
            }
            
            for ru_key, en_key in field_map.items():
                if ru_key in key:
                    if en_key == 'year' and value.isdigit():
                        value = int(value)
                    elif en_key == 'engine_power':
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
    ]
    
    for label, value in fields:
        if value:
            info_text += f"<b>{label}:</b> {value}\n"
    
    info_text += "\n<b>Продолжить?</b>"
    
    await reply(message, info_text, reply_markup=get_confirmation_keyboard())
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
        
        if motohours < 0 or motohours > 100000:
            await reply(message, "❌ Введите разумное количество (0-100000)")
            return
        
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
    registration_result = data.get('registration_result', {})
    equipment_name = data.get('equipment_name')
    motohours = data.get('motohours', 0)
    
    # Формируем данные для регистрации
    vin = registration_result.get('vin')
    if not vin or vin.startswith('TEMP_'):
        vin = f"TEMP_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Добавляем технику в базу
    equipment_id = await db.add_equipment(
        name=equipment_name,
        model=registration_result.get('model', 'Неизвестно'),
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
        success_text += f"<b>VIN:</b> {vin}\n"
        success_text += f"<b>Госномер:</b> {registration_result.get('registration_number', 'Без номера')}\n"
        
        if registration_result.get('year'):
            success_text += f"<b>Год выпуска:</b> {registration_result['year']}\n"
        
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

# ========== КОМАНДА ДЛЯ РУЧНОГО АНАЛИЗА ==========
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

# ========== КОМАНДА ДЛЯ ПРОВЕРКИ СТАТУСА ==========
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
    
    for u in users[:10]:
        role_emoji = {
            'botadmin': '👑',
            'director': '👨‍💼',
            'fleetmanager': '👷',
            'driver': '🚛',
            'unassigned': '❓'
        }.get(u['role'], '❓')
        
        text += f"{role_emoji} <b>{u['full_name']}</b>\n"
        text += f"ID: <code>{u['telegram_id']}</code>\n"
        text += f"Роль: {u['role']}\n\n"
    
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
        text += f"<b>Название:</b> {org['name']}\n\n"
    
    await reply(message, text)

@dp.message(F.text == "⚙️ Настройки ИИ")
async def ai_settings(message: types.Message):
    """Показывает настройки ИИ (админ)"""
    user = await db.get_user(message.from_user.id)
    if user['role'] != 'botadmin':
        await reply(message, "⛔ Доступ только для администратора!")
        return
    
    text = "⚙️ <b>Настройки ИИ</b>\n\n"
    
    text += f"<b>Анализ документов:</b> {'✅ ВКЛ' if AI_CONFIG[AIModule.DOCUMENT_ANALYSIS]['enabled'] else '❌ ВЫКЛ'}\n"
    text += f"<b>Vision API:</b> {'✅ ВКЛ' if VISION_ENABLED else '❌ ВЫКЛ'}\n"
    text += f"<b>Всего промптов:</b> {len(PROMPTS)}\n"
    
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
        
        logger.info("🚀 Бот запущен!")
        
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
