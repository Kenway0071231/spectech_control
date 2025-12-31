import aiohttp
import base64
import json
import logging
from typing import Dict, Any, Optional
import os

logger = logging.getLogger(__name__)

class YandexVisionAnalyzer:
    def __init__(self):
        self.api_key = os.getenv('VISION_API_KEY', os.getenv('YANDEX_API_KEY'))
        self.folder_id = os.getenv('VISION_FOLDER_ID', os.getenv('YANDEX_FOLDER_ID'))
        self.base_url = "https://vision.api.cloud.yandex.net/vision/v1/"
    
    async def analyze_image(self, image_bytes: bytes, feature_type: str = "TEXT_DETECTION") -> Dict[str, Any]:
        """
        Анализирует изображение с помощью Yandex Vision API
        
        feature_type может быть:
        - TEXT_DETECTION: распознавание текста
        - FACE_DETECTION: обнаружение лиц
        - CLASSIFICATION: классификация изображений
        """
        try:
            if not self.api_key or not self.folder_id:
                logger.error("Yandex Vision не настроен: отсутствует API ключ или Folder ID")
                return {"error": "Yandex Vision не настроен"}
            
            # Кодируем изображение в base64
            image_base64 = base64.b64encode(image_bytes).decode('utf-8')
            
            # Формируем запрос
            url = f"{self.base_url}batchAnalyze"
            
            headers = {
                "Authorization": f"Api-Key {self.api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "folderId": self.folder_id,
                "analyzeSpecs": [{
                    "content": image_base64,
                    "features": [{
                        "type": feature_type,
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
                        return self._process_vision_result(result, feature_type)
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка Vision API: {response.status} - {error_text}")
                        return {"error": f"Ошибка API: {response.status}"}
                        
        except Exception as e:
            logger.error(f"Ошибка анализа изображения: {e}")
            return {"error": str(e)}
    
    def _process_vision_result(self, result: Dict, feature_type: str) -> Dict:
        """Обрабатывает результат Vision API"""
        if feature_type == "TEXT_DETECTION":
            return self._extract_text(result)
        else:
            return result
    
    def _extract_text(self, result: Dict) -> Dict:
        """Извлекает текст из результата Vision API"""
        try:
            extracted_text = ""
            
            # Проходим по всем уровням вложенности результата
            for result_item in result.get('results', []):
                for analysis_result in result_item.get('results', []):
                    text_detection = analysis_result.get('textDetection', {})
                    
                    for page in text_detection.get('pages', []):
                        for block in page.get('blocks', []):
                            for line in block.get('lines', []):
                                line_text = ""
                                for word in line.get('words', []):
                                    line_text += word.get('text', '') + ' '
                                extracted_text += line_text.strip() + '\n'
            
            return {
                "success": True,
                "extracted_text": extracted_text.strip(),
                "raw_result": result  # Можно убрать для экономии места
            }
            
        except Exception as e:
            logger.error(f"Ошибка извлечения текста: {e}")
            return {"error": f"Ошибка обработки: {e}"}
    
    async def analyze_document(self, image_bytes: bytes) -> Dict[str, Any]:
        """Специальный метод для анализа документов (СТС/ПТС)"""
        result = await self.analyze_image(image_bytes, "TEXT_DETECTION")
        
        if "extracted_text" in result:
            # Очищаем текст от мусора
            text = result["extracted_text"]
            
            # Ищем ключевые поля документа
            document_info = self._parse_document_text(text)
            
            result.update({
                "document_info": document_info,
                "is_document": self._is_likely_document(text)
            })
        
        return result
    
    def _parse_document_text(self, text: str) -> Dict[str, str]:
        """Пытается найти ключевые поля в тексте документа"""
        info = {}
        
        # Поиск VIN (17 символов, буквы и цифры)
        import re
        vin_pattern = r'[A-HJ-NPR-Z0-9]{17}'
        vin_match = re.search(vin_pattern, text.upper())
        if vin_match:
            info['vin'] = vin_match.group(0)
        
        # Поиск госномера (русские буквы, цифры)
        plate_pattern = r'[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}'
        plate_match = re.search(plate_pattern, text.upper())
        if plate_match:
            info['registration_number'] = plate_match.group(0)
        
        # Поиск года
        year_pattern = r'\b(19[0-9]{2}|20[0-2][0-9])\b'
        year_match = re.search(year_pattern, text)
        if year_match:
            info['year'] = year_match.group(0)
        
        # Простые поиски по ключевым словам
        lines = text.split('\n')
        for line in lines:
            if 'МОДЕЛЬ' in line.upper() or 'MODEL' in line.upper():
                parts = line.split(':')
                if len(parts) > 1:
                    info['model'] = parts[1].strip()
            
            if 'МАРКА' in line.upper() or 'BRAND' in line.upper():
                parts = line.split(':')
                if len(parts) > 1:
                    info['brand'] = parts[1].strip()
        
        return info
    
    def _is_likely_document(self, text: str) -> bool:
        """Определяет, похож ли текст на документ"""
        keywords = ['ПТС', 'СТС', 'VIN', 'МОДЕЛЬ', 'ГОС', 'НОМЕР', 'РЕГИСТРАЦИЯ', 'PTS', 'STS']
        text_upper = text.upper()
        
        # Если есть хотя бы 2 ключевых слова
        found_keywords = sum(1 for keyword in keywords if keyword in text_upper)
        return found_keywords >= 2
    
    async def analyze_instrument_panel(self, image_bytes: bytes) -> Dict[str, Any]:
        """Анализирует приборную панель"""
        result = await self.analyze_image(image_bytes, "TEXT_DETECTION")
        
        if "extracted_text" in result:
            text = result["extracted_text"]
            panel_info = self._parse_instrument_panel(text)
            
            result.update({
                "panel_info": panel_info,
                "contains_digits": any(char.isdigit() for char in text)
            })
        
        return result
    
    def _parse_instrument_panel(self, text: str) -> Dict[str, str]:
        """Парсит показания приборной панели"""
        info = {}
        
        # Ищем пробег (одометр)
        import re
        # Паттерны для пробега: цифры с "km", "км", или просто большие числа
        patterns = [
            r'(\d{1,6}[.,]?\d*)\s*(km|км|к\.м\.)',
            r'(Пробег|Одометр|ODO)[:\s]*(\d{1,6}[.,]?\d*)',
            r'\b(\d{4,6})\b'  # Просто 4-6 цифр подряд
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                for match in matches:
                    if isinstance(match, tuple):
                        for item in match:
                            if item and item.isdigit():
                                info['odometer'] = int(item.replace('.', '').replace(',', ''))
                                break
                    elif match.isdigit():
                        info['odometer'] = int(match)
                    if 'odometer' in info:
                        break
            if 'odometer' in info:
                break
        
        return info

# Создаем глобальный экземпляр анализатора
vision_analyzer = YandexVisionAnalyzer()
