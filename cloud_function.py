import json
import base64
import io
import logging
from PIL import Image

# Настройка логирования
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Обработчик Cloud Function для анализа документов СТС/ПТС
    
    Ожидает JSON с полями:
    - image: base64 изображения
    - prompt: промпт для анализа
    - document_type: тип документа
    
    Возвращает JSON с результатами анализа
    """
    try:
        logger.info("Начало обработки документа")
        
        # Получаем данные из запроса
        if 'body' not in event:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Тело запроса отсутствует'})
            }
        
        body = json.loads(event['body'])
        
        image_base64 = body.get('image', '')
        prompt = body.get('prompt', '')
        document_type = body.get('document_type', 'СТС')
        
        logger.info(f"Тип документа: {document_type}")
        
        if not image_base64:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Отсутствует изображение'})
            }
        
        if not prompt:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Отсутствует промпт'})
            }
        
        # Декодируем изображение
        try:
            image_data = base64.b64decode(image_base64)
            image = Image.open(io.BytesIO(image_data))
            
            # Получаем информацию об изображении
            image_info = {
                'format': image.format,
                'size': image.size,
                'mode': image.mode,
                'size_kb': len(image_data) / 1024
            }
            
            logger.info(f"Изображение: {image_info}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки изображения: {e}")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Ошибка обработки изображения: {str(e)}'})
            }
        
        # Здесь должна быть интеграция с ИИ-моделью для анализа документа
        # В реальной реализации здесь будет вызов:
        # 1. Vision API для распознавания текста
        # 2. GPT для структурирования данных
        # 3. Валидация и очистка результатов
        
        # Временный ответ для тестирования
        result = {
            'document_type': document_type,
            'vin': 'X9F12345678901234',
            'registration_number': 'А123ВС77',
            'model': 'Камаз-6520',
            'brand': 'Камаз',
            'year': 2022,
            'category': 'Грузовой автомобиль',
            'engine_power': 400,
            'engine_volume': 11900,
            'color': 'Красный',
            'weight': 12000,
            'max_weight': 25000,
            'owner': 'ООО "Грузоперевозки"',
            'passport_number': '77НН123456',
            'registration_date': '15.05.2022',
            'engine_number': '1234567890',
            'chassis_number': 'CH12345678901234',
            'body_number': 'BD12345678901234',
            'environmental_class': 'Евро-5',
            'extracted_text': 'СТС 77НН123456\nVIN: X9F12345678901234\nМарка: Камаз\nМодель: 6520\nГод выпуска: 2022\nЦвет: Красный\nМощность: 400 л.с.',
            'image_info': image_info,
            'processing_time': 0.5,
            'success': True
        }
        
        logger.info("Анализ завершен успешно")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'result': result})
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка JSON: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Неверный формат JSON'})
        }
        
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Внутренняя ошибка: {str(e)}'})
        }
