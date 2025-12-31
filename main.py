import os
import logging
import asyncio
import json
import base64
import aiohttp
from datetime import datetime
from typing import Dict, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

from database import db

# ========== НАСТРОЙКА ==========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Настройки Yandex Cloud
YANDEX_API_KEY = os.getenv('YANDEX_API_KEY', '')
YANDEX_FOLDER_ID = os.getenv('YANDEX_FOLDER_ID', '')

# Проверяем настройки
if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
    logger.warning("⚠️ Yandex Cloud API ключи не настроены!")

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
    waiting_for_document_photo = State()
    waiting_for_document_analysis = State()
    waiting_for_motohours = State()
    waiting_for_last_service = State()
    waiting_for_equipment_type = State()
    waiting_for_equipment_name = State()

# ========== YANDEX VISION API ==========
async def recognize_text_from_image(image_bytes: bytes) -> str:
    """Распознает текст с изображения через Yandex Vision API"""
    try:
        if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
            return "Ошибка: не настроены API ключи"
        
        # Кодируем изображение в base64
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        
        url = "https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze"
        
        headers = {
            "Authorization": f"Api-Key {YANDEX_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "folderId": YANDEX_FOLDER_ID,
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
                    
                    # Извлекаем текст из сложной структуры ответа
                    extracted_text = ""
                    
                    try:
                        # Первый уровень
                        results = result.get('results', [])
                        for res in results:
                            # Второй уровень
                            sub_results = res.get('results', [])
                            for sub_res in sub_results:
                                text_detection = sub_res.get('textDetection', {})
                                pages = text_detection.get('pages', [])
                                
                                for page in pages:
                                    blocks = page.get('blocks', [])
                                    for block in blocks:
                                        lines = block.get('lines', [])
                                        for line in lines:
                                            words = line.get('words', [])
                                            line_text = ' '.join([word.get('text', '') for word in words])
                                            extracted_text += line_text + '\n'
                    
                    except Exception as e:
                        logger.error(f"Ошибка парсинга ответа Vision API: {e}")
                        # Пробуем альтернативный путь
                        extracted_text = str(result)
                    
                    if extracted_text.strip():
                        return extracted_text.strip()
                    else:
                        return "Не удалось распознать текст на изображении"
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка Vision API: {response.status} - {error_text}")
                    return f"Ошибка Vision API: {response.status}"
                    
    except Exception as e:
        logger.error(f"Ошибка в recognize_text_from_image: {e}")
        return f"Ошибка: {str(e)}"

# ========== YANDEX GPT API ==========
async def ask_yandex_gpt(prompt: str, context: str = "") -> str:
    """Запрашивает ответ у Yandex GPT"""
    try:
        if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
            return "Ошибка: не настроены API ключи Yandex Cloud"
        
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        
        headers = {
            "Authorization": f"Api-Key {YANDEX_API_KEY}",
            "x-folder-id": YANDEX_FOLDER_ID,
            "Content-Type": "application/json"
        }
        
        # Системный промт для анализа документов о технике
        system_prompt = """Ты - специалист по анализу документов на спецтехнику.
Твоя задача - извлекать информацию из текстов документов (СТС, ПТС, технических паспортов).
Отвечай ТОЛЬКО в формате JSON со следующими полями:
{
  "model": "модель техники",
  "brand": "марка/производитель", 
  "vin": "VIN номер",
  "registration_number": "государственный номер",
  "year": "год выпуска",
  "category": "тип техники",
  "engine_power": "мощность двигателя",
  "color": "цвет"
}
Если поле не найдено, поставь "неизвестно"."""
        
        data = {
            "modelUri": f"gpt://{YANDEX_FOLDER_ID}/yandexgpt-lite",
            "completionOptions": {
                "stream": False,
                "temperature": 0.1,
                "maxTokens": 1000
            },
            "messages": [
                {
                    "role": "system",
                    "text": system_prompt
                },
                {
                    "role": "user", 
                    "text": f"{context}\n\nПроанализируй этот текст документа и верни JSON:\n\n{prompt}"
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
                    logger.error(f"Ошибка Yandex GPT: {response.status} - {error_text}")
                    return f"Ошибка GPT API: {response.status}"
                    
    except Exception as e:
        logger.error(f"Ошибка в ask_yandex_gpt: {e}")
        return f"Ошибка: {str(e)}"

# ========== ОБРАБОТКА ДОКУМЕНТА ==========
async def process_document_with_ai(image_bytes: bytes) -> Dict[str, Any]:
    """Полный процесс обработки документа: Vision -> GPT -> JSON"""
    try:
        logger.info("🔄 Начинаю обработку документа...")
        
        # Шаг 1: Распознаем текст с изображения
        logger.info("🔍 Распознаю текст с изображения...")
        extracted_text = await recognize_text_from_image(image_bytes)
        
        if "Ошибка" in extracted_text or "Не удалось" in extracted_text:
            logger.error(f"Ошибка распознавания: {extracted_text}")
            return {
                "success": False,
                "error": extracted_text,
                "extracted_text": ""
            }
        
        logger.info(f"✅ Распознано символов: {len(extracted_text)}")
        
        # Если текст слишком короткий, вероятно ошибка
        if len(extracted_text) < 50:
            logger.warning(f"Текст слишком короткий: {len(extracted_text)} символов")
            return {
                "success": False,
                "error": "Текст на изображении слишком короткий или не распознан",
                "extracted_text": extracted_text
            }
        
        # Шаг 2: Анализируем текст через GPT
        logger.info("🤖 Анализирую текст через GPT...")
        
        # Создаем промт для GPT
        gpt_prompt = f"""
        Проанализируй следующий текст, извлеченный из документа на технику (СТС/ПТС).
        Найди и извлеки информацию:
        
        {extracted_text[:2000]}  # Ограничиваем длину
        
        Важно найти:
        1. VIN номер (17 символов)
        2. Модель техники
        3. Марку/бренд
        4. Госномер (например, А123БВ77)
        5. Год выпуска
        6. Тип техники (экскаватор, погрузчик и т.д.)
        7. Мощность двигателя
        8. Цвет
        """
        
        gpt_response = await ask_yandex_gpt(gpt_prompt)
        
        # Шаг 3: Парсим ответ GPT
        logger.info("📊 Парсинг ответа GPT...")
        
        # Пытаемся извлечь JSON из ответа
        import re
        json_match = re.search(r'\{.*\}', gpt_response, re.DOTALL)
        
        if json_match:
            try:
                json_str = json_match.group(0)
                ai_analysis = json.loads(json_str)
                logger.info("✅ JSON успешно распарсен")
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга JSON: {e}")
                logger.error(f"Ответ GPT: {gpt_response}")
                ai_analysis = {
                    "model": "Не распознано",
                    "brand": "Не распознано", 
                    "vin": "Не распознано",
                    "registration_number": "Не распознано",
                    "year": "Не распознано",
                    "category": "Не распознано",
                    "engine_power": "Не распознано",
                    "color": "Не распознано",
                    "raw_response": gpt_response
                }
        else:
            logger.warning("JSON не найден в ответе GPT")
            # Ручной поиск ключевых данных
            ai_analysis = extract_info_manually(extracted_text, gpt_response)
        
        return {
            "success": True,
            "extracted_text": extracted_text,
            "ai_analysis": ai_analysis,
            "gpt_response": gpt_response
        }
        
    except Exception as e:
        logger.error(f"Критическая ошибка в process_document_with_ai: {e}")
        return {
            "success": False,
            "error": f"Критическая ошибка: {str(e)}",
            "extracted_text": ""
        }

def extract_info_manually(text: str, gpt_response: str) -> Dict[str, str]:
    """Ручное извлечение информации если GPT не вернул JSON"""
    import re
    
    info = {
        "model": "Не распознано",
        "brand": "Не распознано", 
        "vin": "Не распознано",
        "registration_number": "Не распознано",
        "year": "Не распознано",
        "category": "Не распознано",
        "engine_power": "Не распознано",
        "color": "Не распознано",
        "raw_response": gpt_response
    }
    
    # Ищем VIN (17 символов, буквы и цифры)
    vin_pattern = r'[A-HJ-NPR-Z0-9]{17}'
    vin_match = re.search(vin_pattern, text.upper())
    if vin_match:
        info['vin'] = vin_match.group(0)
    
    # Ищем госномер (русские буквы, цифры)
    plate_pattern = r'[АВЕКМНОРСТУХ]{1}\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}'
    plate_match = re.search(plate_pattern, text.upper())
    if plate_match:
        info['registration_number'] = plate_match.group(0)
    
    # Ищем год (4 цифры)
    year_pattern = r'\b(19[0-9]{2}|20[0-2][0-9])\b'
    year_match = re.search(year_pattern, text)
    if year_match:
        info['year'] = year_match.group(0)
    
    # Ключевые слова для определения типа техники
    tech_keywords = {
        "экскаватор": ["экскаватор", "excavator"],
        "погрузчик": ["погрузчик", "loader", "frontloader"],
        "бульдозер": ["бульдозер", "bulldozer"],
        "самосвал": ["самосвал", "dumper", "dump truck"],
        "кран": ["кран", "crane"],
        "грейдер": ["грейдер", "grader"],
        "каток": ["каток", "roller"]
    }
    
    text_lower = text.lower()
    for tech_type, keywords in tech_keywords.items():
        for keyword in keywords:
            if keyword in text_lower:
                info['category'] = tech_type
                break
        if info['category'] != "Не распознано":
            break
    
    return info

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
    """Упрощенная клавиатура"""
    if role == 'director' and has_organization:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="🏢 Моя организация")],
                [types.KeyboardButton(text="🚜 Автопарк")],
                [types.KeyboardButton(text="👥 Сотрудники")],
                [types.KeyboardButton(text="📷 Зарегистрировать технику")],
            ],
            resize_keyboard=True
        )
    elif role == 'director' and not has_organization:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="🏢 Создать организацию")],
            ],
            resize_keyboard=True
        )
    elif role == 'unassigned':
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="ℹ️ Информация о боте")],
            ],
            resize_keyboard=True
        )
    else:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="ℹ️ Информация о боте")],
            ],
            resize_keyboard=True
        )

def get_cancel_keyboard():
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

# ========== КОМАНДА СТАРТ ==========
@dp.message(Command("start"))
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
    has_org = bool(user.get('organization_id'))
    
    welcome_text = f"👋 Добро пожаловать!\nРоль: {role}\n"
    if has_org:
        org = await db.get_organization(user['organization_id'])
        if org:
            welcome_text += f"Организация: {org['name']}"
    
    await reply(message, welcome_text, reply_markup=get_main_keyboard(role, has_org))

# ========== РЕГИСТРАЦИЯ ТЕХНИКИ ==========
@dp.message(F.text == "📷 Зарегистрировать технику")
async def register_equipment_with_photo(message: types.Message, state: FSMContext):
    """Начинает регистрацию техники"""
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
        "Отправьте фото СТС или ПТС техники:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.waiting_for_document_photo)

@dp.message(UserStates.waiting_for_document_photo, F.photo)
async def process_document_photo(message: types.Message, state: FSMContext):
    """Обрабатывает фото документа"""
    try:
        await reply(message, "🔍 <b>Анализирую документ...</b>\n\nПожалуйста, подождите...")
        
        # Скачиваем фото
        photo = message.photo[-1]  # Берем фото наибольшего размера
        file = await bot.get_file(photo.file_id)
        photo_bytes = await bot.download_file(file.file_path)
        
        # Преобразуем в байты
        image_data = await photo_bytes.read()
        
        # Анализируем документ
        logger.info(f"📸 Получено фото: {len(image_data)} байт")
        result = await process_document_with_ai(image_data)
        
        if not result["success"]:
            await reply(
                message,
                f"❌ <b>Ошибка анализа:</b>\n{result.get('error', 'Неизвестная ошибка')}\n\n"
                "Попробуйте отправить более четкое фото.",
                reply_markup=get_cancel_keyboard()
            )
            return
        
        # Сохраняем результат
        await state.update_data(
            document_analysis=result,
            document_photo_id=photo.file_id
        )
        
        # Показываем результат
        analysis = result["ai_analysis"]
        info_text = "✅ <b>ИИ распознал данные:</b>\n\n"
        
        # Безопасное извлечение данных
        info_text += f"🚜 <b>Модель:</b> {analysis.get('model', 'Не распознано')}\n"
        info_text += f"🏷️ <b>Марка:</b> {analysis.get('brand', 'Не распознано')}\n"
        
        vin = analysis.get('vin', 'Не распознано')
        if vin and vin != "Не распознано" and vin != "null":
            info_text += f"🔢 <b>VIN:</b> {vin}\n"
        
        reg_num = analysis.get('registration_number', 'Не распознано')
        if reg_num and reg_num != "Не распознано" and reg_num != "null":
            info_text += f"🚗 <b>Госномер:</b> {reg_num}\n"
        
        year = analysis.get('year', 'Не распознано')
        if year and year != "Не распознано" and year != "null":
            info_text += f"📅 <b>Год:</b> {year}\n"
        
        category = analysis.get('category', 'Не распознано')
        if category and category != "Не распознано" and category != "null":
            info_text += f"🏗️ <b>Тип:</b> {category}\n"
        
        info_text += "\n<b>Продолжить регистрацию?</b>"
        
        keyboard = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✅ Да, продолжить")],
                [types.KeyboardButton(text="🔄 Отправить другое фото")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
        
        await reply(message, info_text, reply_markup=keyboard)
        await state.set_state(UserStates.waiting_for_document_analysis)
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки фото: {e}")
        await reply(message, f"❌ Ошибка: {str(e)}")

@dp.message(UserStates.waiting_for_document_analysis)
async def process_document_confirmation(message: types.Message, state: FSMContext):
    """Обрабатывает подтверждение"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Регистрация отменена",
                   reply_markup=get_main_keyboard(user['role'], user.get('organization_id')))
        return
    
    if message.text == "🔄 Отправить другое фото":
        await reply(
            message,
            "🔄 <b>Отправьте новое фото документа</b>",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_document_photo)
        return
    
    if message.text == "✅ Да, продолжить":
        await reply(
            message,
            "📊 <b>Введите текущие моточасы техники:</b>\n"
            "<i>Например: 1250</i>",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.waiting_for_motohours)

# ========== ЗАПУСК БОТА ==========
async def main():
    """Основная функция"""
    try:
        await db.connect()
        logger.info("✅ Бот запущен!")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
