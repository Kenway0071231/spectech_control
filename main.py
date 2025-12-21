import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ContentType, ReplyKeyboardRemove
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

from database import db

# ========== НАСТРОЙКА ==========
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Оптимизация для быстрых ответов
bot = Bot(
    token=os.getenv('BOT_TOKEN'),
    default=DefaultBotProperties(parse_mode="HTML")
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== СОСТОЯНИЯ ==========
class ShiftStates(StatesGroup):
    choosing_equipment = State()
    safety_instruction = State()
    pre_inspection = State()
    waiting_for_photos = State()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

async def typing_action(chat_id):
    """Показываем "печатает..." для быстрого отклика"""
    try:
        await bot.send_chat_action(chat_id, "typing")
        await asyncio.sleep(0.1)  # Короткая пауза
    except:
        pass

async def quick_reply(message: types.Message, text: str, **kwargs):
    """Быстрый ответ пользователю"""
    await typing_action(message.chat.id)
    return await message.answer(text, **kwargs)

def get_main_keyboard(user_id, has_active_shift, is_admin=False):
    """Генерация основной клавиатуры"""
    if has_active_shift:
        buttons = [
            [types.KeyboardButton(text="⏹️ Завершить смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="📸 Мои фото")]
        ]
    else:
        buttons = [
            [types.KeyboardButton(text="🚛 Начать смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="ℹ️ Информация")]
        ]
    
    if is_admin:
        buttons.append([types.KeyboardButton(text="👨‍💼 Админ")])
    
    return types.ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

# ========== ОСНОВНЫЕ ОБРАБОТЧИКИ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Быстрый старт"""
    await typing_action(message.chat.id)
    
    # Регистрация
    user_id = message.from_user.id
    await db.register_driver(
        user_id,
        f"{message.from_user.first_name} {message.from_user.last_name or ''}"
    )
    
    # Проверка состояния
    active_shift = await db.get_active_shift(user_id)
    user_role = await db.get_user_role(user_id)
    
    # Быстрый ответ
    await quick_reply(
        message,
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        f"Я бот для контроля спецтехники.\n"
        f"Статус: {'🟢 На смене' if active_shift else '⚪ Свободен'}\n"
        f"Роль: {user_role}",
        reply_markup=get_main_keyboard(user_id, bool(active_shift), user_role == 'admin')
    )

@dp.message(F.text == "🚛 Начать смену")
async def start_shift_process(message: types.Message, state: FSMContext):
    """Начинаем смену с быстрыми ответами"""
    await typing_action(message.chat.id)
    
    equipment = await db.get_equipment_list()
    if not equipment:
        await quick_reply(message, "❌ Нет доступной техники. Обратитесь к администратору.")
        return
    
    # Быстрое меню выбора
    keyboard = []
    for eq in equipment[:5]:  # Ограничиваем 5 элементами для скорости
        eq_id, name, model = eq
        keyboard.append([types.KeyboardButton(text=f"🚜 {name}")])
    
    keyboard.append([types.KeyboardButton(text="❌ Отмена")])
    
    await quick_reply(
        message,
        "🚛 <b>Выберите технику:</b>\n\n"
        "Нажмите на нужную технику ниже:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=keyboard,
            resize_keyboard=True
        )
    )
    
    await state.update_data(equipment_list=equipment)
    await state.set_state(ShiftStates.choosing_equipment)

@dp.message(ShiftStates.choosing_equipment)
async def process_equipment_choice(message: types.Message, state: FSMContext):
    """Обработка выбора техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    data = await state.get_data()
    equipment_list = data.get('equipment_list', [])
    
    # Быстрый поиск
    selected_eq = None
    search_text = message.text.replace("🚜 ", "")
    
    for eq in equipment_list:
        eq_id, name, model = eq
        if search_text in name or name in search_text:
            selected_eq = eq
            break
    
    if not selected_eq:
        await quick_reply(message, "⚠️ Выберите технику из списка ниже.")
        return
    
    eq_id, name, model = selected_eq
    await state.update_data(selected_equipment=selected_eq)
    
    # Быстрая инструкция
    await quick_reply(
        message,
        f"📋 <b>Инструктаж по безопасности</b>\n\n"
        f"<b>Техника:</b> {name} ({model})\n\n"
        "Основные правила:\n"
        "1. Проверьте средства пожаротушения\n"
        "2. Убедитесь в исправности ремней безопасности\n"
        "3. Проверьте сигналы и огни\n"
        "4. Осмотрите на утечки\n"
        "5. Проверьте давление в шинах\n\n"
        "Подтвердите ознакомление:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✅ Подтверждаю")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    
    await state.set_state(ShiftStates.safety_instruction)

@dp.message(ShiftStates.safety_instruction)
async def process_safety_instruction(message: types.Message, state: FSMContext):
    """Быстрое подтверждение инструктажа"""
    if message.text == "❌ Отмена":
        await state.clear()
        await cmd_start(message)
        return
    
    if message.text != "✅ Подтверждаю":
        await quick_reply(message, "⚠️ Нажмите '✅ Подтверждаю' для продолжения.")
        return
    
    # Переходим к осмотру
    await quick_reply(
        message,
        "🔍 <b>Предсменный осмотр</b>\n\n"
        "Проверьте основные узлы:\n"
        "• Уровень масла и жидкости\n"
        "• Гидравлические шланги\n"
        "• Работу приборов\n\n"
        "Вы можете добавить фото или продолжить без фото:",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="📷 Сделать фото")],
                [types.KeyboardButton(text="⏭️ Без фото")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    
    await state.update_data(inspection_photos=[])
    await state.set_state(ShiftStates.pre_inspection)

@dp.message(ShiftStates.pre_inspection, F.text == "📷 Сделать фото")
async def request_photos(message: types.Message, state: FSMContext):
    """Запрос фотографий"""
    await quick_reply(
        message,
        "📸 <b>Отправьте фотографии</b>\n\n"
        "Можно отправить несколько фото сразу.\n"
        "После отправки нажмите '✅ Готово'.",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✅ Готово")],
                [types.KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(ShiftStates.waiting_for_photos)

@dp.message(ShiftStates.pre_inspection, F.text == "⏭️ Без фото")
async def skip_photos(message: types.Message, state: FSMContext):
    """Пропуск фото"""
    await complete_shift(message, state, photos=[])

# ========== ОБРАБОТКА ФОТО (ОПТИМИЗИРОВАННАЯ) ==========

@dp.message(ShiftStates.waiting_for_photos, F.content_type == ContentType.PHOTO)
async def handle_photo_fast(message: types.Message, state: FSMContext):
    """Быстрая обработка фото"""
    try:
        # Быстрый отклик
        await message.reply("🔄 Получаю фото...")
        
        # Берем фото среднего качества для скорости
        photo_idx = min(1, len(message.photo) - 1)  # Второе фото или первое
        photo = message.photo[photo_idx]
        
        # Обновляем данные
        data = await state.get_data()
        photos = data.get('inspection_photos', [])
        photos.append(photo.file_id)
        await state.update_data(inspection_photos=photos)
        
        # Быстрый ответ
        await quick_reply(
            message,
            f"✅ Фото #{len(photos)} получено!\n"
            f"Всего фото: {len(photos)}\n\n"
            f"Можете отправить ещё или нажать '✅ Готово'."
        )
        
    except Exception as e:
        logger.error(f"Ошибка обработки фото: {e}")
        await quick_reply(message, "⚠️ Не удалось обработать фото. Попробуйте ещё раз.")

@dp.message(ShiftStates.waiting_for_photos, F.text == "✅ Готово")
async def finish_with_photos(message: types.Message, state: FSMContext):
    """Завершение с фото"""
    data = await state.get_data()
    photos = data.get('inspection_photos', [])
    
    if not photos:
        await quick_reply(message, "❌ Вы не отправили фото. Попробуйте снова.")
        return
    
    await message.reply(f"📊 Обрабатываю {len(photos)} фото...")
    await complete_shift(message, state, photos)

async def complete_shift(message: types.Message, state: FSMContext, photos=None):
    """Завершение процесса начала смены"""
    data = await state.get_data()
    selected_eq = data.get('selected_equipment')
    
    if not selected_eq:
        await quick_reply(message, "❌ Ошибка: данные не найдены.")
        await state.clear()
        return
    
    eq_id, name, model = selected_eq
    
    try:
        # Быстрый старт смены в БД
        shift_id = await db.start_shift(message.from_user.id, eq_id)
        
        # Сохраняем фото (если есть)
        if photos:
            await db.add_inspection_with_photos(shift_id, photos, f"Осмотр {name}")
        
        await quick_reply(
            message,
            f"🎉 <b>Смена начата!</b>\n\n"
            f"<b>Техника:</b> {name}\n"
            f"<b>ID смены:</b> {shift_id}\n"
            f"<b>Время:</b> {message.date.strftime('%H:%M')}\n"
            f"<b>Фото:</b> {len(photos) if photos else 0} шт.\n\n"
            f"Удачной работы! 🚀"
        )
        
    except Exception as e:
        logger.error(f"Ошибка начала смены: {e}")
        await quick_reply(message, "❌ Ошибка при начале смены. Попробуйте ещё раз.")
    
    # Возвращаем меню
    await state.clear()
    await cmd_start(message)

# ========== ДРУГИЕ КОМАНДЫ ==========

@dp.message(F.text == "⏹️ Завершить смену")
async def end_shift_fast(message: types.Message):
    """Быстрое завершение смены"""
    active_shift = await db.get_active_shift(message.from_user.id)
    
    if not active_shift:
        await quick_reply(message, "❌ У вас нет активной смены.")
        return
    
    shift_id, equipment_id = active_shift
    await db.end_shift(shift_id)
    
    await quick_reply(
        message,
        f"✅ <b>Смена завершена!</b>\n\n"
        f"ID смены: {shift_id}\n"
        f"Время: {message.date.strftime('%H:%M')}\n\n"
        f"Спасибо за работу! 👷"
    )
    
    await cmd_start(message)

@dp.message(F.text == "📋 Мои смены")
async def show_shifts_fast(message: types.Message):
    """Быстрая история смен"""
    await quick_reply(
        message,
        "📊 <b>Статистика</b>\n\n"
        "Этот раздел в разработке.\n"
        "Скоро здесь появится история смен.\n\n"
        "А пока можете начать новую смену! 🚛"
    )

@dp.message(F.text == "📸 Мои фото")
async def show_photos_fast(message: types.Message):
    """Быстрый просмотр фото"""
    await quick_reply(
        message,
        "📷 <b>Фотографии</b>\n\n"
        "Фото из ваших осмотров будут отображаться здесь.\n"
        "Пока фото нет — начните смену с фото! 📸"
    )

@dp.message(F.text == "ℹ️ Информация")
async def show_info_fast(message: types.Message):
    """Быстрая информация"""
    await quick_reply(
        message,
        "🤖 <b>ТехКонтроль v2.0</b>\n\n"
        "Оптимизированная версия бота.\n\n"
        "<b>Функции:</b>\n"
        "✅ Быстрый старт смены\n"
        "✅ Инструктаж по безопасности\n"
        "✅ Осмотр с фото\n"
        "✅ Завершение смены\n\n"
        "<b>Скоро:</b>\n"
        "📊 Статистика\n"
        "👨‍💼 Админ-панель\n"
        "🤖 ИИ анализ фото"
    )

@dp.message(F.text == "👨‍💼 Админ")
async def admin_panel_fast(message: types.Message):
    """Быстрая админ-панель"""
    user_role = await db.get_user_role(message.from_user.id)
    
    if user_role != 'admin':
        await quick_reply(message, "⛔ Доступ только для администраторов.")
        return
    
    await quick_reply(
        message,
        "👨‍💼 <b>Админ-панель</b>\n\n"
        "Статистика:\n"
        "• Активные смены: 0\n"
        "• Всего водителей: 3\n"
        "• Техника: 3 единицы\n\n"
        "Полная версия скоро!",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="📊 Статистика")],
                [types.KeyboardButton(text="👥 Водители")],
                [types.KeyboardButton(text="🔙 Назад")]
            ],
            resize_keyboard=True
        )
    )

# ========== ОБРАБОТКА ОШИБОК ==========

@dp.message()
async def handle_other_messages(message: types.Message):
    """Обработка всех остальных сообщений"""
    await quick_reply(
        message,
        "🤔 <b>Не понял команду</b>\n\n"
        "Используйте кнопки меню или команды:\n"
        "/start - Главное меню\n"
        "/help - Помощь\n\n"
        "Если что-то не работает — перезапустите бота /start"
    )

# ========== ЗАПУСК БОТА ==========

async def on_startup():
    """Быстрый запуск"""
    try:
        await db.connect()
        await db.add_test_data()
        
        # Добавляем тестового админа (замени ID на свой)
        ADMIN_ID = 1079922982  # <-- ЗАМЕНИ НА СВОЙ ID
        await db.register_driver(ADMIN_ID, "Администратор", "admin")
        
        logger.info("✅ Бот готов к работе!")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

async def main():
    """Основная функция"""
    await on_startup()
    
    try:
        logger.info("🚀 Запускаю бота...")
        await dp.start_polling(bot, skip_updates=True)  # skip_updates для скорости
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())

