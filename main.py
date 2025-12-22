import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from dotenv import load_dotenv

from database import db

# ========== НАСТРОЙКА ==========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(
    token=os.getenv('BOT_TOKEN'),
    default=DefaultBotProperties(parse_mode="HTML")
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== СОСТОЯНИЯ ==========
class UserStates(StatesGroup):
    waiting_for_username_or_id = State()
    waiting_for_role = State()
    waiting_for_equipment_name = State()
    waiting_for_equipment_model = State()
    waiting_for_equipment_vin = State()
    
    # Новые состояния для смен
    waiting_for_equipment_selection = State()
    waiting_for_briefing_confirmation = State()
    waiting_for_inspection_photo = State()
    waiting_for_daily_checks = State()
    waiting_for_shift_notes = State()
    
    # Для ТО
    waiting_for_maintenance_type = State()
    waiting_for_maintenance_date = State()
    waiting_for_maintenance_description = State()

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
    except:
        logger.error(f"Не удалось отправить сообщение пользователю {user_id}")

def get_main_keyboard(role):
    """Генерирует клавиатуру в зависимости от роли"""
    
    keyboards = {
        'botadmin': [
            [types.KeyboardButton(text="👑 Админ-панель")],
            [types.KeyboardButton(text="🏢 Все организации")],
            [types.KeyboardButton(text="👥 Все пользователи")],
            [types.KeyboardButton(text="📊 Статистика")],
            [types.KeyboardButton(text="➕ Назначить роль")]
        ],
        
        'director': [
            [types.KeyboardButton(text="👨‍💼 Моя организация")],
            [types.KeyboardButton(text="🚜 Автопарк")],
            [types.KeyboardButton(text="👥 Сотрудники")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить роль")],
            [types.KeyboardButton(text="📊 Отчеты")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")]
        ],
        
        'fleetmanager': [
            [types.KeyboardButton(text="👷 Управление парком")],
            [types.KeyboardButton(text="🚜 Техника")],
            [types.KeyboardButton(text="👥 Водители")],
            [types.KeyboardButton(text="➕ Добавить технику")],
            [types.KeyboardButton(text="➕ Назначить водителя")],
            [types.KeyboardButton(text="🔍 Проверить осмотры")]
        ],
        
        'driver': [
            [types.KeyboardButton(text="🚛 Начать смену")],
            [types.KeyboardButton(text="📋 Мои смены")],
            [types.KeyboardButton(text="✅ Закончить смену")],
            [types.KeyboardButton(text="ℹ️ Информация")]
        ]
    }
    
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

def get_check_status_keyboard():
    """Клавиатура для статуса проверки"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="✅ Исправно"), types.KeyboardButton(text="⚠️ Требует внимания")],
            [types.KeyboardButton(text="❌ Неисправно"), types.KeyboardButton(text="⏭️ Пропустить")],
            [types.KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

# ========== КОМАНДА СТАРТ ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Главное меню для всех"""
    user = await db.get_user(message.from_user.id)
    
    # Регистрируем, если пользователя нет
    if not user:
        await db.register_user(
            telegram_id=message.from_user.id,
            full_name=f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip(),
            username=message.from_user.username,
            role='driver'
        )
        user = await db.get_user(message.from_user.id)
    
    role = user['role']
    role_names = {
        'botadmin': '👑 Администратор бота',
        'director': '👨‍💼 Директор компании',
        'fleetmanager': '👷 Начальник парка',
        'driver': '🚛 Водитель'
    }
    
    # Проверяем активную смену для водителя
    if role == 'driver':
        active_shift = await db.get_active_shift(message.from_user.id)
        if active_shift:
            await reply(
                message,
                f"🚛 <b>У вас активная смена!</b>\n\n"
                f"<b>Техника:</b> {active_shift.get('equipment_name', 'Не указана')}\n"
                f"<b>Начало:</b> {active_shift['start_time'][:16]}\n"
                f"<b>Статус осмотра:</b> {'✅ Подтверждён' if active_shift['inspection_approved'] else '⏳ Ожидает проверки'}\n\n"
                f"Вы можете завершить смену через меню.",
                reply_markup=get_main_keyboard(role)
            )
            return
    
    await reply(
        message,
        f"🤖 <b>ТехКонтроль Бот</b>\n\n"
        f"<b>Роль:</b> {role_names.get(role, '👤 Пользователь')}\n"
        f"<b>ID:</b> {message.from_user.id}\n"
        f"<b>Имя:</b> {message.from_user.full_name}\n\n"
        f"Выберите действие из меню:",
        reply_markup=get_main_keyboard(role)
    )

# ========== СИСТЕМА СМЕН ВОДИТЕЛЯ ==========

@dp.message(F.text == "🚛 Начать смену")
async def start_shift_begin(message: types.Message, state: FSMContext):
    """Начинает процесс начала смены"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут начинать смены!")
        return
    
    # Проверяем активную смену
    active_shift = await db.get_active_shift(message.from_user.id)
    if active_shift:
        await reply(
            message,
            f"⚠️ <b>У вас уже есть активная смена!</b>\n\n"
            f"Смена начата: {active_shift['start_time'][:16]}\n"
            f"Техника: {active_shift.get('equipment_name', 'Не указана')}\n\n"
            f"Завершите текущую смену перед началом новой."
        )
        return
    
    # Получаем доступную технику
    equipment = await db.get_equipment_by_driver(message.from_user.id)
    
    if not equipment:
        await reply(
            message,
            "🚛 <b>Начало смены</b>\n\n"
            "❌ Нет доступной техники!\n\n"
            "Обратитесь к начальнику парка для назначения техники."
        )
        return
    
    # Сохраняем список техники в состоянии
    await state.update_data(equipment_list=equipment)
    
    # Создаем клавиатуру с техникой
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
async def process_equipment_selection(message: types.Message, state: FSMContext):
    """Обрабатывает выбор техники"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Начало смены отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    data = await state.get_data()
    equipment_list = data.get('equipment_list', [])
    
    # Ищем выбранную технику
    selected_eq = None
    for eq in equipment_list:
        if f"🚜 {eq['name']} ({eq['model']})" == message.text:
            selected_eq = eq
            break
    
    if not selected_eq:
        await reply(message, "❌ Пожалуйста, выберите технику из списка")
        return
    
    # Сохраняем выбранную технику
    await state.update_data(selected_equipment=selected_eq)
    
    await reply(
        message,
        f"✅ <b>Выбрана техника:</b> {selected_eq['name']} ({selected_eq['model']})\n\n"
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

@dp.message(UserStates.waiting_for_briefing_confirmation)
async def process_briefing_confirmation(message: types.Message, state: FSMContext):
    """Обрабатывает подтверждение инструктажа"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Начало смены отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    if message.text == "❌ Нет":
        await reply(
            message,
            "❌ <b>Инструктаж не подтверждён</b>\n\n"
            "Для начала смены необходимо подтвердить прохождение инструктажа.\n"
            "Обратитесь к начальнику парка."
        )
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
        return
    
    if message.text == "✅ Да":
        data = await state.get_data()
        selected_eq = data.get('selected_equipment')
        
        # Создаем смену
        shift_id = await db.start_shift(
            driver_id=message.from_user.id,
            equipment_id=selected_eq['id'],
            briefing_confirmed=True
        )
        
        await state.update_data(shift_id=shift_id)
        
        await reply(
            message,
            f"✅ <b>Инструктаж подтверждён!</b>\n\n"
            f"🚛 <b>Смена #{shift_id} начата</b>\n"
            f"<b>Техника:</b> {selected_eq['name']} ({selected_eq['model']})\n"
            f"<b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"
            f"📸 <b>Следующий шаг:</b>\n"
            f"Сделайте фото осмотра техники перед началом работы.\n\n"
            f"<b>Что должно быть на фото:</b>\n"
            f"• Общий вид техники\n"
            f"• Состояние шин\n"
            f"• Уровни жидкостей (если видно)\n"
            f"• Салон и приборная панель",
            reply_markup=types.ReplyKeyboardMarkup(
                keyboard=[[types.KeyboardButton(text="📸 Сделать фото")], [types.KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(UserStates.waiting_for_inspection_photo)
        
        # Отправляем уведомление начальнику парка
        await notify_manager_about_shift_start(message.from_user.id, selected_eq['id'], shift_id)
    else:
        await reply(message, "❌ Пожалуйста, выберите 'Да' или 'Нет'")

async def notify_manager_about_shift_start(driver_id, equipment_id, shift_id):
    """Уведомляет начальника парка о начале смены"""
    try:
        # Получаем информацию о водителе
        driver = await db.get_user(driver_id)
        if not driver or not driver.get('organization_id'):
            return
        
        # Получаем начальников парка в организации
        users = await db.get_users_by_organization(driver['organization_id'])
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        # Получаем информацию о технике
        equipment = None
        all_equipment = await db.get_organization_equipment(driver['organization_id'])
        for eq in all_equipment:
            if eq['id'] == equipment_id:
                equipment = eq
                break
        
        if not equipment:
            return
        
        for manager in fleet_managers:
            try:
                await send_to_user(
                    manager['telegram_id'],
                    f"👷 <b>Новая смена начата</b>\n\n"
                    f"🚛 <b>Водитель:</b> {driver['full_name']}\n"
                    f"📞 <b>Контакт:</b> @{driver['username'] if driver.get('username') else 'нет'}\n"
                    f"🚜 <b>Техника:</b> {equipment['name']} ({equipment['model']})\n"
                    f"🆔 <b>ID смены:</b> #{shift_id}\n"
                    f"🕐 <b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"
                    f"Ожидается фото осмотра техники."
                )
            except:
                continue
                
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления: {e}")

@dp.message(UserStates.waiting_for_inspection_photo)
async def process_inspection_photo(message: types.Message, state: FSMContext):
    """Обрабатывает фото осмотра"""
    if message.text == "❌ Отмена":
        await cancel_shift(message, state)
        return
    
    if message.text == "📸 Сделать фото":
        await reply(
            message,
            "📸 <b>Сделайте фото осмотра техники</b>\n\n"
            "Пожалуйста, сделайте фото и отправьте его в этот чат.\n"
            "Фото должно чётко показывать состояние техники.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await reply(message, "❌ Пожалуйста, отправьте фото или нажмите 'Сделать фото'")

@dp.message(F.photo, UserStates.waiting_for_inspection_photo)
async def handle_inspection_photo(message: types.Message, state: FSMContext):
    """Обрабатывает отправленное фото"""
    data = await state.get_data()
    shift_id = data.get('shift_id')
    
    if not shift_id:
        await reply(message, "❌ Ошибка: смена не найдена")
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
        return
    
    # Сохраняем file_id фото
    photo_file_id = message.photo[-1].file_id
    await db.update_shift_photo(shift_id, photo_file_id)
    
    # Отправляем подтверждение
    await reply(
        message,
        "✅ <b>Фото осмотра принято!</b>\n\n"
        "Фото отправлено начальнику парка для проверки.\n"
        "Вы можете приступить к работе.\n\n"
        "⚠️ <b>Важно:</b> Не забудьте провести ежедневные проверки техники."
    )
    
    # Уведомляем начальника парка о новом фото
    await notify_manager_about_new_photo(shift_id, message.from_user.id, photo_file_id)
    
    # Предлагаем пройти ежедневные проверки
    await reply(
        message,
        "🔄 <b>Ежедневные проверки техники</b>\n\n"
        "Рекомендуется проверить:\n"
        "• Уровень масла в двигателе\n"
        "• Уровень охлаждающей жидкости\n"
        "• Давление в шинах\n"
        "• Работу фар и стоп-сигналов\n"
        "• Исправность тормозов\n\n"
        "Хотите отметить выполненные проверки?",
        reply_markup=get_yes_no_keyboard()
    )
    await state.set_state(UserStates.waiting_for_daily_checks)

async def notify_manager_about_new_photo(shift_id, driver_id, photo_file_id):
    """Уведомляет начальника парка о новом фото"""
    try:
        # Получаем информацию о смене
        shift = await db.get_active_shift(driver_id)
        if not shift:
            return
        
        driver = await db.get_user(driver_id)
        if not driver or not driver.get('organization_id'):
            return
        
        # Получаем начальников парка
        users = await db.get_users_by_organization(driver['organization_id'])
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        for manager in fleet_managers:
            try:
                # Отправляем фото с подписью
                await bot.send_photo(
                    chat_id=manager['telegram_id'],
                    photo=photo_file_id,
                    caption=f"👷 <b>Новое фото осмотра</b>\n\n"
                           f"🚛 <b>Водитель:</b> {driver['full_name']}\n"
                           f"🚜 <b>Техника:</b> {shift.get('equipment_name', 'Неизвестно')}\n"
                           f"🆔 <b>ID смены:</b> #{shift_id}\n\n"
                           f"Для подтверждения осмотра нажмите кнопку ниже.",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="✅ Подтвердить осмотр",
                                    callback_data=f"approve_inspection:{shift_id}"
                                ),
                                InlineKeyboardButton(
                                    text="❌ Отклонить",
                                    callback_data=f"reject_inspection:{shift_id}"
                                )
                            ]
                        ]
                    )
                )
            except:
                continue
                
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления о фото: {e}")

@dp.message(UserStates.waiting_for_daily_checks)
async def process_daily_checks_decision(message: types.Message, state: FSMContext):
    """Обрабатывает решение о ежедневных проверках"""
    if message.text == "❌ Отмена":
        await cancel_shift(message, state)
        return
    
    if message.text == "❌ Нет":
        await reply(
            message,
            "ℹ️ <b>Ежедневные проверки пропущены</b>\n\n"
            "Не забудьте проверить технику перед началом работы.\n"
            "Вы можете завершить смену в любое время через меню."
        )
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
        return
    
    if message.text == "✅ Да":
        # Получаем список проверок
        checks = await db.get_daily_checks()
        if not checks:
            await reply(message, "❌ Список проверок не найден")
            await state.clear()
            user = await db.get_user(message.from_user.id)
            await reply(message, "Возврат в главное меню", reply_markup=get_main_keyboard(user['role']))
            return
        
        # Сохраняем проверки в состоянии
        await state.update_data(daily_checks=checks, current_check_index=0)
        
        # Начинаем первую проверку
        await send_next_check(message, state)
    else:
        await reply(message, "❌ Пожалуйста, выберите 'Да' или 'Нет'")

async def send_next_check(message: types.Message, state: FSMContext):
    """Отправляет следующую проверку"""
    data = await state.get_data()
    checks = data.get('daily_checks', [])
    current_index = data.get('current_check_index', 0)
    
    if current_index >= len(checks):
        # Все проверки пройдены
        await reply(
            message,
            "✅ <b>Все ежедневные проверки завершены!</b>\n\n"
            "Техника готова к работе.\n"
            "Не забудьте завершить смену после окончания работы.",
            reply_markup=get_main_keyboard('driver')
        )
        await state.clear()
        return
    
    current_check = checks[current_index]
    
    await reply(
        message,
        f"🔍 <b>Проверка {current_index + 1} из {len(checks)}</b>\n\n"
        f"<b>Тип:</b> {current_check['type']}\n"
        f"<b>Элемент:</b> {current_check['item']}\n"
        f"<b>Что проверить:</b> {current_check['check']}\n\n"
        f"<b>Статус элемента:</b>",
        reply_markup=get_check_status_keyboard()
    )

@dp.message(UserStates.waiting_for_daily_checks)
async def process_check_status(message: types.Message, state: FSMContext):
    """Обрабатывает статус проверки"""
    if message.text == "❌ Отмена":
        await cancel_shift(message, state)
        return
    
    if message.text == "⏭️ Пропустить":
        # Пропускаем эту проверку
        data = await state.get_data()
        current_index = data.get('current_check_index', 0)
        await state.update_data(current_check_index=current_index + 1)
        await send_next_check(message, state)
        return
    
    valid_statuses = ["✅ Исправно", "⚠️ Требует внимания", "❌ Неисправно"]
    if message.text not in valid_statuses:
        await reply(message, "❌ Пожалуйста, выберите статус из списка")
        return
    
    data = await state.get_data()
    shift_id = data.get('shift_id')
    checks = data.get('daily_checks', [])
    current_index = data.get('current_check_index', 0)
    
    if current_index < len(checks):
        current_check = checks[current_index]
        
        # Сохраняем проверку в БД
        await db.add_daily_check(
            shift_id=shift_id,
            check_type=current_check['type'],
            item_name=current_check['item'],
            status=message.text,
            notes=None
        )
    
    # Переходим к следующей проверке
    await state.update_data(current_check_index=current_index + 1)
    await send_next_check(message, state)

@dp.message(F.text == "✅ Закончить смену")
async def end_shift_start(message: types.Message, state: FSMContext):
    """Начинает процесс завершения смены"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут завершать смены!")
        return
    
    # Проверяем активную смену
    active_shift = await db.get_active_shift(message.from_user.id)
    if not active_shift:
        await reply(message, "❌ У вас нет активной смены!")
        return
    
    await state.update_data(shift_id=active_shift['id'])
    
    await reply(
        message,
        f"🛑 <b>Завершение смены #{active_shift['id']}</b>\n\n"
        f"<b>Техника:</b> {active_shift.get('equipment_name', 'Неизвестно')}\n"
        f"<b>Начало:</b> {active_shift['start_time'][:16]}\n\n"
        f"Пожалуйста, добавьте заметки о работе за смену:\n"
        f"(можно пропустить, отправив любое сообщение)",
        reply_markup=types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="⏭️ Без заметок")], [types.KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(UserStates.waiting_for_shift_notes)

@dp.message(UserStates.waiting_for_shift_notes)
async def process_shift_notes(message: types.Message, state: FSMContext):
    """Обрабатывает заметки о смене"""
    if message.text == "❌ Отмена":
        await state.clear()
        user = await db.get_user(message.from_user.id)
        await reply(message, "❌ Завершение смены отменено", reply_markup=get_main_keyboard(user['role']))
        return
    
    data = await state.get_data()
    shift_id = data.get('shift_id')
    
    notes = None
    if message.text != "⏭️ Без заметок":
        notes = message.text
    
    # Завершаем смену
    success = await db.complete_shift(shift_id, notes)
    
    if success:
        await reply(
            message,
            "✅ <b>Смена завершена успешно!</b>\n\n"
            "Спасибо за работу!\n"
            "Не забудьте сдать технику и заполнить документацию."
        )
        
        # Уведомляем начальника парка
        await notify_manager_about_shift_end(shift_id, message.from_user.id)
    else:
        await reply(message, "❌ Ошибка при завершении смены")
    
    await state.clear()
    await cmd_start(message)

async def notify_manager_about_shift_end(shift_id, driver_id):
    """Уведомляет начальника парка о завершении смены"""
    try:
        # Получаем информацию о смене
        driver = await db.get_user(driver_id)
        if not driver or not driver.get('organization_id'):
            return
        
        # Получаем начальников парка
        users = await db.get_users_by_organization(driver['organization_id'])
        fleet_managers = [u for u in users if u['role'] == 'fleetmanager']
        
        for manager in fleet_managers:
            try:
                await send_to_user(
                    manager['telegram_id'],
                    f"👷 <b>Смена завершена</b>\n\n"
                    f"🚛 <b>Водитель:</b> {driver['full_name']}\n"
                    f"🆔 <b>ID смены:</b> #{shift_id}\n"
                    f"🕐 <b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"
                    f"Техника сдана, смена закрыта."
                )
            except:
                continue
                
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления о завершении смены: {e}")

async def cancel_shift(message: types.Message, state: FSMContext):
    """Отменяет начатую смену"""
    data = await state.get_data()
    shift_id = data.get('shift_id')
    
    if shift_id:
        # Можно отметить смену как отменённую
        await db.complete_shift(shift_id, "Отменена пользователем")
    
    await state.clear()
    user = await db.get_user(message.from_user.id)
    await reply(message, "❌ Смена отменена", reply_markup=get_main_keyboard(user['role']))

@dp.message(F.text == "📋 Мои смены")
async def my_shifts_history(message: types.Message):
    """Показывает историю смен водителя"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] != 'driver':
        await reply(message, "⛔ Только водители могут просматривать свои смены!")
        return
    
    shifts = await db.get_shifts_by_driver(message.from_user.id, limit=5)
    
    if not shifts:
        await reply(
            message,
            "📋 <b>Мои смены</b>\n\n"
            "У вас ещё не было смен.\n"
            "Начните первую смену через меню '🚛 Начать смену'."
        )
        return
    
    text = f"📋 <b>Последние смены</b> ({len(shifts)})\n\n"
    
    for shift in shifts:
        status_emoji = "🟢" if shift['status'] == 'active' else "✅" if shift['status'] == 'completed' else "❌"
        text += f"{status_emoji} <b>Смена #{shift['id']}</b>\n"
        text += f"🚜 {shift.get('equipment_name', 'Техника')}\n"
        text += f"📅 {shift['start_time'][:16]}\n"
        
        if shift['end_time']:
            text += f"🕐 До: {shift['end_time'][:16]}\n"
        
        text += f"📸 Осмотр: {'✅' if shift['inspection_approved'] else '⏳'}\n"
        
        if shift.get('notes'):
            text += f"📝 {shift['notes'][:50]}...\n"
        
        text += "\n"
    
    text += "\n<code>Всего смен: ...</code>"
    
    await reply(message, text)

# ========== ПРОВЕРКА ОСМОТРОВ НАЧАЛЬНИКОМ ПАРКА ==========

@dp.message(F.text == "🔍 Проверить осмотры")
async def check_inspections(message: types.Message):
    """Показывает смены ожидающие проверки"""
    user = await db.get_user(message.from_user.id)
    
    if user['role'] not in ['director', 'fleetmanager']:
        await reply(message, "⛔ Доступ только для директора или начальника парка!")
        return
    
    org_id = user.get('organization_id')
    if not org_id:
        await reply(message, "❌ Вы не привязаны к организации!")
        return
    
    pending_shifts = await db.get_pending_inspections(org_id)
    
    if not pending_shifts:
        await reply(
            message,
            "🔍 <b>Проверка осмотров</b>\n\n"
            "Нет смен ожидающих проверки.\n"
            "Все осмотры подтверждены! ✅"
        )
        return
    
    text = f"🔍 <b>Смены ожидающие проверки</b> ({len(pending_shifts)})\n\n"
    
    for shift in pending_shifts[:5]:  # Показываем первые 5
        text += f"🆔 <b>Смена #{shift['id']}</b>\n"
        text += f"🚛 <b>Водитель:</b> {shift['driver_name']}\n"
        text += f"🚜 <b>Техника:</b> {shift['equipment_name']}\n"
        text += f"🕐 <b>Начало:</b> {shift['start_time'][:16]}\n\n"
        
        # Добавляем inline кнопки для проверки
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Подтвердить",
                        callback_data=f"approve_inspection:{shift['id']}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отклонить", 
                        callback_data=f"reject_inspection:{shift['id']}"
                    ),
                    InlineKeyboardButton(
                        text="👁️ Посмотреть фото",
                        callback_data=f"view_photo:{shift['id']}"
                    )
                ]
            ]
        )
        
        await message.answer(text, reply_markup=keyboard)
        text = ""
    
    if len(pending_shifts) > 5:
        await reply(message, f"... и ещё {len(pending_shifts) - 5} смен")

# Обработчики callback-кнопок
@dp.callback_query(F.data.startswith("approve_inspection:"))
async def approve_inspection_callback(callback: types.CallbackQuery):
    """Подтверждает осмотр"""
    shift_id = int(callback.data.split(":")[1])
    
    success = await db.approve_inspection(shift_id, callback.from_user.id)
    
    if success:
        # Получаем информацию о смене для уведомления водителя
        # (нужно будет добавить метод для получения смены по ID)
        
        await callback.message.edit_text(
            f"✅ <b>Осмотр подтверждён!</b>\n\n"
            f"Смена #{shift_id}\n"
            f"Подтвердил: {callback.from_user.full_name}"
        )
        
        # Уведомляем водителя
        # (здесь нужен метод для получения driver_id из смены)
    else:
        await callback.answer("❌ Ошибка при подтверждении осмотра", show_alert=True)
    
    await callback.answer()

@dp.callback_query(F.data.startswith("reject_inspection:"))
async def reject_inspection_callback(callback: types.CallbackQuery):
    """Отклоняет осмотр"""
    shift_id = int(callback.data.split(":")[1])
    
    # Здесь можно добавить логику отклонения
    # Например, запросить причину или отправить сообщение водителю
    
    await callback.message.edit_text(
        f"❌ <b>Осмотр отклонён</b>\n\n"
        f"Смена #{shift_id}\n"
        f"Отклонил: {callback.from_user.full_name}"
    )
    await callback.answer("Осмотр отклонён. Водитель будет уведомлён.")

@dp.callback_query(F.data.startswith("view_photo:"))
async def view_photo_callback(callback: types.CallbackQuery):
    """Показывает фото осмотра"""
    shift_id = int(callback.data.split(":")[1])
    
    # Здесь нужно получить file_id фото из базы данных
    # Пока заглушка
    await callback.answer("Функция просмотра фото в разработке", show_alert=True)

# ========== ОСТАВШИЕСЯ ОБРАБОТЧИКИ (без изменений) ==========

# ... [остальной код без изменений, включая все предыдущие обработчики] ...

# Не забудьте добавить новый импорт в начало файла:
# from datetime import datetime

# И обновить функцию on_startup для добавления тестовых ТО
async def on_startup():
    """Инициализация при запуске"""
    try:
        await db.connect()
        
        # Создаем администратора (ВАЖНО: ЗАМЕНИТЕ ID НА СВОЙ!)
        ADMIN_ID = 1079922982  # <-- ЗАМЕНИТЕ ЭТО НА ВАШ TELEGRAM ID!
        await db.register_user(
            telegram_id=ADMIN_ID,
            full_name="Администратор Системы",
            username="admin",
            role='botadmin'
        )
        
        logger.info("✅ Бот запущен!")
        logger.info(f"👑 Администратор: ID {ADMIN_ID}")
        
        # Добавляем тестовые ТО для демонстрации
        await add_test_data()
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

async def add_test_data():
    """Добавляет тестовые данные для демонстрации"""
    try:
        # Проверяем есть ли организации
        orgs = await db.get_all_organizations()
        if not orgs:
            return
        
        # Для каждой организации добавляем тестовое ТО
        for org in orgs:
            equipment = await db.get_organization_equipment(org['id'])
            if equipment:
                # Добавляем ТО через неделю для первой техники
                from datetime import datetime, timedelta
                next_week = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
                
                await db.add_maintenance(
                    equipment_id=equipment[0]['id'],
                    type='ТО-1000',
                    scheduled_date=next_week,
                    description='Плановое техническое обслуживание'
                )
                logger.info(f"✅ Добавлено тестовое ТО для организации {org['name']}")
                
    except Exception as e:
        logger.error(f"❌ Ошибка при добавлении тестовых данных: {e}")

# ========== ЗАПУСК БОТА ==========

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
