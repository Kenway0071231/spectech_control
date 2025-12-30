import sqlite3
import aiosqlite
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
import asyncio

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path='techcontrol.db'):
        self.db_path = db_path
        self.conn = None
        
    async def connect(self):
        """Устанавливает соединение с базой данных"""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            self.conn.row_factory = aiosqlite.Row
            await self.create_tables()
            logger.info("✅ База данных подключена")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            return False
            
    async def close(self):
        """Закрывает соединение с базой данных"""
        if self.conn:
            await self.conn.close()
            logger.info("🔌 Соединение с БД закрыто")
    
    async def create_tables(self):
        """Создает все необходимые таблицы"""
        try:
            # Таблица пользователей
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY,
                    full_name TEXT NOT NULL,
                    username TEXT,
                    role TEXT NOT NULL DEFAULT 'unassigned',
                    organization_id INTEGER,
                    phone_number TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id)
                )
            ''')
            
            # Таблица организаций
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS organizations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    director_id INTEGER UNIQUE,
                    address TEXT,
                    contact_phone TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (director_id) REFERENCES users(telegram_id)
                )
            ''')
            
            # Таблица техники
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS equipment (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    model TEXT NOT NULL,
                    vin TEXT NOT NULL UNIQUE,
                    registration_number TEXT,
                    organization_id INTEGER NOT NULL,
                    status TEXT DEFAULT 'active',
                    next_maintenance DATE,
                    last_maintenance DATE,
                    fuel_type TEXT DEFAULT 'diesel',
                    fuel_capacity REAL,
                    current_fuel_level REAL DEFAULT 0,
                    odometer INTEGER DEFAULT 0,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id)
                )
            ''')
            
            # Таблица расписания ТО
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS maintenance_schedule (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_id INTEGER NOT NULL,
                    maintenance_type TEXT NOT NULL,
                    interval_km INTEGER,
                    interval_days INTEGER,
                    last_done_km INTEGER DEFAULT 0,
                    last_done_date DATE,
                    next_due_km INTEGER,
                    next_due_date DATE,
                    description TEXT,
                    parts_needed TEXT,
                    estimated_hours REAL,
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id)
                )
            ''')
            
            # Таблица учета топлива
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS fuel_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_id INTEGER NOT NULL,
                    driver_id INTEGER,
                    fuel_amount REAL NOT NULL,
                    fuel_type TEXT DEFAULT 'diesel',
                    cost_per_liter REAL,
                    total_cost REAL,
                    odometer_reading INTEGER,
                    fueling_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fueling_station TEXT,
                    receipt_photo TEXT,
                    notes TEXT,
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id),
                    FOREIGN KEY (driver_id) REFERENCES users(telegram_id)
                )
            ''')
            
            # Таблица запчастей
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS spare_parts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    part_name TEXT NOT NULL,
                    part_number TEXT,
                    description TEXT,
                    category TEXT,
                    quantity INTEGER DEFAULT 0,
                    min_quantity INTEGER DEFAULT 1,
                    supplier TEXT,
                    supplier_contact TEXT,
                    last_ordered DATE,
                    unit_price REAL,
                    location TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id)
                )
            ''')
            
            # Таблица заказов
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENTINCREMENT,
                    organization_id INTEGER NOT,
                    organization_id INTEGER NOT NULL,
 NULL,
                    order_type TEXT NOT NULL                    order_type TEXT NOT NULL, -- 'fuel', ', -- 'fuel', 'parts', 'serviceparts', 'service'
                    equipment_id'
                    equipment_id INTEGER,
                    part_id INTEGER,
                    INTEGER,
                    part_id INTEGER,
                    quantity INTEGER,
                    quantity INTEGER,
                    urgent BOOLE urgent BOOLEAN DEFAULT FALSE,
AN DEFAULT FALSE,
                    status TEXT DEFAULT                    status TEXT DEFAULT 'pending', -- 'pending', -- pending, approved, pending, approved, ordered, delivered, ordered, delivered, cancelled
                    requested cancelled
                    requested_by INTEGER_by INTEGER,
                    approved_by INTEGER,
                    notes TEXT,
,
                    approved_by INTEGER,
                    notes TEXT,
                    created                    created_at TIMESTAMP DEFAULT CURRENT_TIM_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGNESTAMP,
                    FOREIGN KEY (organization_id KEY (organization_id) REFERENCES organizations(id) REFERENCES organizations(id),
                    FOREIGN KEY),
                    FOREIGN KEY (equipment_id (equipment_id) REFERENCES) REFERENCES equipment(id),
                    FOREIGN KEY equipment(id),
                    FOREIGN KEY (part_id) (part_id) REFERENCES spare_parts REFERENCES spare_parts(id),
(id),
                    FOREIGN                    FOREIGN KEY (requested_by) KEY (requested_by) REFERENCES users(telegram_id REFERENCES users(telegram_id),
                   ),
                    FOREIGN KEY (approved_by) FOREIGN KEY (approved_by) REFERENCES users REFERENCES users(telegram_id)
               (telegram_id)
                )
            )
            ''')
            
            # Та ''')
            
            # Таблица инструкблица инструкций
            await self.ций
            await self.conn.executeconn.execute('''
                CREATE TABLE('''
                CREATE TABLE IF NOT IF NOT EXISTS instructions (
                    id INTEGER EXISTS instructions (
                    id INTEGER PRIMARY KEY PRIMARY KEY AUTOINCREMENT,
                    equipment AUTOINCREMENT,
                    equipment_model TEXT_model TEXT NOT NULL,
                    NOT NULL,
                    instruction_type TEXT NOT NULL, -- 'greasing instruction_type TEXT NOT NULL, -- 'greasing', '', 'maintenance', 'operation'
maintenance', 'operation'
                    title                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    description TEXT,
 TEXT,
                    steps TEXT,                    steps TEXT, -- JSON список шагов
                    diagram_photo -- JSON список шагов
                    diagram_photo TEXT,
 TEXT,
                    video_url TEXT,
                                       video_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
           _TIMESTAMP
                )
            ''')
            
            # Та # Таблица назначения техблица назначения техники водителямники водителям
            await self
            await self.conn.execute('.conn.execute('''
                CREATE''
                CREATE TABLE IF NOT EXISTS TABLE IF NOT EXISTS driver_equipment driver_equipment (
                    driver_id (
                    driver_id INTEGER NOT NULL,
 INTEGER NOT NULL,
                    equipment_id INTEGER                    equipment_id INTEGER NOT NULL,
                    NOT NULL,
                    assigned_at TIMESTAMP assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   ,
                    PRIMARY KEY (driver_id, PRIMARY KEY (driver_id, equipment_id),
                    equipment_id),
                    FOREIGN KEY (driver FOREIGN KEY (driver_id) REFERENCES users_id) REFERENCES users(telegram_id(telegram_id),
                   ),
                    FOREIGN KEY (equipment_id FOREIGN KEY (equipment_id) REFERENCES equipment(id) REFERENCES equipment(id)
                )
           )
                )
            ''')
            
            ''')
            
            # Таблица с # Таблица смен
            await self.conn.execute('''
               мен
            await self.conn.execute('''
                CREATE TABLE IF NOT CREATE TABLE IF NOT EXISTS shifts (
                    EXISTS shifts (
                    id INTEGER PRIMARY KEY id INTEGER PRIMARY KEY AUTOIN AUTOINCREMENT,
                    driver_id INTEGERCREMENT,
                    driver_id INTEGER NOT NULL,
                    NOT NULL,
                    equipment_id INTEGER NOT equipment_id INTEGER NOT NULL,
                    start NULL,
                    start_time TIMESTAMP DEFAULT_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 CURRENT_TIMESTAMP,
                    end                    end_time TIMESTAMP,
                    start_time TIMESTAMP,
                    start_odometer INTEGER_odometer INTEGER,
                    end_,
                    end_odometer INTEGER,
odometer INTEGER,
                    briefing_confirmed                    briefing_confirmed BOOLEAN DEFAULT FALSE,
                    inspection_photo TEXT BOOLEAN DEFAULT FALSE,
                    inspection_photo TEXT,
                    inspection_approved BOOLEAN DEFAULT,
                    inspection_approved BOOLEAN DEFAULT FALSE,
                    approved_by INTEGER FALSE,
                    approved_by INTEGER,
                    notes TEXT,
                    status TEXT DEFAULT ',
                    notes TEXT,
                    status TEXT DEFAULT 'active',
                    FOREIGNactive',
                    FOREIGN KEY (driver_id) REFERENCES users(telegram_id),
                    FOREIGN KEY (equipment_id) KEY (driver_id) REFERENCES users(telegram_id),
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id),
                    FOREIGN KEY (approved_by) REFERENCES users(telegram_id)
                )
 REFERENCES equipment(id),
                    FOREIGN KEY (approved_by) REFERENCES users(telegram_id)
                )
            ''')
            
            ''')
            
            # Таблица            # Таблица ежедневных провер ежедневных проверок
ок
            await self.conn.execute            await self.conn.execute('''
               ('''
                CREATE TABLE CREATE TABLE IF NOT EXISTS daily_checks IF NOT EXISTS daily_checks (
                    id INTEGER (
                    id INTEGER PRIMARY KEY AUTOIN PRIMARY KEY AUTOINCREMENT,
                    shiftCREMENT,
                    shift_id INTEGER NOT NULL_id INTEGER NOT NULL,
                    check_type,
                    check_type TEXT NOT NULL,
 TEXT NOT NULL,
                    item_name TEXT                    item_name TEXT NOT NULL,
                    NOT NULL,
                    status TEXT NOT NULL status TEXT NOT NULL,
                    notes,
                    notes TEXT,
                    created_at TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   _TIMESTAMP,
                    FOREIGN KEY (shift FOREIGN KEY (shift_id)_id) REFERENCES shifts(id)
 REFERENCES shifts(id)
                )
            ''                )
            ''')
            
            #')
            
            # Таблица технического обслуживания Таблица технического обслуживания
           
            await self.conn.execute(' await self.conn.execute('''
''
                CREATE TABLE IF NOT EXISTS                CREATE TABLE IF NOT EXISTS maintenance (
 maintenance (
                    id INTEGER PRIMARY KEY AUTO                    id INTEGER PRIMARY KEY AUTOINCREMENTINCREMENT,
                    equipment_id INTEGER,
                    equipment_id INTEGER NOT NULL,
 NOT NULL,
                    type TEXT NOT                    type TEXT NOT NULL,
                    scheduled NULL,
                    scheduled_date DATE NOT NULL_date DATE NOT NULL,
                    completed_date,
                    completed_date DATE,
                    description DATE,
                    description TEXT,
                    status TEXT,
                    status TEXT DEFAULT 'scheduled',
                    cost REAL,
                    performed TEXT DEFAULT 'scheduled',
                    cost REAL,
                    performed_by TEXT,
                   _by TEXT,
                    parts_used TEXT parts_used TEXT,
                    odometer,
                    odometer_at_service INTEGER,
_at_service INTEGER,
                    next_service_km INTEGER,
                                       next_service_km INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY,
                    FOREIGN KEY (equipment_id (equipment_id) REFERENCES equipment(id) REFERENCES equipment(id)
                )
           )
                )
            ''')
 ''')
            
            # Таблица л            
            # Таблица логов действийогов действий
            await self
            await self.conn.execute('''
.conn.execute('''
                CREATE TABLE IF                CREATE TABLE IF NOT EXISTS NOT EXISTS action_logs (
                    id action_logs (
                    id INTEGER PRIMARY KEY AUTO INTEGER PRIMARY KEY AUTOINCREMENTINCREMENT,
                    user_id INTEGER NOT,
                    user_id INTEGER NOT NULL,
                    action NULL,
                    action_type TEXT NOT NULL_type TEXT NOT NULL,
                    details TEXT,
                   ,
                    details TEXT,
                    created_at TIMESTAMP created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user FOREIGN KEY (user_id) REFERENCES users_id) REFERENCES users(telegram_id(telegram_id)
                )
           )
                )
            ''')
            
            ''')
            
            # Та # Таблица шаблонблица шаблонов проверок
ов проверок
            await self.            await self.conn.execute('''
               conn.execute('''
                CREATE TABLE CREATE TABLE IF NOT EXISTS daily_check_t IF NOT EXISTS daily_check_templates (
emplates (
                    id INTEGER PRIMARY KEY AUTO                    id INTEGER PRIMARY KEY AUTOINCREMENTINCREMENT,
                    check_type TEXT NOT,
                    check_type TEXT NOT NULL,
 NULL,
                    item TEXT NOT NULL,
                    item TEXT NOT NULL,
                    check                    check_description TEXT NOT NULL,
                   _description TEXT NOT NULL,
                    order_index order_index INTEGER DEFAULT 0
                INTEGER DEFAULT 0
                )
            )
            ''')
            
            ''')
            
            # Таблица для # Таблица для AI контекста AI контекста
            await self
            await self.conn.execute('.conn.execute('''
                CREATE''
                CREATE TABLE IF TABLE IF NOT EXISTS ai_context (
                    NOT EXISTS ai_context (
                    id INTEGER PRIMARY KEY id INTEGER PRIMARY KEY AUTOINCREMENT,
 AUTOINCREMENT,
                    organization_id INTEGER                    organization_id INTEGER,
                   ,
                    context_type TEXT NOT NULL, context_type TEXT NOT NULL, -- 'greasing -- 'greasing', 'maintenance', 'maintenance', 'trou', 'troubleshooting'
                   bleshooting'
                    equipment_model TEXT,
 equipment_model TEXT,
                    question TEXT NOT                    question TEXT NOT NULL,
                    answer NULL,
                    answer TEXT NOT NULL,
 TEXT NOT NULL,
                    source TEXT,                    source TEXT, -- 'ai', -- 'ai', 'manual', ' 'manual', 'instruction'
                    verified BOOLEANinstruction'
                    verified BOOLEAN DEFAULT FALSE,
                    DEFAULT FALSE,
                    verified_by INTEGER,
 verified_by INTEGER,
                    usage_count INTEGER                    usage_count INTEGER DEFAULT  DEFAULT 0,
                    created_at TIM0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGNESTAMP,
                    FOREIGN KEY (organization_id KEY (organization_id) REFERENCES organizations(id) REFERENCES organizations(id),
                    FOREIGN KEY),
                    FOREIGN KEY (verified_by) (verified_by) REFERENCES users(tele REFERENCES users(telegram_id)
               gram_id)
                )
            ''')
 )
            ''')
            
            # Та            
            # Таблица настроек ублица настроек уведомведомлений
            await selfлений
            await self.conn.execute('.conn.execute(''''
                CREATE TABLE IF NOT'
                CREATE TABLE IF NOT EXISTS notification_settings (
                    id EXISTS notification_settings (
                    id INTEGER PRIMARY INTEGER PRIMARY KEY AUTOINCREMENT,
                    KEY AUTOINCREMENT,
                    organization_id organization_id INTEGER NOT NULL,
                    notification INTEGER NOT NULL,
                    notification_type TEXT_type TEXT NOT NULL,
                    days_b NOT NULL,
                    days_before INTEGERefore INTEGER DEFAULT 7,
                    enabled DEFAULT 7,
                    enabled BOO BOOLEAN DEFAULT TRUELEAN DEFAULT TRUE,
                    notify_d,
                    notify_director BOOirector BOOLEAN DEFAULT TRUELEAN DEFAULT TRUE,
                    notify_fleetmanager,
                    notify_fleetmanager BOO BOOLEAN DEFAULT TRUE,
                   LEAN DEFAULT TRUE,
                    notify_d notify_driver BOOLEriver BOOLEAN DEFAULT FALSE,
AN DEFAULT FALSE,
                    FOREIGN KEY (                    FOREIGN KEY (organization_id) REFERENCESorganization_id) REFERENCES organizations(id organizations(id)
                )
           )
                )
            ''')
            
            await self ''')
            
            await self.conn.commit()
.conn.commit()
            
            # Доба            
            # Добавляем шабвляем шаблоны провероклоны проверок
            await self
            await self.init_daily_.init_daily_checks()
           checks()
            # Добавля # Добавляем стандартные настройки уведомленийем стандартные настройки уведомлений
           
            await self.init_notification_settings await self.init_notification_settings()
            
()
            
            logger.info("✅ Все            logger.info("✅ Все таблицы таблицы созданы/провер созданы/проверены")
ены")
            
        except Exception as e            
        except Exception as e:
           :
            logger.error(f"❌ logger.error(f"❌ Ошиб Ошибка создания таблиц: {ка создания таблиц: {e}")
e}")
            raise
    
    async def            raise
    
    async def init_d init_daily_checks(self):
       aily_checks(self):
        """И """Инициализирует шабнициализирует шаблонылоны ежедневных провер ежедневных проверок"""
        checksок"""
        checks = [
            ("engine", = [
            ("engine", "М "Масло двигателя", "асло двигателя", "Проверить уровень и состояние"),
Проверить уровень и состояние"),
            ("            ("engine",engine", "Охлаждающая "Охлаждающая жидкость", " жидкость", "ПроверитьПроверить уровень"),
 уровень"),
            ("engine", "Т            ("engine", "Тормозная жидкостьормозная жидкость", "", "Проверить уровень"),
Проверить уровень"),
            ("            ("engine",engine", "Жидкость ГУ "Жидкость ГУР", "ПровериР", "Проверить уровеньть уровень"),
            ("tires","),
            ("tires", "Да "Давление в шинах",вление в шинах", "П "Проверить давление (пероверить давление (передниередние/задние)"),
           /задние)"),
            ("tires", "Протектор ("tires", "Протектор шин шин", "Проверить", "Проверить износ износ"),
            ("tires","),
            ("tires", "В "Внешний вид шиннешний вид шин", "", "Проверить на поПроверить на порезырезы, гвозди"),
           , гвозди"),
            ("lights ("lights", "Фары бли", "Фары ближнегожнего света", "Провери света", "Проверить работуть работу"),
            ("lights", ""),
            ("lights", "ФаФары дальнего светары дальнего света", "", "Проверить работу"),
Проверить работу"),
            ("            ("lights", "Стопlights", "Стоп-си-сигналы", "Пгналы", "Проверироверить работу"),
            ("lightsть работу"),
            ("lights", "", "Поворотники", "Поворотники", "ПровПроверитьерить работу"),
            ("lights", "Габаритные работу"),
            ("lights", "Габаритные огни огни", "Проверить", "Проверить работу"),
 работу"),
            ("safety", "            ("safety", "ЗерЗеркала", "Провкала", "Проверитьерить чистоту и регулировку чистоту и регулировку"),
           "),
            ("safety", "Р ("safety", "Ремниемни безопасности", "Провери безопасности", "Проверить испть исправность"),
            ("sравность"),
            ("safety",afety", "Огнетуши "Огнетушитель",тель", "Наличие и "Наличие и срок годности"),
            ("safety срок годности"),
            ("safety", "", "Аптечка", "Аптечка", "НаличиеНаличие и срок годности"),
            и срок годности"),
            ("s ("safety", "Знак аafety", "Знак аварийварийной остановки", "Наной остановки", "Наличие"),
личие"),
            ("fluids", "            ("fluids", "ТопТопливный фильтр", "ливный фильтр", "ПровПроверить на подтекиерить на подтеки"),
           "),
            ("fluids", "М ("fluids", "Масляасляный фильтр", "Пный фильтр", "Проверироверить на подтеки"),
ть на подтеки"),
            ("            ("brakes", "Торbrakes", "Тормозмозные колодки", "ные колодки", "ПровПроверить износ"),
           ерить износ"),
            ("bra ("brakes", "Тормkes", "Тормозныеозные диски", "Пров диски", "Проверитьерить состояние"),
            ("interior состояние"),
            ("interior", "Приборная панель", "Приборная панель", "", "Проверить показанияПроверить показания"),
           "),
            ("interior", "З ("interior", "Звуковой сигнал", "Проввуковой сигнал", "Проверить работу"),
            ("interior", "Стеклоочистиерить работу"),
            ("interior", "Стеклоочистители", "Проверить работу и состояние щеток"),
            ("тели", "Проверить работу и состояние щеток"),
            ("interior", "Омывательinterior", "Омыватель стекла", "Проверить уровень жидкости"),
            ("exterior", стекла", "Проверить уровень жидкости"),
            ("exterior", "Кузов", "Проверить на повреждения"),
            (" "Кузов", "Проверить на повреждения"),
            ("exterior", "Зерexterior", "Зеркала заднего вида", "Проверикала заднего вида", "Проверить целостность"),
ть целостность"),
            ("            ("exterior", "Стеexterior", "Стекла", "Проверить на трещины"),
        ]
        
        for checkкла", "Проверить на трещины"),
        ]
        
        for check_type, item, description in checks:
            try:
                await self_type, item, description in checks:
            try:
                await self.conn.execute(
                    "INSERT OR.conn.execute(
                    "INSERT OR IGNORE INTO daily_check_templates IGNORE INTO daily_check_templates (check_type, item, check_description (check_type, item, check_description) VALUES (?, ?, ?) VALUES (?, ?, ?)",
                   )",
                    (check_type, item, description)
 (check_type, item, description)
                )
            except:
                pass
        
                )
            except:
                pass
        
        await self.conn.commit()
    
        await self.conn.commit()
    
    async def init_notification_settings(self    async def init_notification_settings(self):
        """Инициализирует):
        """Инициализирует настройки уведомлений"""
 настройки уведомлений"""
        settings = [
            ("maintenance        settings = [
            ("maintenance", 7, True, True,", 7, True, True, True, False),
            ("maintenance True, False),
            ("maintenance", 1, True, True,", 1, True, True, True, True),
            ("fuel True, True),
            ("fuel_l_low", 0, Trueow", 0, True, False, False, True, True),
           , True, True),
            ("in ("inspection_due", 0spection_due", 0, True, True, True, True, False, True, True, False),
           ),
            ("order_approved",  ("order_approved", 0, True, True, True,0, True, True, True, False),
        ]
        
        for setting False),
        ]
        
        for setting in settings in settings:
            try:
                await:
            try:
                await self self.conn.execute('''
                   .conn.execute('''
                    INSERT OR IGNORE INTO notification_settings INSERT OR IGNORE INTO notification_settings 
                    (notification_type, days_b 
                    (notification_type, days_before, enabled, notify_directorefore, enabled, notify_director, notify_fleetmanager, notify_d, notify_fleetmanager, notify_driver)
                    VALUES (?, ?, ?,river)
                    VALUES (?, ?, ?, ?, ?, ?)
                '' ?, ?, ?)
                ''', setting', setting)
            except:
                pass)
            except:
                pass
        
       
        
        await self.conn.commit()
 await self.conn.commit()
    
       
    # ========== БА # ========== БАЗОВЗОВЫЕ МЕТОДЫЫЕ МЕТОДЫ ========= ==========
    
    async def get=
    
    async def get_user(self_user(self, telegram_id:, telegram_id: int) -> Optional int) -> Optional[Dict]:
        """Пол[Dict]:
        """Получает пользователя по Telegramучает пользователя по Telegram ID"""
        try ID"""
        try:
            cursor =:
            cursor = await self.conn.execute(
                "SELECT * FROM users await self.conn.execute(
                "SELECT * FROM users WHERE telegram_id = ? WHERE telegram_id = ?",", 
                (telegram_id 
                (telegram_id,)
,)
            )
            row = await            )
            row = await cursor.fetch cursor.fetchone()
            await cursor.closeone()
            await cursor.close()
           ()
            return dict(row) if row return dict(row) if row else None else None
        except Exception as e
        except Exception as e:
           :
            logger.error(f"Ошиб logger.error(f"Ошибка полученияка получения пользователя {telegram_id пользователя {telegram_id}: {}: {e}")
            return None
    
e}")
            return None
    
    async def register_user(self, telegram    async def register_user(self, telegram_id:_id: int, full_name: str int, full_name: str, username, username: str = None: str = None, role: str, role: str = 'unassigned') = 'unassigned') -> bool -> bool:
        """Регистри:
        """Регистрирует новогорует нового пользователя"""
        try:
            await пользователя"""
        try:
            await self.conn.execute(
                self.conn.execute(
                "INSERT "INSERT OR IGNORE OR IGNORE INTO users (tele INTO users (telegram_id, fullgram_id, full_name,_name, username, role) username, role) VALUES (?, ?, ?, ?)",
                (tele VALUES (?, ?, ?, ?)",
                (telegram_id, fullgram_id, full_name,_name, username, role)
            )
 username, role)
            )
            await self.            await self.conn.commit()
           conn.commit()
            return True
        return True
        except Exception as e except Exception as e:
            logger.error:
            logger.error(f"Ошибка регистрации пользователя {telegram_id}: {(f"Ошибка регистрации пользователя {telegram_id}: {e}")
e}")
            return False
    
    async            return False
    
    async def update def update_user_role(self, telegram_id_user_role(self, telegram_id: int: int, role: str, organization, role: str, organization_id:_id: int = None) int = None) -> bool:
        -> bool:
        """Обновляет роль пользова """Обновляет роль пользователя ителя и организацию"""
        организацию"""
        try:
            if try:
            if organization_id:
                organization_id:
                await self.conn await self.conn.execute(
                    ".execute(
                    "UPDATE users SET role = ?, organization_id = ? WHERE telegram_id = ?",
                    (role,UPDATE users SET role = ?, organization_id = ? WHERE telegram_id = ?",
                    (role, organization_id, telegram organization_id, telegram_id)
                )
_id)
                )
            else:
                           else:
                await self.conn.execute(
                    "UPDATE users SET role await self.conn.execute(
                    "UPDATE users SET role = ? = ? WHERE telegram_id = ?",
                    (role, telegram_id)
                )
            await self.conn WHERE telegram_id = ?",
                    (role, telegram_id)
                )
            await self.conn.commit()
            return True
.commit()
            return True
        except        except Exception as e:
            logger Exception as e:
            logger.error(f.error(f"Ошибка обнов"Ошибка обновления рления роли {telegram_idоли {telegram_id}: {e}: {e}")
            return False
    
    async def}")
            return False
    
    async def get_all_users(self get_all_users(self) -> List) -> List[Dict]:
        """[Dict]:
        """Получает всехПолучает всех пользователей"""
        пользователей"""
        try:
            cursor try:
            cursor = await self. = await self.conn.execute("SELECTconn.execute("SELECT * FROM users ORDER * FROM users ORDER BY created_at DESC")
            rows = BY created_at DESC")
            rows = await cursor.fetchall await cursor.fetchall()
            await cursor()
            await cursor.close()
            return.close()
            return [dict(row) [dict(row) for row in rows]
        except Exception for row in rows]
        except Exception as e as e:
            logger.error(f":
            logger.error(f"Ошибка полученияОшибка получения пользователей пользователей: {e}")
: {e}")
            return []
    
    async            return []
    
    async def get def get_all_users_simple(self)_all_users_simple(self) -> List -> List[Dict]:
       [Dict]:
        """Получает """Получает всех пользователей ( всех пользователей (упрощеннаяупрощенная версия)"""
 версия)"""
        try:
                   try:
            cursor = await self cursor = await self.conn.execute(
.conn.execute(
                "SELECT telegram                "SELECT telegram_id,_id, full_name, role, organization full_name, role, organization_id FROM users ORDER_id FROM users ORDER BY created_at DESC BY created_at DESC"
            )
           "
            )
            rows = await cursor rows = await cursor.fetchall.fetchall()
            await cursor.close()
()
            await cursor.close()
            return [dict            return [dict(row)(row) for row in rows]
        for row in rows]
        except Exception as e except Exception as e:
            logger.error:
            logger.error(f"Ошиб(f"Ошибка полученияка получения пользователей: {e}")
 пользователей: {e}")
            return []
    
            return []
    
    async    async def get_users_by_organization def get_users_by_organization(self, org_id(self, org_id: int) ->: int) -> List[Dict]:
 List[Dict]:
        """        """Получает пользоваПолучает пользователей организации"""
        try:
телей организации"""
        try:
            cursor = await            cursor = await self.conn.execute self.conn.execute(
               (
                "SELECT * FROM users WHERE "SELECT * FROM users WHERE organization_id organization_id = ? ORDER BY role, = ? ORDER BY role, full_name full_name",
                (org_id,",
                (org_id,)
           )
            )
            rows = )
            rows = await cursor.fetchall await cursor.fetchall()
            await cursor.close()
()
            await cursor.close()
            return            return [dict(row) for row [dict(row) for row in rows in rows]
        except Exception as e]
        except Exception as e:
           :
            logger.error(f"Ошиб logger.error(f"Ошибка полученияка получения пользователей организации {org_id пользователей организации {org_id}: {e}")
            return []
    
}: {e}")
            return []
    
    async    async def get_organization def get_organization(self, org_id: int) ->(self, org_id: int) -> Optional Optional[Dict]:
        """Получа[Dict]:
        """Получает организает организацию по ID"""
        tryцию по ID"""
        try:
           :
            cursor = await self cursor = await self.conn.execute(
.conn.execute(
                "SELECT * FROM organizations                "SELECT * FROM organizations WHERE id WHERE id = ?", 
                ( = ?", 
                (org_idorg_id,)
            )
,)
            )
            row = await            row = await cursor.fetchone()
            await cursor.fetchone()
            await cursor.close cursor.close()
            return dict(row)()
            return dict(row) if row else None
        except Exception if row else None
        except Exception as e:
            logger.error as e:
            logger.error(f"(f"Ошибка получения организации {org_idОшибка получения организации {org_id}: {e}")
            return}: {e}")
            return None
    
    async def create_organization_for_d None
    
    async def create_organization_for_director(self, director_id:irector(self, director_id: int int, org_name: str, address:, org_name: str, address: str = None, contact_ str = None, contact_phone: str = None):
        """phone: str = None):
        """СозСоздает организацию и назначаетдает организацию и назначает директора"""
        try:
            user = await директора"""
        try:
            user = await self.get_user(director self.get_user(director_id)
_id)
            if user and            if user and user.get('organization_id'):
                return None, user.get('organization_id'):
                return None, "У "У этого пользователя уже есть организация этого пользователя уже есть организация"
            
"
            
            cursor = await self.            cursor = await self.conn.executeconn.execute(
                "INSERT INTO organizations(
                "INSERT INTO organizations (name (name, director_id, address,, director_id, address, contact_ contact_phone) VALUES (?, ?,phone) VALUES (?, ?, ?, ? ?, ?)",
                (org_name,)",
                (org_name, director_id director_id, address, contact_phone, address, contact_phone)
           )
            )
            org_id = cursor )
            org_id = cursor.lastrowid
            
            await self..lastrowid
            
            await self.conn.execute(
                "UPDATE users SETconn.execute(
                "UPDATE users SET organization_id = ?, role = 'direct organization_id = ?, role = 'director' WHERE telegram_id = ?",
or' WHERE telegram_id = ?",
                (org_id, director_id)
                (org_id, director_id)
            )
            
            await self            )
            
            await self.conn.conn.commit()
            return org_id, None.commit()
            return org_id, None
        except Exception as e:
           
        except Exception as e:
            logger.error(f"Ошибка создания logger.error(f"Ошибка создания организации: {e}")
            return None организации: {e}")
            return None, str(e)
    
    async def, str(e)
    
    async def get_all_organizations(self) -> get_all_organizations(self) -> List[Dict]:
        """Получа List[Dict]:
        """Получает все организации"""
        try:
           ет все организации"""
        try:
            cursor = await self.conn.execute(" cursor = await self.conn.execute("SELECT * FROM organizations ORDER BY created_atSELECT * FROM organizations ORDER BY created_at DESC")
            rows = await cursor.fetch DESC")
            rows = await cursor.fetchall()
            await cursor.close()
all()
            await cursor.close()
                       return [dict(row) for return [dict(row) for row in rows]
        except Exception as row in rows]
        except Exception as e:
            logger.error(f"О e:
            logger.error(f"Ошибка получения организаций: {eшибка получения организаций: {e}")
            return []
    
    async def}")
            return []
    
    async def get_all_organizations_s get_all_organizations_simple(self) -> List[Dict]:
        """Получает все организации (упроimple(self) -> List[Dict]:
        """Получает все организации (упрощенная версия)"""
щенная версия)"""
        try:
            cursor = await self.conn        try:
            cursor = await self.conn.execute(
                "SELECT id.execute(
                "SELECT id, name, director_id FROM organizations ORDER BY created, name, director_id FROM organizations ORDER BY created_at DESC"
            )
            rows =_at DESC"
            )
            rows = await cursor.fetchall()
            await cursor.fetchall()
            await cursor await cursor.close()
            return [dict(row).close()
            return [dict(row) for row in rows]
        except Exception for row in rows]
        except Exception as e:
            logger.error(f" as e:
            logger.error(f"Ошибка получения организаций: {Ошибка получения организаций: {e}")
            return []
e}")
            return []
    
    async def update_organization_name(self, org    
    async def update_organization_name(self, org_id:_id: int, new_name: str) -> bool:
        """ int, new_name: str) -> bool:
        """ОбновОбновляет название организации"""
        tryляет название организации"""
        try:
           :
            await self.conn.execute(
 await self.conn.execute(
                "                "UPDATE organizations SET name = ?UPDATE organizations SET name = ? WHERE id WHERE id = ?",
                (new = ?",
                (new_name, org_id)
            )
           _name, org_id)
            )
            await self await self.conn.commit()
            return.conn.commit()
            return True
 True
        except Exception as e:
        except Exception as e:
            logger            logger.error(f"Ошибка.error(f"Ошибка обнов обновления организации {org_id}:ления организации {org_id}: {e {e}")
            return False
    
   }")
            return False
    
    async def async def get_organization_stats(self, get_organization_stats(self, org_id org_id: int) ->: int) -> Dict:
        """ Dict:
        """Получает статистиПолучает статистику организациику организации"""
        stats = {}
        try:
"""
        stats = {}
        try:
            cursor            cursor = await self.conn.execute = await self.conn.execute(
               (
                "SELECT role, COUNT(*) "SELECT role, COUNT(*) as count as count FROM users WHERE organization_id = FROM users WHERE organization_id = ? GROUP ? GROUP BY role",
                BY role",
                (org (org_id,)
            )
           _id,)
            )
            roles_data roles_data = await cursor.fetchall()
 = await cursor.fetchall()
            stats            stats['roles'] = {row['roles'] = {row['role['role']: row['count']']: row['count'] for row for row in roles_data}
            await in roles_data}
            await cursor.close cursor.close()
            
            cursor = await()
            
            cursor = await self. self.conn.execute(
                "SELECTconn.execute(
                "SELECT status, status, COUNT(*) as count FROM equipment COUNT(*) as count FROM equipment WHERE organization WHERE organization_id = ? GROUP BY status_id = ? GROUP BY status",
               ",
                (org_id,)
            (org_id,)
            )
            )
            eq_data = await cursor.fetch eq_data = await cursor.fetchall()
all()
            stats['equipment']            stats['equipment'] = { = {row['status']row['status']: row['count: row['count'] for row in'] for row in eq_data}
            eq_data}
            await cursor.close()
            
 await cursor.close()
            
            cursor =            cursor = await self.conn.execute await self.conn.execute('''
('''
                SELECT COUNT(*)                SELECT COUNT(*) as count FROM shifts s
                JOIN users u as count FROM shifts s
                JOIN users u ON s ON s.driver_id = u..driver_id = u.telegramtelegram_id
                WHERE u._id
                WHERE u.organization_idorganization_id = ? AND s = ? AND s.status =.status = 'active'
            ''', 'active'
            ''', (org_id, (org_id,))
            active_shifts =))
            active_shifts = await cursor await cursor.fetchone()
            stats['.fetchone()
            stats['active_shactive_shifts'] = activeifts'] = active_shifts_shifts['count'] if active_sh['count'] if active_shifts elseifts else 0
            await cursor 0
            await cursor.close()
.close()
            
            next_week =            
            next_week = (datetime (datetime.now() + timedelta(d.now() + timedelta(days=ays=7)).strftime('%Y7)).strftime('%Y-%m-%m-%d')
            cursor =-%d')
            cursor = await self await self.conn.execute('''
.conn.execute('''
                SELECT                SELECT COUNT(*) as count FROM maintenance COUNT(*) as count FROM maintenance m
 m
                JOIN equipment e                JOIN equipment e ON m.equipment_id = e.id
 ON m.equipment_id = e.id
                WHERE                WHERE e.organization_id = ? e.organization_id = ? AND m.scheduled_date <= AND m.scheduled_date <= ? AND m.status ? AND m.status = 'scheduled'
            = 'scheduled'
            ''', ''', (org_id, next_ (org_id, next_week))
week))
            weekly_maint = await            weekly_maint = await cursor.fetch cursor.fetchone()
            stats['weeklyone()
            stats['weekly_maintenance_maintenance'] = weekly_m'] = weekly_maint['count']aint['count'] if weekly_maint else  if weekly_maint else 0
0
            await cursor.close()
            
            await cursor.close()
            
        except Exception as e:
            logger        except Exception as e:
            logger.error(f.error(f"Ошибка получения статисти"Ошибка получения статистики организациики организации {org_id}: {e {org_id}: {e}")
        
}")
        
        return stats
    
    async        return stats
    
    async def add def add_equipment(self, name_equipment(self, name: str: str, model: str, vin, model: str, vin: str: str, org_id:, org_id: int, 
                          registration_number: str = None int, 
                          registration_number: str = None, fuel, fuel_type: str =_type: str = 'diesel',
 'diesel',
                          fuel_capacity: float                          fuel_capacity: float = None = None) -> Optional[int]:
       ) -> Optional[int]:
        """Д """Добавляет новую техниобавляет новую технику"""
ку"""
        try:
            cursor =        try:
            cursor = await self await self.conn.conn.execute(
                """INSERT INTO.execute(
                """INSERT INTO equipment 
                (name, equipment 
                (name, model, model, vin, organization_id, registration vin, organization_id, registration_number,_number, fuel_type, fuel_capacity fuel_type, fuel_capacity) 
) 
                VALUES (?, ?, ?,                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ?, ?)""",
                (name (name, model, vin, model, vin, org_id,, org_id, registration_number, fuel registration_number, fuel_type, fuel_c_type, fuel_capacity)
            )
apacity)
            )
            await self.            await self.conn.commit()
           conn.commit()
            return cursor.lastrow return cursor.lastrowid
        exceptid
        except Exception as e:
 Exception as e:
            logger.error(f            logger.error(f"Ошибка"Ошибка добавления добавления техники: {e}")
 техники: {e}")
            return None
    
            return None
    
    async def get    async def get_organization_equ_organization_equipment(self, orgipment(self, org_id: int)_id: int) -> List[Dict -> List[Dict]:
        """Получает технику]:
        """Получает технику организации"""
        try организации"""
        try:
            cursor =:
            cursor = await self.conn await self.conn.execute(
.execute(
                "SELECT *                "SELECT * FROM equipment WHERE organization_id = FROM equipment WHERE organization_id = ? ORDER BY name ? ORDER BY name",
                (org_id,)
           ",
                (org_id,)
            )
            rows = )
            rows = await cursor.fetchall await cursor.fetchall()
           ()
            await cursor.close()
 await cursor.close()
            return [dict            return [dict(row)(row) for row in rows]
        for row in rows]
        except Exception as e except Exception as e:
            logger.error:
            logger.error(f"Ошиб(f"Ошибка получения техникика получения техники организации {org_id организации {org_id}: {e}")
}: {e}")
            return            return []
    
    async def get []
    
    async def get_equipment_by_equipment_by_id(self, equipment_id(self, equipment_id: int)_id: int) -> Optional -> Optional[Dict]:
        """Пол[Dict]:
        """Получает техникуучает технику по ID"""
        по ID"""
        try:
            cursor try:
            cursor = await = await self.conn.execute(
                self.conn.execute(
                "SELECT * FROM "SELECT * FROM equipment WHERE id = equipment WHERE id = ?", ?", 
                (equipment_id 
                (equipment_id,)
            )
,)
            )
            row = await            row = await cursor.fetchone()
 cursor.fetchone()
            await cursor.close            await cursor.close()
            return dict()
            return dict(row)(row) if row else None if row else None
        except Exception as e
        except Exception as e:
            logger.error:
            logger.error(f"Ошиб(f"Ошибка получения техникика получения техники {equ {equipment_id}: {e}")
ipment_id}: {e}")
            return None
    
            return None
    
    async def get    async def get_equipment_by_equipment_by_driver(self,_driver(self, driver_id driver_id: int) -> List: int) -> List[Dict]:
[Dict]:
        """Получает техни        """Получает технику назнаку назначенную водителю"""
ченную водителю"""
        try        try:
            cursor = await self:
            cursor = await self.conn.conn.execute('''
                SELECT.execute('''
                SELECT e.* e.* FROM equipment e
 FROM equipment e
                JOIN driver_                JOIN driver_equipment de ONequipment de ON e.id = de e.id = de.equipment_id.equipment_id
                WHERE de
                WHERE de.driver_id =.driver_id = ? AND e.status = 'active'
 ? AND e.status = 'active'
                ORDER BY e                ORDER BY e.name
            ''.name
            ''', (driver_id', (driver_id,))
            rows,))
            rows = await cursor.fetch = await cursor.fetchall()
            awaitall()
            await cursor.close()
            cursor.close()
            return [dict(row return [dict(row) for row in) for row in rows]
 rows]
        except Exception as e:
        except Exception as e:
            logger.error(f            logger.error(f"Ошибка"Ошибка получения техники в получения техники водителя {driverодителя {driver_id}: {e_id}: {e}")
            return []
}")
            return []
    
    async def    
    async def update_equipment update_equipment(self, eq_id(self, eq_id: int, **: int, **kwargs) -> boolkwargs) -> bool:
        """Об:
        """Обновляет данные техновляет данные техники"""
        tryники"""
        try:
           :
            if not kwargs:
 if not kwargs:
                return False
            
                return False
            
            set_clause            set_clause = ', '.join = ', '.join([f"{key([f"{key} = ?"} = ?" for key in kwargs for key in kwargs.keys()])
           .keys()])
            values = list(k values = list(kwargs.values())
wargs.values())
            values            values.append(eq_id)
            
.append(eq_id)
            
            await self.            await self.conn.execute(
               conn.execute(
                f"UPDATE equipment f"UPDATE equipment SET {set_cl SET {set_clause} WHERE idause} WHERE id = ?",
                = ?",
                values
            )
 values
            )
            await            await self.conn.commit()
            self.conn.commit()
            return True return True
        except Exception
        except Exception as e:
            as e:
            logger.error(f" logger.error(f"Ошибка обОшибка обновления техникиновления техники {eq_id}: {eq_id}: {e}")
            {e}")
            return False
    
    return False
    
    async def assign_ async def assign_equipment_to_dequipment_to_driver(selfriver(self, driver_id: int,, driver_id: int, equipment_id: int equipment_id: int) -> bool:
) -> bool:
        """Назнача        """Назначает технику вет технику водителю"""
       одителю"""
        try:
            await try:
            await self.conn.execute self.conn.execute(
                "INSERT(
                "INSERT OR REPLACE OR REPLACE INTO driver_equ INTO driver_equipment (driver_idipment (driver_id, equipment_id), equipment_id) VALUES (?, ? VALUES (?, ?)",
                (driver)",
                (driver_id, equipment_id_id, equipment_id)
            )
           )
            )
            await self.conn await self.conn.commit()
.commit()
            return True
        except            return True
        except Exception as Exception as e:
            logger.error(f e:
            logger.error(f"О"Ошибка назначения техникишибка назначения техники водителю {driver_id}: { водителю {driver_id}: {e}")
e}")
            return False
    
    async            return False
    
    async def start def start_shift(self, driver_id_shift(self, driver_id: int: int, equipment_id: int,, equipment_id: int, briefing_conf briefing_confirmed: bool = False,irmed: bool = False, start_ start_odometer: int = Noneodometer: int = None) ->) -> Optional[int Optional[int]:
        """Начинает]:
        """Начинает новую смен новую смену"""
        tryу"""
        try:
            cursor = await self:
            cursor = await self.conn.conn.execute(
                "INSERT INTO.execute(
                "INSERT INTO shifts ( shifts (driver_iddriver_id, equipment_id, briefing_conf, equipment_id, briefing_confirmed, start_odometerirmed, start_odometer) VALUES) VALUES (?, ?, ?, (?, ?, ?, ?)",
                (driver_id, equipment_id, briefing_confirmed, start_odometer ?)",
                (driver_id, equipment_id, briefing_confirmed, start_odometer)
            )
            await self)
            )
            await self.conn.conn.commit()
            return cursor.last.commit()
            return cursor.lastrowid
       rowid
        except Exception as e except Exception as e:
            logger.error:
            logger.error(f"Ошибка начала смены для води(f"Ошибка начала смены для водителя {driver_idтеля {driver_id}: {e}")
}: {e}")
            return None
    
    async def get_active_shift(self, driver_id: int) -> Optional[Dict]:
        """Пол            return None
    
    async def get_active_shift(self, driver_id: int) -> Optional[Dict]:
        """Получает активную сменучает активную смену ву водителя"""
        try:
одителя"""
        try:
            cursor            cursor = await self.conn.execute('' = await self.conn.execute('''
                SELECT s.*, e.name'
                SELECT s.*, e.name as equipment_name, e.odometer as equipment_name, e.odometer
                FROM shifts s
                LEFT
                FROM shifts s
                LEFT JOIN equipment e ON s.equipment JOIN equipment e ON s.equipment_id = e.id
                WHERE s_id = e.id
                WHERE s.driver_id = ? AND s.status.driver_id = ? AND s.status = 'active'
                ORDER BY s = 'active'
                ORDER BY s.start_time DESC LIMIT 1
.start_time DESC LIMIT 1
            ''', (            ''', (driver_id,))
            row = await cursor.fetchone()
driver_id,))
            row = await cursor.fetchone()
            await cursor.close()
            return dict            await cursor.close()
            return dict(row) if row else None
       (row) if row else None
        except Exception as e:
            logger.error except Exception as e:
            logger.error(f"Ошиб(f"Ошибка получения активной смены {driver_id}:ка получения активной смены {driver_id}: { {e}")
            return None
    
    asynce}")
            return None
    
    async def update_shift_photo(self, def update_shift_photo(self, shift_id: int, photo_file_id shift_id: int, photo_file_id: str) -> bool:
        """Обновляет фото осмотра в: str) -> bool:
        """Обновляет фото осмотра в смене"""
        try:
            смене"""
        try:
            await self.conn.execute(
                " await self.conn.execute(
                "UPDATE shifts SET inspection_photo = ?UPDATE shifts SET inspection_photo = ? WHERE id = ?",
                (photo WHERE id = ?",
                (photo_file_id, shift_id)
            )
_file_id, shift_id)
            )
            await self.conn.commit()
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошиб            return True
        except Exception as e:
            logger.error(f"Ошибка обновления фото смены {shift_id}: {e}")
           ка обновления фото смены {shift_id}: {e}")
            return False
    
    async def get_d return False
    
    async defaily_checks(self) -> get_daily_checks(self) -> List[Dict]:
        """Получа List[Dict]:
        """Получает шаблоет шаблоны ежедневных проверок"""
        try:
           ны ежедневных проверок"""
        try:
            cursor cursor = await self.conn.execute(
                = await self.conn.execute(
                "SELECT * FROM daily_check_templates "SELECT * FROM daily_check_templates ORDER BY order_index, check_type"
 ORDER BY order_index, check_type"
            )
            rows = await cursor.fetch            )
            rows = await cursor.fetchall()
            await cursor.close()
           all()
            await cursor.close()
            return [dict(row) for row in return [dict(row) for row in rows]
        except Exception as e:
 rows]
        except Exception as e:
            logger.error(f"Ошибка            logger.error(f"Ошибка получения проверок: {e}")
            получения проверок: {e}")
            return []
    
    async def add_d return []
    
    async def add_daily_check(self, shift_id: intaily_check(self, shift_id: int, check_type: str, item_name, check_type: str, item_name: str, status: str, notes: str, status: str, notes: str = None) -> bool:
: str = None) -> bool:
        """Добавляет ежеднев        """Добавляет ежедневную проверку"""
        tryную проверку"""
        try:
            await self.conn.execute(
:
            await self.conn.execute(
                "INSERT INTO daily_checks (                "INSERT INTO daily_checks (shift_id, check_type, item_nameshift_id, check_type, item_name, status, notes) VALUES (?,, status, notes) VALUES (?, ?, ?, ?, ?)",
                (shift ?, ?, ?, ?)",
                (shift_id, check_type, item_name,_id, check_type, item_name, status, notes)
            )
            await status, notes)
            )
            await self.conn.commit()
            return True self.conn.commit()
            return True
        except Exception as e:
           
        except Exception as e:
            logger.error(f"Ошибка добав logger.error(f"Ошибка добавления проверки для смены {ления проверки для смены {shift_id}: {e}")
            returnshift_id}: {e}")
            return False
    
    async def complete_shift False
    
    async def complete_shift(self, shift_id: int, end(self, shift_id: int, end_odometer: int = None,_odometer: int = None, notes: str = None) -> bool notes: str = None) -> bool:
        """Завершает:
        """Завершает смен смену"""
        try:
           у"""
        try:
            await self await self.conn.execute(
                """.conn.execute(
                """UPDATE shiftsUPDATE shifts SET end_time = CURRENT_TIM SET end_time = CURRENT_TIMESTAMP,ESTAMP, 
                status = 'completed 
                status = 'completed', end_odometer = ?, notes', end_odometer = ?, notes = ? = ? 
                WHERE id = ? 
                WHERE id = ?""",
""",
                (end_odometer                (end_odometer, notes, notes, shift_id)
            )
, shift_id)
            )
            
                       
            # Обновляем одомет # Обновляем одометр в технике
            if end_odр в технике
            if endometer:
                cursor = await self._odometer:
                cursor = awaitconn.execute(
                    "SELECT equipment_id self.conn.execute(
                    "SELECT equipment_id FROM shifts WHERE id = ? FROM shifts WHERE id = ?",
                   ",
                    (shift_id,)
                (shift_id,)
                )
                )
                shift = await cursor.fetchone shift = await cursor.fetchone()
               ()
                await cursor.close()
                
 await cursor.close()
                
                if shift                if shift:
                    await self:
                    await self.conn.execute(
                        "UPDATE equipment SET od.conn.execute(
                        "UPDATE equipment SET odometer = ? WHERE id =ometer = ? WHERE id = ?",
                        ( ?",
                        (end_odometerend_odometer, shift['equ, shift['equipment_id'])
                   ipment_id'])
                    )
            
            await )
            
            await self.conn.commit self.conn.commit()
           ()
            return True
        return True
        except Exception as e except Exception as e:
            logger.error(f":
            logger.error(f"ОшибОшибка завершения сменыка завершения смены {shift {shift_id}: {e}")
           _id}: {e}")
            return False return False
    
    async def get_sh
    
    async def get_shifts_byifts_by_driver(self, driver_id_driver(self, driver_id: int, limit: int = : int, limit: int = 10) -> List[Dict]:
       10) -> List[Dict]:
        """Пол """Получает смены вучает смены водителя"""
        try:
            cursorодителя"""
        try:
            cursor = await self.conn.execute('''
                SELECT s.*, e.name as equipment = await self.conn.execute('''
                SELECT s.*, e.name as equipment_name 
                FROM shifts s_name 
                FROM shifts s
                LEFT JOIN equipment e ON s.equ
                LEFT JOIN equipment e ON sipment_id = e.id
                WHERE s.driver_id = ? 
               .equipment_id = e.id
                WHERE s.driver_id = ? 
                ORDER BY s.start_time DESC ORDER BY s.start_time DESC 
                
                LIMIT ?
            ''', (driver_id, limit))
            rows = await cursor.fetchall()
 LIMIT ?
            ''', (driver_id, limit))
            rows = await cursor.fetchall()
            await cursor.close            await cursor.close()
()
            return [dict(row) for row in rows]
        except Exception as            return [dict(row) for row in rows]
        except Exception as e:
            logger e:
            logger.error(f"Ошибка получения смен водителя.error(f"Ошибка получения смен водителя {driver_id}: {driver_id}: {e}")
            {e}")
            return []
    
    return []
    
    async def get_pending_inspections(self, async def get_pending_inspections(self, org_id: int) -> List[Dict]:
        """Получает смены ожидающие провер org_id: int) -> List[Dict]:
        """Получает смены ожидающие проверки осмотра"""
        try:
            cursor = awaitки осмотра"""
        try:
            cursor = await self.conn.execute self.conn.execute(''('''
                SELECT s.*,'
                SELECT s.*, u.full_name as driver_name, e.name as equipment_name
                FROM shifts s
 u.full_name as driver_name, e.name as equipment_name
                FROM shifts s
                JOIN users u                JOIN users u ON s.driver_id = u.telegram_id
                JOIN equipment e ON s.equ ON s.driver_id = u.telegram_id
                JOIN equipment e ON s.equipment_id = eipment_id = e.id
                WHERE u.organization_id = ? 
                AND s.inspection_photo IS NOT.id
                WHERE u.organization_id = ? 
                AND s.inspection_photo IS NOT NULL 
                AND NULL 
                AND s.inspection_ s.inspection_approved = FALSE
approved = FALSE
                AND s.status                AND s.status = 'active'
                ORDER BY s.start_time
            ''', (org = 'active'
                ORDER BY s.start_time
           _id,))
            rows = await cursor ''', (org_id,))
            rows = await cursor.fetchall()
           .fetchall()
            await cursor.close()
 await cursor.close()
            return [dict            return [dict(row) for row(row) for row in rows]
        in rows]
        except Exception as e except Exception as e:
            logger.error:
            logger.error(f"Ошиб(f"Ошибка получения ожидающих проверок для организации {org_id}: {e}")
            return []
    
    async def approveка получения ожидающих проверок для организации {org_id}: {e}")
            return []
    
    async def approve_inspection(self,_inspection(self, shift_id shift_id: int, approved_by:: int, approved_by: int) -> bool:
        """Подтвержда int) -> bool:
        """Подтверждает осмотр техники"""
ет осмотр техники"""
        try        try:
            await self.conn:
            await self.conn.execute(
.execute(
                "UPDATE shifts SET inspection                "UPDATE shifts SET inspection_approved_approved = TRUE, approved = TRUE, approved_by =_by = ? WHERE id = ?",
 ? WHERE id = ?",
                (approved_by                (approved_by, shift_id)
, shift_id)
            )
            await            )
            await self.conn.commit self.conn.commit()
            return True
        except Exception as e:
           ()
            return True
        except Exception as e:
            logger.error(f" logger.error(f"Ошибка подтверОшибка подтверждения осмотра {shift_id}:ждения осмотра {shift_id}: {e}")
            {e}")
            return False
    
    return False
    
    async def add_main async def add_maintenance(selftenance(self, equipment_id: int,, equipment_id: int, type: str, type: str, scheduled_date: str scheduled_date: str, description: str = None) -> Optional[int]:
       , description: str = None) -> Optional[int]:
        """Добавля """Добавляет запись оет запись о ТО"""
        ТО"""
        try:
            cursor try:
            cursor = await self. = await self.conn.execute(
               conn.execute(
                "INSERT INTO maintenance "INSERT INTO maintenance (equipment_id (equipment_id, type, type, scheduled_date, description) VALUES (?, ?, ?, ?)",
               , scheduled_date, description) VALUES (?, ?, ?, ?)",
                (equipment_id (equipment_id, type, type, scheduled_date,, scheduled_date, description)
            )
            await description)
            )
            await self.conn.commit()
            
            await self.conn.execute self.conn.commit()
            
            await self.conn.execute(
                "UPDATE(
                "UPDATE equipment SET equipment SET next_maintenance = ? WHERE next_maintenance = ? WHERE id = id = ?",
                (scheduled_date, equipment_id)
            )
            ?",
                (scheduled_date, await self.conn.commit()
            
            equipment_id)
            )
            await self.conn.commit()
            
            return cursor return cursor.lastrowid
        except.lastrowid
        except Exception as e:
            logger.error(f"О Exception as e:
            logger.error(fшибка добавления ТО: {"Ошибка добавления ТО: {e}")
            return None
    
e}")
            return None
    
    async def log_action(self, user_id:    async def log_action(self, user_id: int, action_type: str int, action_type: str, details, details: str: str = None) -> bool:
 = None) -> bool:
        """        """Логирует действие пользователя"""
        try:
            await self.Логирует действие пользователя"""
        try:
            await self.conn.execute(
                "INSERTconn.execute(
                "INSERT INTO action INTO action_logs (user_id,_logs (user_id, action_type, details) VALUES (?, ?, ? action_type, details) VALUES (?, ?, ?)",
                (user)",
                (user_id, action_type_id, action_type, details)
           , details)
            )
            await )
            await self.conn.commit()
            return True
        except Exception as self.conn.commit()
            return True
        except Exception as e:
 e:
            logger.error(f"Ошибка логирования действия: {e            logger.error(f"Ошибка логирования действия: {e}")
            return False
    
    async def}")
            return False
    
    async def get_recent_actions(self, org_id: get_recent_actions(self, org int = None, limit: int =_id: int = None, limit: int = 20) -> List 20) -> List[Dict]:
        """Получает последние действия[Dict]:
        """Получает последние действия"""
        try:
"""
        try:
            if            if org_id:
                cursor = org_id:
                cursor = await self.conn.execute('''
                    SELECT await self.conn.execute('''
                    SELECT al.*, u.full_name al.*, u.full_name, u, u.role 
                    FROM action.role 
                    FROM action_logs al
                    JOIN users u ON al_logs al
                    JOIN users u ON al.user_id = u.tele.user_id = u.telegram_idgram_id
                    WHERE u.organization
                    WHERE u.organization_id = ?
                    ORDER BY al.created_at DESC_id = ?
                    ORDER BY al.created 
                    LIMIT ?
               _at DESC 
                    LIMIT ?
                ''', (org_id, limit))
 ''', (org_id, limit))
            else:
                cursor = await self.conn            else:
                cursor = await self.conn.execute('''
.execute('''
                    SELECT                    SELECT al.*, u.full_name al.*, u.full_name, u.role 
                    FROM action, u.role 
                    FROM action_logs al
                    JOIN users u_logs al
                    JOIN users u ON al ON al.user_id = u.tele.user_id = u.telegram_id
                    ORDER BY al.created_at DESCgram_id
                    ORDER BY al.created_at DESC 
                    LIMIT ?
                
                    LIMIT ?
                ''', ''', (limit,))
 (limit,))
            
            rows = await cursor.fetchall()
            await cursor            
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
       .close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error except Exception as e:
            logger.error(f"Ошибка получения действий:(f"Ошибка получения действий: {e}")
            return []
    
    {e}")
            return []
    
    async def get_driver_stats async def get_driver_stats(self, driver_id: int, days: int(self, driver_id: int, days: int = 30) -> Dict = 30) -> Dict:
       :
        """Получает статистику водителя"""
        stats = {}
 """Получает статистику водителя"""
        stats = {}
        try:
            start_date = (        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as count FROM shifts 
                WHERE driver_id = ? AND start_timem-%d %H:%M:%S')
            
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as count FROM shifts 
                WHERE driver_id = ? AND start_time >= ? AND status = 'completed'
 >= ? AND status = 'completed'
            ''', (driver_id, start_date))
            result = await cursor.fetch            ''', (driver_id, start_date))
            result = await cursor.fetchone()
            stats['shifts_count'] = result['count']one()
            stats['shifts_count'] = result['count'] if result else 0
            await cursor.close()
            
            cursor = await self.conn.execute('''
                if result else 0
            await cursor.close()
            
            cursor = await self.conn.execute('''
                SELECT AVG((julianday SELECT AVG((julianday(end_time) - julianday(start_time)) * 24) as avg(end_time) - julianday(start_time)) * 24) as avg_hours
                FROM shifts 
                WHERE driver_id = ? AND end_time_hours
                FROM shifts 
                WHERE driver_id = ? AND end_time IS NOT NULL AND IS NOT NULL AND start_time >= ? AND status = 'completed'
            '' start_time >= ? AND status = 'completed'
            ''', (driver_id, start', (driver_id, start_date))
            result = await cursor.fetch_date))
            result = await cursor.fetchone()
            stats['avg_shiftone()
            stats['avg_shift_hours'] = round(result['avg_hours'] = round(result['avg_hours'], 1) if result_hours'], 1) if result and result['avg_hours'] else and result['avg_hours'] else 0
            await cursor.close()
 0
            await cursor.close()
            
            cursor = await self.conn            
            cursor = await self.conn.execute('''
                SELECT COUNT(D.execute('''
                SELECT COUNT(DISTINCT equipment_id) as count FROMISTINCT equipment_id) as count FROM shifts 
                WHERE driver_id shifts 
                WHERE driver_id = ? AND start_time >= ? AND status = 'completed'
            ''', (driver = ? AND start_time >= ? AND status = 'completed'
            ''', (driver_id, start_date))
            result =_id, start_date))
            result = await cursor.fetchone()
            stats[' await cursor.fetchone()
            stats['equipment_used'] =equipment_used'] = result result['count'] if result else ['count'] if result else 0
0
            await cursor.close()
            
        except Exception as e:
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения статисти            logger.error(f"Ошибка полученияки водителя {driver_id}: статистики водителя {driver_id}: {e}")
        
        return {e}")
        
        return stats
    
    # ========== НОВЫ stats
    
    # ========== НОВЫЕ МЕТОДЫ ДЕ МЕТОДЫ ДЛЯ ИИ ==========
    
    asyncЛЯ ИИ ==========
    
    async def add_ai_context(self def add_ai_context(self, organization_id: int, context_type: str, organization_id: int, context_type: str, equipment_model: str,, equipment_model: str, 
                           
                           question: str, answer: str, source: str = question: str, answer: str, source: str = 'ai 'ai') -> Optional[int]:
       ') -> Optional[int]:
        """Добавля """Добавляет контекст для ИИ"""
        try:
            cursor =ет контекст для ИИ"""
        try:
            cursor = await self.conn.execute(
 await self.conn.execute(
                """INSERT INTO ai_context 
                               """INSERT INTO ai_context 
                (organization_id, context_type, equipment_model, (organization_id, context_type, equipment_model, question, answer, source) 
                VALUES (?, ?, ?, ?, question, answer, source) 
                VALUES (?, ?, ?, ?, ?, ?)""",
                (organization ?, ?)""",
                (organization_id, context_type, equipment_model, question,_id, context_type, equipment_model, question, answer, source)
            )
 answer, source)
            )
            await            await self.conn.commit()
            return cursor.lastrowid
 self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger        except Exception as e:
            logger.error(f"Ошибка добавления AI контекста.error(f"Ошибка добавления AI контекста: {e}")
: {e}")
            return None
    
    async def get_ai_context(self            return None
    
    async def get_ai_context(self, organization_id:, organization_id: int = None, context_type: str = None, int = None, context_type: str = None, 
                           equipment_model: str = None, limit: int = 
                           equipment_model: str = None, limit: int = 5 5) -> List[Dict]:
        """Получает контекст) -> List[Dict]:
        """Получа для ИИ"""
        tryет контекст для ИИ"""
        try:
            query = "SELECT * FROM ai_context WHERE 1=:
            query = "SELECT * FROM ai_context1"
            params = []
            
            WHERE 1=1"
            params = []
            
            if organization_id:
 if organization_id:
                query                query += " AND organization_id = ?"
                params.append( += " AND organization_id = ?"
                params.append(organization_id)
            
            if context_type:
                query +=organization_id)
            
            if context_type:
                query += " AND context_type " AND context_type = ?"
                = ?"
                params.append(context_type)
            
            if equipment_model:
                params.append(context_type)
            
            if equipment_model:
                query += " AND equipment_model LIKE ?"
                params.append query += " AND equipment_model LIKE ?"
                params.append(f"%{equipment_model}%")
            
            query(f"%{equipment_model}%")
            
            query += " ORDER BY += " ORDER BY usage_count DESC, created_at DESC LIMIT ?"
            usage_count DESC, created_at DESC LIMIT ?"
            params.append(limit params.append(limit)
            
            cursor = await self.conn.execute)
            
            cursor = await self.conn.execute(query, params)
            rows(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            = await cursor.fetchall()
            await cursor.close()
            return [dict(row return [dict(row) for row in) for row in rows]
        except Exception as e:
            logger rows]
        except Exception as e:
.error(f"Ошибка получения AI контекста: {e}")
                       logger.error(f"Ошибка получения AI контекста: return []
    
    async def increment_ai_usage(self, context_id: {e}")
            return []
    
    async def increment_ai int) -> bool:
        """У_usage(self, context_id: int) -> bool:
        """Увеличивает счетчик использования AI контекста"""
       величивает счетчик использования AI контекста"""
        try:
            await try:
            await self.conn.execute self.conn.execute(
                "UPDATE ai_context SET usage_count = usage_count(
                "UPDATE ai_context SET usage_count = usage_count + 1 WHERE id = ?",
                ( + 1 WHERE id = ?",
                (context_id,)
            )
context_id,)
            )
            await self.conn.commit()
            return True            await self.conn.commit()
            return True
        except Exception as e
        except Exception as e:
           :
            logger.error(f"Ошибка обновления AI использования: { logger.error(f"Ошибка обновления AI использования: {e}")
            return False
    
    # =========e}")
            return False
    
    # ========== НОВЫЕ М= НОВЫЕ МЕТОДЕТОДЫ ДЛЯ ТОПЛИВА ==========
Ы ДЛЯ ТОПЛИВА ==========
    
       
    async def add_fuel_log(self, equipment_id: int, driver async def add_fuel_log(self,_id: int, fuel_amount: float, 
                         fuel_type: str = equipment_id: int, driver_id: int, fuel_amount: float, 
                         fuel_type: str = 'd 'diesel', cost_per_literiesel', cost_per_liter: float = None,
                         total_cost: float: float = None,
                         total_cost: float = None, odometer_ = None, odometer_reading: int = None,
                         fueling_stationreading: int = None,
                         fueling_station: str = None, receipt: str = None, receipt_photo: str = None, 
                         notes_photo: str = None, 
                         notes: str = None: str = None) -> Optional[int]:
        """Добавляет зап) -> Optional[int]:
        """Добавляет запись о заправись о заправке"""
        try:
            cursor = await self.ке"""
        try:
            cursor = await self.conn.execute(
                """conn.execute(
                """INSERT INTO fuel_logs 
                (equipmentINSERT INTO fuel_logs 
                (equipment_id, driver_id, fuel_id, driver_id, fuel_amount, fuel_type, cost_per_liter,_amount, fuel_type, cost_per_liter, 
                 total_cost, od 
                 total_cost, odometer_ometer_reading, fueling_station, receipt_photo, notes) 
               reading, fueling_station, receipt_photo, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ?, ?)""",
                (equipment_id, driver_id, fuel_amount (equipment_id, driver_id, fuel_amount, fuel, fuel_type, cost_per_liter,
                 total_cost, odometer__type, cost_per_liter,
                 total_cost, odometer_reading, fueling_station,reading, fueling_station, receipt_photo, notes)
            )
            
            receipt_photo, notes)
            )
            
            # Обновляем теку # Обновляем текущий уровень топлива в технике
           щий уровень топлива в технике await self.conn.execute(
                "UPDATE equipment SET current_fuel_level =
            await self.conn.execute(
                "UPDATE equipment SET current_fuel current_fuel_level + ? WHERE id = ?",
                (fuel_amount,_level = current_fuel_level + ? WHERE id = ?",
                (fuel_amount, equipment_id)
            )
            
            # Обновляем одометр если указа equipment_id)
            )
            
            # Обновляем одометр если указан
            if odometerн
            if odometer_reading:
                await self.conn.execute(
_reading:
                await self.conn.execute(
                    "UPDATE equipment SET od                    "UPDATE equipment SET odometer = ? WHERE id = ?",
                    (ometer = ? WHERE id = ?",
odometer_reading, equipment_id)
                )
            
            await self                    (odometer_reading, equipment_id)
                )
            
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e.conn.commit()
            return cursor.lastrowid
        except Exception:
            logger.error(f"Ошибка добавления записи о топли as e:
            logger.error(f"Ошибка добавления записи ове: {e}")
            return None
    
    async def get_fuel_log топливе: {e}")
            return None
    
    async def get_fs(self, equipment_id: int = None, driver_id: int = Noneuel_logs(self, equipment_id: int = None, driver_id: int = None, 
                          days: int = 30) -> List[Dict, 
                          days: int = 30) -> List[Dict]:
        """Получает записи о зап]:
        """Получает записи о заправках"""
        try:
равках"""
        try:
            query            query = """
                SELECT fl.*, e.name as equipment_name, u = """
                SELECT fl.*, e.name as equipment_name, u.full_name.full_name as driver_name 
                FROM as driver_name 
                FROM fuel_log fuel_logs fl
                LEFT JOIN equipment e ON fl.equipment_ids fl
                LEFT JOIN equipment e ON fl.equipment_id = e = e.id
                LEFT JOIN users u ON fl.driver_id = u.telegram_id
               .id
                LEFT JOIN users u ON fl.driver_id = u.telegram_id
                WHERE 1=1
            """
 WHERE 1=1
            """
            params = []
            
            if equipment            params = []
            
            if equipment_id:
                query += " AND fl.equ_id:
                query += " AND flipment_id = ?"
                params.append(equipment_id)
            
            if.equipment_id = ?"
                params.append(equipment_id)
            
            if driver_id:
                query += " AND fl.driver_id = ? driver_id:
                query += " AND fl.driver_id = ?"
               "
                params.append(driver_id)
            
            if days:
                query += params.append(driver_id)
            
            if days:
                query += " AND DATE(fl.fueling_date) " AND DATE(fl.fueling_date) >= DATE('now', ? >= DATE('now', ?)"
                params.append(f'-{days)"
                params.append(f'-{days} days')
            
            query += "} days')
            
            query += " ORDER BY fl.fueling ORDER BY fl.fueling_date DESC"
            
            cursor = await_date DESC"
            
            cursor = await self. self.conn.execute(query, params)
            rows = await cursor.fetchall()
conn.execute(query, params)
            rows = await cursor.fetchall()
            await            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения записей о топливе: {e}")
"Ошибка получения записей о топливе: {e}")
            return []
    
    async def get_fuel_statistics(self, equipment_id            return []
    
    async def get_fuel_statistics(self, equipment_id: int: int, days: int = 30) -> Dict:
       , days: int = 30) -> Dict:
        """Получает статистику по топливу"""
        stats = {}
        """Получает статистику по топливу"""
        stats = {}
        try:
            # Общее количество топ try:
            # Общее количество топлива
            cursor = await self.conn.execute('''
                SELECT SUMлива
            cursor = await self.conn.execute('''
                SELECT SUM(fuel_amount) as total_fuel(fuel_amount) as total_fuel, SUM(total_cost) as total_cost,
                       AVG(cost_per_l, SUM(total_cost) as total_cost,
                       AVG(cost_per_liter) as avg_price
                FROMiter) as avg_price
                FROM fuel_logs 
                WHERE equipment_id fuel_logs 
                WHERE equipment_id = ? AND DATE(fueling_date = ? AND DATE(fueling_date) >= DATE('now', ?)
) >= DATE('now', ?)
            ''', (equipment_id,            ''', (equipment_id, f'-{days} days'))
            result = await cursor.fetchone()
            f'-{days} days'))
            result = await cursor.fetchone()
            stats.update(dict(result) if result else {})
            await cursor.close()
 stats.update(dict(result) if result else {})
            await cursor.close()
            
            # Средний расход (если есть данные об одомет            
            # Средний расход (если есть данные об одометре)
            cursor = await self.ре)
            cursor = await self.conn.execute('''
                SELECT fl1.odometer_reading as start_odo, fl2.odometerconn.execute('''
                SELECT fl1.odometer_reading as start_odo, fl2.odometer_reading as end_odo,
                      _reading as end_odo,
                       SUM(fl2.fuel_amount) as fuel_used
                SUM(fl2.fuel_amount) as fuel_used
                FROM fuel_logs fl1
                JOIN fuel FROM fuel_logs fl1
                JOIN fuel_logs fl2 ON fl1.equipment_id = fl2.equipment_id 
                    AND fl2.f_logs fl2 ON fl1.equipment_id = fl2.equipment_id 
                    AND fl2.fueling_date > fl1.fueling_date
                WHERE fl1.eueling_date > fl1.fueling_date
                WHERE fl1.equipment_id = ? 
                    AND DATE(fl1quipment_id = ? 
                    AND DATE(fl1.fueling_date) >= DATE('now', ?)
                GROUP BY fl1.id.fueling_date) >= DATE('now', ?)
                GROUP BY fl1.id
                ORDER BY fl1.fueling_date
                ORDER BY fl1.fueling_date
                LIMIT 1
                LIMIT 1
           
            ''', (equipment_id, f'-{days} days'))
 ''', (equipment_id, f'-{days} days'))
            
            result = await cursor.fetchone()
                       
            result = await cursor.fetchone()
            if result and result['end_odo'] and result['start_ if result and result['end_odo'] and result['start_odo'] and result['fuel_used']:
odo'] and result                km_traveled = result['end_odo'] - result['start['fuel_used']:
                km_traveled = result['end_odo'] - result['start_odo']
                if km_odo']
                if km_traveled > 0 and result_traveled > 0 and result['fuel_used'] > 0:
                    stats['avg_consumption['fuel_used'] > 0:
                   '] = round((result['fuel_used'] / km_traveled) stats['avg_consumption'] = round((result['fuel_used'] / km_traveled) *  * 100, 2)  # л/100км
                   100, 2)  # л/100км
                    stats['km_traveled'] = km_t stats['km_traveled'] = km_traveled
            
           raveled
            
            await cursor.close()
            
        except Exception as e await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики топлива:
            logger.error(f"Ошибка получения статистики топлива: {: {e}")
        
        return stats
    
    async def get_low_fe}")
        
        return stats
    
    async def get_low_fuel_equipment(self, organization_id: intuel_equipment(self, organization_id: int, threshold: float, threshold: float = 20. = 20.0) -> List[Dict]:
        """Получает технику с низким уровнем топлива0) -> List[Dict]:
        """Получает технику с низким уровнем топлива"""
        try:
            cursor = await self.conn"""
        try:
            cursor = await self.execute('''
                SELECT e.*, 
                       (e.current_fuel.conn.execute('''
                SELECT e.*, 
                       (e.current_level / e.fuel_capacity * 100) as fuel_percentage
_fuel_level / e.fuel_capacity * 100) as fuel_percentage
                FROM equipment e
                               FROM equipment e
                WHERE e.organization_id = ? 
                  AND e.fuel_capacity > 0 WHERE e.organization_id = ? 
                  AND e.fuel_capacity > 0 
                  AND e 
                  AND e.current_fuel_level / e.fuel_capacity * .current_fuel_level / e.fuel_capacity * 100 < ?
                ORDER BY100 < ?
                ORDER BY fuel_percentage
            ''', (organization_id, threshold))
            rows = await cursor fuel_percentage
            ''', (organization_id, threshold))
            rows = await cursor.fetchall()
            await.fetchall()
            await cursor.close()
            return [dict(row) for row cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error in rows]
        except Exception as e:
            logger.error(f"Ошибка получения техники с низким топ(f"Ошибка получения техникиливом: {e}")
            return []
    
    # ========== Н с низким топливом: {e}")
            return []
    
    # ========== НОВЫЕ МОВЫЕ МЕТОДЫ ДЛЯ ЗАПЧАСТЕЙ ==========
    
    asyncЕТОДЫ ДЛЯ ЗАПЧАСТЕЙ ==========
    
    async def add_spare def add_spare_part(self, organization_part(self, organization_id: int, part_name: str, part_number: str = None,
                           description: str_id: int, part_name: str, part_number: str = None,
                           description: str = None, category: str = None, quantity = None, category: str = None, quantity: int = 0,
: int = 0,
                           min_quantity: int = 1, supplier:                           min_quantity: int = 1, supplier: str = None, supplier_contact: str = None,
 str = None, supplier_contact: str = None,
                           unit_price: float = None, location:                           unit_price: float = None, location: str = None, notes: str = None, notes: str = None) -> Optional[int]:
        """ str = None) -> Optional[int]:
        """Добавляет запчасть в склад"""
       Добавляет запчасть в склад"""
        try:
            cursor = await self.conn.execute(
                """INSERT try:
            cursor = await self.conn.execute(
                """INSERT INTO spare_parts 
                (organization_id, INTO spare_parts 
                (organization_id, part_name, part_number, part_name, part_number, description, category, 
                 quantity, min_ description, category, 
                 quantity, min_quantity, supplier, supplier_contact, unit_price, 
                 locationquantity, supplier, supplier_contact, unit_price, 
                 location, notes) 
                VALUES (?, ?, ?,, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (organization_id ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (organization_id, part_name, part_number, description, category,
                 quantity,, part_name, part_number, description, category,
                 quantity, min_ min_quantity, supplier, supplier_contact, unit_price,
                 location, notes)
            )
            awaitquantity, supplier, supplier_contact, unit_price,
                 location, notes)
 self.conn.commit()
            return cursor.lastrowid
        except            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка Exception as e:
            logger.error(f"Ошибка добавления запчасти: {e}")
            return None
    
    async def добавления запчасти: {e}")
            return None
    
    async def get_sp get_spare_parts(self, organization_id: int, category: str = None, 
                            low_stock_onlyare_parts(self, organization_id: int, category: str = None, 
                            low_stock_only: bool = False) -> List[Dict]:
        """Получает список: bool = False) -> List[Dict]:
        """Получает список запчастей"""
        try:
            query = запчастей"""
        try:
            "SELECT * FROM spare_parts WHERE organization_id = ?"
            params = query = "SELECT * FROM spare_parts WHERE organization_id = ? [organization_id]
            
            if category:
                query += " AND category = ?"
                params.append(c"
            params = [organization_id]
            
            if category:
                query += " AND category = ?"
                params.append(category)
            
            if low_stockategory)
            
            if low_stock_only:
                query += " AND quantity <= min_only:
                query += " AND quantity <= min_quantity"
            
            query += " ORDER BY part_name"
            
_quantity"
            
            query += " ORDER BY part_name"
            
            cursor = await self.conn.execute(query,            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in()
            return [dict(row) for row in rows]
        except Exception as rows]
        except Exception as e:
            logger.error(f"Ошибка получения запчастей: {e}")
 e:
            logger.error(f"Ошибка получения запчастей: {e}")
            return []
    
    async            return []
    
    async def update def update_spare_part_quantity(self, part_id: int, quantity_change: int) -> bool:
        """_spare_part_quantity(self, part_id: int, quantity_change: int) -> bool:
        """ОбновОбновляет количество запчастей на складе"""
        try:
            awaitляет количество запчастей на складе"""
        try:
            await self.conn.execute(
                "UPDATE spare_parts SET quantity = quantity + self.conn.execute(
                "UPDATE spare_parts SET quantity = quantity + ? WHERE id = ?",
                (quantity_change, part_id)
            )
 ? WHERE id = ?",
                (quantity_change, part_id)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошиб            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления количества запчастейка обновления количества запчастей: {e}")
            return False
    
    # ========== НОВЫ: {e}")
            return False
    
    # ========== НОВЕ МЕТОДЫ ДЛЯ ЗАКАЗОВ ==========
ЫЕ МЕТОДЫ ДЛЯ ЗАКАЗОВ ==========
    
    async def create_order(self, organization_id: int, order    
    async def create_order(self, organization_id: int, order_type: str, equipment_id: int_type: str, equipment_id: int = None,
                         part_id: int = None, quantity: int = 1, urgent: bool = False,
 = None,
                         part_id: int = None, quantity: int = 1, urgent: bool = False,
                         requested_by: int = None, notes: str = None) -> Optional                         requested_by: int = None, notes: str = None) -> Optional[int]:
        """Создает новый заказ"""
        try:
            cursor[int]:
        """Создает новый заказ"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO orders 
                (organization_id, order_type, equipment_id, part_id, quantity, 
                 urgent = await self.conn.execute(
                """INSERT INTO orders 
                (organization_id, order_type, equipment_id, part_id, quantity, 
                 urgent, requested_by, notes) 
               , requested_by, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (organization_id, order_type, equipment_id, VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (organization_id, order_type, equipment_id, part_id, quantity,
                 part_id, quantity,
                 urgent, requested_by, notes)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception urgent, requested_by, notes)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f" as e:
            logger.error(f"Ошибка создания заказа: {e}")
            return None
    
    asyncОшибка создания заказа: {e}")
            return None
    
    async def get_orders(self, organization_id: int, status: str = None, 
                       order_type: def get_orders(self, organization_id: int, status: str = None, 
                       order_type: str = str = None) -> List[Dict]:
        """Получает список за None) -> List[Dict]:
        """Получает список заказовказов"""
        try:
            query = """
                SELECT o.*, e.name as equipment_name, p.part"""
        try:
            query = """
                SELECT o.*, e.name as equipment_name, p.part_name,
_name,
                u1.full_name as requested_by_name, u2.full_name as approved_by_name
                FROM orders                u1.full_name as requested_by_name, u2.full_name as approved_by_name
                FROM orders o
                LEFT JOIN equipment e ON o.e o
                LEFT JOIN equipment e ON o.equipment_id = e.id
                LEFT JOIN spare_parts pquipment_id = e.id
                LEFT JOIN spare_parts p ON o ON o.part_id = p.id
                LEFT JOIN users u1 ON.part_id = p.id
                LEFT JOIN users u1 ON o.requested_by = u1.telegram_id
                LEFT JOIN users u2 o.requested_by = u1.telegram_id
                LEFT JOIN users u2 ON o.approved_by ON o.approved_by = u2.telegram_id
                WHERE = u2.telegram_id
                WHERE o.organization_id = ?
            """
            params = o.organization_id = ?
            """
            params = [organization_id]
            
            if status:
                query += " AND o.status = ?"
                params.append(status)
            
            if [organization_id]
            
            if status:
                query += " AND o.status = ?"
 order_type:
                query += " AND o.order_type = ?"
                params.append(status)
            
            if order_type:
                query += " AND o.order_type = ?"
                params.append(order_type)
            
            query += " ORDER BY o.created_at DESC"
                params.append(order_type)
            
            query += " ORDER BY o            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.created_at DESC"
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict.fetchall()
            await cursor(row) for row in rows]
        except Exception as e:
            logger.error.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения заказов: {(f"Ошибка получения заказов: {e}")
            return []
    
    async def get_orders_bye}")
            return []
    
    async def get_orders_by_id(self, order_id: int) -> Optional[Dict_id(self, order_id: int) -> Optional[Dict]:
        """Получает заказ по ID"""
        try:
]:
        """Получает заказ по ID"""
        try:
            cursor = await self.conn.execute(
                "SELECT            cursor = await self.conn.execute * FROM orders WHERE id = ?",
                (order_id,)
            )
            row = await cursor.fetchone()
            await(
                "SELECT * FROM orders WHERE id = ?",
                (order_id,)
            )
            row = await cursor.fetch cursor.close()
            return dict(row) if row else None
        except Exceptionone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения заказа as e:
            logger.error(f"Ошибка получения заказа {order_id}: {e}")
            return None {order_id}: {e}")
            return None
    
    async def update_order_status(self, order_id: int,
    
    async def update_order_status(self, order_id: int, status: status: str, approved_by: int = None) -> bool:
        """Обновляет статус за str, approved_by: int = None) -> bool:
        """Обновляет статус заказа"""
        try:
            if approved_by:
                awaitказа"""
        try:
            if approved_by:
                await self.conn.execute(
                    "UPDATE orders SET self.conn.execute(
                    "UPDATE status = ?, approved_by = ? WHERE id = ?",
                    (status, approved_by orders SET status = ?, approved_by = ? WHERE id = ?",
                    (status, approved_by, order, order_id)
                )
            else:
                await self.conn.execute(
                    "UPDATE orders SET status = ?_id)
                )
            else:
                await self.conn.execute(
                    "UPDATE orders SET status = ? WHERE id = ?",
                    (status WHERE id = ?",
                    (status, order_id)
                )
            
            await self.conn.commit()
            return, order_id)
                )
            
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f True
        except Exception as e:
            logger.error(f"Ошибка обновления статуса заказа:"Ошибка обновления статуса заказа: {e}")
            return False
    
    # ========== {e}")
            return False
    
    # ========== НОВЫЕ НОВЫЕ МЕТОДЫ ДЛЯ ИНСТРУ МЕТОДЫ ДЛЯ ИНСТРУКЦИЙ ==========
    
    async def add_КЦИЙ ==========
    
    async def add_instruction(self, equipment_model: str, instruction_type: strinstruction(self, equipment_model: str, instruction_type: str, 
                            title: str, description: str, 
                            title: str, description: str = None, steps: str = None,
                            diagram_ = None, steps: str = None,
                            diagram_photo:photo: str = None, video_url: str = None) -> Optional[int]:
        """Добавляет инструкцию"""
 str = None, video_url: str = None) -> Optional[int]:
        """Добавляет инструкцию"""
        try:
            cursor = await self.conn        try:
            cursor = await self.conn.execute(
                """INSERT INTO instructions 
                (equipment_model,.execute(
                """INSERT INTO instructions 
                (equipment_model, instruction_type instruction_type, title, description, steps, 
                 diagram_photo, video, title, description, steps, 
                 diagram_photo, video_url) 
                VALUES (?, ?, ?, ?,_url) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
 ?, ?, ?)""",
                (equipment_model, instruction_type, title, description, steps,
                 diagram_                (equipment_model, instruction_type, title, description, steps,
                photo, video_url)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception diagram_photo, video_url)
            )
            await self.conn.commit()
            return cursor.lastrowid as e:
            logger.error(f"Ошибка добавления инструкции: {e}")
            return None
    
   
        except Exception as e:
            logger.error(f"Ошибка добавления инструкции: {e}")
            async def get_instructions(self, equipment_model: str = None, 
                             instruction_type: str = None) -> return None
    
    async def get_instructions(self, equipment_model: str = None, 
                             instruction_type: str = None) -> List[Dict]:
 List[Dict]:
        """Получает инструкции"""
        try:
                   """Получает инструкции"""
        try:
            query = "SELECT * FROM instructions WHERE 1=1 query = "SELECT * FROM instructions WHERE 1=1"
            params ="
            params = []
            
            if equipment_model:
                query += " AND equipment_model LIKE ?"
                params.append []
            
            if equipment_model:
                query += " AND equipment_model LIKE ?"
                params.append(f"%{equipment_model}%")
            
(f"%{equipment_model}%")
            
            if instruction_type:
                query += " AND instruction_type = ?            if instruction_type:
                query += " AND instruction_type = ?"
                params.append(instruction_type)
            
           "
                params.append(instruction_type)
 query += " ORDER BY title"
            
            cursor = await self.conn.execute(query, params)
            rows = await            
            query += " ORDER BY title"
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.fetchall()
            await cursor.close cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"О"Ошибка получения инструкций: {e}")
            return []
    
    # ========== НОВЫЕшибка получения инструкций: {e}")
            return []
    
    # ========== НОВЫЕ МЕТОДЫ ДЛЯ РАСШИРЕННОГО ТО ==========
    
    async МЕТОДЫ ДЛЯ РАСШИРЕННОГО ТО ==========
    
    async def add def add_maintenance_schedule(self, equipment_id: int, maintenance_type: str,
                                     interval_km: int_maintenance_schedule(self, equipment_id: int, maintenance_type: str,
 = None, interval_days: int = None,
                                     description: str = None, parts_needed: str =                                     interval_km: int = None, interval_days: int = None,
                                     description: str = None, None,
                                     estimated_hours: float = None) -> Optional[int]:
        parts_needed: str = None,
                                     estimated_hours: float = None) -> Optional[int]:
        """Д """Добавляет расписание ТО"""
        try:
            # Вычисляем следующую дату/побавляет расписание ТО"""
        try:
            # Вычисляем следующую дату/пробег
            next_due_kmробег
            next_due_km = None
            next_due_date = None
            
            if interval = None
            next_due_date = None
            
            if interval_km:
                equipment = await self.get__km:
                equipment = await selfequipment_by_id(equipment_id)
                if equipment and equipment.get('odometer'):
                    next_d.get_equipment_by_id(equipment_id)
                if equipment and equipment.get('odometer'):
                    next_due_km = equipment['odometer'] + interval_km
            
            if interval_days:
                next_due_date =ue_km = equipment['odometer'] + interval_km
            
            if interval_days:
                next_due (datetime.now() + timedelta(days=interval_days)).strftime('%Y-%m-%d')
            
_date = (datetime.now() + timedelta(days=interval_days)).strftime('%Y-%m-%d            cursor = await self.conn.execute(
                """INSERT INTO maintenance_schedule 
                (equipment_id, maintenance')
            
            cursor = await self.conn.execute(
                """INSERT INTO maintenance_schedule 
                (equipment_id, maintenance_type,_type, interval_km, interval_days,
                 last_done_km, last_done_date, next_d interval_km, interval_days,
                 last_done_km, last_done_date,ue_km, next_due_date,
                 description, parts_needed, next_due_km, next_due_date,
                 description, parts_needed, estimated_hours) 
                VALUES (?, ?, ?, ?, ?, ?, estimated_hours) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ( ?, ?, ?, ?, ?)""",
                (equipment_id, maintenance_type, interval_km, interval_daysequipment_id, maintenance_type, interval_km, interval_days,
                 0, None, next_due_,
                 0, None, next_due_km, next_due_date, description, 
                 parts_needed, estimatedkm, next_due_date, description, 
                 parts_needed, estimated_hours_hours)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добав as e:
            logger.error(f"Ошибления расписания ТО: {e}")
            return None
    
    async def get_upcoming_maintenanceка добавления расписания ТО: {e}")
            return None
    
    async def get_upcoming_maintenance(self, organization_id: int, days(self, organization_id: int, days_ahead: int = 30) -> List[Dict]:
        """Получает предстоящие ТО"""
_ahead: int = 30) -> List[Dict]:
        """Получает предстоящие ТО"""
        try        try:
            query = """
                SELECT ms.*, e.name as equipment_name, e.model, e.od:
            query = """
                SELECT ms.*, e.name as equipment_name, e.model, e.odometer,
                       e.organization_id, o.nameometer,
                       e.organization_id, o.name as org_name
                FROM maintenance_schedule ms
                JOIN equipment as org_name
                FROM maintenance_schedule ms
                JOIN equipment e ON ms.equipment_id = e.id e ON ms.equipment_id =
                JOIN organizations o ON e.organization_id = o.id
                WHERE e.organization_id = ? e.id
                JOIN organizations o ON e.organization_id = o.id
                WHERE e.organization_id = ? AND (
                    (ms.next_due_date IS NOT NULL AND ms AND (
                    (ms.next_due_date IS NOT NULL AND ms.next_d.next_due_date <= DATE('now', ?)) OR
                    (ms.next_due_km ISue_date <= DATE('now', ?)) OR
                    (ms.next_due_km IS NOT NULL AND ms.next_due_km <= e. NOT NULL AND ms.next_due_km <= e.odometer + ?)
                )
               odometer + ?)
                )
                ORDER BY ms.next_due_date, ORDER BY ms.next_due_date, ms.next_due_km
            """
            params = [organization_id, ms.next_due_km
            """
            params = [organization_id, f'+{days_ahead} days', 500]  # 500 км вперед
            
            cursor f'+{days_ahead} days', 500]  # 500 км вперед
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения предстоящих ТО: {e}")
            return []
    
    async def complete.error(f"Ошибка получения предстоящих ТО: {e}")
            return []
    
    async def complete_maintenance(self, schedule_id: int, odometer: int = None) -> bool:
        """Отмечает_maintenance(self, schedule_id: int, odometer: int = None) -> bool:
        """Отмечает ТО как выполненное"""
        ТО как выполненное"""
        try:
            # Получаем данные о расписании
            cursor = await self.conn.execute(
                " try:
            # Получаем данные о расписании
            cursor = await self.conn.execute(
                "SELECT * FROM maintenance_schedule WHERE idSELECT * FROM maintenance_schedule WHERE id = ?",
                (schedule_id,)
            )
            schedule = await = ?",
                (schedule_id,)
            )
            schedule = await cursor.fetch cursor.fetchone()
            await cursor.close()
            
            if not schedule:
                return False
            
            # Обновляем последнее выполнение
            update_queryone()
            await cursor.close()
            
            if not schedule:
                return False
            
            # Обновляем последнее выполнение
            update_query = """
                UPDATE maintenance_schedule 
 = """
                UPDATE maintenance_schedule 
                SET last_done_date = DATE('now'), last_done_km                SET last_done_date = DATE('now'), last_done_km = ?
            """
            params = = ?
            """
            params = [odometer]
            
            # Вычисляем следующую дату/пробег
            if schedule['interval [odometer]
            
            # Вычисляем следующую дату/пробег
            if schedule['interval_days']:
                update_days']:
                update_query += ", next_due_date = DATE('now', ?)"
                params.append(f_query += ", next_due_date = DATE('now', ?)"
                params.append(f'+{schedule["interval_days'+{schedule["interval_days"]} days')
            
            if schedule['interval_km'] and odometer"]} days')
            
            if schedule['interval_km'] and odometer:
                update_query += ", next_due_km = ?"
                params.append(odometer + schedule['interval:
                update_query += ", next_due_km = ?"
                params.append(odometer + schedule['interval_km'])
            
            update_km'])
            
            update_query += " WHERE id = ?"
            params.append(schedule_id)
            
            await_query += " WHERE id = ?"
            params.append(schedule_id)
            
            await self.conn.execute(update_query, self.conn.execute(update_query, params)
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f" params)
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка завершения ТО: {eОшибка завершения ТО: {e}")
            return False
    
    # ========== ВСПОМО}")
            return False
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========
    
    async def assign_role_to_user(self, user_id:ГАТЕЛЬНЫЕ МЕТОДЫ ==========
    
    async def assign_role_to_user(self, user_id: int, role: str, organization_id: int = None) -> bool:
        """Назначает роль пользователю"""
        int, role: str, organization_id: int = None) -> bool:
        """Назначает роль пользователю"""
        try:
            if organization_id:
                await self try:
            if organization_id:
                await self.conn.execute(
                    "UPDATE users SET role = ?, organization_id = ?.conn.execute(
                    "UPDATE users SET role = ?, organization_id = ? WHERE telegram_id = ?",
                    (role, WHERE telegram_id = ?",
                    (role, organization_id, user_id)
                )
            else:
                await self.conn.execute(
                    "UPDATE users SET role = ? WHERE telegram organization_id, user_id)
                )
            else:
                await self.conn.execute(
                    "UPDATE users SET role = ? WHERE telegram_id = ?",
                    (role, user_id)
                )
            await self_id = ?",
                    (role, user_id)
                )
            await self.conn.commit()
            return True
        except.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка назначения роли Exception as e:
            logger.error(f"Ошибка назначения роли {user_id}: {e}")
            return False {user_id}: {e}")
            return False
    
    async def get_organization_
    
    async def get_organization_analytics(self, org_id: int, period_days: int = 30) -> Dict:
        """Получаетanalytics(self, org_id: int, period_days: int = 30) -> Dict:
        """Получает аналитику организации"""
        analytics = {}
        try:
            start_date = (datetime аналитику организации"""
        analytics = {}
        try:
            start_date = (datetime.now() - timedelta(days=period_days)).strftime('%Y-%m-%d')
            
            # Статистика по.now() - timedelta(days=period_days)).strftime('%Y-%m-%d')
            
            # Статистика по сменам
            cursor = await self.conn.execute('''
                сменам
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as total_shifts,
                       SUM SELECT COUNT(*) as total_shifts,
(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_shifts,
                       AVG                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_shifts,
                       AVG((julianday(end_time) - juli((julianday(end_time) - julianday(start_time)) * 24) as avg_shift_hours
                FROM shifts s
                JOIN users u ONanday(start_time)) * 24) as avg_shift_hours
                FROM shifts s
                JOIN users u ON s.driver_id = u.telegram_id
                WHERE u.organization_id = s.driver_id = u.telegram_id
                WHERE u.organization_id = ? AND s.start_time >= ?
            ? AND s.start_time >= ?
            ''', (org_id, start_date))
            shift_stats = await cursor.fetchone()
            analytics ''', (org_id, start_date))
            shift_stats = await cursor.fetchone()
['shifts'] = dict(shift_stats) if shift_stats else {}
            await cursor            analytics['shifts'] = dict(shift_stats) if shift_stats else {}
           .close()
            
            # Статистика по топливу
            cursor = await self.conn.execute('''
 await cursor.close()
            
            # Статистика по топливу
            cursor = await self.conn.execute('''
                SELECT SUM(fl.fuel_amount) as total_fuel,
                       SUM(fl                SELECT SUM(fl.fuel_amount) as total_fuel,
                       SUM(fl.total_cost) as total_fuel_cost,
                      .total_cost) as total_fuel_cost,
                       AVG(fl.cost AVG(fl.cost_per_liter) as avg_fuel_price
                FROM fuel_logs fl
                JOIN equipment_per_liter) as avg_fuel_price
                FROM fuel_logs fl
                JOIN equipment e ON fl.equipment_id = e.id
                WHERE e.organization e ON fl.equipment_id = e.id
                WHERE e.organization_id =_id = ? AND DATE(fl.fueling_date) >= ?
            ''', (org_id, start_date))
            fuel ? AND DATE(fl.fueling_date) >= ?
            ''', (org_id, start_date))
            fuel_stats =_stats = await cursor.fetchone()
            analytics['fuel'] = dict(fuel_stats) if fuel_stats else {}
            await cursor await cursor.fetchone()
            analytics['fuel'] = dict(fuel_stats) if fuel_stats else {}
            await cursor.close()
            
            # Статистика по ТО
.close()
            
            # Статистика по ТО
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as            cursor = await self.conn.execute('''
                SELECT COUNT(*) as total_maintenance,
                       SUM(CASE WHEN status total_maintenance,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_maintenance = 'completed' THEN 1 ELSE 0 END) as completed_maintenance,
                      ,
                       SUM(cost) as total SUM(cost) as total_maintenance_cost
                FROM maintenance m
                JOIN equipment e ON m.equipment_id = e.id_maintenance_cost
                FROM maintenance m
                JOIN equipment e ON m.equipment_id = e.id
                WHERE e.organization_id = ? AND m.created_at >= ?
            ''', (org_id,
                WHERE e.organization_id = ? AND m.created_at >= ?
            ''', (org_id, start_date))
            maintenance_stats = await cursor.fetchone()
 start_date))
            maintenance_stats = await cursor.fetchone()
            analytics['maintenance'] = dict(maintenance_stats) if            analytics['maintenance'] = dict(maintenance_stats) if maintenance_stats else {}
            await cursor.close()
            
            # Тех maintenance_stats else {}
            await cursor.close()
            
ника по статусам
            cursor = await self.conn.execute('''
                SELECT status, COUNT(*) as count
            # Техника по статусам
            cursor = await self.conn.execute('''
                SELECT status, COUNT(*) as count
                FROM equipment
                WHERE organization_id = ?
                GROUP BY status                FROM equipment
                WHERE organization_id = ?
                GROUP BY status
            ''', (org_id,))
            status_data
            ''', (org_id,))
            status_data = await cursor.fetchall()
            analytics['equipment_by_status'] = await cursor.fetchall()
            analytics['equipment_by_status'] = {row['status']: row['count'] for = {row['status']: row['count row in status_data}
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения аналити'] for row in status_data}
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения аналитики организации {org_id}: {e}")
        
        return analytics

ки организации {org_id}: {e}")
        
        return analytics

# Создаем глобальный экземпляр# Создаем глобальный экземпляр базы данных
db = Database()
