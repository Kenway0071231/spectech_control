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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    order_type TEXT NOT NULL,
                    equipment_id INTEGER,
                    part_id INTEGER,
                    quantity INTEGER,
                    urgent BOOLEAN DEFAULT FALSE,
                    status TEXT DEFAULT 'pending',
                    requested_by INTEGER,
                    approved_by INTEGER,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id),
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id),
                    FOREIGN KEY (part_id) REFERENCES spare_parts(id),
                    FOREIGN KEY (requested_by) REFERENCES users(telegram_id),
                    FOREIGN KEY (approved_by) REFERENCES users(telegram_id)
                )
            ''')
            
            # Таблица инструкций (ИСПРАВЛЕНА СТРОКА 200)
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS instructions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_model TEXT NOT NULL,
                    instruction_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    steps TEXT,
                    diagram_photo TEXT,
                    video_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица назначения техники водителям
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS driver_equipment (
                    driver_id INTEGER NOT NULL,
                    equipment_id INTEGER NOT NULL,
                    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (driver_id, equipment_id),
                    FOREIGN KEY (driver_id) REFERENCES users(telegram_id),
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id)
                )
            ''')
            
            # Таблица смен
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS shifts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_id INTEGER NOT NULL,
                    equipment_id INTEGER NOT NULL,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    start_odometer INTEGER,
                    end_odometer INTEGER,
                    briefing_confirmed BOOLEAN DEFAULT FALSE,
                    inspection_photo TEXT,
                    inspection_approved BOOLEAN DEFAULT FALSE,
                    approved_by INTEGER,
                    notes TEXT,
                    status TEXT DEFAULT 'active',
                    FOREIGN KEY (driver_id) REFERENCES users(telegram_id),
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id),
                    FOREIGN KEY (approved_by) REFERENCES users(telegram_id)
                )
            ''')
            
            # Таблица ежедневных проверок
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shift_id INTEGER NOT NULL,
                    check_type TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (shift_id) REFERENCES shifts(id)
                )
            ''')
            
            # Таблица технического обслуживания
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS maintenance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_id INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    scheduled_date DATE NOT NULL,
                    completed_date DATE,
                    description TEXT,
                    status TEXT DEFAULT 'scheduled',
                    cost REAL,
                    performed_by TEXT,
                    parts_used TEXT,
                    odometer_at_service INTEGER,
                    next_service_km INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id)
                )
            ''')
            
            # Таблица логов действий
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS action_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    action_type TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(telegram_id)
                )
            ''')
            
            # Таблица шаблонов проверок
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_check_templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_type TEXT NOT NULL,
                    item TEXT NOT NULL,
                    check_description TEXT NOT NULL,
                    order_index INTEGER DEFAULT 0
                )
            ''')
            
            # Таблица для AI контекста
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS ai_context (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER,
                    context_type TEXT NOT NULL,
                    equipment_model TEXT,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL,
                    source TEXT,
                    verified BOOLEAN DEFAULT FALSE,
                    verified_by INTEGER,
                    usage_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id),
                    FOREIGN KEY (verified_by) REFERENCES users(telegram_id)
                )
            ''')
            
            # Таблица настроек уведомлений
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS notification_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    organization_id INTEGER NOT NULL,
                    notification_type TEXT NOT NULL,
                    days_before INTEGER DEFAULT 7,
                    enabled BOOLEAN DEFAULT TRUE,
                    notify_director BOOLEAN DEFAULT TRUE,
                    notify_fleetmanager BOOLEAN DEFAULT TRUE,
                    notify_driver BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id)
                )
            ''')
            
            await self.conn.commit()
            
            # Добавляем шаблоны проверок
            await self.init_daily_checks()
            # Добавляем стандартные настройки уведомлений
            await self.init_notification_settings()
            
            logger.info("✅ Все таблицы созданы/проверены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
            raise
    
    async def init_daily_checks(self):
        """Инициализирует шаблоны ежедневных проверок"""
        checks = [
            ("engine", "Масло двигателя", "Проверить уровень и состояние"),
            ("engine", "Охлаждающая жидкость", "Проверить уровень"),
            ("engine", "Тормозная жидкость", "Проверить уровень"),
            ("engine", "Жидкость ГУР", "Проверить уровень"),
            ("tires", "Давление в шинах", "Проверить давление"),
            ("tires", "Протектор шин", "Проверить износ"),
            ("tires", "Внешний вид шин", "Проверить на порезы"),
            ("lights", "Фары ближнего света", "Проверить работу"),
            ("lights", "Фары дальнего света", "Проверить работу"),
            ("lights", "Стоп-сигналы", "Проверить работу"),
            ("lights", "Поворотники", "Проверить работу"),
            ("lights", "Габаритные огни", "Проверить работу"),
            ("safety", "Зеркала", "Проверить чистоту и регулировку"),
            ("safety", "Ремни безопасности", "Проверить исправность"),
            ("safety", "Огнетушитель", "Наличие и срок годности"),
            ("safety", "Аптечка", "Наличие и срок годности"),
            ("safety", "Знак аварийной остановки", "Наличие"),
            ("fluids", "Топливный фильтр", "Проверить на подтеки"),
            ("fluids", "Масляный фильтр", "Проверить на подтеки"),
            ("brakes", "Тормозные колодки", "Проверить износ"),
            ("brakes", "Тормозные диски", "Проверить состояние"),
            ("interior", "Приборная панель", "Проверить показания"),
            ("interior", "Звуковой сигнал", "Проверить работу"),
            ("interior", "Стеклоочистители", "Проверить работу"),
            ("interior", "Омыватель стекла", "Проверить уровень"),
            ("exterior", "Кузов", "Проверить на повреждения"),
            ("exterior", "Зеркала заднего вида", "Проверить целостность"),
            ("exterior", "Стекла", "Проверить на трещины"),
        ]
        
        for check_type, item, description in checks:
            try:
                await self.conn.execute(
                    "INSERT OR IGNORE INTO daily_check_templates (check_type, item, check_description) VALUES (?, ?, ?)",
                    (check_type, item, description)
                )
            except:
                pass
        
        await self.conn.commit()
    
    async def init_notification_settings(self):
        """Инициализирует настройки уведомлений"""
        settings = [
            ("maintenance", 7, True, True, True, False),
            ("maintenance", 1, True, True, True, True),
            ("fuel_low", 0, True, False, True, True),
            ("inspection_due", 0, True, True, True, False),
            ("order_approved", 0, True, True, True, False),
        ]
        
        for setting in settings:
            try:
                await self.conn.execute('''
                    INSERT OR IGNORE INTO notification_settings 
                    (notification_type, days_before, enabled, notify_director, notify_fleetmanager, notify_driver)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', setting)
            except:
                pass
        
        await self.conn.commit()
    
    # ========== БАЗОВЫЕ МЕТОДЫ ==========
    
    async def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получает пользователя по Telegram ID"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM users WHERE telegram_id = ?", 
                (telegram_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения пользователя {telegram_id}: {e}")
            return None
    
    async def register_user(self, telegram_id: int, full_name: str, username: str = None, role: str = 'unassigned') -> bool:
        """Регистрирует нового пользователя"""
        try:
            await self.conn.execute(
                "INSERT OR IGNORE INTO users (telegram_id, full_name, username, role) VALUES (?, ?, ?, ?)",
                (telegram_id, full_name, username, role)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка регистрации пользователя {telegram_id}: {e}")
            return False
    
    async def update_user_role(self, telegram_id: int, role: str, organization_id: int = None) -> bool:
        """Обновляет роль пользователя и организацию"""
        try:
            if organization_id:
                await self.conn.execute(
                    "UPDATE users SET role = ?, organization_id = ? WHERE telegram_id = ?",
                    (role, organization_id, telegram_id)
                )
            else:
                await self.conn.execute(
                    "UPDATE users SET role = ? WHERE telegram_id = ?",
                    (role, telegram_id)
                )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления роли {telegram_id}: {e}")
            return False
    
    async def get_all_users(self) -> List[Dict]:
        """Получает всех пользователей"""
        try:
            cursor = await self.conn.execute("SELECT * FROM users ORDER BY created_at DESC")
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    async def get_all_users_simple(self) -> List[Dict]:
        """Получает всех пользователей (упрощенная версия)"""
        try:
            cursor = await self.conn.execute(
                "SELECT telegram_id, full_name, role, organization_id FROM users ORDER BY created_at DESC"
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    async def get_users_by_organization(self, org_id: int) -> List[Dict]:
        """Получает пользователей организации"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM users WHERE organization_id = ? ORDER BY role, full_name",
                (org_id,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей организации {org_id}: {e}")
            return []
    
    async def get_organization(self, org_id: int) -> Optional[Dict]:
        """Получает организацию по ID"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM organizations WHERE id = ?", 
                (org_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения организации {org_id}: {e}")
            return None
    
    async def create_organization_for_director(self, director_id: int, org_name: str, address: str = None, contact_phone: str = None):
        """Создает организацию и назначает директора"""
        try:
            user = await self.get_user(director_id)
            if user and user.get('organization_id'):
                return None, "У этого пользователя уже есть организация"
            
            cursor = await self.conn.execute(
                "INSERT INTO organizations (name, director_id, address, contact_phone) VALUES (?, ?, ?, ?)",
                (org_name, director_id, address, contact_phone)
            )
            org_id = cursor.lastrowid
            
            await self.conn.execute(
                "UPDATE users SET organization_id = ?, role = 'director' WHERE telegram_id = ?",
                (org_id, director_id)
            )
            
            await self.conn.commit()
            return org_id, None
        except Exception as e:
            logger.error(f"Ошибка создания организации: {e}")
            return None, str(e)
    
    async def get_all_organizations(self) -> List[Dict]:
        """Получает все организации"""
        try:
            cursor = await self.conn.execute("SELECT * FROM organizations ORDER BY created_at DESC")
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения организаций: {e}")
            return []
    
    async def get_all_organizations_simple(self) -> List[Dict]:
        """Получает все организации (упрощенная версия)"""
        try:
            cursor = await self.conn.execute(
                "SELECT id, name, director_id FROM organizations ORDER BY created_at DESC"
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения организаций: {e}")
            return []
    
    async def update_organization_name(self, org_id: int, new_name: str) -> bool:
        """Обновляет название организации"""
        try:
            await self.conn.execute(
                "UPDATE organizations SET name = ? WHERE id = ?",
                (new_name, org_id)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления организации {org_id}: {e}")
            return False
    
    async def get_organization_stats(self, org_id: int) -> Dict:
        """Получает статистику организации"""
        stats = {}
        try:
            cursor = await self.conn.execute(
                "SELECT role, COUNT(*) as count FROM users WHERE organization_id = ? GROUP BY role",
                (org_id,)
            )
            roles_data = await cursor.fetchall()
            stats['roles'] = {row['role']: row['count'] for row in roles_data}
            await cursor.close()
            
            cursor = await self.conn.execute(
                "SELECT status, COUNT(*) as count FROM equipment WHERE organization_id = ? GROUP BY status",
                (org_id,)
            )
            eq_data = await cursor.fetchall()
            stats['equipment'] = {row['status']: row['count'] for row in eq_data}
            await cursor.close()
            
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as count FROM shifts s
                JOIN users u ON s.driver_id = u.telegram_id
                WHERE u.organization_id = ? AND s.status = 'active'
            ''', (org_id,))
            active_shifts = await cursor.fetchone()
            stats['active_shifts'] = active_shifts['count'] if active_shifts else 0
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики организации {org_id}: {e}")
        
        return stats
    
    async def add_equipment(self, name: str, model: str, vin: str, org_id: int, 
                          registration_number: str = None, fuel_type: str = 'diesel',
                          fuel_capacity: float = None) -> Optional[int]:
        """Добавляет новую технику"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO equipment 
                (name, model, vin, organization_id, registration_number, fuel_type, fuel_capacity) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (name, model, vin, org_id, registration_number, fuel_type, fuel_capacity)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления техники: {e}")
            return None
    
    async def get_organization_equipment(self, org_id: int) -> List[Dict]:
        """Получает технику организации"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM equipment WHERE organization_id = ? ORDER BY name",
                (org_id,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения техники организации {org_id}: {e}")
            return []
    
    async def get_equipment_by_id(self, equipment_id: int) -> Optional[Dict]:
        """Получает технику по ID"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM equipment WHERE id = ?", 
                (equipment_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения техники {equipment_id}: {e}")
            return None
    
    async def get_equipment_by_driver(self, driver_id: int) -> List[Dict]:
        """Получает технику назначенную водителю"""
        try:
            cursor = await self.conn.execute('''
                SELECT e.* FROM equipment e
                JOIN driver_equipment de ON e.id = de.equipment_id
                WHERE de.driver_id = ? AND e.status = 'active'
                ORDER BY e.name
            ''', (driver_id,))
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения техники водителя {driver_id}: {e}")
            return []
    
    async def update_equipment(self, eq_id: int, **kwargs) -> bool:
        """Обновляет данные техники"""
        try:
            if not kwargs:
                return False
            
            set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
            values = list(kwargs.values())
            values.append(eq_id)
            
            await self.conn.execute(
                f"UPDATE equipment SET {set_clause} WHERE id = ?",
                values
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления техники {eq_id}: {e}")
            return False
    
    async def assign_equipment_to_driver(self, driver_id: int, equipment_id: int) -> bool:
        """Назначает технику водителю"""
        try:
            await self.conn.execute(
                "INSERT OR REPLACE INTO driver_equipment (driver_id, equipment_id) VALUES (?, ?)",
                (driver_id, equipment_id)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка назначения техники водителю {driver_id}: {e}")
            return False
    
    async def start_shift(self, driver_id: int, equipment_id: int, briefing_confirmed: bool = False, start_odometer: int = None) -> Optional[int]:
        """Начинает новую смену"""
        try:
            cursor = await self.conn.execute(
                "INSERT INTO shifts (driver_id, equipment_id, briefing_confirmed, start_odometer) VALUES (?, ?, ?, ?)",
                (driver_id, equipment_id, briefing_confirmed, start_odometer)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка начала смены для водителя {driver_id}: {e}")
            return None
    
    async def get_active_shift(self, driver_id: int) -> Optional[Dict]:
        """Получает активную смену водителя"""
        try:
            cursor = await self.conn.execute('''
                SELECT s.*, e.name as equipment_name, e.odometer
                FROM shifts s
                LEFT JOIN equipment e ON s.equipment_id = e.id
                WHERE s.driver_id = ? AND s.status = 'active'
                ORDER BY s.start_time DESC LIMIT 1
            ''', (driver_id,))
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения активной смены {driver_id}: {e}")
            return None
    
    async def update_shift_photo(self, shift_id: int, photo_file_id: str) -> bool:
        """Обновляет фото осмотра в смене"""
        try:
            await self.conn.execute(
                "UPDATE shifts SET inspection_photo = ? WHERE id = ?",
                (photo_file_id, shift_id)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления фото смены {shift_id}: {e}")
            return False
    
    async def get_daily_checks(self) -> List[Dict]:
        """Получает шаблоны ежедневных проверок"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM daily_check_templates ORDER BY order_index, check_type"
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения проверок: {e}")
            return []
    
    async def add_daily_check(self, shift_id: int, check_type: str, item_name: str, status: str, notes: str = None) -> bool:
        """Добавляет ежедневную проверку"""
        try:
            await self.conn.execute(
                "INSERT INTO daily_checks (shift_id, check_type, item_name, status, notes) VALUES (?, ?, ?, ?, ?)",
                (shift_id, check_type, item_name, status, notes)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления проверки для смены {shift_id}: {e}")
            return False
    
    async def complete_shift(self, shift_id: int, end_odometer: int = None, notes: str = None) -> bool:
        """Завершает смену"""
        try:
            await self.conn.execute(
                """UPDATE shifts SET end_time = CURRENT_TIMESTAMP, 
                status = 'completed', end_odometer = ?, notes = ? 
                WHERE id = ?""",
                (end_odometer, notes, shift_id)
            )
            
            # Обновляем одометр в технике
            if end_odometer:
                cursor = await self.conn.execute(
                    "SELECT equipment_id FROM shifts WHERE id = ?",
                    (shift_id,)
                )
                shift = await cursor.fetchone()
                await cursor.close()
                
                if shift:
                    await self.conn.execute(
                        "UPDATE equipment SET odometer = ? WHERE id = ?",
                        (end_odometer, shift['equipment_id'])
                    )
            
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка завершения смены {shift_id}: {e}")
            return False
    
    async def get_shifts_by_driver(self, driver_id: int, limit: int = 10) -> List[Dict]:
        """Получает смены водителя"""
        try:
            cursor = await self.conn.execute('''
                SELECT s.*, e.name as equipment_name 
                FROM shifts s
                LEFT JOIN equipment e ON s.equipment_id = e.id
                WHERE s.driver_id = ? 
                ORDER BY s.start_time DESC 
                LIMIT ?
            ''', (driver_id, limit))
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения смен водителя {driver_id}: {e}")
            return []
    
    async def get_pending_inspections(self, org_id: int) -> List[Dict]:
        """Получает смены ожидающие проверки осмотра"""
        try:
            cursor = await self.conn.execute('''
                SELECT s.*, u.full_name as driver_name, e.name as equipment_name
                FROM shifts s
                JOIN users u ON s.driver_id = u.telegram_id
                JOIN equipment e ON s.equipment_id = e.id
                WHERE u.organization_id = ? 
                AND s.inspection_photo IS NOT NULL 
                AND s.inspection_approved = FALSE
                AND s.status = 'active'
                ORDER BY s.start_time
            ''', (org_id,))
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения ожидающих проверок для организации {org_id}: {e}")
            return []
    
    async def approve_inspection(self, shift_id: int, approved_by: int) -> bool:
        """Подтверждает осмотр техники"""
        try:
            await self.conn.execute(
                "UPDATE shifts SET inspection_approved = TRUE, approved_by = ? WHERE id = ?",
                (approved_by, shift_id)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка подтверждения осмотра {shift_id}: {e}")
            return False
    
    async def add_maintenance(self, equipment_id: int, type: str, scheduled_date: str, description: str = None) -> Optional[int]:
        """Добавляет запись о ТО"""
        try:
            cursor = await self.conn.execute(
                "INSERT INTO maintenance (equipment_id, type, scheduled_date, description) VALUES (?, ?, ?, ?)",
                (equipment_id, type, scheduled_date, description)
            )
            await self.conn.commit()
            
            await self.conn.execute(
                "UPDATE equipment SET next_maintenance = ? WHERE id = ?",
                (scheduled_date, equipment_id)
            )
            await self.conn.commit()
            
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления ТО: {e}")
            return None
    
    async def log_action(self, user_id: int, action_type: str, details: str = None) -> bool:
        """Логирует действие пользователя"""
        try:
            await self.conn.execute(
                "INSERT INTO action_logs (user_id, action_type, details) VALUES (?, ?, ?)",
                (user_id, action_type, details)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка логирования действия: {e}")
            return False
    
    async def get_recent_actions(self, org_id: int = None, limit: int = 20) -> List[Dict]:
        """Получает последние действия"""
        try:
            if org_id:
                cursor = await self.conn.execute('''
                    SELECT al.*, u.full_name, u.role 
                    FROM action_logs al
                    JOIN users u ON al.user_id = u.telegram_id
                    WHERE u.organization_id = ?
                    ORDER BY al.created_at DESC 
                    LIMIT ?
                ''', (org_id, limit))
            else:
                cursor = await self.conn.execute('''
                    SELECT al.*, u.full_name, u.role 
                    FROM action_logs al
                    JOIN users u ON al.user_id = u.telegram_id
                    ORDER BY al.created_at DESC 
                    LIMIT ?
                ''', (limit,))
            
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения действий: {e}")
            return []
    
    async def get_driver_stats(self, driver_id: int, days: int = 30) -> Dict:
        """Получает статистику водителя"""
        stats = {}
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as count FROM shifts 
                WHERE driver_id = ? AND start_time >= ? AND status = 'completed'
            ''', (driver_id, start_date))
            result = await cursor.fetchone()
            stats['shifts_count'] = result['count'] if result else 0
            await cursor.close()
            
            cursor = await self.conn.execute('''
                SELECT AVG((julianday(end_time) - julianday(start_time)) * 24) as avg_hours
                FROM shifts 
                WHERE driver_id = ? AND end_time IS NOT NULL AND start_time >= ? AND status = 'completed'
            ''', (driver_id, start_date))
            result = await cursor.fetchone()
            stats['avg_shift_hours'] = round(result['avg_hours'], 1) if result and result['avg_hours'] else 0
            await cursor.close()
            
            cursor = await self.conn.execute('''
                SELECT COUNT(DISTINCT equipment_id) as count FROM shifts 
                WHERE driver_id = ? AND start_time >= ? AND status = 'completed'
            ''', (driver_id, start_date))
            result = await cursor.fetchone()
            stats['equipment_used'] = result['count'] if result else 0
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики водителя {driver_id}: {e}")
        
        return stats
    
    # ========== НОВЫЕ МЕТОДЫ ДЛЯ ИИ ==========
    
    async def add_ai_context(self, organization_id: int, context_type: str, equipment_model: str, 
                           question: str, answer: str, source: str = 'ai') -> Optional[int]:
        """Добавляет контекст для ИИ"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO ai_context 
                (organization_id, context_type, equipment_model, question, answer, source) 
                VALUES (?, ?, ?, ?, ?, ?)""",
                (organization_id, context_type, equipment_model, question, answer, source)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления AI контекста: {e}")
            return None
    
    async def get_ai_context(self, organization_id: int = None, context_type: str = None, 
                           equipment_model: str = None, limit: int = 5) -> List[Dict]:
        """Получает контекст для ИИ"""
        try:
            query = "SELECT * FROM ai_context WHERE 1=1"
            params = []
            
            if organization_id:
                query += " AND organization_id = ?"
                params.append(organization_id)
            
            if context_type:
                query += " AND context_type = ?"
                params.append(context_type)
            
            if equipment_model:
                query += " AND equipment_model LIKE ?"
                params.append(f"%{equipment_model}%")
            
            query += " ORDER BY usage_count DESC, created_at DESC LIMIT ?"
            params.append(limit)
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения AI контекста: {e}")
            return []
    
    async def increment_ai_usage(self, context_id: int) -> bool:
        """Увеличивает счетчик использования AI контекста"""
        try:
            await self.conn.execute(
                "UPDATE ai_context SET usage_count = usage_count + 1 WHERE id = ?",
                (context_id,)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления AI использования: {e}")
            return False
    
    # ========== НОВЫЕ МЕТОДЫ ДЛЯ ТОПЛИВА ==========
    
    async def add_fuel_log(self, equipment_id: int, driver_id: int, fuel_amount: float, 
                         fuel_type: str = 'diesel', cost_per_liter: float = None,
                         total_cost: float = None, odometer_reading: int = None,
                         fueling_station: str = None, receipt_photo: str = None, 
                         notes: str = None) -> Optional[int]:
        """Добавляет запись о заправке"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO fuel_logs 
                (equipment_id, driver_id, fuel_amount, fuel_type, cost_per_liter, 
                 total_cost, odometer_reading, fueling_station, receipt_photo, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (equipment_id, driver_id, fuel_amount, fuel_type, cost_per_liter,
                 total_cost, odometer_reading, fueling_station, receipt_photo, notes)
            )
            
            # Обновляем текущий уровень топлива в технике
            await self.conn.execute(
                "UPDATE equipment SET current_fuel_level = current_fuel_level + ? WHERE id = ?",
                (fuel_amount, equipment_id)
            )
            
            # Обновляем одометр если указан
            if odometer_reading:
                await self.conn.execute(
                    "UPDATE equipment SET odometer = ? WHERE id = ?",
                    (odometer_reading, equipment_id)
                )
            
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления записи о топливе: {e}")
            return None
    
    async def get_fuel_logs(self, equipment_id: int = None, driver_id: int = None, 
                          days: int = 30) -> List[Dict]:
        """Получает записи о заправках"""
        try:
            query = """
                SELECT fl.*, e.name as equipment_name, u.full_name as driver_name 
                FROM fuel_logs fl
                LEFT JOIN equipment e ON fl.equipment_id = e.id
                LEFT JOIN users u ON fl.driver_id = u.telegram_id
                WHERE 1=1
            """
            params = []
            
            if equipment_id:
                query += " AND fl.equipment_id = ?"
                params.append(equipment_id)
            
            if driver_id:
                query += " AND fl.driver_id = ?"
                params.append(driver_id)
            
            if days:
                query += " AND DATE(fl.fueling_date) >= DATE('now', ?)"
                params.append(f'-{days} days')
            
            query += " ORDER BY fl.fueling_date DESC"
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения записей о топливе: {e}")
            return []
    
    async def get_fuel_statistics(self, equipment_id: int, days: int = 30) -> Dict:
        """Получает статистику по топливу"""
        stats = {}
        try:
            # Общее количество топлива
            cursor = await self.conn.execute('''
                SELECT SUM(fuel_amount) as total_fuel, SUM(total_cost) as total_cost,
                       AVG(cost_per_liter) as avg_price
                FROM fuel_logs 
                WHERE equipment_id = ? AND DATE(fueling_date) >= DATE('now', ?)
            ''', (equipment_id, f'-{days} days'))
            result = await cursor.fetchone()
            stats.update(dict(result) if result else {})
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики топлива: {e}")
        
        return stats
    
    async def get_low_fuel_equipment(self, organization_id: int, threshold: float = 20.0) -> List[Dict]:
        """Получает технику с низким уровнем топлива"""
        try:
            cursor = await self.conn.execute('''
                SELECT e.*, 
                       (e.current_fuel_level / e.fuel_capacity * 100) as fuel_percentage
                FROM equipment e
                WHERE e.organization_id = ? 
                  AND e.fuel_capacity > 0 
                  AND e.current_fuel_level / e.fuel_capacity * 100 < ?
                ORDER BY fuel_percentage
            ''', (organization_id, threshold))
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения техники с низким топливом: {e}")
            return []
    
    # ========== НОВЫЕ МЕТОДЫ ДЛЯ ЗАПЧАСТЕЙ ==========
    
    async def add_spare_part(self, organization_id: int, part_name: str, part_number: str = None,
                           description: str = None, category: str = None, quantity: int = 0,
                           min_quantity: int = 1, supplier: str = None, supplier_contact: str = None,
                           unit_price: float = None, location: str = None, notes: str = None) -> Optional[int]:
        """Добавляет запчасть в склад"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO spare_parts 
                (organization_id, part_name, part_number, description, category, 
                 quantity, min_quantity, supplier, supplier_contact, unit_price, 
                 location, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (organization_id, part_name, part_number, description, category,
                 quantity, min_quantity, supplier, supplier_contact, unit_price,
                 location, notes)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления запчасти: {e}")
            return None
    
    async def get_spare_parts(self, organization_id: int, category: str = None, 
                            low_stock_only: bool = False) -> List[Dict]:
        """Получает список запчастей"""
        try:
            query = "SELECT * FROM spare_parts WHERE organization_id = ?"
            params = [organization_id]
            
            if category:
                query += " AND category = ?"
                params.append(category)
            
            if low_stock_only:
                query += " AND quantity <= min_quantity"
            
            query += " ORDER BY part_name"
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения запчастей: {e}")
            return []
    
    async def update_spare_part_quantity(self, part_id: int, quantity_change: int) -> bool:
        """Обновляет количество запчастей на складе"""
        try:
            await self.conn.execute(
                "UPDATE spare_parts SET quantity = quantity + ? WHERE id = ?",
                (quantity_change, part_id)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления количества запчастей: {e}")
            return False
    
    # ========== НОВЫЕ МЕТОДЫ ДЛЯ ЗАКАЗОВ ==========
    
    async def create_order(self, organization_id: int, order_type: str, equipment_id: int = None,
                         part_id: int = None, quantity: int = 1, urgent: bool = False,
                         requested_by: int = None, notes: str = None) -> Optional[int]:
        """Создает новый заказ"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO orders 
                (organization_id, order_type, equipment_id, part_id, quantity, 
                 urgent, requested_by, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (organization_id, order_type, equipment_id, part_id, quantity,
                 urgent, requested_by, notes)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка создания заказа: {e}")
            return None
    
    async def get_orders(self, organization_id: int, status: str = None, 
                       order_type: str = None) -> List[Dict]:
        """Получает список заказов"""
        try:
            query = """
                SELECT o.*, e.name as equipment_name, p.part_name,
                u1.full_name as requested_by_name, u2.full_name as approved_by_name
                FROM orders o
                LEFT JOIN equipment e ON o.equipment_id = e.id
                LEFT JOIN spare_parts p ON o.part_id = p.id
                LEFT JOIN users u1 ON o.requested_by = u1.telegram_id
                LEFT JOIN users u2 ON o.approved_by = u2.telegram_id
                WHERE o.organization_id = ?
            """
            params = [organization_id]
            
            if status:
                query += " AND o.status = ?"
                params.append(status)
            
            if order_type:
                query += " AND o.order_type = ?"
                params.append(order_type)
            
            query += " ORDER BY o.created_at DESC"
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения заказов: {e}")
            return []
    
    async def get_orders_by_id(self, order_id: int) -> Optional[Dict]:
        """Получает заказ по ID"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM orders WHERE id = ?",
                (order_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения заказа {order_id}: {e}")
            return None
    
    async def update_order_status(self, order_id: int, status: str, approved_by: int = None) -> bool:
        """Обновляет статус заказа"""
        try:
            if approved_by:
                await self.conn.execute(
                    "UPDATE orders SET status = ?, approved_by = ? WHERE id = ?",
                    (status, approved_by, order_id)
                )
            else:
                await self.conn.execute(
                    "UPDATE orders SET status = ? WHERE id = ?",
                    (status, order_id)
                )
            
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления статуса заказа: {e}")
            return False
    
    # ========== НОВЫЕ МЕТОДЫ ДЛЯ ИНСТРУКЦИЙ ==========
    
    async def add_instruction(self, equipment_model: str, instruction_type: str, 
                            title: str, description: str = None, steps: str = None,
                            diagram_photo: str = None, video_url: str = None) -> Optional[int]:
        """Добавляет инструкцию"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO instructions 
                (equipment_model, instruction_type, title, description, steps, 
                 diagram_photo, video_url) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (equipment_model, instruction_type, title, description, steps,
                 diagram_photo, video_url)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления инструкции: {e}")
            return None
    
    async def get_instructions(self, equipment_model: str = None, 
                             instruction_type: str = None) -> List[Dict]:
        """Получает инструкции"""
        try:
            query = "SELECT * FROM instructions WHERE 1=1"
            params = []
            
            if equipment_model:
                query += " AND equipment_model LIKE ?"
                params.append(f"%{equipment_model}%")
            
            if instruction_type:
                query += " AND instruction_type = ?"
                params.append(instruction_type)
            
            query += " ORDER BY title"
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения инструкций: {e}")
            return []
    
    # ========== НОВЫЕ МЕТОДЫ ДЛЯ РАСШИРЕННОГО ТО ==========
    
    async def add_maintenance_schedule(self, equipment_id: int, maintenance_type: str,
                                     interval_km: int = None, interval_days: int = None,
                                     description: str = None, parts_needed: str = None,
                                     estimated_hours: float = None) -> Optional[int]:
        """Добавляет расписание ТО"""
        try:
            # Вычисляем следующую дату/пробег
            next_due_km = None
            next_due_date = None
            
            if interval_km:
                equipment = await self.get_equipment_by_id(equipment_id)
                if equipment and equipment.get('odometer'):
                    next_due_km = equipment['odometer'] + interval_km
            
            if interval_days:
                next_due_date = (datetime.now() + timedelta(days=interval_days)).strftime('%Y-%m-%d')
            
            cursor = await self.conn.execute(
                """INSERT INTO maintenance_schedule 
                (equipment_id, maintenance_type, interval_km, interval_days,
                 last_done_km, last_done_date, next_due_km, next_due_date,
                 description, parts_needed, estimated_hours) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (equipment_id, maintenance_type, interval_km, interval_days,
                 0, None, next_due_km, next_due_date, description, 
                 parts_needed, estimated_hours)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления расписания ТО: {e}")
            return None
    
    async def get_upcoming_maintenance(self, organization_id: int, days_ahead: int = 30) -> List[Dict]:
        """Получает предстоящие ТО"""
        try:
            query = """
                SELECT ms.*, e.name as equipment_name, e.model, e.odometer,
                       e.organization_id, o.name as org_name
                FROM maintenance_schedule ms
                JOIN equipment e ON ms.equipment_id = e.id
                JOIN organizations o ON e.organization_id = o.id
                WHERE e.organization_id = ? AND (
                    (ms.next_due_date IS NOT NULL AND ms.next_due_date <= DATE('now', ?)) OR
                    (ms.next_due_km IS NOT NULL AND ms.next_due_km <= e.odometer + ?)
                )
                ORDER BY ms.next_due_date, ms.next_due_km
            """
            params = [organization_id, f'+{days_ahead} days', 500]
            
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения предстоящих ТО: {e}")
            return []
    
    async def complete_maintenance(self, schedule_id: int, odometer: int = None) -> bool:
        """Отмечает ТО как выполненное"""
        try:
            # Получаем данные о расписании
            cursor = await self.conn.execute(
                "SELECT * FROM maintenance_schedule WHERE id = ?",
                (schedule_id,)
            )
            schedule = await cursor.fetchone()
            await cursor.close()
            
            if not schedule:
                return False
            
            # Обновляем последнее выполнение
            update_query = """
                UPDATE maintenance_schedule 
                SET last_done_date = DATE('now'), last_done_km = ?
            """
            params = [odometer]
            
            # Вычисляем следующую дату/пробег
            if schedule['interval_days']:
                update_query += ", next_due_date = DATE('now', ?)"
                params.append(f'+{schedule["interval_days"]} days')
            
            if schedule['interval_km'] and odometer:
                update_query += ", next_due_km = ?"
                params.append(odometer + schedule['interval_km'])
            
            update_query += " WHERE id = ?"
            params.append(schedule_id)
            
            await self.conn.execute(update_query, params)
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка завершения ТО: {e}")
            return False
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========
    
    async def assign_role_to_user(self, user_id: int, role: str, organization_id: int = None) -> bool:
        """Назначает роль пользователю"""
        try:
            if organization_id:
                await self.conn.execute(
                    "UPDATE users SET role = ?, organization_id = ? WHERE telegram_id = ?",
                    (role, organization_id, user_id)
                )
            else:
                await self.conn.execute(
                    "UPDATE users SET role = ? WHERE telegram_id = ?",
                    (role, user_id)
                )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка назначения роли {user_id}: {e}")
            return False
    
    async def get_organization_analytics(self, org_id: int, period_days: int = 30) -> Dict:
        """Получает аналитику организации"""
        analytics = {}
        try:
            start_date = (datetime.now() - timedelta(days=period_days)).strftime('%Y-%m-%d')
            
            # Статистика по сменам
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as total_shifts,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_shifts,
                       AVG((julianday(end_time) - julianday(start_time)) * 24) as avg_shift_hours
                FROM shifts s
                JOIN users u ON s.driver_id = u.telegram_id
                WHERE u.organization_id = ? AND s.start_time >= ?
            ''', (org_id, start_date))
            shift_stats = await cursor.fetchone()
            analytics['shifts'] = dict(shift_stats) if shift_stats else {}
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения аналитики организации {org_id}: {e}")
        
        return analytics

# Создаем глобальный экземпляр базы данных
db = Database()
