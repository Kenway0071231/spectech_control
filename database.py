import sqlite3
import aiosqlite
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path='techcontrol.db'):
        self.db_path = db_path
        self.conn = None
        
    async def connect(self):
        """Устанавливает соединение с базой данных"""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            self.conn.row_factory = aiosqlite.Row  # Для доступа по имени столбца
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
                    role TEXT NOT NULL DEFAULT 'driver',
                    organization_id INTEGER,
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
                    organization_id INTEGER NOT NULL,
                    status TEXT DEFAULT 'active',
                    next_maintenance DATE,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id)
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
            
            # Таблица технического обслуживания (ТО)
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS maintenance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_id INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    scheduled_date DATE NOT NULL,
                    completed_date DATE,
                    description TEXT,
                    status TEXT DEFAULT 'scheduled',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (equipment_id) REFERENCES equipment(id)
                )
            ''')
            
            # Таблица журнала действий
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
            
            # Таблица ежедневных проверок (шаблоны)
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_check_templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_type TEXT NOT NULL,
                    item TEXT NOT NULL,
                    check_description TEXT NOT NULL,
                    order_index INTEGER DEFAULT 0
                )
            ''')
            
            await self.conn.commit()
            logger.info("✅ Таблицы созданы/проверены")
            
            # Добавляем шаблоны проверок, если их нет
            await self.init_daily_checks()
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
            raise
    
    async def init_daily_checks(self):
        """Инициализирует шаблоны ежедневных проверок"""
        checks = [
            ("engine", "Масло двигателя", "Проверить уровень и состояние"),
            ("engine", "Охлаждающая жидкость", "Проверить уровень"),
            ("engine", "Тормозная жидкость", "Проверить уровень"),
            ("tires", "Давление в шинах", "Проверить давление (передние/задние)"),
            ("tires", "Протектор шин", "Проверить износ"),
            ("lights", "Фары ближнего света", "Проверить работу"),
            ("lights", "Фары дальнего света", "Проверить работу"),
            ("lights", "Стоп-сигналы", "Проверить работу"),
            ("lights", "Поворотники", "Проверить работу"),
            ("safety", "Зеркала", "Проверить чистоту и регулировку"),
            ("safety", "Ремни безопасности", "Проверить исправность"),
            ("safety", "Огнетушитель", "Наличие и срок годности"),
            ("interior", "Приборная панель", "Проверить показания"),
            ("interior", "Звуковой сигнал", "Проверить работу"),
            ("interior", "Стеклоочистители", "Проверить работу и состояние щеток")
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
    
    # ========== ПОЛЬЗОВАТЕЛИ ==========
    
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
    
    async def register_user(self, telegram_id: int, full_name: str, username: str = None, role: str = 'driver') -> bool:
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
    
    # ========== ОРГАНИЗАЦИИ ==========
    
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
    
    async def create_organization_for_director(self, director_id: int, org_name: str):
        """Создает организацию и назначает директора"""
        try:
            # Проверяем, что у директора еще нет организации
            user = await self.get_user(director_id)
            if user and user.get('organization_id'):
                return None, "У этого пользователя уже есть организация"
            
            # Создаем организацию
            cursor = await self.conn.execute(
                "INSERT INTO organizations (name, director_id) VALUES (?, ?)",
                (org_name, director_id)
            )
            org_id = cursor.lastrowid
            
            # Обновляем пользователя
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
            # Количество пользователей по ролям
            cursor = await self.conn.execute(
                "SELECT role, COUNT(*) as count FROM users WHERE organization_id = ? GROUP BY role",
                (org_id,)
            )
            roles_data = await cursor.fetchall()
            stats['roles'] = {row['role']: row['count'] for row in roles_data}
            await cursor.close()
            
            # Количество техники по статусам
            cursor = await self.conn.execute(
                "SELECT status, COUNT(*) as count FROM equipment WHERE organization_id = ? GROUP BY status",
                (org_id,)
            )
            eq_data = await cursor.fetchall()
            stats['equipment'] = {row['status']: row['count'] for row in eq_data}
            await cursor.close()
            
            # Активные смены
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as count FROM shifts s
                JOIN users u ON s.driver_id = u.telegram_id
                WHERE u.organization_id = ? AND s.status = 'active'
            ''', (org_id,))
            active_shifts = await cursor.fetchone()
            stats['active_shifts'] = active_shifts['count'] if active_shifts else 0
            await cursor.close()
            
            # ТО на следующую неделю
            next_week = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as count FROM maintenance m
                JOIN equipment e ON m.equipment_id = e.id
                WHERE e.organization_id = ? AND m.scheduled_date <= ? AND m.status = 'scheduled'
            ''', (org_id, next_week))
            weekly_maint = await cursor.fetchone()
            stats['weekly_maintenance'] = weekly_maint['count'] if weekly_maint else 0
            await cursor.close()
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики организации {org_id}: {e}")
        
        return stats
    
    # ========== ТЕХНИКА ==========
    
    async def add_equipment(self, name: str, model: str, vin: str, org_id: int) -> Optional[int]:
        """Добавляет новую технику"""
        try:
            cursor = await self.conn.execute(
                "INSERT INTO equipment (name, model, vin, organization_id) VALUES (?, ?, ?, ?)",
                (name, model, vin, org_id)
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
    
    # ========== СМЕНЫ ==========
    
    async def start_shift(self, driver_id: int, equipment_id: int, briefing_confirmed: bool = False) -> Optional[int]:
        """Начинает новую смену"""
        try:
            cursor = await self.conn.execute(
                "INSERT INTO shifts (driver_id, equipment_id, briefing_confirmed) VALUES (?, ?, ?)",
                (driver_id, equipment_id, briefing_confirmed)
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
                SELECT s.*, e.name as equipment_name 
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
    
    async def complete_shift(self, shift_id: int, notes: str = None) -> bool:
        """Завершает смену"""
        try:
            await self.conn.execute(
                "UPDATE shifts SET end_time = CURRENT_TIMESTAMP, status = 'completed', notes = ? WHERE id = ?",
                (notes, shift_id)
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
    
    # ========== ТЕХНИЧЕСКОЕ ОБСЛУЖИВАНИЕ ==========
    
    async def add_maintenance(self, equipment_id: int, type: str, scheduled_date: str, description: str = None) -> Optional[int]:
        """Добавляет запись о ТО"""
        try:
            cursor = await self.conn.execute(
                "INSERT INTO maintenance (equipment_id, type, scheduled_date, description) VALUES (?, ?, ?, ?)",
                (equipment_id, type, scheduled_date, description)
            )
            await self.conn.commit()
            
            # Обновляем дату следующего ТО в технике
            await self.conn.execute(
                "UPDATE equipment SET next_maintenance = ? WHERE id = ?",
                (scheduled_date, equipment_id)
            )
            await self.conn.commit()
            
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления ТО: {e}")
            return None
    
    # ========== ЖУРНАЛ ДЕЙСТВИЙ ==========
    
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
    
    # ========== СТАТИСТИКА ==========
    
    async def get_driver_stats(self, driver_id: int, days: int = 30) -> Dict:
        """Получает статистику водителя"""
        stats = {}
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            # Количество смен
            cursor = await self.conn.execute('''
                SELECT COUNT(*) as count FROM shifts 
                WHERE driver_id = ? AND start_time >= ? AND status = 'completed'
            ''', (driver_id, start_date))
            result = await cursor.fetchone()
            stats['shifts_count'] = result['count'] if result else 0
            await cursor.close()
            
            # Средняя продолжительность смены
            cursor = await self.conn.execute('''
                SELECT AVG((julianday(end_time) - julianday(start_time)) * 24) as avg_hours
                FROM shifts 
                WHERE driver_id = ? AND end_time IS NOT NULL AND start_time >= ? AND status = 'completed'
            ''', (driver_id, start_date))
            result = await cursor.fetchone()
            stats['avg_shift_hours'] = round(result['avg_hours'], 1) if result and result['avg_hours'] else 0
            await cursor.close()
            
            # Количество разной использованной техники
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

# Создаем глобальный экземпляр базы данных
db = Database()
