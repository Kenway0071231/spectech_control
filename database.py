import sqlite3
import aiosqlite
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
import asyncio

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path=None):
        self.db_path = db_path or os.getenv('DATABASE_PATH', 'techcontrol.db')
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
                    year INTEGER,
                    color TEXT,
                    engine_power INTEGER,
                    weight REAL,
                    max_weight REAL,
                    category TEXT DEFAULT 'Спецтехника',
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id)
                )
            ''')
            
            # Таблица для хранения анализа документов
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS document_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_id INTEGER,
                    document_type TEXT NOT NULL,
                    analysis_data TEXT NOT NULL,
                    analysis_quality TEXT,
                    quality_score REAL,
                    missing_fields TEXT,
                    motohours INTEGER,
                    last_service TEXT,
                    registration_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
            
            # Таблица ежедневных отчетов
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shift_id INTEGER NOT NULL,
                    report_date DATE NOT NULL,
                    status TEXT NOT NULL,
                    description TEXT NOT NULL,
                    hours_worked REAL,
                    fuel_used REAL,
                    problems TEXT,
                    recommendations TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (shift_id) REFERENCES shifts(id)
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
            
            # Таблица для обучения ИИ
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS ai_training_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    module TEXT NOT NULL,
                    input_text TEXT NOT NULL,
                    correct_output TEXT,
                    ai_output TEXT,
                    is_correct BOOLEAN,
                    corrected_by INTEGER,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (corrected_by) REFERENCES users(telegram_id)
                )
            ''')
            
            # Таблица для статистики анализа документов
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS document_analysis_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL,
                    total_analyses INTEGER DEFAULT 0,
                    successful_analyses INTEGER DEFAULT 0,
                    failed_analyses INTEGER DEFAULT 0,
                    avg_quality_score REAL DEFAULT 0,
                    avg_processing_time REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await self.conn.commit()
            logger.info("✅ Все таблицы созданы/проверены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
            raise
    
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
    
    # ========== МЕТОДЫ ДЛЯ ТЕХНИКИ ==========
    
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
    
    # ========== МЕТОДЫ ДЛЯ АНАЛИЗА ДОКУМЕНТОВ ==========
    
    async def save_document_analysis(self, analysis_data: Dict) -> Optional[int]:
        """Сохраняет результат анализа документа"""
        try:
            equipment_id = analysis_data.get("equipment_id")
            document_type = analysis_data.get("document_type", "СТС")
            
            # Сериализуем данные анализа
            analysis_json = json.dumps(analysis_data.get("analysis_data", {}), ensure_ascii=False)
            
            quality = analysis_data.get("analysis_quality")
            quality_score = analysis_data.get("quality_score")
            
            # Сериализуем список отсутствующих полей
            missing_fields = analysis_data.get("missing_fields", [])
            missing_fields_json = json.dumps(missing_fields, ensure_ascii=False) if missing_fields else None
            
            motohours = analysis_data.get("motohours")
            last_service = analysis_data.get("last_service")
            registration_date = analysis_data.get("registration_date")
            
            cursor = await self.conn.execute(
                """INSERT INTO document_analysis 
                (equipment_id, document_type, analysis_data, analysis_quality, quality_score,
                 missing_fields, motohours, last_service, registration_date) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (equipment_id, document_type, analysis_json, quality, quality_score,
                 missing_fields_json, motohours, last_service, registration_date)
            )
            
            # Обновляем статистику
            await self.update_analysis_stats(success=True, quality_score=quality_score)
            
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка сохранения анализа документа: {e}")
            # Обновляем статистику с ошибкой
            await self.update_analysis_stats(success=False)
            return None
    
    async def get_document_analysis(self, equipment_id: int) -> Optional[Dict]:
        """Получает анализ документа для техники"""
        try:
            cursor = await self.conn.execute(
                "SELECT * FROM document_analysis WHERE equipment_id = ? ORDER BY created_at DESC LIMIT 1",
                (equipment_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            
            if row:
                data = dict(row)
                # Парсим JSON данные
                if data.get("analysis_data"):
                    data["analysis_data"] = json.loads(data["analysis_data"])
                if data.get("missing_fields"):
                    data["missing_fields"] = json.loads(data["missing_fields"])
                return data
            return None
        except Exception as e:
            logger.error(f"Ошибка получения анализа документа: {e}")
            return None
    
    async def update_analysis_stats(self, success: bool, quality_score: float = None):
        """Обновляет статистику анализа документов"""
        try:
            today = datetime.now().date().isoformat()
            
            # Проверяем есть ли запись на сегодня
            cursor = await self.conn.execute(
                "SELECT * FROM document_analysis_stats WHERE date = ?",
                (today,)
            )
            existing = await cursor.fetchone()
            await cursor.close()
            
            if existing:
                # Обновляем существующую запись
                update_query = """
                    UPDATE document_analysis_stats 
                    SET total_analyses = total_analyses + 1,
                        successful_analyses = successful_analyses + ?,
                        failed_analyses = failed_analyses + ?,
                        avg_quality_score = ((avg_quality_score * (successful_analyses - ?)) + ?) / successful_analyses
                    WHERE date = ?
                """
                await self.conn.execute(
                    update_query,
                    (1 if success else 0, 0 if success else 1, 
                     1 if success else 0, quality_score if quality_score else 0, 
                     today)
                )
            else:
                # Создаем новую запись
                await self.conn.execute(
                    """INSERT INTO document_analysis_stats 
                    (date, total_analyses, successful_analyses, failed_analyses, avg_quality_score) 
                    VALUES (?, ?, ?, ?, ?)""",
                    (today, 1, 1 if success else 0, 0 if success else 1, quality_score if quality_score else 0)
                )
            
            await self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
    
    # ========== МЕТОДЫ ДЛЯ ОБУЧЕНИЯ ИИ ==========
    
    async def add_ai_training_data(self, module: str, input_text: str, 
                                 correct_output: str = None, ai_output: str = None,
                                 is_correct: bool = None, corrected_by: int = None,
                                 notes: str = None) -> Optional[int]:
        """Добавляет данные для обучения ИИ"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO ai_training_data 
                (module, input_text, correct_output, ai_output, is_correct, corrected_by, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (module, input_text, correct_output, ai_output, is_correct, corrected_by, notes)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления данных обучения ИИ: {e}")
            return None
    
    # ========== МЕТОДЫ ДЛЯ СМЕН ==========
    
    async def start_shift(self, driver_id: int, equipment_id: int, briefing_confirmed: bool = False, 
                         start_odometer: int = None) -> Optional[int]:
        """Начинает новую смену"""
        try:
            cursor = await self.conn.execute(
                """INSERT INTO shifts (driver_id, equipment_id, briefing_confirmed, start_odometer) 
                VALUES (?, ?, ?, ?)""",
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
    
    # ========== МЕТОДЫ ДЛЯ ТО ==========
    
    async def add_maintenance(self, equipment_id: int, type: str, scheduled_date: str, 
                             description: str = None) -> Optional[int]:
        """Добавляет запись о ТО"""
        try:
            cursor = await self.conn.execute(
                "INSERT INTO maintenance (equipment_id, type, scheduled_date, description) VALUES (?, ?, ?, ?)",
                (equipment_id, type, scheduled_date, description)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления ТО: {e}")
            return None
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========
    
    async def assign_role_to_user(self, user_id: int, role: str, organization_id: int = None) -> bool:
        """Назначает роль пользователю"""
        return await self.update_user_role(user_id, role, organization_id)

# Создаем глобальный экземпляр базы данных
db = Database()
