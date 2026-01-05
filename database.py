import sqlite3
import aiosqlite
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица организаций
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS organizations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    director_id INTEGER UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    fuel_type TEXT DEFAULT 'diesel',
                    fuel_capacity REAL,
                    odometer INTEGER DEFAULT 0,
                    year INTEGER,
                    color TEXT,
                    engine_power INTEGER,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    motohours INTEGER,
                    last_service TEXT,
                    registration_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица технического обслуживания
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS maintenance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_id INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    scheduled_date DATE NOT NULL,
                    description TEXT,
                    status TEXT DEFAULT 'scheduled',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await self.conn.commit()
            logger.info("✅ Все таблицы созданы")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
            raise
    
    # ========== МЕТОДЫ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ ==========
    
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
    
    async def get_all_users_simple(self) -> List[Dict]:
        """Получает всех пользователей"""
        try:
            cursor = await self.conn.execute(
                "SELECT telegram_id, full_name, role FROM users ORDER BY created_at DESC"
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    # ========== МЕТОДЫ ДЛЯ ОРГАНИЗАЦИЙ ==========
    
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
    
    async def get_all_organizations_simple(self) -> List[Dict]:
        """Получает все организации"""
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
    
    # ========== МЕТОДЫ ДЛЯ АНАЛИЗА ДОКУМЕНТОВ ==========
    
    async def save_document_analysis(self, analysis_data: Dict) -> Optional[int]:
        """Сохраняет результат анализа документа"""
        try:
            equipment_id = analysis_data.get("equipment_id")
            document_type = analysis_data.get("document_type", "СТС")
            analysis_json = json.dumps(analysis_data.get("analysis_data", {}), ensure_ascii=False)
            quality = analysis_data.get("analysis_quality")
            motohours = analysis_data.get("motohours")
            last_service = analysis_data.get("last_service")
            registration_date = analysis_data.get("registration_date")
            
            cursor = await self.conn.execute(
                """INSERT INTO document_analysis 
                (equipment_id, document_type, analysis_data, analysis_quality, 
                 motohours, last_service, registration_date) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (equipment_id, document_type, analysis_json, quality,
                 motohours, last_service, registration_date)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка сохранения анализа документа: {e}")
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

# Создаем глобальный экземпляр базы данных
db = Database()
