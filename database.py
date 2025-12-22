import aiosqlite
import logging
import os
import json
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path=None):
        self.db_path = db_path or ':memory:' if os.getenv('BOTHOST') else 'tech_control.db'
        print(f"📦 База данных: {self.db_path}")

    async def connect(self):
        self.connection = await aiosqlite.connect(self.db_path)
        self.connection.row_factory = aiosqlite.Row
        await self.create_tables()
        logger.info("✅ База данных подключена")

    async def create_tables(self):
        tables = [
            '''CREATE TABLE IF NOT EXISTS organizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                director_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                username TEXT,
                role TEXT DEFAULT 'driver',
                organization_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS equipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                model TEXT,
                vin TEXT UNIQUE,
                organization_id INTEGER,
                status TEXT DEFAULT 'active',
                last_maintenance DATE,
                next_maintenance DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id INTEGER NOT NULL,
                equipment_id INTEGER NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                briefing_confirmed BOOLEAN DEFAULT 0,
                inspection_photo TEXT,
                inspection_approved BOOLEAN DEFAULT 0,
                approved_by INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (driver_id) REFERENCES users (telegram_id),
                FOREIGN KEY (equipment_id) REFERENCES equipment (id)
            )''',
            '''CREATE TABLE IF NOT EXISTS maintenance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                equipment_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                scheduled_date DATE NOT NULL,
                completed_date DATE,
                status TEXT DEFAULT 'scheduled',
                description TEXT,
                cost REAL,
                notified BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (equipment_id) REFERENCES equipment (id)
            )''',
            '''CREATE TABLE IF NOT EXISTS daily_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_id INTEGER NOT NULL,
                check_type TEXT NOT NULL,
                item_name TEXT NOT NULL,
                status TEXT NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (shift_id) REFERENCES shifts (id)
            )'''
        ]
        
        for table_sql in tables:
            await self.connection.execute(table_sql)
        await self.connection.commit()

    # Существующие методы остаются без изменений до метода add_equipment...

    async def add_equipment(self, name, model, vin, organization_id):
        cursor = await self.connection.execute(
            'INSERT INTO equipment (name, model, vin, organization_id) VALUES (?, ?, ?, ?)',
            (name, model, vin, organization_id)
        )
        await self.connection.commit()
        return cursor.lastrowid

    # Новые методы для смен
    async def start_shift(self, driver_id, equipment_id, briefing_confirmed=False):
        """Начинает новую смену"""
        cursor = await self.connection.execute(
            '''INSERT INTO shifts (driver_id, equipment_id, briefing_confirmed, status) 
               VALUES (?, ?, ?, 'active')''',
            (driver_id, equipment_id, briefing_confirmed)
        )
        shift_id = cursor.lastrowid
        await self.connection.commit()
        return shift_id

    async def get_active_shift(self, driver_id):
        """Получает активную смену водителя"""
        cursor = await self.connection.execute(
            '''SELECT s.*, e.name as equipment_name, e.model as equipment_model
               FROM shifts s
               JOIN equipment e ON s.equipment_id = e.id
               WHERE s.driver_id = ? AND s.status = 'active'
               ORDER BY s.start_time DESC LIMIT 1''',
            (driver_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def update_shift_photo(self, shift_id, photo_file_id):
        """Обновляет фото осмотра для смены"""
        await self.connection.execute(
            'UPDATE shifts SET inspection_photo = ? WHERE id = ?',
            (photo_file_id, shift_id)
        )
        await self.connection.commit()
        return True

    async def complete_shift(self, shift_id, notes=None):
        """Завершает смену"""
        await self.connection.execute(
            '''UPDATE shifts 
               SET end_time = CURRENT_TIMESTAMP, 
                   status = 'completed',
                   notes = ?
               WHERE id = ?''',
            (notes, shift_id)
        )
        await self.connection.commit()
        return True

    async def approve_inspection(self, shift_id, approved_by):
        """Подтверждает осмотр техники"""
        await self.connection.execute(
            '''UPDATE shifts 
               SET inspection_approved = 1,
                   approved_by = ?
               WHERE id = ?''',
            (approved_by, shift_id)
        )
        await self.connection.commit()
        return True

    async def get_shifts_by_driver(self, driver_id, limit=10):
        """Получает смены водителя"""
        cursor = await self.connection.execute(
            '''SELECT s.*, e.name as equipment_name
               FROM shifts s
               JOIN equipment e ON s.equipment_id = e.id
               WHERE s.driver_id = ?
               ORDER BY s.start_time DESC
               LIMIT ?''',
            (driver_id, limit)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_pending_inspections(self, organization_id):
        """Получает смены ожидающие проверки осмотра"""
        cursor = await self.connection.execute(
            '''SELECT s.*, u.full_name as driver_name, e.name as equipment_name
               FROM shifts s
               JOIN users u ON s.driver_id = u.telegram_id
               JOIN equipment e ON s.equipment_id = e.id
               WHERE e.organization_id = ? 
               AND s.inspection_photo IS NOT NULL 
               AND s.inspection_approved = 0
               AND s.status = 'active'
               ORDER BY s.start_time DESC''',
            (organization_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    # Методы для ТО (технического обслуживания)
    async def add_maintenance(self, equipment_id, type, scheduled_date, description=None):
        """Добавляет запись о ТО"""
        cursor = await self.connection.execute(
            '''INSERT INTO maintenance (equipment_id, type, scheduled_date, description) 
               VALUES (?, ?, ?, ?)''',
            (equipment_id, type, scheduled_date, description)
        )
        maintenance_id = cursor.lastrowid
        
        # Обновляем дату следующего ТО в технике
        await self.connection.execute(
            'UPDATE equipment SET next_maintenance = ? WHERE id = ?',
            (scheduled_date, equipment_id)
        )
        await self.connection.commit()
        return maintenance_id

    async def get_upcoming_maintenance(self, days=7):
        """Получает предстоящие ТО в ближайшие дни"""
        cursor = await self.connection.execute(
            '''SELECT m.*, e.name as equipment_name, e.model, o.name as org_name
               FROM maintenance m
               JOIN equipment e ON m.equipment_id = e.id
               JOIN organizations o ON e.organization_id = o.id
               WHERE m.status = 'scheduled' 
               AND m.scheduled_date <= date('now', ?)
               AND m.notified = 0
               ORDER BY m.scheduled_date''',
            (f'+{days} days',)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def mark_maintenance_notified(self, maintenance_id):
        """Отмечает что уведомление о ТО отправлено"""
        await self.connection.execute(
            'UPDATE maintenance SET notified = 1 WHERE id = ?',
            (maintenance_id,)
        )
        await self.connection.commit()
        return True

    async def get_equipment_by_driver(self, driver_id):
        """Получает технику доступную водителю"""
        # Сначала получаем организацию водителя
        user = await self.get_user(driver_id)
        if not user or not user.get('organization_id'):
            return []
        
        cursor = await self.connection.execute(
            '''SELECT * FROM equipment 
               WHERE organization_id = ? 
               AND status = 'active'
               ORDER BY name''',
            (user['organization_id'],)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_daily_checks(self):
        """Получает список ежедневных проверок"""
        # Это можно сделать конфигурируемым, но пока захардкодим
        checks = [
            {"type": "engine", "item": "Уровень масла", "check": "Нормальный"},
            {"type": "engine", "item": "Уровень охлаждающей жидкости", "check": "Нормальный"},
            {"type": "tires", "item": "Давление в шинах", "check": "Нормальное"},
            {"type": "tires", "item": "Состояние протектора", "check": "Нормальное"},
            {"type": "lights", "item": "Фары", "check": "Работают"},
            {"type": "lights", "item": "Стоп-сигналы", "check": "Работают"},
            {"type": "safety", "item": "Тормоза", "check": "Исправны"},
            {"type": "safety", "item": "Ремни безопасности", "check": "Исправны"},
        ]
        return checks

    async def add_daily_check(self, shift_id, check_type, item_name, status, notes=None):
        """Добавляет запись о ежедневной проверке"""
        await self.connection.execute(
            '''INSERT INTO daily_checks (shift_id, check_type, item_name, status, notes) 
               VALUES (?, ?, ?, ?, ?)''',
            (shift_id, check_type, item_name, status, notes)
        )
        await self.connection.commit()
        return True

    # Существующие методы остаются ниже...
    async def register_user(self, telegram_id, full_name, username=None, role='driver', organization_id=None):
        """Регистрирует или обновляет пользователя"""
        await self.connection.execute(
            '''INSERT OR REPLACE INTO users 
               (telegram_id, full_name, username, role, organization_id) 
               VALUES (?, ?, ?, ?, ?)''',
            (telegram_id, full_name, username, role, organization_id)
        )
        await self.connection.commit()
        return True

    async def get_user(self, telegram_id):
        cursor = await self.connection.execute(
            'SELECT * FROM users WHERE telegram_id = ?',
            (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def create_organization(self, name, director_id):
        """Создает организацию и возвращает её ID"""
        cursor = await self.connection.execute(
            'INSERT INTO organizations (name, director_id) VALUES (?, ?)',
            (name, director_id)
        )
        org_id = cursor.lastrowid
        
        await self.connection.execute(
            'UPDATE users SET organization_id = ? WHERE telegram_id = ?',
            (org_id, director_id)
        )
        await self.connection.commit()
        return org_id

    async def get_organization(self, org_id):
        cursor = await self.connection.execute(
            'SELECT * FROM organizations WHERE id = ?',
            (org_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def get_all_organizations(self):
        cursor = await self.connection.execute('SELECT * FROM organizations')
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_all_users(self):
        cursor = await self.connection.execute('SELECT * FROM users')
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_users_by_organization(self, org_id):
        cursor = await self.connection.execute(
            'SELECT * FROM users WHERE organization_id = ? ORDER BY role',
            (org_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_users_by_role(self, role):
        cursor = await self.connection.execute(
            'SELECT * FROM users WHERE role = ?',
            (role,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def update_user_role(self, telegram_id, new_role, organization_id=None):
        """Обновляет роль пользователя"""
        if organization_id:
            await self.connection.execute(
                'UPDATE users SET role = ?, organization_id = ? WHERE telegram_id = ?',
                (new_role, organization_id, telegram_id)
            )
        else:
            await self.connection.execute(
                'UPDATE users SET role = ? WHERE telegram_id = ?',
                (new_role, telegram_id)
            )
        await self.connection.commit()
        return True

    async def get_organization_equipment(self, org_id):
        cursor = await self.connection.execute(
            'SELECT * FROM equipment WHERE organization_id = ?',
            (org_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def close(self):
        await self.connection.close()

db = Database()
