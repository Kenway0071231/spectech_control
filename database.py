import aiosqlite
import logging
import os
import json
from datetime import datetime

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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )'''
        ]
        
        for table_sql in tables:
            await self.connection.execute(table_sql)
        await self.connection.commit()

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

    async def add_equipment(self, name, model, vin, organization_id):
        cursor = await self.connection.execute(
            'INSERT INTO equipment (name, model, vin, organization_id) VALUES (?, ?, ?, ?)',
            (name, model, vin, organization_id)
        )
        await self.connection.commit()
        return cursor.lastrowid

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
