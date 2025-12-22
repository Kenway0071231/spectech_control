import aiosqlite
import logging
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы ролей
ROLES = {
    'botadmin': {'name': 'Администратор бота', 'level': 100},
    'director': {'name': 'Директор компании', 'level': 80},
    'fleetmanager': {'name': 'Начальник парка', 'level': 60},
    'driver': {'name': 'Водитель', 'level': 40}
}

class Database:
    def __init__(self, db_path=None):
        self.db_path = db_path or ':memory:' if os.getenv('BOTHOST') else 'tech_control.db'
        
        # Кэши
        self._org_cache = {}
        self._equipment_cache = {}
        print(f"📦 База данных: {self.db_path}")

    async def connect(self):
        """Подключаемся к базе данных"""
        try:
            self.connection = await aiosqlite.connect(self.db_path)
            self.connection.row_factory = aiosqlite.Row
            await self.create_tables()
            logger.info("✅ База данных подключена")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise

    async def create_tables(self):
        """Создаем таблицы если их нет"""
        tables = [
            '''CREATE TABLE IF NOT EXISTS organizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                director_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                username TEXT,
                role TEXT DEFAULT 'driver',
                organization_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (organization_id) REFERENCES organizations(id)
            )''',
            '''CREATE TABLE IF NOT EXISTS equipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                model TEXT,
                vin TEXT UNIQUE,
                organization_id INTEGER NOT NULL,
                status TEXT DEFAULT 'active',
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (organization_id) REFERENCES organizations(id),
                FOREIGN KEY (created_by) REFERENCES users(telegram_id)
            )''',
            '''CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id INTEGER NOT NULL,
                equipment_id INTEGER NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (driver_id) REFERENCES users(telegram_id),
                FOREIGN KEY (equipment_id) REFERENCES equipment(id)
            )''',
            '''CREATE TABLE IF NOT EXISTS inspections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_id INTEGER NOT NULL,
                check_type TEXT NOT NULL,
                photos TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (shift_id) REFERENCES shifts(id)
            )'''
        ]
        
        for table_sql in tables:
            await self.connection.execute(table_sql)
        await self.connection.commit()

    async def add_test_data(self):
        """Добавляем тестовые данные"""
        try:
            # Тестовая организация
            await self.connection.execute(
                'INSERT OR IGNORE INTO organizations (name, director_id) VALUES (?, ?)',
                ('ООО "СпецТех Контроль"', 123456789)
            )
            
            # Тестовая техника
            equipment = [
                ('Экскаватор CAT 320', 'CAT 320', 'CAT123456789', 1),
                ('Бульдозер Komatsu D65', 'Komatsu D65', 'KOM987654321', 1),
                ('Автокран Liebherr LTM 1100', 'Liebherr LTM 1100', 'LIE555666777', 1)
            ]
            
            for eq in equipment:
                await self.connection.execute(
                    'INSERT OR IGNORE INTO equipment (name, model, vin, organization_id, created_by) VALUES (?, ?, ?, ?, ?)',
                    (*eq, 123456789)
                )
            
            await self.connection.commit()
            logger.info("✅ Тестовые данные добавлены")
        except Exception as e:
            logger.error(f"Ошибка добавления тестовых данных: {e}")

    # ========== ОСНОВНЫЕ МЕТОДЫ ==========

    async def register_user(self, telegram_id: int, full_name: str, username: str = None, 
                          role: str = 'driver', organization_id: int = None) -> bool:
        """Регистрирует или обновляет пользователя"""
        try:
            await self.connection.execute(
                '''INSERT OR REPLACE INTO users 
                   (telegram_id, full_name, username, role, organization_id) 
                   VALUES (?, ?, ?, ?, ?)''',
                (telegram_id, full_name, username, role, organization_id)
            )
            await self.connection.commit()
            
            # Очищаем кэш
            if 'users' in self._org_cache:
                del self._org_cache['users']
                
            logger.info(f"✅ Пользователь {telegram_id} зарегистрирован как {role}")
            return True
        except Exception as e:
            logger.error(f"Ошибка регистрации пользователя: {e}")
            return False

    async def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получает информацию о пользователе"""
        cursor = await self.connection.execute(
            'SELECT * FROM users WHERE telegram_id = ?',
            (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def get_user_role(self, telegram_id: int) -> str:
        """Получает роль пользователя"""
        user = await self.get_user(telegram_id)
        return user['role'] if user else 'driver'

    async def get_user_organization(self, telegram_id: int) -> Optional[int]:
        """Получает организацию пользователя"""
        user = await self.get_user(telegram_id)
        return user['organization_id'] if user else None

    async def create_organization(self, name: str, director_id: int) -> int:
        """Создает новую организацию"""
        try:
            cursor = await self.connection.execute(
                'INSERT INTO organizations (name, director_id) VALUES (?, ?)',
                (name, director_id)
            )
            org_id = cursor.lastrowid
            
            # Обновляем организацию у директора
            await self.connection.execute(
                'UPDATE users SET organization_id = ? WHERE telegram_id = ?',
                (org_id, director_id)
            )
            await self.connection.commit()
            
            logger.info(f"✅ Организация создана: {name} (ID: {org_id})")
            return org_id
        except Exception as e:
            logger.error(f"Ошибка создания организации: {e}")
            return 0

    async def get_organization(self, org_id: int) -> Optional[Dict]:
        """Получает информацию об организации"""
        cursor = await self.connection.execute(
            'SELECT * FROM organizations WHERE id = ?',
            (org_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def get_organization_users(self, org_id: int) -> List[Dict]:
        """Получает всех пользователей организации"""
        cursor = await self.connection.execute(
            'SELECT * FROM users WHERE organization_id = ? ORDER BY role DESC',
            (org_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def update_user_role(self, telegram_id: int, new_role: str, 
                             organization_id: int = None) -> bool:
        """Изменяет роль пользователя"""
        try:
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
            logger.info(f"✅ Роль пользователя {telegram_id} изменена на {new_role}")
            return True
        except Exception as e:
            logger.error(f"Ошибка изменения роли: {e}")
            return False

    async def add_equipment(self, name: str, model: str, vin: str, 
                          organization_id: int, created_by: int) -> int:
        """Добавляет технику"""
        try:
            cursor = await self.connection.execute(
                '''INSERT INTO equipment (name, model, vin, organization_id, created_by) 
                   VALUES (?, ?, ?, ?, ?)''',
                (name, model, vin, organization_id, created_by)
            )
            await self.connection.commit()
            
            # Очищаем кэш техники
            if organization_id in self._equipment_cache:
                del self._equipment_cache[organization_id]
                
            logger.info(f"✅ Техника добавлена: {name}")
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления техники: {e}")
            return 0

    async def get_organization_equipment(self, org_id: int) -> List[Dict]:
        """Получает технику организации"""
        if org_id in self._equipment_cache:
            return self._equipment_cache[org_id]
            
        cursor = await self.connection.execute(
            'SELECT id, name, model, status FROM equipment WHERE organization_id = ? ORDER BY name',
            (org_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        
        equipment = [dict(row) for row in rows]
        self._equipment_cache[org_id] = equipment
        return equipment

    async def start_shift(self, driver_id: int, equipment_id: int) -> int:
        """Начинает смену"""
        cursor = await self.connection.execute(
            'INSERT INTO shifts (driver_id, equipment_id) VALUES (?, ?)',
            (driver_id, equipment_id)
        )
        await self.connection.commit()
        return cursor.lastrowid

    async def end_shift(self, shift_id: int) -> bool:
        """Завершает смену"""
        await self.connection.execute(
            'UPDATE shifts SET end_time = CURRENT_TIMESTAMP, status = "completed" WHERE id = ?',
            (shift_id,)
        )
        await self.connection.commit()
        return True

    async def get_active_shift(self, driver_id: int) -> Optional[Dict]:
        """Получает активную смену"""
        cursor = await self.connection.execute(
            '''SELECT s.id, s.equipment_id, e.name, e.model 
               FROM shifts s 
               JOIN equipment e ON s.equipment_id = e.id 
               WHERE s.driver_id = ? AND s.status = "active" 
               LIMIT 1''',
            (driver_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def add_inspection(self, shift_id: int, photos: List[str], notes: str = "") -> int:
        """Добавляет осмотр"""
        photos_json = json.dumps(photos)
        cursor = await self.connection.execute(
            'INSERT INTO inspections (shift_id, check_type, photos, notes) VALUES (?, ?, ?, ?)',
            (shift_id, 'pre_shift', photos_json, notes)
        )
        await self.connection.commit()
        return cursor.lastrowid

    async def get_user_shifts(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Получает смены пользователя"""
        cursor = await self.connection.execute('''
            SELECT s.id, s.start_time, s.end_time, s.status,
                   e.name as equipment_name, e.model as equipment_model
            FROM shifts s
            JOIN equipment e ON s.equipment_id = e.id
            WHERE s.driver_id = ?
            ORDER BY s.start_time DESC
            LIMIT ?
        ''', (user_id, limit))
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def close(self):
        """Закрывает соединение"""
        await self.connection.close()

# Глобальный экземпляр
db = Database()
