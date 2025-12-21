import aiosqlite
import logging
import os
import json
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path=None):
        # Кэш для быстрого доступа
        self._equipment_cache = None
        self._admins_cache = None
        
        # Для хостинга используем базу в памяти
        if os.getenv('BOTHOST') or os.getenv('ON_HOSTING'):
            self.db_path = ':memory:'
        else:
            self.db_path = db_path or 'tech_control.db'
        
        print(f"📦 База данных: {self.db_path}")

    async def connect(self):
        """Подключаемся к базе данных"""
        try:
            self.connection = await aiosqlite.connect(self.db_path)
            self.connection.row_factory = aiosqlite.Row  # Для удобного доступа
            await self.create_tables()
            logger.info("✅ База данных подключена")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise

    async def create_tables(self):
        """Создаем таблицы если их нет"""
        tables = [
            '''CREATE TABLE IF NOT EXISTS equipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                model TEXT,
                vin TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                phone TEXT,
                role TEXT DEFAULT 'driver',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id INTEGER NOT NULL,
                equipment_id INTEGER NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (driver_id) REFERENCES drivers (id),
                FOREIGN KEY (equipment_id) REFERENCES equipment (id)
            )''',
            '''CREATE TABLE IF NOT EXISTS inspections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_id INTEGER NOT NULL,
                check_type TEXT NOT NULL,
                photos TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (shift_id) REFERENCES shifts (id)
            )'''
        ]
        
        for table_sql in tables:
            try:
                await self.connection.execute(table_sql)
            except Exception as e:
                logger.error(f"❌ Ошибка создания таблицы: {e}")
        
        await self.connection.commit()

    async def add_test_data(self):
        """Добавляем тестовые данные"""
        equipment = [
            ('Экскаватор CAT 320', 'CAT 320', 'CAT123456789'),
            ('Бульдозер Komatsu D65', 'Komatsu D65', 'KOM987654321'),
            ('Автокран Liebherr LTM 1100', 'Liebherr LTM 1100', 'LIE555666777')
        ]
        
        for eq in equipment:
            try:
                await self.connection.execute(
                    'INSERT OR IGNORE INTO equipment (name, model, vin) VALUES (?, ?, ?)',
                    eq
                )
            except Exception:
                pass
        
        await self.connection.commit()
        logger.info("✅ Тестовые данные добавлены")

    async def get_equipment_list(self):
        """Быстро получаем список техники (с кэшированием)"""
        if self._equipment_cache is None:
            cursor = await self.connection.execute(
                'SELECT id, name, model FROM equipment ORDER BY name'
            )
            rows = await cursor.fetchall()
            self._equipment_cache = rows
            await cursor.close()
        return self._equipment_cache

    async def clear_cache(self):
        """Очищаем кэш"""
        self._equipment_cache = None
        self._admins_cache = None

    async def register_driver(self, telegram_id, full_name, role='driver'):
        """Регистрируем пользователя"""
        await self.connection.execute(
            'INSERT OR REPLACE INTO drivers (telegram_id, full_name, role) VALUES (?, ?, ?)',
            (telegram_id, full_name, role)
        )
        await self.connection.commit()
        
        # Очищаем кэш админов при изменении ролей
        if role == 'admin':
            self._admins_cache = None
        
        return telegram_id

    async def get_user_role(self, telegram_id):
        """Быстро получаем роль пользователя"""
        cursor = await self.connection.execute(
            'SELECT role FROM drivers WHERE telegram_id = ?',
            (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row['role'] if row else 'driver'

    async def get_all_admins(self):
        """Быстро получаем всех администраторов (с кэшированием)"""
        if self._admins_cache is None:
            cursor = await self.connection.execute(
                'SELECT telegram_id, full_name FROM drivers WHERE role = "admin"'
            )
            rows = await cursor.fetchall()
            self._admins_cache = rows
            await cursor.close()
        return self._admins_cache

    async def start_shift(self, driver_id, equipment_id):
        """Начинаем новую смену"""
        cursor = await self.connection.execute(
            'INSERT INTO shifts (driver_id, equipment_id) VALUES (?, ?)',
            (driver_id, equipment_id)
        )
        await self.connection.commit()
        return cursor.lastrowid

    async def end_shift(self, shift_id):
        """Завершаем смену"""
        await self.connection.execute(
            'UPDATE shifts SET end_time = CURRENT_TIMESTAMP, status = "completed" WHERE id = ?',
            (shift_id,)
        )
        await self.connection.commit()
        return True

    async def get_active_shift(self, driver_id):
        """Быстро получаем активную смену водителя"""
        cursor = await self.connection.execute(
            'SELECT id, equipment_id FROM shifts WHERE driver_id = ? AND status = "active" ORDER BY start_time DESC LIMIT 1',
            (driver_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def get_shift_details(self, shift_id):
        """Получаем детали смены"""
        cursor = await self.connection.execute('''
            SELECT s.id, s.start_time, s.end_time, s.status,
                   d.full_name, d.telegram_id,
                   e.name, e.model, e.id as equipment_id
            FROM shifts s
            JOIN drivers d ON s.driver_id = d.telegram_id
            JOIN equipment e ON s.equipment_id = e.id
            WHERE s.id = ?
        ''', (shift_id,))
        row = await cursor.fetchone()
        await cursor.close()
        
        if row:
            return dict(row)
        return None

    async def add_inspection_with_photos(self, shift_id, photo_ids, notes=""):
        """Добавляем осмотр с фотографиями"""
        photos_json = json.dumps(photo_ids) if photo_ids else None
        
        cursor = await self.connection.execute(
            'INSERT INTO inspections (shift_id, check_type, photos, notes) VALUES (?, ?, ?, ?)',
            (shift_id, 'pre_shift', photos_json, notes)
        )
        await self.connection.commit()
        return cursor.lastrowid

    async def close(self):
        """Закрываем соединение с базой"""
        await self.connection.close()

# Глобальный экземпляр базы данных
db = Database()
