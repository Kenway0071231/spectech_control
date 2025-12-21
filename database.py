import aiosqlite
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path=None):
        # Для bothost.ru используем базу в памяти
        if os.getenv('BOTHOST') or os.getenv('ON_HOSTING'):
            self.db_path = ':memory:'  # База в оперативной памяти
            print("⚡ Используется база данных в памяти (хостинг)")
        else:
            self.db_path = db_path or 'tech_control.db'
            print(f"💾 Используется файловая база: {self.db_path}")

    async def connect(self):
        """Подключаемся к базе данных"""
        self.connection = await aiosqlite.connect(self.db_path)
        await self.create_tables()
        logger.info("✅ База данных подключена")

    async def create_tables(self):
        """Создаем таблицы если их нет"""
        
        # Таблица техники
        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS equipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                model TEXT,
                vin TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица водителей
        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                phone TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица смен
        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id INTEGER NOT NULL,
                equipment_id INTEGER NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (driver_id) REFERENCES drivers (id),
                FOREIGN KEY (equipment_id) REFERENCES equipment (id)
            )
        ''')
        
        # Таблица проверок/осмотров
        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS inspections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_id INTEGER NOT NULL,
                check_type TEXT NOT NULL,
                photos TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (shift_id) REFERENCES shifts (id)
            )
        ''')
        
        await self.connection.commit()
        logger.info("✅ Таблицы созданы/проверены")

    async def add_test_data(self):
        """Добавляем тестовые данные для демонстрации"""
        
        # Тестовая техника
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
            except:
                pass
        
        await self.connection.commit()
        logger.info("✅ Тестовые данные добавлены")

    async def get_equipment_list(self):
        """Получаем список всей техники"""
        cursor = await self.connection.execute('SELECT id, name, model FROM equipment ORDER BY name')
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def register_driver(self, telegram_id, full_name):
        """Регистрируем водителя"""
        await self.connection.execute(
            'INSERT OR REPLACE INTO drivers (telegram_id, full_name) VALUES (?, ?)',
            (telegram_id, full_name)
        )
        await self.connection.commit()
        return telegram_id

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
        logger.info(f"✅ Смена {shift_id} завершена")
        return True

    async def get_active_shift(self, driver_id):
        """Получаем активную смену водителя"""
        cursor = await self.connection.execute(
            'SELECT id, equipment_id FROM shifts WHERE driver_id = ? AND status = "active" ORDER BY start_time DESC LIMIT 1',
            (driver_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def get_driver_shifts(self, driver_id, limit=10):
        """Получаем последние смены водителя"""
        cursor = await self.connection.execute('''
            SELECT s.id, s.start_time, s.end_time, s.status, 
                   e.name, e.model
            FROM shifts s
            LEFT JOIN equipment e ON s.equipment_id = e.id
            WHERE s.driver_id = ?
            ORDER BY s.start_time DESC
            LIMIT ?
        ''', (driver_id, limit))
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def close(self):
        """Закрываем соединение с базой"""
        await self.connection.close()

# Создаем глобальный экземпляр базы данных
db = Database()
