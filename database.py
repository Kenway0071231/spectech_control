import aiosqlite
import logging
import os
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы ролей (иерархия сверху вниз)
ROLES = {
    'botadmin': {'name': 'Администратор бота', 'level': 100, 'can_manage': ['director', 'fleetmanager', 'driver']},
    'director': {'name': 'Директор компании', 'level': 80, 'can_manage': ['fleetmanager', 'driver']},
    'fleetmanager': {'name': 'Начальник парка', 'level': 60, 'can_manage': ['driver']},
    'driver': {'name': 'Водитель', 'level': 40, 'can_manage': []}
}

class Database:
    def __init__(self, db_path=None):
        # Кэш для быстрого доступа
        self._equipment_cache = None
        self._users_cache = {}
        
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
                name TEXT NOT NULL UNIQUE,
                director_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (director_id) REFERENCES drivers(telegram_id)
            )''',
            '''CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                phone TEXT,
                role TEXT DEFAULT 'driver',
                organization_id INTEGER,
                assigned_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (organization_id) REFERENCES organizations(id),
                FOREIGN KEY (assigned_by) REFERENCES drivers(telegram_id)
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
                FOREIGN KEY (created_by) REFERENCES drivers(telegram_id)
            )''',
            '''CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id INTEGER NOT NULL,
                equipment_id INTEGER NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                organization_id INTEGER NOT NULL,
                FOREIGN KEY (driver_id) REFERENCES drivers(telegram_id),
                FOREIGN KEY (equipment_id) REFERENCES equipment(id),
                FOREIGN KEY (organization_id) REFERENCES organizations(id)
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
            try:
                await self.connection.execute(table_sql)
            except Exception as e:
                logger.error(f"❌ Ошибка создания таблицы: {e}")
        
        await self.connection.commit()

    async def add_test_data(self):
        """Добавляем тестовые данные"""
        # Тестовая организация
        try:
            await self.connection.execute(
                'INSERT OR IGNORE INTO organizations (name, director_id) VALUES (?, ?)',
                ('Тестовая компания ООО "СпецТех"', 123456789)  # ID администратора
            )
            await self.connection.commit()
        except:
            pass

        # Тестовая техника
        equipment = [
            ('Экскаватор CAT 320', 'CAT 320', 'CAT123456789', 1),
            ('Бульдозер Komatsu D65', 'Komatsu D65', 'KOM987654321', 1),
            ('Автокран Liebherr LTM 1100', 'Liebherr LTM 1100', 'LIE555666777', 1)
        ]
        
        for eq in equipment:
            try:
                await self.connection.execute(
                    'INSERT OR IGNORE INTO equipment (name, model, vin, organization_id, created_by) VALUES (?, ?, ?, ?, ?)',
                    (*eq, 123456789)
                )
            except Exception:
                pass
        
        await self.connection.commit()
        logger.info("✅ Тестовые данные добавлены")

    # ========== МЕТОДЫ ДЛЯ РОЛЕЙ ==========

    async def register_user(self, telegram_id: int, full_name: str, role: str = 'driver', 
                          organization_id: int = None, assigned_by: int = None) -> int:
        """Регистрируем пользователя с ролью"""
        await self.connection.execute(
            '''INSERT OR REPLACE INTO drivers 
               (telegram_id, full_name, role, organization_id, assigned_by) 
               VALUES (?, ?, ?, ?, ?)''',
            (telegram_id, full_name, role, organization_id, assigned_by)
        )
        await self.connection.commit()
        
        # Очищаем кэш
        if telegram_id in self._users_cache:
            del self._users_cache[telegram_id]
        
        logger.info(f"✅ Пользователь {telegram_id} зарегистрирован как {role}")
        return telegram_id

    async def get_user_role(self, telegram_id: int) -> str:
        """Получаем роль пользователя"""
        if telegram_id in self._users_cache:
            return self._users_cache[telegram_id].get('role', 'driver')
        
        cursor = await self.connection.execute(
            'SELECT role FROM drivers WHERE telegram_id = ?',
            (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        
        role = row['role'] if row else 'driver'
        
        # Сохраняем в кэш
        if telegram_id not in self._users_cache:
            self._users_cache[telegram_id] = {}
        self._users_cache[telegram_id]['role'] = role
        
        return role

    async def get_user_info(self, telegram_id: int) -> Dict:
        """Получаем полную информацию о пользователе"""
        cursor = await self.connection.execute('''
            SELECT d.*, o.name as organization_name 
            FROM drivers d 
            LEFT JOIN organizations o ON d.organization_id = o.id 
            WHERE d.telegram_id = ?
        ''', (telegram_id,))
        row = await cursor.fetchone()
        await cursor.close()
        
        if row:
            info = dict(row)
            info['role_name'] = ROLES.get(info['role'], {}).get('name', 'Неизвестно')
            return info
        return None

    async def can_manage_role(self, manager_role: str, target_role: str) -> bool:
        """Проверяет, может ли менеджер управлять целевой ролью"""
        if manager_role not in ROLES:
            return False
        return target_role in ROLES[manager_role]['can_manage']

    async def change_user_role(self, telegram_id: int, new_role: str, 
                             changed_by: int, organization_id: int = None) -> bool:
        """Изменяет роль пользователя"""
        # Получаем текущую роль
        current_role = await self.get_user_role(telegram_id)
        
        # Получаем роль того, кто меняет
        changer_role = await self.get_user_role(changed_by)
        
        # Проверяем права
        if not await self.can_manage_role(changer_role, new_role):
            logger.warning(f"❌ {changer_role} не может назначить роль {new_role}")
            return False
        
        # Меняем роль
        await self.connection.execute(
            'UPDATE drivers SET role = ?, organization_id = ?, assigned_by = ? WHERE telegram_id = ?',
            (new_role, organization_id, changed_by, telegram_id)
        )
        await self.connection.commit()
        
        # Очищаем кэш
        if telegram_id in self._users_cache:
            del self._users_cache[telegram_id]
        
        logger.info(f"✅ Роль пользователя {telegram_id} изменена с {current_role} на {new_role}")
        return True

    async def get_users_by_role(self, role: str, organization_id: int = None) -> List[Dict]:
        """Получает всех пользователей с указанной ролью"""
        query = 'SELECT * FROM drivers WHERE role = ?'
        params = [role]
        
        if organization_id:
            query += ' AND organization_id = ?'
            params.append(organization_id)
        
        cursor = await self.connection.execute(query, params)
        rows = await cursor.fetchall()
        await cursor.close()
        
        return [dict(row) for row in rows]

    async def get_users_in_organization(self, organization_id: int) -> List[Dict]:
        """Получает всех пользователей в организации"""
        cursor = await self.connection.execute(
            'SELECT * FROM drivers WHERE organization_id = ? ORDER BY role DESC',
            (organization_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        
        return [dict(row) for row in rows]

    # ========== МЕТОДЫ ДЛЯ ОРГАНИЗАЦИЙ ==========

    async def create_organization(self, name: str, director_id: int) -> int:
        """Создает новую организацию"""
        cursor = await self.connection.execute(
            'INSERT INTO organizations (name, director_id) VALUES (?, ?)',
            (name, director_id)
        )
        await self.connection.commit()
        
        org_id = cursor.lastrowid
        
        # Обновляем организацию у директора
        await self.connection.execute(
            'UPDATE drivers SET organization_id = ? WHERE telegram_id = ?',
            (org_id, director_id)
        )
        await self.connection.commit()
        
        logger.info(f"✅ Создана организация {name} (ID: {org_id})")
        return org_id

    async def get_organization_info(self, organization_id: int) -> Dict:
        """Получает информацию об организации"""
        cursor = await self.connection.execute(
            'SELECT * FROM organizations WHERE id = ?',
            (organization_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        
        return dict(row) if row else None

    async def get_user_organization(self, telegram_id: int) -> Optional[int]:
        """Получает ID организации пользователя"""
        cursor = await self.connection.execute(
            'SELECT organization_id FROM drivers WHERE telegram_id = ?',
            (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        
        return row['organization_id'] if row else None

    # ========== МЕТОДЫ ДЛЯ ТЕХНИКИ ==========

    async def add_equipment(self, name: str, model: str, vin: str, 
                          organization_id: int, created_by: int) -> int:
        """Добавляет технику в организацию"""
        cursor = await self.connection.execute(
            '''INSERT INTO equipment (name, model, vin, organization_id, created_by) 
               VALUES (?, ?, ?, ?, ?)''',
            (name, model, vin, organization_id, created_by)
        )
        await self.connection.commit()
        
        # Очищаем кэш техники
        self._equipment_cache = None
        
        logger.info(f"✅ Добавлена техника: {name} ({model})")
        return cursor.lastrowid

    async def get_equipment_list(self, organization_id: int = None) -> List[Tuple]:
        """Получает список техники (все или по организации)"""
        if organization_id:
            cursor = await self.connection.execute(
                'SELECT id, name, model, status FROM equipment WHERE organization_id = ? ORDER BY name',
                (organization_id,)
            )
        else:
            cursor = await self.connection.execute(
                'SELECT id, name, model, status FROM equipment ORDER BY name'
            )
        
        rows = await cursor.fetchall()
        await cursor.close()
        
        return [(row['id'], row['name'], row['model'], row['status']) for row in rows]

    async def update_equipment_status(self, equipment_id: int, status: str) -> bool:
        """Обновляет статус техники"""
        await self.connection.execute(
            'UPDATE equipment SET status = ? WHERE id = ?',
            (status, equipment_id)
        )
        await self.connection.commit()
        
        # Очищаем кэш
        self._equipment_cache = None
        
        return True

    # ========== МЕТОДЫ ДЛЯ СМЕН ==========

    async def start_shift(self, driver_id: int, equipment_id: int, organization_id: int) -> int:
        """Начинает новую смену"""
        cursor = await self.connection.execute(
            '''INSERT INTO shifts (driver_id, equipment_id, organization_id) 
               VALUES (?, ?, ?)''',
            (driver_id, equipment_id, organization_id)
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

    async def get_active_shift(self, driver_id: int):
        """Получает активную смену водителя"""
        cursor = await self.connection.execute(
            '''SELECT s.id, s.equipment_id, e.name, e.model 
               FROM shifts s 
               JOIN equipment e ON s.equipment_id = e.id 
               WHERE s.driver_id = ? AND s.status = "active" 
               ORDER BY s.start_time DESC LIMIT 1''',
            (driver_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def get_organization_shifts(self, organization_id: int, limit: int = 50) -> List[Dict]:
        """Получает смены организации"""
        cursor = await self.connection.execute('''
            SELECT s.id, s.start_time, s.end_time, s.status,
                   d.full_name as driver_name,
                   e.name as equipment_name, e.model as equipment_model
            FROM shifts s
            JOIN drivers d ON s.driver_id = d.telegram_id
            JOIN equipment e ON s.equipment_id = e.id
            WHERE s.organization_id = ?
            ORDER BY s.start_time DESC
            LIMIT ?
        ''', (organization_id, limit))
        rows = await cursor.fetchall()
        await cursor.close()
        
        return [dict(row) for row in rows]

    async def add_inspection_with_photos(self, shift_id: int, photo_ids: List[str], notes: str = "") -> int:
        """Добавляет осмотр с фотографиями"""
        photos_json = json.dumps(photo_ids) if photo_ids else None
        
        cursor = await self.connection.execute(
            'INSERT INTO inspections (shift_id, check_type, photos, notes) VALUES (?, ?, ?, ?)',
            (shift_id, 'pre_shift', photos_json, notes)
        )
        await self.connection.commit()
        return cursor.lastrowid

    async def close(self):
        """Закрывает соединение с базой"""
        await self.connection.close()

# Глобальный экземпляр базы данных
db = Database()
