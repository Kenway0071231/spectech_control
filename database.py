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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
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
                FOREIGN KEY (equipment_id) REFERENCES equipment (id),
                FOREIGN KEY (approved_by) REFERENCES users (telegram_id)
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
            )''',
            '''CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                read BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (telegram_id)
            )''',
            '''CREATE TABLE IF NOT EXISTS action_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (telegram_id)
            )''',
            '''CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                organization_id INTEGER NOT NULL,
                report_type TEXT NOT NULL,
                period TEXT NOT NULL,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
            )'''
        ]
        
        for table_sql in tables:
            try:
                await self.connection.execute(table_sql)
            except Exception as e:
                logger.error(f"Ошибка создания таблицы: {e}")
        
        await self.connection.commit()

    # ========== МЕТОДЫ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ ==========

    async def register_user(self, telegram_id, full_name, username=None, role='driver', organization_id=None):
        """Регистрирует или обновляет пользователя"""
        try:
            await self.connection.execute(
                '''INSERT OR REPLACE INTO users 
                   (telegram_id, full_name, username, role, organization_id) 
                   VALUES (?, ?, ?, ?, ?)''',
                (telegram_id, full_name, username, role, organization_id)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка регистрации пользователя: {e}")
            return False

    async def get_user(self, telegram_id):
        """Получает пользователя по ID"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM users WHERE telegram_id = ?',
                (telegram_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения пользователя: {e}")
            return None

    async def get_all_users(self):
        """Получает всех пользователей"""
        try:
            cursor = await self.connection.execute('SELECT * FROM users ORDER BY role, full_name')
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения всех пользователей: {e}")
            return []

    async def get_users_by_organization(self, org_id):
        """Получает пользователей организации"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM users WHERE organization_id = ? ORDER BY role, full_name',
                (org_id,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей организации: {e}")
            return []

    async def get_users_by_role(self, role):
        """Получает пользователей по роли"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM users WHERE role = ?',
                (role,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей по роли: {e}")
            return []

    async def update_user_role(self, telegram_id, new_role, organization_id=None):
        """Обновляет роль пользователя"""
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
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления роли пользователя: {e}")
            return False

    # ========== МЕТОДЫ ДЛЯ ОРГАНИЗАЦИЙ ==========

    async def create_organization_for_director(self, director_id, name):
        """Создает организацию для директора с проверкой"""
        try:
            # Проверяем, есть ли у директора уже организация
            director = await self.get_user(director_id)
            if director and director.get('organization_id'):
                # Получаем текущую организацию
                current_org = await self.get_organization(director['organization_id'])
                if current_org:
                    return None, f"У вас уже есть организация: {current_org['name']}"
            
            # Создаем организацию
            cursor = await self.connection.execute(
                'INSERT INTO organizations (name, director_id) VALUES (?, ?)',
                (name, director_id)
            )
            org_id = cursor.lastrowid
            
            # Обновляем пользователя
            await self.connection.execute(
                'UPDATE users SET organization_id = ?, role = ? WHERE telegram_id = ?',
                (org_id, 'director', director_id)
            )
            await self.connection.commit()
            return org_id, None
        except Exception as e:
            logger.error(f"Ошибка создания организации для директора: {e}")
            return None, str(e)

    async def create_organization(self, name, director_id):
        """Создает организацию (старый метод для совместимости)"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка создания организации: {e}")
            return None

    async def get_organization(self, org_id):
        """Получает организацию по ID"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM organizations WHERE id = ?',
                (org_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения организации: {e}")
            return None

    async def get_all_organizations(self):
        """Получает все организации"""
        try:
            cursor = await self.connection.execute('SELECT * FROM organizations ORDER BY name')
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения всех организаций: {e}")
            return []

    async def get_organization_by_director(self, director_id):
        """Получает организацию по ID директора"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM organizations WHERE director_id = ?',
                (director_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения организации по директору: {e}")
            return None

    async def update_organization_name(self, org_id, new_name):
        """Обновляет название организации"""
        try:
            await self.connection.execute(
                'UPDATE organizations SET name = ? WHERE id = ?',
                (new_name, org_id)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления названия организации: {e}")
            return False

    async def get_organization_stats(self, org_id):
        """Получает статистику организации"""
        try:
            stats = {}
            
            # Количество сотрудников по ролям
            cursor = await self.connection.execute(
                '''SELECT role, COUNT(*) as count FROM users 
                   WHERE organization_id = ? 
                   GROUP BY role''',
                (org_id,)
            )
            roles = await cursor.fetchall()
            stats['roles'] = {role['role']: role['count'] for role in roles}
            
            # Количество техники по статусам
            cursor = await self.connection.execute(
                '''SELECT status, COUNT(*) as count FROM equipment 
                   WHERE organization_id = ? 
                   GROUP BY status''',
                (org_id,)
            )
            equipment_stats = await cursor.fetchall()
            stats['equipment'] = {item['status']: item['count'] for item in equipment_stats}
            
            # Активные смены
            cursor = await self.connection.execute(
                '''SELECT COUNT(*) as count FROM shifts s
                   JOIN equipment e ON s.equipment_id = e.id
                   WHERE e.organization_id = ? AND s.status = 'active' ''',
                (org_id,)
            )
            active_shifts = await cursor.fetchone()
            stats['active_shifts'] = active_shifts['count'] if active_shifts else 0
            
            # Количество ТО на этой неделе
            cursor = await self.connection.execute(
                '''SELECT COUNT(*) as count FROM maintenance m
                   JOIN equipment e ON m.equipment_id = e.id
                   WHERE e.organization_id = ? 
                   AND m.scheduled_date BETWEEN date('now') AND date('now', '+7 days')
                   AND m.status = 'scheduled' ''',
                (org_id,)
            )
            weekly_maintenance = await cursor.fetchone()
            stats['weekly_maintenance'] = weekly_maintenance['count'] if weekly_maintenance else 0
            
            return stats
        except Exception as e:
            logger.error(f"Ошибка получения статистики организации: {e}")
            return {}

    # ========== МЕТОДЫ ДЛЯ ТЕХНИКИ ==========

    async def add_equipment(self, name, model, vin, organization_id):
        """Добавляет технику"""
        try:
            cursor = await self.connection.execute(
                'INSERT INTO equipment (name, model, vin, organization_id) VALUES (?, ?, ?, ?)',
                (name, model, vin, organization_id)
            )
            await self.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления техники: {e}")
            return None

    async def get_organization_equipment(self, org_id):
        """Получает технику организации"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM equipment WHERE organization_id = ? ORDER BY name',
                (org_id,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения техники организации: {e}")
            return []

    async def get_equipment_by_driver(self, driver_id):
        """Получает технику доступную водителю"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка получения техники водителя: {e}")
            return []

    async def get_equipment_by_id(self, equipment_id):
        """Получает технику по ID"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM equipment WHERE id = ?',
                (equipment_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения техники по ID: {e}")
            return None

    async def update_equipment_maintenance_date(self, equipment_id, next_date):
        """Обновляет дату следующего ТО для техники"""
        try:
            await self.connection.execute(
                'UPDATE equipment SET next_maintenance = ? WHERE id = ?',
                (next_date, equipment_id)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления даты ТО техники: {e}")
            return False

    # ========== МЕТОДЫ ДЛЯ СМЕН ==========

    async def start_shift(self, driver_id, equipment_id, briefing_confirmed=False):
        """Начинает новую смену"""
        try:
            cursor = await self.connection.execute(
                '''INSERT INTO shifts (driver_id, equipment_id, briefing_confirmed, status) 
                   VALUES (?, ?, ?, 'active')''',
                (driver_id, equipment_id, briefing_confirmed)
            )
            shift_id = cursor.lastrowid
            await self.connection.commit()
            return shift_id
        except Exception as e:
            logger.error(f"Ошибка начала смены: {e}")
            return None

    async def get_active_shift(self, driver_id):
        """Получает активную смену водителя"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка получения активной смены: {e}")
            return None

    async def get_shift_by_id(self, shift_id):
        """Получает смену по ID"""
        try:
            cursor = await self.connection.execute(
                '''SELECT s.*, e.name as equipment_name, e.model, u.full_name as driver_name
                   FROM shifts s
                   JOIN equipment e ON s.equipment_id = e.id
                   JOIN users u ON s.driver_id = u.telegram_id
                   WHERE s.id = ?''',
                (shift_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения смены по ID: {e}")
            return None

    async def update_shift_photo(self, shift_id, photo_file_id):
        """Обновляет фото осмотра для смены"""
        try:
            await self.connection.execute(
                'UPDATE shifts SET inspection_photo = ? WHERE id = ?',
                (photo_file_id, shift_id)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления фото смены: {e}")
            return False

    async def complete_shift(self, shift_id, notes=None):
        """Завершает смену"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка завершения смены: {e}")
            return False

    async def approve_inspection(self, shift_id, approved_by):
        """Подтверждает осмотр техники"""
        try:
            await self.connection.execute(
                '''UPDATE shifts 
                   SET inspection_approved = 1,
                       approved_by = ?
                   WHERE id = ?''',
                (approved_by, shift_id)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка подтверждения осмотра: {e}")
            return False

    async def get_shifts_by_driver(self, driver_id, limit=10):
        """Получает смены водителя"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка получения смен водителя: {e}")
            return []

    async def get_pending_inspections(self, organization_id):
        """Получает смены ожидающие проверки осмотра"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка получения ожидающих проверок: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ ТО (ТЕХНИЧЕСКОГО ОБСЛУЖИВАНИЯ) ==========

    async def add_maintenance(self, equipment_id, type, scheduled_date, description=None):
        """Добавляет запись о ТО"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка добавления ТО: {e}")
            return None

    async def get_upcoming_maintenance(self, days=7):
        """Получает предстоящие ТО в ближайшие дни"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка получения предстоящих ТО: {e}")
            return []

    async def get_maintenance_by_equipment(self, equipment_id):
        """Получает ТО для конкретной техники"""
        try:
            cursor = await self.connection.execute(
                '''SELECT * FROM maintenance 
                   WHERE equipment_id = ? 
                   ORDER BY scheduled_date DESC''',
                (equipment_id,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения ТО техники: {e}")
            return []

    async def get_maintenance_by_id(self, maintenance_id):
        """Получает ТО по ID"""
        try:
            cursor = await self.connection.execute(
                '''SELECT m.*, e.name as equipment_name, e.model, e.vin
                   FROM maintenance m
                   JOIN equipment e ON m.equipment_id = e.id
                   WHERE m.id = ?''',
                (maintenance_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения ТО по ID: {e}")
            return None

    async def mark_maintenance_notified(self, maintenance_id):
        """Отмечает что уведомление о ТО отправлено"""
        try:
            await self.connection.execute(
                'UPDATE maintenance SET notified = 1 WHERE id = ?',
                (maintenance_id,)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка отметки уведомления ТО: {e}")
            return False

    async def complete_maintenance(self, maintenance_id, cost=None):
        """Отмечает ТО как выполненное"""
        try:
            await self.connection.execute(
                '''UPDATE maintenance 
                   SET status = 'completed', 
                       completed_date = CURRENT_DATE,
                       cost = ?
                   WHERE id = ?''',
                (cost, maintenance_id)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка завершения ТО: {e}")
            return False

    async def get_active_maintenance(self, organization_id=None):
        """Получает активные (не выполненные) ТО"""
        try:
            if organization_id:
                cursor = await self.connection.execute(
                    '''SELECT m.*, e.name as equipment_name, e.model
                       FROM maintenance m
                       JOIN equipment e ON m.equipment_id = e.id
                       WHERE e.organization_id = ? 
                       AND m.status = 'scheduled'
                       ORDER BY m.scheduled_date''',
                    (organization_id,)
                )
            else:
                cursor = await self.connection.execute(
                    '''SELECT m.*, e.name as equipment_name, e.model
                       FROM maintenance m
                       JOIN equipment e ON m.equipment_id = e.id
                       WHERE m.status = 'scheduled'
                       ORDER BY m.scheduled_date''',
                )
            
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения активных ТО: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ ЕЖЕДНЕВНЫХ ПРОВЕРОК ==========

    async def get_daily_checks(self):
        """Получает список ежедневных проверок"""
        # Стандартный список проверок
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
        try:
            await self.connection.execute(
                '''INSERT INTO daily_checks (shift_id, check_type, item_name, status, notes) 
                   VALUES (?, ?, ?, ?, ?)''',
                (shift_id, check_type, item_name, status, notes)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления ежедневной проверки: {e}")
            return False

    async def get_checks_by_shift(self, shift_id):
        """Получает проверки для смены"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM daily_checks WHERE shift_id = ? ORDER BY created_at',
                (shift_id,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения проверок смены: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ УВЕДОМЛЕНИЙ ==========

    async def add_notification(self, user_id, message):
        """Добавляет уведомление"""
        try:
            await self.connection.execute(
                'INSERT INTO notifications (user_id, message) VALUES (?, ?)',
                (user_id, message)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления уведомления: {e}")
            return False

    async def get_unread_notifications(self, user_id):
        """Получает непрочитанные уведомления пользователя"""
        try:
            cursor = await self.connection.execute(
                'SELECT * FROM notifications WHERE user_id = ? AND read = 0 ORDER BY created_at DESC',
                (user_id,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения непрочитанных уведомлений: {e}")
            return []

    async def mark_notification_read(self, notification_id):
        """Отмечает уведомление как прочитанное"""
        try:
            await self.connection.execute(
                'UPDATE notifications SET read = 1 WHERE id = ?',
                (notification_id,)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка отметки уведомления как прочитанного: {e}")
            return False

    # ========== МЕТОДЫ ДЛЯ ЛОГИРОВАНИЯ ДЕЙСТВИЙ ==========

    async def log_action(self, user_id, action_type, details):
        """Логирует действия пользователей"""
        try:
            await self.connection.execute(
                'INSERT INTO action_logs (user_id, action_type, details) VALUES (?, ?, ?)',
                (user_id, action_type, details)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка логирования действия: {e}")
            return False

    async def get_recent_actions(self, org_id=None, limit=20):
        """Получает последние действия"""
        try:
            if org_id:
                cursor = await self.connection.execute(
                    '''SELECT al.*, u.full_name, u.role 
                       FROM action_logs al
                       JOIN users u ON al.user_id = u.telegram_id
                       WHERE u.organization_id = ?
                       ORDER BY al.created_at DESC
                       LIMIT ?''',
                    (org_id, limit)
                )
            else:
                cursor = await self.connection.execute(
                    '''SELECT al.*, u.full_name, u.role 
                       FROM action_logs al
                       JOIN users u ON al.user_id = u.telegram_id
                       ORDER BY al.created_at DESC
                       LIMIT ?''',
                    (limit,)
                )
            
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения последних действий: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ СТАТИСТИКИ ==========

    async def get_driver_stats(self, driver_id, days=30):
        """Получает статистику водителя"""
        try:
            stats = {}
            
            # Количество смен за период
            cursor = await self.connection.execute(
                '''SELECT COUNT(*) as count FROM shifts 
                   WHERE driver_id = ? 
                   AND start_time >= datetime('now', ?)''',
                (driver_id, f'-{days} days')
            )
            shifts_count = await cursor.fetchone()
            stats['shifts_count'] = shifts_count['count'] if shifts_count else 0
            
            # Средняя продолжительность смены
            cursor = await self.connection.execute(
                '''SELECT AVG(
                    (julianday(end_time) - julianday(start_time)) * 24
                   ) as avg_hours FROM shifts 
                   WHERE driver_id = ? 
                   AND end_time IS NOT NULL
                   AND start_time >= datetime('now', ?)''',
                (driver_id, f'-{days} days')
            )
            avg_hours = await cursor.fetchone()
            stats['avg_shift_hours'] = round(avg_hours['avg_hours'], 1) if avg_hours and avg_hours['avg_hours'] else 0
            
            # Количество использованной техники
            cursor = await self.connection.execute(
                '''SELECT COUNT(DISTINCT equipment_id) as count FROM shifts 
                   WHERE driver_id = ? 
                   AND start_time >= datetime('now', ?)''',
                (driver_id, f'-{days} days')
            )
            equipment_count = await cursor.fetchone()
            stats['equipment_used'] = equipment_count['count'] if equipment_count else 0
            
            return stats
        except Exception as e:
            logger.error(f"Ошибка получения статистики водителя: {e}")
            return {}

    async def get_statistics(self):
        """Получает общую статистику"""
        try:
            stats = {}
            
            # Количество организаций
            cursor = await self.connection.execute('SELECT COUNT(*) FROM organizations')
            stats['organizations'] = (await cursor.fetchone())[0]
            
            # Количество пользователей
            cursor = await self.connection.execute('SELECT COUNT(*) FROM users')
            stats['users'] = (await cursor.fetchone())[0]
            
            # Количество техники
            cursor = await self.connection.execute('SELECT COUNT(*) FROM equipment')
            stats['equipment'] = (await cursor.fetchone())[0]
            
            # Количество активных смен
            cursor = await self.connection.execute("SELECT COUNT(*) FROM shifts WHERE status = 'active'")
            stats['active_shifts'] = (await cursor.fetchone())[0]
            
            # Количество предстоящих ТО
            cursor = await self.connection.execute("SELECT COUNT(*) FROM maintenance WHERE status = 'scheduled'")
            stats['upcoming_maintenance'] = (await cursor.fetchone())[0]
            
            # Распределение по ролям
            cursor = await self.connection.execute('SELECT role, COUNT(*) FROM users GROUP BY role')
            roles = await cursor.fetchall()
            stats['roles'] = {role: count for role, count in roles}
            
            return stats
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}

    # ========== МЕТОДЫ ДЛЯ ОТЧЕТОВ ==========

    async def save_report(self, organization_id, report_type, period, data):
        """Сохраняет отчет"""
        try:
            data_json = json.dumps(data, ensure_ascii=False)
            await self.connection.execute(
                'INSERT INTO reports (organization_id, report_type, period, data) VALUES (?, ?, ?, ?)',
                (organization_id, report_type, period, data_json)
            )
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения отчета: {e}")
            return False

    async def get_reports(self, organization_id, limit=10):
        """Получает отчеты организации"""
        try:
            cursor = await self.connection.execute(
                '''SELECT * FROM reports 
                   WHERE organization_id = ? 
                   ORDER BY created_at DESC 
                   LIMIT ?''',
                (organization_id, limit)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            
            reports = []
            for row in rows:
                report = dict(row)
                try:
                    report['data'] = json.loads(report['data'])
                except:
                    report['data'] = {}
                reports.append(report)
            
            return reports
        except Exception as e:
            logger.error(f"Ошибка получения отчетов: {e}")
            return []

    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========

    async def reset_database(self):
        """Сбрасывает базу данных (только для тестов!)"""
        try:
            tables = ['organizations', 'users', 'equipment', 'shifts', 'maintenance', 
                     'daily_checks', 'notifications', 'action_logs', 'reports']
            for table in tables:
                await self.connection.execute(f'DROP TABLE IF EXISTS {table}')
            await self.connection.commit()
            await self.create_tables()
            logger.warning("⚠️ База данных сброшена!")
            return True
        except Exception as e:
            logger.error(f"Ошибка сброса базы данных: {e}")
            return False

    async def backup_database(self):
        """Создает резервную копию базы данных"""
        try:
            backup_path = f"{self.db_path}.backup"
            
            # Простое копирование файла SQLite
            import shutil
            if os.path.exists(self.db_path):
                shutil.copy2(self.db_path, backup_path)
                logger.info(f"✅ Резервная копия создана: {backup_path}")
                return backup_path
            return None
        except Exception as e:
            logger.error(f"Ошибка создания резервной копии: {e}")
            return None

    async def close(self):
        """Закрывает соединение с базой данных"""
        try:
            await self.connection.close()
            logger.info("✅ Соединение с базой данных закрыто")
        except Exception as e:
            logger.error(f"Ошибка закрытия соединения: {e}")

# Создаем глобальный экземпляр базы данных
db = Database()
