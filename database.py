"""
Database module for Telegram Bot Factory
Uses SQLite for data storage with full CRUD operations
"""

import sqlite3
import json
import os
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)

DATABASE_PATH = "data/bot_factory.db"

def get_connection():
    """Get database connection"""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize database tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS members (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            username TEXT,
            joined_at TEXT,
            bots_created INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT UNIQUE NOT NULL,
            bot_username TEXT,
            bot_type TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            created_at TEXT,
            active INTEGER DEFAULT 1,
            required_channel TEXT,
            users_count INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_token TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            first_name TEXT,
            username TEXT,
            joined_at TEXT,
            banned INTEGER DEFAULT 0,
            UNIQUE(bot_token, user_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS developers (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            added_at TEXT,
            added_by INTEGER
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS banned_makers (
            user_id INTEGER PRIMARY KEY,
            banned_at TEXT,
            banned_by INTEGER
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fake_subs (
            bot_token TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            message TEXT,
            updated_at TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS remember (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_token TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS adhkar_schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_token TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            interval_minutes INTEGER DEFAULT 5,
            end_time TEXT,
            created_at TEXT,
            UNIQUE(bot_token, chat_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS guard_data (
            bot_token TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            admin_id INTEGER NOT NULL,
            kick_count INTEGER DEFAULT 0,
            PRIMARY KEY (bot_token, chat_id, admin_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS guard_settings (
            bot_token TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            kick_limit INTEGER DEFAULT 5,
            PRIMARY KEY (bot_token, chat_id)
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")


# ============== MEMBERS ==============

def add_member(user_id: int, first_name: str, username: str = None):
    """Add a new member"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO members (user_id, first_name, username, joined_at, bots_created)
            VALUES (?, ?, ?, ?, COALESCE((SELECT bots_created FROM members WHERE user_id = ?), 0))
        ''', (user_id, first_name, username, datetime.now().isoformat(), user_id))
        conn.commit()
    except Exception as e:
        logger.error(f"Error adding member: {e}")
    finally:
        conn.close()

def get_member(user_id: int) -> Optional[Dict]:
    """Get member by user_id"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM members WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_all_members() -> List[Dict]:
    """Get all members"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM members')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def increment_bots_created(user_id: int):
    """Increment bots_created counter for a member"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE members SET bots_created = bots_created + 1 WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()


# ============== BOTS ==============

def add_bot(token: str, bot_username: str, bot_type: str, owner_id: int, required_channel: str = None):
    """Add a new bot"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO bots (token, bot_username, bot_type, owner_id, created_at, required_channel)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (token, bot_username, bot_type, owner_id, datetime.now().isoformat(), required_channel))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        logger.warning(f"Bot already exists: {bot_username}")
        return False
    except Exception as e:
        logger.error(f"Error adding bot: {e}")
        return False
    finally:
        conn.close()

def get_bot_by_token(token: str) -> Optional[Dict]:
    """Get bot by token"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM bots WHERE token = ?', (token,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_bot_by_username(username: str) -> Optional[Dict]:
    """Get bot by username"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM bots WHERE bot_username = ?', (username,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_all_bots() -> List[Dict]:
    """Get all bots"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM bots')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_bots_by_type(bot_type: str) -> List[Dict]:
    """Get all bots of a specific type"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM bots WHERE bot_type = ?', (bot_type,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def toggle_bot_active(token: str) -> bool:
    """Toggle bot active status"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE bots SET active = NOT active WHERE token = ?', (token,))
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return affected > 0

def update_bot_channel(token: str, channel: str):
    """Update required channel for a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE bots SET required_channel = ? WHERE token = ?', (channel, token))
    conn.commit()
    conn.close()

def update_bot_users_count(token: str, count: int):
    """Update users count for a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE bots SET users_count = ? WHERE token = ?', (count, token))
    conn.commit()
    conn.close()

def delete_bot(token: str):
    """Delete a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM bots WHERE token = ?', (token,))
    cursor.execute('DELETE FROM bot_users WHERE bot_token = ?', (token,))
    cursor.execute('DELETE FROM fake_subs WHERE bot_token = ?', (token,))
    cursor.execute('DELETE FROM remember WHERE bot_token = ?', (token,))
    conn.commit()
    conn.close()


# ============== BOT USERS ==============

def add_bot_user(bot_token: str, user_id: int, first_name: str, username: str = None):
    """Add a user to a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO bot_users (bot_token, user_id, first_name, username, joined_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (bot_token, user_id, first_name, username, datetime.now().isoformat()))
        conn.commit()
        cursor.execute('SELECT COUNT(*) FROM bot_users WHERE bot_token = ? AND banned = 0', (bot_token,))
        count = cursor.fetchone()[0]
        cursor.execute('UPDATE bots SET users_count = ? WHERE token = ?', (count, bot_token))
        conn.commit()
    except Exception as e:
        logger.error(f"Error adding bot user: {e}")
    finally:
        conn.close()

def get_bot_users(bot_token: str) -> List[Dict]:
    """Get all users of a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM bot_users WHERE bot_token = ?', (bot_token,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def ban_bot_user(bot_token: str, user_id: int):
    """Ban a user from a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE bot_users SET banned = 1 WHERE bot_token = ? AND user_id = ?', (bot_token, user_id))
    conn.commit()
    conn.close()

def unban_bot_user(bot_token: str, user_id: int):
    """Unban a user from a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE bot_users SET banned = 0 WHERE bot_token = ? AND user_id = ?', (bot_token, user_id))
    conn.commit()
    conn.close()

def is_bot_user_banned(bot_token: str, user_id: int) -> bool:
    """Check if user is banned from a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT banned FROM bot_users WHERE bot_token = ? AND user_id = ?', (bot_token, user_id))
    row = cursor.fetchone()
    conn.close()
    return row['banned'] == 1 if row else False


# ============== DEVELOPERS ==============

def add_developer(user_id: int, username: str = None, added_by: int = None):
    """Add a new developer"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO developers (user_id, username, added_at, added_by)
            VALUES (?, ?, ?, ?)
        ''', (user_id, username, datetime.now().isoformat(), added_by))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding developer: {e}")
        return False
    finally:
        conn.close()

def remove_developer(user_id: int):
    """Remove a developer"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM developers WHERE user_id = ?', (user_id,))
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return affected > 0

def get_all_developers() -> List[Dict]:
    """Get all developers"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM developers')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def is_developer(user_id: int) -> bool:
    """Check if user is a developer"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM developers WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    conn.close()
    return row is not None


# ============== BANNED MAKERS ==============

def ban_maker(user_id: int, banned_by: int = None):
    """Ban a user from making bots"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO banned_makers (user_id, banned_at, banned_by)
            VALUES (?, ?, ?)
        ''', (user_id, datetime.now().isoformat(), banned_by))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error banning maker: {e}")
        return False
    finally:
        conn.close()

def unban_maker(user_id: int):
    """Unban a user from making bots"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM banned_makers WHERE user_id = ?', (user_id,))
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return affected > 0

def is_maker_banned(user_id: int) -> bool:
    """Check if user is banned from making bots"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM banned_makers WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    conn.close()
    return row is not None

def get_all_banned_makers() -> List[int]:
    """Get all banned maker user IDs"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM banned_makers')
    rows = cursor.fetchall()
    conn.close()
    return [row['user_id'] for row in rows]


# ============== FAKE SUBSCRIPTIONS ==============

def set_fake_sub(bot_token: str, enabled: bool, message: str = None):
    """Set fake subscription settings for a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO fake_subs (bot_token, enabled, message, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (bot_token, 1 if enabled else 0, message, datetime.now().isoformat()))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error setting fake sub: {e}")
        return False
    finally:
        conn.close()

def get_fake_sub(bot_token: str) -> Optional[Dict]:
    """Get fake subscription settings for a bot"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM fake_subs WHERE bot_token = ?', (bot_token,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_all_fake_subs() -> List[Dict]:
    """Get all fake subscription settings"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM fake_subs WHERE enabled = 1')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


# ============== REMEMBER (AI MEMORY) ==============

def add_memory(bot_token: str, user_id: int, role: str, content: str):
    """Add a message to AI memory"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO remember (bot_token, user_id, role, content, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (bot_token, user_id, role, content, datetime.now().isoformat()))
        conn.commit()
        cursor.execute('''
            DELETE FROM remember WHERE bot_token = ? AND user_id = ? AND id NOT IN (
                SELECT id FROM remember WHERE bot_token = ? AND user_id = ?
                ORDER BY created_at DESC LIMIT 20
            )
        ''', (bot_token, user_id, bot_token, user_id))
        conn.commit()
    except Exception as e:
        logger.error(f"Error adding memory: {e}")
    finally:
        conn.close()

def get_memory(bot_token: str, user_id: int, limit: int = 20) -> List[Dict]:
    """Get AI memory for a user"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT role, content FROM remember 
        WHERE bot_token = ? AND user_id = ?
        ORDER BY created_at DESC LIMIT ?
    ''', (bot_token, user_id, limit))
    rows = cursor.fetchall()
    conn.close()
    return [{"role": row['role'], "content": row['content']} for row in reversed(rows)]

def clear_memory(bot_token: str, user_id: int = None):
    """Clear AI memory"""
    conn = get_connection()
    cursor = conn.cursor()
    if user_id:
        cursor.execute('DELETE FROM remember WHERE bot_token = ? AND user_id = ?', (bot_token, user_id))
    else:
        cursor.execute('DELETE FROM remember WHERE bot_token = ?', (bot_token,))
    conn.commit()
    conn.close()


# ============== ADHKAR SCHEDULES ==============

def add_adhkar_schedule(bot_token: str, chat_id: int, interval_minutes: int, end_time: str = None):
    """Add or update adhkar schedule"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO adhkar_schedules (bot_token, chat_id, interval_minutes, end_time, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (bot_token, chat_id, interval_minutes, end_time, datetime.now().isoformat()))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding adhkar schedule: {e}")
        return False
    finally:
        conn.close()

def get_adhkar_schedules(bot_token: str = None) -> List[Dict]:
    """Get adhkar schedules"""
    conn = get_connection()
    cursor = conn.cursor()
    if bot_token:
        cursor.execute('SELECT * FROM adhkar_schedules WHERE bot_token = ?', (bot_token,))
    else:
        cursor.execute('SELECT * FROM adhkar_schedules')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def remove_adhkar_schedule(bot_token: str, chat_id: int):
    """Remove adhkar schedule"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM adhkar_schedules WHERE bot_token = ? AND chat_id = ?', (bot_token, chat_id))
    conn.commit()
    conn.close()


# ============== GUARD DATA ==============

def get_guard_kick_count(bot_token: str, chat_id: int, admin_id: int) -> int:
    """Get kick count for an admin"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT kick_count FROM guard_data WHERE bot_token = ? AND chat_id = ? AND admin_id = ?', 
                   (bot_token, chat_id, admin_id))
    row = cursor.fetchone()
    conn.close()
    return row['kick_count'] if row else 0

def increment_guard_kick(bot_token: str, chat_id: int, admin_id: int) -> int:
    """Increment kick count for an admin and return new count"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO guard_data (bot_token, chat_id, admin_id, kick_count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(bot_token, chat_id, admin_id) 
        DO UPDATE SET kick_count = kick_count + 1
    ''', (bot_token, chat_id, admin_id))
    conn.commit()
    cursor.execute('SELECT kick_count FROM guard_data WHERE bot_token = ? AND chat_id = ? AND admin_id = ?',
                   (bot_token, chat_id, admin_id))
    row = cursor.fetchone()
    conn.close()
    return row['kick_count'] if row else 1

def reset_guard_kicks(bot_token: str, chat_id: int, admin_id: int):
    """Reset kick count for an admin"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM guard_data WHERE bot_token = ? AND chat_id = ? AND admin_id = ?',
                   (bot_token, chat_id, admin_id))
    conn.commit()
    conn.close()

def get_guard_settings(bot_token: str, chat_id: int) -> Dict:
    """Get guard settings for a chat"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM guard_settings WHERE bot_token = ? AND chat_id = ?', (bot_token, chat_id))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else {"kick_limit": 5}

def set_guard_kick_limit(bot_token: str, chat_id: int, limit: int):
    """Set kick limit for a chat"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO guard_settings (bot_token, chat_id, kick_limit)
        VALUES (?, ?, ?)
    ''', (bot_token, chat_id, limit))
    conn.commit()
    conn.close()


# ============== STATISTICS ==============

def get_statistics() -> Dict:
    """Get overall statistics"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM members')
    total_members = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bots')
    total_bots = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bots WHERE active = 1')
    active_bots = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bot_users')
    total_bot_users = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM remember')
    total_messages = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT bot_username, users_count FROM bots 
        ORDER BY users_count DESC LIMIT 1
    ''')
    most_active = cursor.fetchone()
    
    conn.close()
    
    return {
        "total_members": total_members,
        "total_bots": total_bots,
        "active_bots": active_bots,
        "total_bot_users": total_bot_users,
        "total_messages": total_messages,
        "most_active_bot": most_active['bot_username'] if most_active else None,
        "most_active_users": most_active['users_count'] if most_active else 0
    }


# ============== CLEAR DATABASE ==============

def clear_all_data():
    """Clear all data from the database (fresh start)"""
    conn = get_connection()
    cursor = conn.cursor()
    tables = ['members', 'bots', 'bot_users', 'developers', 'banned_makers', 
              'fake_subs', 'remember', 'adhkar_schedules', 'guard_data', 'guard_settings']
    for table in tables:
        cursor.execute(f'DELETE FROM {table}')
    conn.commit()
    conn.close()
    logger.info("All data cleared from database")


# Initialize on import
init_database()
