"""
Advanced Logging System for Telegram Bot Factory
Provides separate log files for different components
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FORMAT = "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    """Setup a logger with file and console handlers"""
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.handlers:
        return logger
    
    file_path = os.path.join(LOGS_DIR, log_file)
    file_handler = RotatingFileHandler(
        file_path,
        maxBytes=5*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


main_logger = setup_logger("main_bot", "main_bot.log")

child_logger = setup_logger("child_bots", "child_bots.log")

error_logger = setup_logger("errors", "errors.log", logging.ERROR)


def log_main(message: str, level: str = "info"):
    """Log message for main bot"""
    getattr(main_logger, level.lower())(message)

def log_child(bot_name: str, message: str, level: str = "info"):
    """Log message for child bots"""
    getattr(child_logger, level.lower())(f"[@{bot_name}] {message}")

def log_error(source: str, error: Exception, context: str = ""):
    """Log error with full details"""
    error_msg = f"[{source}] {type(error).__name__}: {str(error)}"
    if context:
        error_msg += f" | Context: {context}"
    error_logger.error(error_msg, exc_info=True)
    main_logger.error(error_msg)

def log_user_action(user_id: int, action: str, details: str = ""):
    """Log user actions"""
    msg = f"User {user_id}: {action}"
    if details:
        msg += f" | {details}"
    main_logger.info(msg)

def log_bot_created(bot_type: str, bot_username: str, owner_id: int):
    """Log bot creation"""
    main_logger.info(f"NEW BOT CREATED: Type={bot_type}, Username=@{bot_username}, Owner={owner_id}")

def log_broadcast(source: str, success: int, failed: int, bot_name: str = None):
    """Log broadcast results"""
    if bot_name:
        msg = f"BROADCAST from @{bot_name}: Success={success}, Failed={failed}"
    else:
        msg = f"BROADCAST from {source}: Success={success}, Failed={failed}"
    main_logger.info(msg)

def log_startup():
    """Log bot startup"""
    main_logger.info("="*50)
    main_logger.info("BOT FACTORY STARTED")
    main_logger.info(f"Startup time: {datetime.now().isoformat()}")
    main_logger.info("="*50)

def log_child_startup(bot_username: str, bot_type: str):
    """Log child bot startup"""
    child_logger.info(f"[@{bot_username}] Started - Type: {bot_type}")

def log_child_error(bot_username: str, error: str):
    """Log child bot error"""
    child_logger.error(f"[@{bot_username}] ERROR: {error}")
