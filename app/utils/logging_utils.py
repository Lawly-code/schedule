import logging
import sys
from config import LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT

def setup_logging() -> logging.Logger:
    """
    Настройка логирования для приложения.
    
    Returns:
        logging.Logger: Корневой логгер приложения
    """
    root_logger: logging.Logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, LOG_LEVEL))

    # Проверяем, не настроено ли уже логирование
    if not root_logger.handlers:
        handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
        formatter: logging.Formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

        # # Отключить лишние сообщения от библиотек
        # logging.getLogger('apscheduler').setLevel(logging.WARNING)
        # logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
    
    return root_logger
