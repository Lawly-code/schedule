# Конфигурация приложения
import os
from datetime import timedelta
from typing import Dict, Any

# Интервалы задач (в секундах)
S3_LINK_UPDATE_INTERVAL: int = 3*24*60*60

# Настройки логирования
LOG_LEVEL: str = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT: str = '%Y-%m-%d %H:%M:%S'
