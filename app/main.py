import logging
import asyncio
from typing import Optional

from utils.logging_utils import setup_logging
from scheduler.scheduler import TaskScheduler
from tasks.s3_link_updater import S3LinkUpdaterTask

from lawly_db.db_models.db_session import global_init


def load_tasks(scheduler: TaskScheduler) -> TaskScheduler:
    """
    Загрузка всех задач в планировщик.

    Args:
        scheduler: Экземпляр планировщика задач

    Returns:
        TaskScheduler: Планировщик с загруженными задачами
    """
    # Добавляем задачу обновления ссылок S3
    s3_task: S3LinkUpdaterTask = S3LinkUpdaterTask()
    scheduler.add_task(s3_task)

    # В будущем здесь можно добавить другие задачи

    return scheduler


async def init_database() -> None:
    """
    Инициализация базы данных.

    Асинхронная функция, которая вызывает global_init для установления
    соединения с базой данных.
    """
    logger: logging.Logger = logging.getLogger('main')
    logger.info("Initializing database connection")
    await global_init()
    logger.info("Database connection initialized")


async def shutdown(scheduler: Optional[TaskScheduler]) -> None:
    """
    Корректное завершение работы.

    Args:
        scheduler: Экземпляр планировщика задач для остановки
    """
    if scheduler:
        scheduler.shutdown()


async def main_async() -> None:
    """
    Асинхронная основная функция запуска приложения.

    Инициализирует базу данных, создает планировщик, загружает задачи
    и запускает цикл выполнения задач.
    """
    # Настраиваем логирование
    logger: logging.Logger = setup_logging()
    logger.info("Starting scheduler application")

    # Инициализируем базу данных
    await init_database()

    # Создаем и настраиваем планировщик
    scheduler: TaskScheduler = TaskScheduler()

    # Загружаем задачи
    load_tasks(scheduler)

    # Запускаем планировщик
    scheduler.start()
    logger.info("Scheduler started successfully")

    try:
        # Держим приложение запущенным
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    finally:
        await shutdown(scheduler)
        logger.info("Application shutdown")


def main() -> None:
    """
    Точка входа в приложение.

    Запускает асинхронную функцию main_async в цикле событий asyncio.
    """
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("Application stopped by user")


if __name__ == '__main__':
    main()
