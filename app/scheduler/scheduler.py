import asyncio
import logging
import aiocron
import datetime
from typing import Dict, List, Any, Optional

from tasks.base_task import BaseTask

class TaskScheduler:
    """Асинхронный планировщик задач, использующий aiocron."""
    
    def __init__(self) -> None:
        """Инициализация планировщика задач."""
        self.tasks: Dict[str, BaseTask] = {}
        self.crons: Dict[str, aiocron.crontab] = {}
        self.logger: logging.Logger = logging.getLogger('scheduler')
        
    def add_task(self, task: BaseTask) -> bool:
        """
        Добавить задачу в планировщик.
        
        Args:
            task: Задача для добавления
            
        Returns:
            bool: True, если задача успешно добавлена, иначе False
        """
        if task.job_id in self.tasks:
            self.logger.warning(f"Task {task.job_id} already exists, skipping")
            return False
        
        # Используем правильный формат cron-выражения (5 полей)
        # Для запуска каждые N секунд превращаем это в минуты/часы
        interval_seconds = int(task.interval)
        
        if interval_seconds < 60:
            # Для маленьких интервалов (меньше минуты) используем специальную функцию aiocron
            # для создания задачи с кастомным интервалом
            cron_job = self._create_seconds_job(task)
        else:
            # Для интервалов от минуты и больше используем стандартный формат cron
            minutes = interval_seconds // 60
            if minutes < 60:
                # Каждые X минут
                cron_expr = f"*/{minutes} * * * *" if minutes > 1 else "* * * * *"
            else:
                # Каждые X часов
                hours = minutes // 60
                cron_expr = f"0 */{hours} * * *" if hours > 1 else "0 * * * *"
                
            self.logger.info(f"Using cron expression: {cron_expr} for task {task.job_id}")
            cron_job = aiocron.crontab(
                cron_expr,
                func=task.execute, 
                start=False
            )
        
        self.crons[task.job_id] = cron_job
        self.tasks[task.job_id] = task
        self.logger.info(f"Added task: {task}")
        return True
    
    def _create_seconds_job(self, task: BaseTask) -> aiocron.crontab:
        """
        Создает задачу с интервалом в секундах через aiocron.
        
        Для маленьких интервалов (меньше минуты) aiocron не имеет прямой 
        поддержки через cron-выражения, поэтому создаем специальную задачу.
        
        Args:
            task: Задача для создания джоба
            
        Returns:
            aiocron.crontab: Созданный джоб
        """
        # Используем хак с минимальным cron-выражением (каждую минуту) и проверкой внутри
        async def wrapped_task():
            # Для интервалов меньше минуты создаем отдельный асинхронный таск,
            # который будет запускать задачу каждые N секунд
            async def seconds_runner():
                while True:
                    try:
                        await task.execute()
                    except Exception as e:
                        self.logger.error(f"Error executing task {task.job_id}: {str(e)}")
                    await asyncio.sleep(task.interval)
            
            # Запускаем runner в отдельной задаче
            asyncio.create_task(seconds_runner())
            
            # Эта функция больше не будет вызываться регулярно по cron
            # Мы просто используем ее для первого запуска
            return None
        
        # Используем простое выражение, которое запускается только один раз
        # (так как внутри мы создаем свой цикл)
        return aiocron.crontab("* * * * *", func=wrapped_task, start=False)
        
    def remove_task(self, task_id: str) -> bool:
        """
        Удалить задачу из планировщика.
        
        Args:
            task_id: Идентификатор задачи для удаления
            
        Returns:
            bool: True, если задача успешно удалена, иначе False
        """
        if task_id not in self.tasks:
            self.logger.warning(f"Task {task_id} not found, cannot remove")
            return False
        
        # Останавливаем cron-задачу
        if task_id in self.crons:
            self.crons[task_id].stop()
            del self.crons[task_id]
        
        del self.tasks[task_id]
        self.logger.info(f"Removed task: {task_id}")
        return True
        
    def start(self) -> None:
        """Запустить планировщик."""
        if not self.tasks:
            self.logger.warning("No tasks added to scheduler")
            return
        
        # Запускаем все cron-задачи
        for job_id, cron in self.crons.items():
            self.logger.info(f"Starting task: {job_id}")
            cron.start()
            
            # Запускаем задачу сразу (асинхронно) в момент старта
            task = self.tasks[job_id]
            asyncio.create_task(self._run_task_now(task))
        
        self.logger.info(f"Scheduler started with {len(self.tasks)} tasks")
    
    async def _run_task_now(self, task: BaseTask) -> None:
        """
        Немедленно выполняет задачу.
        
        Args:
            task: Задача для выполнения
        """
        try:
            self.logger.info(f"Executing task {task.job_id} immediately on startup")
            await task.execute()
            self.logger.info(f"Task {task.job_id} executed successfully on startup")
        except Exception as e:
            self.logger.error(f"Error executing task {task.job_id} on startup: {str(e)}")
        
    def shutdown(self) -> None:
        """Остановить планировщик."""
        # Останавливаем все cron-задачи
        for job_id, cron in self.crons.items():
            cron.stop()
        
        self.crons = {}
        self.logger.info("Scheduler shutdown")
        
    def get_tasks(self) -> List[BaseTask]:
        """
        Получить список всех задач.
        
        Returns:
            List[BaseTask]: Список всех задач
        """
        return list(self.tasks.values())
