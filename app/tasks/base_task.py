from abc import ABC, abstractmethod
import logging
from typing import Any

class BaseTask(ABC):
    """Базовый класс для всех задач планировщика."""
    
    def __init__(self, name: str) -> None:
        """
        Инициализация базового класса задачи.
        
        Args:
            name: Уникальное имя задачи
        """
        self.name: str = name
        self.logger: logging.Logger = logging.getLogger(f"scheduler.tasks.{name}")
    
    @abstractmethod
    async def execute(self, *args: Any, **kwargs: Any) -> None:
        """
        Асинхронное выполнение задачи.
        
        Этот метод вызывается планировщиком при наступлении времени выполнения задачи.
        
        Args:
            *args: Позиционные аргументы
            **kwargs: Именованные аргументы
        """
        pass
    
    @property
    @abstractmethod
    def interval(self) -> float:
        """
        Интервал выполнения задачи (в секундах).
        
        Returns:
            float: Интервал в секундах
        """
        pass
    
    @property
    def job_id(self) -> str:
        """
        Уникальный идентификатор задачи.
        
        Returns:
            str: Идентификатор задачи
        """
        return f"task_{self.name}"
    
    def __str__(self) -> str:
        """
        Строковое представление задачи.
        
        Returns:
            str: Строковое представление
        """
        return f"Task(name={self.name}, interval={self.interval})"
