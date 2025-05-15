import os
import aioboto3
import uuid
import tempfile
import aiohttp
from typing import Any
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from config import S3_LINK_UPDATE_INTERVAL
from tasks.base_task import BaseTask

from lawly_db.db_models.template import Template
from lawly_db.db_models.db_session import create_session


class S3LinkUpdaterTask(BaseTask):
    """Задача для обновления ссылок S3 в шаблонах."""
    
    def __init__(self) -> None:
        super().__init__("s3_link_updater")
        self.bucket_name: str = os.environ.get('S3_BUCKET_NAME', '')
        self.aws_access_key_id: str = os.environ.get('S3_ACCESS_KEY_ID', '')
        self.aws_secret_access_key: str = os.environ.get('S3_SECRET_ACCESS_KEY', '')
        self.endpoint_url: str = os.environ.get('S3_ENDPOINT', '')
        
    @property
    def interval(self) -> float:
        """Интервал выполнения задачи - каждые 3 дня."""
        return S3_LINK_UPDATE_INTERVAL
    
    async def execute(self) -> None:
        """Асинхронное выполнение задачи обновления ссылок S3."""
        self.logger.info("Starting S3 link update task")
        try:
            # Используем create_session из db_session
            async with create_session() as session:
                # Получаем все шаблоны с URL, используя правильный SQL Alchemy подход
                query = select(Template).where(
                    or_(
                        Template.download_url.isnot(None),
                        Template.image_url.isnot(None)
                    )
                )
                result = await session.execute(query)
                templates: list[Template] = list(result.scalars().all())
                
                self.logger.info(f"Found {len(templates)} templates with URLs to update")
                
                # Создаем асинхронную сессию S3
                async with aioboto3.Session(
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                ).client('s3', endpoint_url=self.endpoint_url, verify=False) as s3:
                    # Создаем HTTP сессию
                    async with aiohttp.ClientSession() as http_session:
                        updated_count: int = 0
                        for template in templates:
                            try:
                                if await self._update_template_urls(template, s3, http_session):
                                    updated_count += 1
                            except Exception as e:
                                self.logger.error(f"Error updating template {template.id}: {str(e)}")
                        
                        await session.commit()
                        self.logger.info(f"S3 link update task completed, updated {updated_count} templates")
                
        except Exception as e:
            self.logger.exception(f"Error in S3 link update task: {str(e)}")
    
    async def _update_template_urls(
        self,
        template: Template, 
        s3: Any, 
        http_session: aiohttp.ClientSession
    ) -> bool:
        """Обновление URL-ссылок для конкретного шаблона."""
        updated: bool = False
        
        # Обновление download_url, если он существует
        if template.download_url:
            try:
                # Сохраняем старый путь для последующего удаления
                old_path = self._extract_path_from_url(template.download_url)
                
                # Обновляем ссылку
                new_url: str = await self._refresh_s3_link(template.download_url, s3, http_session)
                if new_url != template.download_url:
                    self.logger.info(f"Updating download_url for template {template.id}")
                    template.download_url = new_url
                    updated = True
                    
                    # Удаляем старый файл, если был загружен новый
                    if old_path:
                        await self._delete_s3_object(s3, old_path)
            except Exception as e:
                self.logger.error(f"Failed to update download_url for template {template.id}: {str(e)}")
        
        # Обновление image_url, если он существует
        if template.image_url:
            try:
                # Сохраняем старый путь для последующего удаления
                old_path = self._extract_path_from_url(template.image_url)
                
                # Обновляем ссылку
                new_url: str = await self._refresh_s3_link(template.image_url, s3, http_session)
                if new_url != template.image_url:
                    self.logger.info(f"Updating image_url for template {template.id}")
                    template.image_url = new_url
                    updated = True
                    
                    # Удаляем старый файл, если был загружен новый
                    if old_path:
                        await self._delete_s3_object(s3, old_path)
            except Exception as e:
                self.logger.error(f"Failed to update image_url for template {template.id}: {str(e)}")
        
        return updated
    
    def _extract_path_from_url(self, url: str) -> str | None:
        """
        Извлекает путь объекта S3 из URL.
        
        Args:
            url: URL S3 объекта
            
        Returns:
            str | None: Путь к объекту или None при ошибке
        """
        try:
            if 's3.amazonaws.com' in url:
                path: str = url.split('s3.amazonaws.com/')[1]
            else:
                parts: list[str] = url.split('/')
                if len(parts) >= 4:
                    path = '/'.join(parts[3:]).split('?')[0]
                else:
                    self.logger.warning(f"Could not parse S3 path from URL: {url}")
                    return None
            return path
        except Exception as e:
            self.logger.warning(f"Error extracting path from URL {url}: {str(e)}")
            return None
            
    async def _delete_s3_object(self, s3: Any, path: str) -> bool:
        """
        Удаляет объект из S3.
        
        Args:
            s3: Клиент S3
            path: Путь к объекту
            
        Returns:
            bool: True если объект успешно удален, иначе False
        """
        try:
            self.logger.info(f"Deleting old S3 object: {path}")
            await s3.delete_object(Bucket=self.bucket_name, Key=path.split("/")[1])
            self.logger.info(f"Successfully deleted old S3 object: {path}")
            return True
        except Exception as e:
            self.logger.warning(f"Failed to delete old S3 object {path}: {str(e)}")
            return False
    
    async def _refresh_s3_link(
        self, 
        url: str, 
        s3: Any, 
        http_session: aiohttp.ClientSession
    ) -> str:
        """
        Обновляет ссылку S3 асинхронно.
        Если возможно, просто генерирует новую ссылку, иначе повторно загружает файл.
        
        Args:
            url: URL ссылки для обновления
            s3: Клиент S3
            http_session: HTTP-сессия
            
        Returns:
            str: Обновленная URL-ссылка
            
        Raises:
            Exception: При ошибке обновления ссылки
        """
        try:
            # Извлекаем путь объекта из URL
            path = self._extract_path_from_url(url)
            if not path:
                raise ValueError(f"Could not parse S3 path from URL: {url}")
            
            try:
                await s3.head_object(Bucket=self.bucket_name, Key=path)
                
                # Генерируем новую presigned URL с новым сроком действия (7 дней)
                new_url: str = await s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': self.bucket_name, 'Key': path},
                    ExpiresIn=60*60*24*7  # 7 дней в секундах
                )
                
                return new_url
                
            except Exception as e:
                self.logger.warning(f"Object not found or error checking object. Error: {str(e)}. Reuploading...")
                # Если объект не существует или произошла другая ошибка,
                # скачиваем файл и загружаем заново
                return await self._reupload_file(url, s3, http_session)
                
        except Exception as e:
            self.logger.error(f"Error refreshing S3 link: {str(e)}")
            raise
    
    async def _reupload_file(
        self, 
        url: str, 
        s3: Any, 
        http_session: aiohttp.ClientSession
    ) -> str | None:
        """
        Асинхронно загружает файл по URL и перезагружает его в S3.
        
        Args:
            url: URL файла для загрузки
            s3: Клиент S3
            http_session: HTTP-сессия
            
        Returns:
            str: Новая URL-ссылка на загруженный файл
            
        Raises:
            Exception: При ошибке загрузки файла
        """
        # Создаем временный файл
        temp_fd, temp_path = tempfile.mkstemp()
        os.close(temp_fd)
        
        try:
            # Асинхронно скачиваем файл
            async with http_session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download file from {url}, status: {response.status}")
                
                with open(temp_path, 'wb') as f:
                    # Асинхронно читаем и записываем содержимое чанками
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
            
            # Определяем имя файла и расширение
            original_filename: str = os.path.basename(url.split('?')[0])
            _, ext = os.path.splitext(original_filename)
            
            # Генерируем уникальное имя файла
            unique_filename: str = f"{uuid.uuid4()}{ext}"
            
            # Асинхронно загружаем файл в S3
            with open(temp_path, 'rb') as f:
                await s3.upload_fileobj(
                    f,
                    self.bucket_name,
                    unique_filename
                )
            
            # Генерируем presigned URL с новым сроком действия (7 дней)
            new_url: str = await s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': unique_filename},
                ExpiresIn=60*60*24*7  # 7 дней в секундах
            )
            
            return new_url
            
        finally:
            # Удаляем временный файл
            if os.path.exists(temp_path):
                os.remove(temp_path)
