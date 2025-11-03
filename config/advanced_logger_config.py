"""
Advanced Logger Configuration với tính năng nén file log cũ

Sử dụng file này nếu muốn tự động nén các file log backup để tiết kiệm dung lượng.
"""

import logging
import os
import gzip
import shutil
from logging.handlers import RotatingFileHandler


class CompressedRotatingFileHandler(RotatingFileHandler):
    """
    RotatingFileHandler với tính năng tự động nén file log cũ bằng gzip

    Khi file log đạt maxBytes:
    1. main.log → main.log.1.gz (nén)
    2. main.log.1.gz → main.log.2.gz
    3. ...
    4. main.log.4.gz → main.log.5.gz
    5. main.log.5.gz bị xóa
    6. Tạo main.log mới và tiếp tục ghi

    Lợi ích: Tiết kiệm ~70-90% dung lượng cho file log backup
    """

    def doRollover(self):
        """Override doRollover để nén file log cũ"""
        if self.stream:
            self.stream.close()
            # type: ignore để tránh lỗi type checker
            self.stream = None  # type: ignore

        if self.backupCount > 0:
            # Rotate các file .gz backup
            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d.gz" % (self.baseFilename, i)
                dfn = "%s.%d.gz" % (self.baseFilename, i + 1)
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)

            # Nén file log hiện tại thành .1.gz
            dfn = "%s.1.gz" % self.baseFilename
            if os.path.exists(dfn):
                os.remove(dfn)

            if os.path.exists(self.baseFilename):
                # Nén file
                with open(self.baseFilename, "rb") as f_in:
                    with gzip.open(dfn, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(self.baseFilename)

        if not self.delay:
            self.stream = self._open()


class AdvancedLoggerConfig:
    """
    Advanced Logger Config với tùy chọn nén file backup

    Example:
        # Sử dụng compression
        logger = AdvancedLoggerConfig.logger_config(
            log_name="MyApp",
            use_compression=True
        )

        # Tùy chỉnh kích thước và số lượng backup
        logger = AdvancedLoggerConfig.logger_config(
            log_name="MyApp",
            max_bytes=100 * 1024 * 1024,  # 100MB
            backup_count=10,               # Giữ 10 backups
            use_compression=True
        )
    """

    @staticmethod
    def logger_config(
        log_name: str,
        log_file: str = "main.log",
        log_level: int = logging.INFO,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,  # 5 backups
        use_compression: bool = False,  # Không nén mặc định
    ):
        """
        Tạo logger với rotating file handler (có thể nén backup)

        Args:
            log_name: Tên của logger
            log_file: Tên file log (mặc định: main.log)
            log_level: Level của log (mặc định: INFO)
            max_bytes: Kích thước tối đa trước khi rotate (mặc định: 50MB)
            backup_count: Số lượng file backup (mặc định: 5)
            use_compression: Nén file backup thành .gz (mặc định: False)

        Returns:
            Logger đã được cấu hình

        Ví dụ file structure:
            Không nén:
            - main.log (đang ghi)
            - main.log.1 (~50MB)
            - main.log.2 (~50MB)
            - main.log.3 (~50MB)
            - main.log.4 (~50MB)
            - main.log.5 (~50MB)
            Tổng: ~300MB

            Có nén:
            - main.log (đang ghi)
            - main.log.1.gz (~5-10MB)
            - main.log.2.gz (~5-10MB)
            - main.log.3.gz (~5-10MB)
            - main.log.4.gz (~5-10MB)
            - main.log.5.gz (~5-10MB)
            Tổng: ~75-100MB (tiết kiệm ~70%)
        """
        root_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        base_path = os.path.join(root_dir, log_file)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(processName)s - %(levelname)s - %(name)s - %(message)s"
        )

        # Chọn handler type
        if use_compression:
            file_handler = CompressedRotatingFileHandler(
                filename=base_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
        else:
            file_handler = RotatingFileHandler(
                filename=base_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )

        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger = logging.getLogger(log_name)

        if not logger.handlers:
            list_handler = [file_handler, console_handler]
            for h in list_handler:
                logger.addHandler(h)

        logger.propagate = False
        logger.setLevel(log_level)
        return logger
