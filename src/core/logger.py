"""
Centralized logging configuration for the ClickUp Sync application.

Usage:
    from src.core.logger import logger
    
    logger.info("This is an info message")
    logger.warning("This is a warning")
    logger.error("This is an error")
    logger.debug("This is a debug message")
"""
import logging
import sys


def setup_logger(name: str = "clickup_sync", level: int = logging.INFO) -> logging.Logger:
    """
    Set up and return a configured logger instance.
    
    Args:
        name: Logger name (default: "clickup_sync")
        level: Logging level (default: logging.INFO)
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    return logger


# Create default logger instance
logger = setup_logger()
