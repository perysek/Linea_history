"""Application configuration."""
import os


class Config:
    """Base configuration."""

    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'

    # Session settings (8-hour workday lifetime)
    PERMANENT_SESSION_LIFETIME = 28800
    SESSION_COOKIE_HTTPONLY = True

    # Local SQLite database for sorting area data
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(os.path.abspath(os.path.dirname(__file__)), 'linea.db')

    # Excel file for automatic sync (override via EXCEL_FILE_PATH env var for local dev)
    EXCEL_FILE_PATH = os.environ.get('EXCEL_FILE_PATH') or (
        r'G:\DOCUMENT\qualita\System Zarządzania Jakością'
        r'\Cele jakościowe\PPM wewnętrzny koszty złej jakości (2023).xlsm'
    )

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,      # Verify connections before using
        'pool_recycle': 3600,       # Recycle connections after 1 hour
        'echo': False               # Set to True for SQL debugging
    }


class DevelopmentConfig(Config):
    """Development configuration."""

    DEBUG = True
    SQLALCHEMY_ENGINE_OPTIONS = {
        **Config.SQLALCHEMY_ENGINE_OPTIONS,
        'echo': True  # Show SQL queries in console for debugging
    }


class ProductionConfig(Config):
    """Production configuration."""

    DEBUG = False

    # Override SECRET_KEY - will use environment variable or fail at runtime
    SECRET_KEY = os.environ.get('SECRET_KEY') or Config.SECRET_KEY


# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': Config
}
