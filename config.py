"""Application configuration."""
import os


class Config:
    """Base configuration."""

    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'

    # Database connection to Pervasive via ODBC
    SQLALCHEMY_DATABASE_URI = (
        "mssql+pyodbc:///?odbc_connect="
        "DSN=STAAMP_DB;"
        "ArrayFetchOn=1;"
        "ArrayBufferSize=8;"
        "TransportHint=TCP;"
        "DecimalSymbol=,;"
    )

    # Disable schema name detection for Pervasive SQL
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,
        'pool_recycle': 3600,
        'echo': False,
        'connect_args': {
            'autocommit': True
        }
    }

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
