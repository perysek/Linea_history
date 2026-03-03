"""
Waitress WSGI server entry point for LINEA production deployment on Windows Server.

Waitress is used instead of Gunicorn because Gunicorn requires Unix fork() which
is not available on Windows. Waitress is pure Python and works natively on Windows.

Usage:
    python run_waitress.py

Environment variables (set in .env.production):
    FLASK_CONFIG      - Configuration class: 'production' (default)
    SERVER_HOST       - Bind host (default: 0.0.0.0 for all interfaces)
    SERVER_PORT       - Bind port (default: 8084)
    WAITRESS_THREADS  - Worker threads (default: 4)
"""

import os
from dotenv import load_dotenv

# Load production environment variables before importing app
env_file = '.env.production' if os.path.exists('.env.production') else '.env'
load_dotenv(env_file)

from waitress import serve
from run import app

if __name__ == '__main__':
    host = os.getenv('SERVER_HOST', '0.0.0.0')
    port = int(os.getenv('SERVER_PORT', '8084'))
    threads = int(os.getenv('WAITRESS_THREADS', '4'))

    print(f"Starting LINEA production server on {host}:{port} with {threads} threads")
    print(f"Application accessible at: http://10.52.10.101:{port}")
    print("Press Ctrl+C to stop.")

    serve(
        app,
        host=host,
        port=port,
        threads=threads,
        url_scheme='http',
        channel_timeout=120,
        cleanup_interval=30,
        asyncore_use_poll=True,
    )
