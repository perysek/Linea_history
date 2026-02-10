# Gunicorn configuration for Windows
import multiprocessing
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.production')

# Server socket
bind = f"0.0.0.0:{os.environ.get('SERVER_PORT', '8084')}"
workers = int(os.environ.get('GUNICORN_WORKERS', '4'))
worker_class = "sync"
timeout = 120
keepalive = 5

# Logging
accesslog = "logs/access.log"
errorlog = "logs/error.log"
loglevel = os.environ.get('GUNICORN_LOG_LEVEL', 'info')

# Process naming
proc_name = "linea-app"

# Worker settings
max_requests = 1000
max_requests_jitter = 50

# Daemon mode (set to False for Windows service)
daemon = False
