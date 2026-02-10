"""Application entry point."""
import os
from app import create_app

# Determine configuration from environment variable
config_name = os.environ.get('FLASK_CONFIG', 'development')

# Create Flask application
app = create_app(config_name)

if __name__ == '__main__':
    # Run development server
    app.run(
        host='0.0.0.0',
        port=8084,
        debug=True
    )
