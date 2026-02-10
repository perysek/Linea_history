"""Flask application factory."""
from flask import Flask


def create_app(config_name='default'):
    """Create and configure Flask application."""
    app = Flask(__name__)

    # Load configuration
    if config_name == 'development':
        from config import DevelopmentConfig
        app.config.from_object(DevelopmentConfig)
    elif config_name == 'production':
        from config import ProductionConfig
        app.config.from_object(ProductionConfig)
    else:
        from config import Config
        app.config.from_object(Config)

    # Register blueprints
    from app.routes.linea import linea_bp
    app.register_blueprint(linea_bp)

    # Add root redirect
    @app.route('/')
    def index():
        from flask import redirect, url_for
        return redirect(url_for('linea.index'))

    return app
