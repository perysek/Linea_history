"""Flask application factory."""
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

# Initialize extensions
db = SQLAlchemy()
migrate = Migrate()


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

    # Initialize extensions with app
    db.init_app(app)
    migrate.init_app(app, db)

    # Import models after db initialization
    with app.app_context():
        from app.models import sorting_area  # noqa: F401

    # Register blueprints
    from app.routes.linea import linea_bp
    from app.routes.placeholder import placeholder_bp
    app.register_blueprint(linea_bp)
    app.register_blueprint(placeholder_bp)

    # Add root redirect
    @app.route('/')
    def index():
        from flask import redirect, url_for
        return redirect(url_for('linea.index'))

    return app
