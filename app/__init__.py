"""Flask application factory."""
from datetime import datetime
from flask import Flask, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager, current_user

# Initialize extensions
db = SQLAlchemy()
migrate = Migrate()
login_manager = LoginManager()


def _first_accessible_url() -> str:
    """Return the URL of the first module the current user can access.

    Priority matches the sidebar order. Falls back to the login page if the
    user somehow has no module permissions at all.
    """
    from flask_login import current_user
    from flask import url_for

    # (module_key, endpoint)  — first entry per sidebar section
    _PRIORITY = [
        ('glowne',      'placeholder.wykaz_zablokowanych'),
        ('analiza',     'placeholder.analiza_danych'),
        ('magazyn',     'matlot.matlot_status'),
        ('zarzadzanie', 'placeholder.utrzymanie_form'),
        ('admin',       'auth.admin_users'),
    ]
    for module_key, endpoint in _PRIORITY:
        if current_user.has_module_access(module_key):
            return url_for(endpoint)
    return url_for('auth.logout')  # no permissions at all — force re-login


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

    # Configure Flask-Login
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Zaloguj się, aby uzyskać dostęp.'
    login_manager.login_message_category = 'info'

    @login_manager.user_loader
    def load_user(user_id):
        from app.models.auth import User
        return db.session.get(User, int(user_id))

    @login_manager.unauthorized_handler
    def handle_unauthorized():
        from flask import request, jsonify
        # Return JSON 401 for AJAX requests, redirect for page requests
        if (request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                or 'application/json' in request.headers.get('Accept', '')):
            return jsonify({'error': 'unauthorized', 'redirect': url_for('auth.login')}), 401
        return redirect(url_for('auth.login', next=request.url))

    # Import models after db initialization
    with app.app_context():
        from app.models import sorting_area  # noqa: F401
        from app.models import matlot  # noqa: F401
        from app.models import auth  # noqa: F401

    # Inject permission flags into all templates
    @app.context_processor
    def inject_permissions():
        if current_user.is_authenticated:
            return {
                'can_glowne': current_user.has_module_access('glowne'),
                'can_analiza': current_user.has_module_access('analiza'),
                'can_magazyn': current_user.has_module_access('magazyn'),
                'can_zarzadzanie': current_user.has_module_access('zarzadzanie'),
                'can_admin': current_user.has_module_access('admin'),
            }
        return {
            'can_glowne': False,
            'can_analiza': False,
            'can_magazyn': False,
            'can_zarzadzanie': False,
            'can_admin': False,
        }

    # Register blueprints
    from app.routes.linea import linea_bp
    from app.routes.placeholder import placeholder_bp
    from app.routes.matlot import matlot_bp
    from app.routes.auth import auth_bp
    app.register_blueprint(linea_bp)
    app.register_blueprint(placeholder_bp)
    app.register_blueprint(matlot_bp)
    app.register_blueprint(auth_bp)

    # Register error handlers
    @app.errorhandler(403)
    def forbidden(e):
        from flask import render_template
        return render_template('errors/403.html'), 403

    # Add root redirect (requires login)
    from flask_login import login_required

    @app.route('/')
    @login_required
    def index():
        return redirect(_first_accessible_url())

    # Seed admin user on first run
    with app.app_context():
        _seed_admin()

    return app


def _seed_admin():
    """Create default admin role and user if no users exist yet.

    No-op if tables don't exist yet (before first migration).
    """
    from app.models.auth import User, Role
    from sqlalchemy import inspect as sa_inspect
    from sqlalchemy.exc import OperationalError
    inspector = sa_inspect(db.engine)
    if not inspector.has_table('users') or not inspector.has_table('roles'):
        return  # Tables not created yet — skip seeding until after migration
    try:
        if User.query.first() is not None:
            return  # Already seeded
    except OperationalError:
        return

    admin_role = Role(
        name='Administrator',
        description='Pełny dostęp do wszystkich modułów',
        can_glowne=True,
        can_analiza=True,
        can_magazyn=True,
        can_zarzadzanie=True,
        can_admin=True,
        matlot_readonly=False,
        is_superadmin=True,
    )
    db.session.add(admin_role)
    db.session.flush()  # get admin_role.id

    admin_user = User(
        username='admin',
        display_name='Administrator',
        is_active=True,
        role_id=admin_role.id,
        created_at=datetime.utcnow(),
    )
    admin_user.set_password('admin123')
    db.session.add(admin_user)
    db.session.commit()
    print('[seed] Created default admin user (admin / admin123)')
