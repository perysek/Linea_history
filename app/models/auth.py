"""Auth models — User and Role."""
from datetime import datetime
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from app import db


class Role(db.Model):
    """Permission role assigned to users."""
    __tablename__ = 'roles'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)
    description = db.Column(db.String(255))

    # Module access flags
    can_glowne = db.Column(db.Boolean, default=False, nullable=False)
    can_analiza = db.Column(db.Boolean, default=False, nullable=False)
    can_magazyn = db.Column(db.Boolean, default=False, nullable=False)
    can_zarzadzanie = db.Column(db.Boolean, default=False, nullable=False)
    can_admin = db.Column(db.Boolean, default=False, nullable=False)

    # MATLOT read-only restriction
    matlot_readonly = db.Column(db.Boolean, default=False, nullable=False)

    # Superadmin bypasses all checks
    is_superadmin = db.Column(db.Boolean, default=False, nullable=False)

    users = db.relationship('User', backref='role', lazy='dynamic')

    def has_module_access(self, module_key: str) -> bool:
        """Return True if this role can access the given module key."""
        if self.is_superadmin:
            return True
        return bool(getattr(self, f'can_{module_key}', False))

    def __repr__(self) -> str:
        return f'<Role {self.name}>'


class User(UserMixin, db.Model):
    """Application user."""
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    display_name = db.Column(db.String(100))
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def has_module_access(self, module_key: str) -> bool:
        return self.role.has_module_access(module_key) if self.role else False

    @property
    def matlot_readonly(self) -> bool:
        if self.role and self.role.is_superadmin:
            return False
        return bool(self.role and self.role.matlot_readonly)

    def get_id(self) -> str:
        return str(self.id)

    def __repr__(self) -> str:
        return f'<User {self.username}>'
