"""Auth decorator helpers for module-level access control."""
from functools import wraps
from flask import abort, request, jsonify
from flask_login import current_user, login_required


def module_required(module_key: str):
    """Decorator: require login AND access to the given module.

    Returns 403 if the user is authenticated but lacks the module permission.
    Falls through to Flask-Login's @login_required behaviour for unauthenticated users.
    """
    def decorator(f):
        @wraps(f)
        @login_required
        def decorated(*args, **kwargs):
            if not current_user.has_module_access(module_key):
                abort(403)
            return f(*args, **kwargs)
        return decorated
    return decorator


def matlot_write_required(f):
    """Decorator: require login + can_magazyn + NOT matlot_readonly.

    Returns JSON 403 for AJAX callers (mutation endpoints).
    """
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        if not current_user.has_module_access('magazyn'):
            return jsonify({'success': False, 'error': 'Brak dostępu do modułu Magazyn'}), 403
        if current_user.matlot_readonly:
            return jsonify({'success': False, 'error': 'Konto tylko do odczytu — operacje zapisu są zablokowane'}), 403
        return f(*args, **kwargs)
    return decorated
