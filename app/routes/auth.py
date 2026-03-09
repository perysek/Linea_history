"""Auth routes — login, logout, and admin user/role management."""
from datetime import datetime
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, abort
from flask_login import login_user, logout_user, login_required, current_user
from app import db
from app.models.auth import User, Role
from app.utils.auth_helpers import module_required

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')


# ── Login / Logout ─────────────────────────────────────────────────────────────

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        user = User.query.filter_by(username=username).first()

        if user is None or not user.check_password(password):
            flash('Nieprawidłowa nazwa użytkownika lub hasło.', 'error')
            return render_template('auth/login.html')

        if not user.is_active:
            flash('Konto jest nieaktywne. Skontaktuj się z administratorem.', 'error')
            return render_template('auth/login.html')

        login_user(user, remember=False)
        user.last_login = datetime.utcnow()
        db.session.commit()

        next_page = request.args.get('next') or url_for('index')
        return redirect(next_page)

    return render_template('auth/login.html')


@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))


# ── Admin: Users ───────────────────────────────────────────────────────────────

@auth_bp.route('/admin/users')
@module_required('admin')
def admin_users():
    users = User.query.order_by(User.username).all()
    roles = Role.query.order_by(Role.name).all()
    return render_template('auth/admin_users.html', users=users, roles=roles)


@auth_bp.route('/admin/users', methods=['POST'])
@module_required('admin')
def admin_create_user():
    username = request.form.get('username', '').strip()
    display_name = request.form.get('display_name', '').strip()
    password = request.form.get('password', '')
    role_id = request.form.get('role_id', type=int)

    if not username or not password or not role_id:
        flash('Uzupełnij wszystkie wymagane pola.', 'error')
        return redirect(url_for('auth.admin_users'))

    if User.query.filter_by(username=username).first():
        flash(f'Użytkownik "{username}" już istnieje.', 'error')
        return redirect(url_for('auth.admin_users'))

    role = Role.query.get(role_id)
    if not role:
        flash('Wybrana rola nie istnieje.', 'error')
        return redirect(url_for('auth.admin_users'))

    user = User(
        username=username,
        display_name=display_name or username,
        role_id=role_id,
        is_active=True,
        created_at=datetime.utcnow(),
    )
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    flash(f'Użytkownik "{username}" został utworzony.', 'success')
    return redirect(url_for('auth.admin_users'))


@auth_bp.route('/admin/users/<int:user_id>/edit', methods=['POST'])
@module_required('admin')
def admin_edit_user(user_id):
    user = User.query.get_or_404(user_id)

    display_name = request.form.get('display_name', '').strip()
    role_id = request.form.get('role_id', type=int)
    is_active = request.form.get('is_active') == '1'
    new_password = request.form.get('password', '').strip()

    if role_id:
        role = Role.query.get(role_id)
        if not role:
            flash('Wybrana rola nie istnieje.', 'error')
            return redirect(url_for('auth.admin_users'))
        # Prevent removing superadmin from the last superadmin user
        if user.role.is_superadmin and not role.is_superadmin:
            other_superadmins = User.query.join(Role).filter(
                Role.is_superadmin == True,
                User.id != user_id
            ).count()
            if other_superadmins == 0:
                flash('Nie można usunąć roli superadmina od ostatniego administratora.', 'error')
                return redirect(url_for('auth.admin_users'))
        user.role_id = role_id

    if display_name:
        user.display_name = display_name

    # Prevent self-deactivation
    if user.id == current_user.id:
        is_active = True
    user.is_active = is_active

    if new_password:
        user.set_password(new_password)

    db.session.commit()
    flash(f'Użytkownik "{user.username}" został zaktualizowany.', 'success')
    return redirect(url_for('auth.admin_users'))


@auth_bp.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@module_required('admin')
def admin_delete_user(user_id):
    if user_id == current_user.id:
        flash('Nie możesz usunąć własnego konta.', 'error')
        return redirect(url_for('auth.admin_users'))

    user = User.query.get_or_404(user_id)
    username = user.username
    db.session.delete(user)
    db.session.commit()
    flash(f'Użytkownik "{username}" został usunięty.', 'success')
    return redirect(url_for('auth.admin_users'))


# ── Admin: Roles ───────────────────────────────────────────────────────────────

@auth_bp.route('/admin/roles')
@module_required('admin')
def admin_roles():
    roles = Role.query.order_by(Role.name).all()
    return render_template('auth/admin_roles.html', roles=roles)


@auth_bp.route('/admin/roles', methods=['POST'])
@module_required('admin')
def admin_create_role():
    name = request.form.get('name', '').strip()
    description = request.form.get('description', '').strip()

    if not name:
        flash('Nazwa roli jest wymagana.', 'error')
        return redirect(url_for('auth.admin_roles'))

    if Role.query.filter_by(name=name).first():
        flash(f'Rola "{name}" już istnieje.', 'error')
        return redirect(url_for('auth.admin_roles'))

    role = Role(
        name=name,
        description=description,
        can_glowne='can_glowne' in request.form,
        can_analiza='can_analiza' in request.form,
        can_magazyn='can_magazyn' in request.form,
        can_zarzadzanie='can_zarzadzanie' in request.form,
        can_admin='can_admin' in request.form,
        matlot_readonly='matlot_readonly' in request.form,
        is_superadmin='is_superadmin' in request.form,
    )
    db.session.add(role)
    db.session.commit()
    flash(f'Rola "{name}" została utworzona.', 'success')
    return redirect(url_for('auth.admin_roles'))


@auth_bp.route('/admin/roles/<int:role_id>/edit', methods=['POST'])
@module_required('admin')
def admin_edit_role(role_id):
    role = Role.query.get_or_404(role_id)

    name = request.form.get('name', '').strip()
    description = request.form.get('description', '').strip()

    if name and name != role.name:
        if Role.query.filter_by(name=name).first():
            flash(f'Rola "{name}" już istnieje.', 'error')
            return redirect(url_for('auth.admin_roles'))
        role.name = name

    role.description = description
    role.can_glowne = 'can_glowne' in request.form
    role.can_analiza = 'can_analiza' in request.form
    role.can_magazyn = 'can_magazyn' in request.form
    role.can_zarzadzanie = 'can_zarzadzanie' in request.form
    role.can_admin = 'can_admin' in request.form
    role.matlot_readonly = 'matlot_readonly' in request.form
    role.is_superadmin = 'is_superadmin' in request.form

    db.session.commit()
    flash(f'Rola "{role.name}" została zaktualizowana.', 'success')
    return redirect(url_for('auth.admin_roles'))


@auth_bp.route('/admin/roles/<int:role_id>/delete', methods=['POST'])
@module_required('admin')
def admin_delete_role(role_id):
    role = Role.query.get_or_404(role_id)

    if role.users.count() > 0:
        flash(f'Nie można usunąć roli "{role.name}" — ma przypisanych użytkowników.', 'error')
        return redirect(url_for('auth.admin_roles'))

    name = role.name
    db.session.delete(role)
    db.session.commit()
    flash(f'Rola "{name}" została usunięta.', 'success')
    return redirect(url_for('auth.admin_roles'))
