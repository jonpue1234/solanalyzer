from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, session
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from GraficoUltimaVersion import analizar_wallet
from celery.result import AsyncResult
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, Length, EqualTo
from flask_login import LoginManager, UserMixin, login_user, current_user, logout_user, login_required
from flask_mail import Mail, Message  # Importar Flask-Mail
from itsdangerous import URLSafeTimedSerializer, SignatureExpired
from flask_migrate import Migrate
from datetime import datetime, timedelta, timezone
import uuid
from flask_socketio import SocketIO, emit, join_room, leave_room
import eventlet
from solana.rpc.api import Client
from solana.transaction import Signature
import time
import redis
from redis.lock import Lock
from celery_config import app, redis_client
import random
from dotenv import load_dotenv
import os
from celery.signals import task_revoked
import uuid
import math
from redis import Redis
import subprocess
import threading
import logging
import docker



# Cargar variables de entorno desde el archivo .env
load_dotenv()

from werkzeug.security import check_password_hash

db = SQLAlchemy()

connection = Client(os.getenv('SOLANA_CLIENT_URL'))


# Crear la aplicación Flask y inicializar la base de datos
def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')

    # Configuración de Flask-Mail
    app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
    app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT'))
    app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS') == 'True'
    app.config['MAIL_USE_SSL'] = os.getenv('MAIL_USE_SSL') == 'True'
    app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
    app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
    app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER')

    mail = Mail(app)
    db.init_app(app)

    # Inicializar Flask-Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'login'

    # Inicializar Flask-Migrate
    migrate = Migrate(app, db)

    # Inicializar Flask-SocketIO
    socketio = SocketIO(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    with app.app_context():
        db.create_all()
        print("Tablas creadas")
    return app

app = create_app()
socketio = SocketIO(app)
mail = Mail(app)  # Inicializar Flask-Mail

s = URLSafeTimedSerializer(app.config['SECRET_KEY'])


class RegistrationForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired(), Length(min=2, max=20)])
    email = StringField('Email', validators=[DataRequired(), Email()])
    confirm_email = StringField('Confirm Email', validators=[DataRequired(), Email(), EqualTo('email', message='Emails must match')])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=6)])
    confirm_password = PasswordField('Confirm Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Sign Up')


class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Login')

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128))
    last_activity = db.Column(db.DateTime, nullable=True)
    session_id = db.Column(db.String(128), nullable=True)
    subscription_expiration = db.Column(db.DateTime, nullable=True)
    referral_code = db.Column(db.String(64), unique=True, nullable=True)  # Código de referido
    referred_by = db.Column(db.String(64), nullable=True)  # Código del referidor
    referral_count = db.Column(db.Integer, default=0)  # Contador de referidos
    referral_fee = db.Column(db.Float, default=0.0)  # Comisión acumulada
    wallet_address = db.Column(db.String(128), nullable=True)  # Dirección de la billetera


    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def acquire_lock_with_timeout(conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lockname, lock_timeout)
        time.sleep(0.001)
    return False

def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    while True:
        try:
            pipe.watch(lockname)
            if pipe.get(lockname).decode('utf-8') == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False

class SolanaEndpointManager:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.endpoints = [
            os.getenv('ENDPOINT_1'),
            os.getenv('ENDPOINT_2'),
            os.getenv('ENDPOINT_3'),
            os.getenv('ENDPOINT_4'),
            os.getenv('ENDPOINT_5'),
            os.getenv('ENDPOINT_6'),
            os.getenv('ENDPOINT_7'),
            os.getenv('ENDPOINT_8'),
            os.getenv('ENDPOINT_9'),
            os.getenv('ENDPOINT_10')
        ]
        self.initialize_endpoints()

    def initialize_endpoints(self):
        for endpoint in self.endpoints:
            self.redis_client.setnx(f"endpoint_available:{endpoint}", 1)
            logger.info(f"Endpoint {endpoint} set to available.")

    def acquire_endpoint_with_retry(self, retries=20, delay=6):
        for attempt in range(retries):
            identifier = acquire_lock_with_timeout(self.redis_client, "endpoint_lock", lock_timeout=10)
            if not identifier:
                logger.warning(f"No endpoints available at the moment. Retrying in {delay} seconds...")
                time.sleep(delay)
                continue

            available_endpoints = [
                endpoint for endpoint in self.endpoints
                if self.redis_client.get(f"endpoint_available:{endpoint}") and self.redis_client.get(f"endpoint_available:{endpoint}").decode() == '1'
            ]

            if available_endpoints:
                endpoint = random.choice(available_endpoints)
                self.redis_client.set(f"endpoint_available:{endpoint}", 0)
                release_lock(self.redis_client, "endpoint_lock", identifier)
                logger.info(f"Endpoint seleccionado: {endpoint}")
                return endpoint

            release_lock(self.redis_client, "endpoint_lock", identifier)
            logger.warning(f"No endpoints available at the moment. Retrying in {delay} seconds...")
            time.sleep(delay)
        raise Exception("Could not acquire endpoint after multiple attempts")

    def release_endpoint(self, url):
        try:
            self.redis_client.set(f"endpoint_available:{url}", 1)
            logger.info(f"Endpoint released: {url}")
        except redis.ConnectionError as e:
            logger.error(f"Error releasing endpoint {url}: {e}. Retrying...")
            time.sleep(5)
            self.release_endpoint(url)

endpoint_manager = SolanaEndpointManager(redis_client)





@app.route("/register", methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        username_exists = User.query.filter_by(username=form.username.data).first()
        email_exists = User.query.filter_by(email=form.email.data).first()

        if username_exists:
            flash('This username already exists, choose another.', 'danger')
        elif email_exists:
            flash('This email already exists, choose another.', 'danger')
        else:
            referral_code = str(uuid.uuid4())[:8]  # Generar un código de referido único
            referred_by = request.args.get('ref')
            user = User(username=form.username.data, email=form.email.data, referral_code=referral_code, referred_by=referred_by)
            user.set_password(form.password.data)
            db.session.add(user)
            db.session.commit()

            # Actualizar el contador de referidos del referidor
            if referred_by:
                referrer = User.query.filter_by(referral_code=referred_by).first()
                if referrer:
                    referrer.referral_count += 1
                    db.session.commit()

            flash('Your account has been created! You are now able to log in', 'success')
            return redirect(url_for('login'))
    return render_template('register.html', title='Register', form=form)



@app.route('/admin/delete_user', methods=['POST'])
def admin_delete_user():
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))

    user_id = request.form.get('user_id')

    user = User.query.get(user_id)
    if user:
        db.session.delete(user)
        db.session.commit()
        # flash(f'User {user.username} deleted successfully', 'success')
    else:
        flash('User not found', 'danger')

    return redirect(url_for('admin_panel'))


@app.route('/admin/update_referral_fee', methods=['POST'])
def admin_update_referral_fee():
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))

    user_id = request.form.get('user_id')
    referral_fee = request.form.get('referral_fee')

    user = User.query.get(user_id)
    if user:
        user.referral_fee = float(referral_fee)
        db.session.commit()
        # flash('Referral fee updated successfully.', 'success')
    else:
        flash('User not found.', 'danger')

    return redirect(url_for('admin_panel'))




@app.route("/login", methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user and user.check_password(form.password.data):
            # Emitir evento de expulsión si ya hay una sesión activa
            if user.session_id:
                socketio.emit('force_logout', {'message': 'You have been logged out due to another login.'}, room=user.session_id)

            # Cerrar cualquier sesión anterior
            logout_user()

            login_user(user)
            user.last_activity = datetime.now(timezone.utc)  # Usar datetime.now(timezone.utc)
            user.session_id = str(uuid.uuid4())  # Generar un nuevo session_id único
            db.session.commit()

            session['session_id'] = user.session_id  # Almacenar session_id en la sesión del navegador

            flash('Login Successful!', 'success')
            return redirect(url_for('index_view'))
        else:
            flash('Login Unsuccessful. Please check username and password', 'danger')
    return render_template('login.html', title='Login', form=form)


@app.route('/index')
@login_required
def index_view():
    now = datetime.now(timezone.utc)
    subscription_expiration = current_user.subscription_expiration
    username = current_user.username
    time_left = None
    referral_link = url_for('register', ref=current_user.referral_code, _external=True)
    referral_count = current_user.referral_count
    referral_fee = current_user.referral_fee
    withdraw_wallet = current_user.wallet_address  # Obtener la dirección de retiro

    referrals = User.query.filter_by(referred_by=current_user.referral_code).all()  # Obtener los referidos

    if subscription_expiration:
        subscription_expiration = subscription_expiration.replace(tzinfo=timezone.utc)
        time_left = subscription_expiration - now

    return render_template('index.html', now=now, time_left=time_left, username=username, 
                           subscription_expiration=subscription_expiration, referral_link=referral_link, 
                           referral_count=referral_count, referral_fee=referral_fee, withdraw_wallet=withdraw_wallet, 
                           referrals=referrals)


@app.route('/logout', methods=['POST'])
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'success')
    return redirect(url_for('login'))

@app.route('/')
def home():
    if current_user.is_authenticated:
        return redirect(url_for('index_view'))
    return render_template('home.html', title='Welcome to SolAnalyzer')


@app.route('/submit-task', methods=['POST'])
@login_required
def submit_task():
    data = request.get_json()
    wallet = data['wallet']
    days_to_analyze = data['days_to_analyze']

    try:
        task = analizar_wallet.delay(wallet, int(days_to_analyze))
        return jsonify({'task_id': task.id}), 202
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/task-status/<task_id>', methods=['GET'])
@login_required
def task_status(task_id):
    task_result = AsyncResult(task_id, app=analizar_wallet)
    if task_result.ready():
        return jsonify({
            'state': task_result.state,
            'result': task_result.get()
        })
    else:
        return jsonify({'state': task_result.state})


@app.route('/reset_password_request', methods=['GET', 'POST'])
def reset_password_request():
    if request.method == 'POST':
        email = request.form['email']
        user = User.query.filter_by(email=email).first()
        if user:
            token = s.dumps(email, salt='password-reset-salt')
            reset_url = url_for('reset_password', token=token, _external=True)
            msg = Message('Password Reset Request', recipients=[email])
            msg.body = f'''
Hey {user.username}, 

This is the link to reset your password: {reset_url}

SolAnalyzer Team.
'''
            mail.send(msg)
            flash('A password reset link has been sent to your email address.', 'info')
            return redirect(url_for('login'))
        else:
            flash('Email address not found.', 'danger')
    return render_template('reset_password_request.html')


@app.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    try:
        email = s.loads(token, salt='password-reset-salt', max_age=3600)
    except SignatureExpired:
        flash('The password reset link has expired.', 'danger')
        return redirect(url_for('reset_password_request'))
    if request.method == 'POST':
        password = request.form['password']
        confirm_password = request.form['confirm_password']
        if password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return redirect(url_for('reset_password', token=token))
        
        user = User.query.filter_by(email=email).first()
        if user:
            user.set_password(password)
            db.session.commit()
            flash('Your password has been updated.', 'success')
            return redirect(url_for('login'))
        else:
            flash('User not found.', 'danger')
            return redirect(url_for('reset_password_request'))
    return render_template('reset_password.html', token=token)


@app.route('/connect_wallet', methods=['POST'])
def connect_wallet():
    data = request.get_json()
    phantom_wallet_address = data.get('publicKey')  # Cambiar el nombre de la variable
    user = current_user if current_user.is_authenticated else None
    if user:
        session['phantom_wallet_address'] = phantom_wallet_address  # Guardar en la sesión en lugar de la base de datos
    session['wallet_addressphantom'] = phantom_wallet_address  # Guardar en la sesión
    return jsonify({'status': 'success', 'message': 'Wallet connected successfully.', 'wallet_addressphantom': phantom_wallet_address})


@app.route('/save_withdraw_wallet', methods=['POST'])
@login_required
def save_withdraw_wallet():
    data = request.get_json()
    wallet = data.get('wallet')
    print('Received Wallet:', wallet)  # Para depuración

    if not wallet:
        return jsonify({'status': 'error', 'message': 'Se requiere la dirección de la billetera'}), 400

    current_user.wallet_address = wallet
    db.session.commit()

    return jsonify({'status': 'success', 'message': 'Dirección de la billetera de retiro guardada exitosamente'})


# Renombrar esta función para evitar conflictos
@app.route('/update_subscription', methods=['POST'])
@login_required
def update_subscription():
    data = request.get_json()
    signature_str = data.get('signature')

    if not signature_str:
        return jsonify({'status': 'error', 'message': 'Signature is required'}), 400

    try:
        # Añadir un retraso inicial de 25 segundos antes de la primera verificación
        time.sleep(15)

        # Convertir la firma de cadena a un objeto Signature
        signature = Signature.from_string(signature_str)
        print("Firma convertida a Signature object:", signature, flush=True)

        max_retries = 10
        retry_delay = 6  # segundos

        for attempt in range(max_retries):
            # Verificar la transacción en la red de Solana directamente con el objeto Signature
            response = connection.get_transaction(signature, "jsonParsed", max_supported_transaction_version=0)
            result = response.value

            print(f"Intento {attempt + 1}: Respuesta de la API de Solana:", response, flush=True)
            print(f"Intento {attempt + 1}: Resultado de la transacción:", result, flush=True)

            if result:
                break

            print(f"Transacción no encontrada. Reintentando en {retry_delay} segundos...", flush=True)
            time.sleep(retry_delay)

        if not result:
            return jsonify({'status': 'error', 'message': 'Transaction not found or not confirmed'}), 400

        # Actualizar la fecha de expiración de la suscripción del usuario
        current_user.subscription_expiration = datetime.now(timezone.utc) + timedelta(days=30)
        db.session.commit()

        # Sumar comisión al referidor si existe
        if current_user.referred_by:
            referrer = User.query.filter_by(referral_code=current_user.referred_by).first()
            if referrer:
                referrer.referral_fee += 0.1 * 0.5  # 10% de 0.5 SOL
                db.session.commit()

        return jsonify({'status': 'success', 'message': 'Subscription updated successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/pay_fee', methods=['POST'])
def pay_fee():
    data = request.get_json()
    user_id = data.get('user_id')
    signature_str = data.get('signature')

    if not user_id or not signature_str:
        return jsonify({'status': 'error', 'message': 'User ID and signature are required'}), 400

    try:
        # Añadir un retraso inicial de 20 segundos antes de la primera verificación
        time.sleep(15)

        # Convertir la firma de cadena a un objeto Signature
        signature = Signature.from_string(signature_str)
        print("Firma convertida a Signature object:", signature, flush=True)

        max_retries = 10
        retry_delay = 5  # segundos

        for attempt in range(max_retries):
            # Verificar la transacción en la red de Solana directamente con el objeto Signature
            response = connection.get_transaction(signature, "jsonParsed", max_supported_transaction_version=0)
            result = response.value

            print(f"Intento {attempt + 1}: Respuesta de la API de Solana:", response, flush=True)
            print(f"Intento {attempt + 1}: Resultado de la transacción:", result, flush=True)

            if result:
                break

            print(f"Transacción no encontrada. Reintentando en {retry_delay} segundos...", flush=True)
            time.sleep(retry_delay)

        if not result:
            return jsonify({'status': 'error', 'message': 'Transaction not found or not confirmed'}), 400

        user = User.query.get(user_id)
        if user:
            # Resetear la comisión acumulada del usuario
            user.referral_fee = 0.0
            db.session.commit()
            return jsonify({'status': 'success', 'message': 'Fee paid and updated successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'User not found'}), 404

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# Añadir nuevas rutas para el panel de administración

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        admin_username = os.getenv('ADMIN_USERNAME')
        admin_password_hash = os.getenv('ADMIN_PASSWORD_HASH')
        if username == admin_username and check_password_hash(admin_password_hash, password):
            session['admin_logged_in'] = True
            return redirect(url_for('admin_panel'))
        else:
            flash('Login Unsuccessful. Please check username and password', 'danger')
    return render_template('admin_login.html')

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))

@app.route('/admin/panel')
def admin_panel():
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))

    users = User.query.all()
    return render_template('admin_panel.html', users=users)

@app.route('/admin/update_subscription', methods=['POST'])
def admin_update_subscription():
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))

    user_id = request.form.get('user_id')
    days = request.form.get('days')

    user = User.query.get(user_id)
    if user:
        user.subscription_expiration = datetime.now(timezone.utc) + timedelta(days=int(days))
        db.session.commit()
        # flash(f'Subscription updated for user {user.username}', 'success')
    else:
        flash('User not found', 'danger')

    return redirect(url_for('admin_panel'))

@app.before_request
def clear_wallet_session():
    if 'wallet_addressphantom' in session:
        session.pop('wallet_addressphantom', None)

import json  # Asegúrate de que este import esté al inicio de tu archivo
import logging
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

import json

@app.route('/release-endpoint', methods=['POST'])
def release_endpoint():
    data = request.get_data(as_text=True)
    json_data = json.loads(data)
    task_id = json_data.get('task_id')

    print(f"Received request to release endpoint for Task ID: {task_id}")
    logger.debug(f"Received request to release endpoint for Task ID: {task_id}")

    if not task_id:
        return jsonify({'status': 'error', 'message': 'Task ID is required'}), 400

    task = AsyncResult(task_id, app=analizar_wallet)

    if not task:
        return jsonify({'status': 'error', 'message': 'Task not found'}), 404

    # Marcar la tarea como revocada en Redis
    redis_client.set(f"task_revoked:{task_id}", 1)
    logger.debug(f"Task {task_id} marked as revoked.")

    endpoint_url = task.info.get('endpoint_url')
    if endpoint_url:
        endpoint_manager.release_endpoint(endpoint_url)
        print(f"Endpoint released: {endpoint_url}")
        logger.debug(f"Endpoint released: {endpoint_url}")

    return jsonify({'status': 'success', 'message': 'Endpoint released successfully'})






@app.before_request
def update_last_activity():
    if current_user.is_authenticated:
        current_user.last_activity = datetime.now(timezone.utc)  # Cambiado
        db.session.commit()

@app.before_request
def limit_active_sessions():
    if current_user.is_authenticated:
        now = datetime.now(timezone.utc)
        
        if current_user.last_activity:
            last_activity_aware = current_user.last_activity.replace(tzinfo=timezone.utc)
        else:
            last_activity_aware = now

        # Verificar y cerrar sesiones inactivas
        if last_activity_aware < now - timedelta(minutes=30):
            logout_user()
            flash('Your session has expired due to inactivity.', 'info')
           
            return redirect(url_for('login'))

        # Verificar si el session_id actual coincide con el session_id en la base de datos
        if 'session_id' in session and session['session_id'] != current_user.session_id:
            socketio.emit('force_logout', {'message': 'You have been logged out due to another login.'}, room=session['session_id'])
            logout_user()
            flash('You have been logged out due to another login.', 'info')
            return redirect(url_for('login'))



@socketio.on('connect')
def handle_connect():
    if current_user.is_authenticated:
        session_id = session.get('session_id')
        if session_id:
            join_room(session_id)

@socketio.on('disconnect')
def handle_disconnect():
    if current_user.is_authenticated:
        session_id = session.get('session_id')
        if session_id:
            leave_room(session_id)

@app.route('/disconnect_wallet', methods=['POST'])
@login_required
def disconnect_wallet():
    # Aquí puedes añadir cualquier lógica adicional que necesites para desconectar la wallet
    session.pop('wallet_addressphantom', None)  # Eliminar la wallet de la sesión
    return jsonify({'status': 'success', 'message': 'Wallet disconnected successfully.'})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', debug=True)
