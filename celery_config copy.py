from celery import Celery
from redis import Redis

# Configuración de Celery
app = Celery('solanalyzer', broker='amqp://guest:guest@rabbitmq:5672//', backend='redis://:prueba@redis:6379/0')
app.conf.task_serializer = 'json'
app.conf.accept_content = ['json']
app.conf.timezone = 'UTC'
app.conf.enable_utc = True

# Configura el cliente de Redis
redis_client = Redis(host='redis', port=6379, db=0, password='prueba')
