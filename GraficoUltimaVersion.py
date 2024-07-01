from datetime import datetime, timezone
import json
from solana.rpc.api import Client
from solders.signature import Signature
import requests
from pprint import pprint
import base58
from solders.pubkey import Pubkey
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from random import uniform
import time
from datetime import datetime, timezone, timedelta
from celery import Celery
from celery.utils.log import get_task_logger
from celery_config import app, redis_client
import threading
from random import choice
import redis
from redis.lock import Lock
import random
from dotenv import load_dotenv
import os
from celery import current_task
import uuid
import math
from redis import Redis
import subprocess
import logging
import docker

# Cargar variables de entorno desde el archivo .env
load_dotenv()


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



RAYDIUM_POOL_ADDRESS = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
SOL_MINT_ADDRESS = "So11111111111111111111111111111111111111112"
mint_address_cache = {}
token_name_cache = {}
logger = get_task_logger(__name__)

def obtener_transacciones(cadena, limite=3000, endpoint_url=None):
    # Decodificar la cadena y convertirla a un objeto Pubkey
    pubkey_cadena = Pubkey(base58.b58decode(cadena))
    transacciones_totales = []
    ultima_firma = None
    
    if not endpoint_url:
        print("No hay endpoints disponibles para obtener transacciones.")
        return []
    
    # Crear un cliente de Solana usando el endpoint proporcionado
    solana_client = Client(endpoint_url)
    
    while len(transacciones_totales) < limite:
        # Obtener transacciones usando paginación
        respuesta = solana_client.get_signatures_for_address(pubkey_cadena, before=ultima_firma, limit=1000)
        transacciones = respuesta.value

        if not transacciones:
            break  # Si no hay más transacciones, termina el bucle

        transacciones_totales.extend(transacciones)

        if len(transacciones) < 1000:
            break  # Si las transacciones recuperadas son menos que el límite, indica el fin de las transacciones disponibles

        ultima_firma = transacciones[-1].signature  # Actualizar la última firma conocida para la próxima consulta

    # Limitar la lista de transacciones al máximo especificado si se supera
    if len(transacciones_totales) > limite:
        transacciones_totales = transacciones_totales[:limite]
    
    return transacciones_totales


    


def get_sol_price_at_time(block_time_unix):
    date = datetime.fromtimestamp(block_time_unix, timezone.utc).strftime('%d-%m-%Y')
    url = f"https://api.coingecko.com/api/v3/coins/solana/history?date={date}&localization=false"
    try:
        response = requests.get(url)
        price = response.json()['market_data']['current_price']['usd']
        return price
    except Exception as e:
        print(f"Error al obtener el precio de SOL: {e}")
        return None
    
sol_price_cache = {"timestamp": None, "price": None}

def get_current_sol_price_usd():
    # Verifica si el precio está en caché y si es reciente (5 minutos)
    if sol_price_cache["timestamp"] is not None and (time.time() - sol_price_cache["timestamp"]) < 300:
        return sol_price_cache["price"]
    
    url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
    try:
        response = requests.get(url)
        data = response.json()
        sol_price_usd = data['solana']['usd']
        # Actualiza el caché
        sol_price_cache["price"] = sol_price_usd
        sol_price_cache["timestamp"] = time.time()
        return sol_price_usd
    except Exception as e:
        print(f"Error al obtener el precio actual de SOL: {e}")
        return None



# Definir las configuraciones de las dos APIs
apis = [
    {
        "url_template": "https://solana-gateway.moralis.io/token/mainnet/{mint_address}/metadata",
        "headers": {
            "Accept": "application/json",
            "X-API-Key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImNhMzcyMTU1LTU1ZGEtNGNmNi05MzIwLWZiMDBhMmI5NGQ3NyIsIm9yZ0lkIjoiMzgxMDY1IiwidXNlcklkIjoiMzkxNTU5IiwidHlwZUlkIjoiYzRjM2I1NmMtZWU4OS00MTE2LTg5Y2EtYzFjNWNjMjQyNTVhIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3MDk0ODI3NTYsImV4cCI6NDg2NTI0Mjc1Nn0.92giuYeXQO52raWnOOOqgiq4V_EU_1-CyLiIzGy2jnk"
        }
    },
    {
        "url_template": "https://solana-gateway.moralis.io/token/mainnet/{mint_address}/metadata",
        "headers": {
            "Accept": "application/json",
            "X-API-Key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjM4Mzk0ZGU5LWYzMjctNDU4NS1hYjYwLTUxNTc3OWE5NzAwOSIsIm9yZ0lkIjoiMzgzMzY0IiwidXNlcklkIjoiMzkzOTA3IiwidHlwZUlkIjoiMDRhZjBiMjUtOWIyZC00ZjNiLTkxZjMtNzBlMzU4NjIwMmRhIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3MTA3MjMxNjMsImV4cCI6NDg2NjQ4MzE2M30.K02HmpBgAojpqcMb8SPzyIpTLydzBmiYcHnUOlvpp0c"
        }
    }
]


def get_token_name(mint_address, max_retries=2):
    # Primero, verifica si el nombre del token ya está en caché
    if mint_address in token_name_cache:
        return token_name_cache[mint_address]

    for attempt in range(max_retries):
        # Selecciona aleatoriamente una configuración de API
        api = random.choice(apis)
        url = api["url_template"].format(mint_address=mint_address)
        headers = api["headers"]

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            token_name = data['name']
            # Guarda el nombre del token en la caché antes de retornarlo
            token_name_cache[mint_address] = token_name
            return token_name
        except Exception as e:
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt
                time.sleep(sleep_time)
            else:
                print(f"Error al obtener el nombre del token después de {max_retries} intentos: {e}")
                return "Nombre del Token no encontrado"

    
def find_wallet_and_mint_in_transaction(transaction_data):
    instructions = transaction_data.get('result', {}).get('transaction', {}).get('message', {}).get('instructions', [])
    
    for instruction in instructions:
        if 'parsed' in instruction:
            info = instruction['parsed'].get('info', {})
            if 'wallet' in info and 'mint' in info:
                return info['wallet'], info['mint']
    
    return None, None

def extract_received_tokens(transaction_meta, user_wallet, transaction_data):
    # Obtener el mint address relevante para la comparación
    _, relevant_mint_address = find_wallet_and_mint_in_transaction(transaction_data)

    post_balances = transaction_meta.get('postTokenBalances', [])
    pre_balances = transaction_meta.get('preTokenBalances', [])
    received_tokens = None
    token_mint_address = None
    
    usdc_mint_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    sol_mint_address = "So11111111111111111111111111111111111111112"
    usdt_mint_address = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"  # Agregado
    raydium_address = RAYDIUM_POOL_ADDRESS
    
    for post_entry in post_balances:
        if post_entry.get('owner') == user_wallet and post_entry.get('mint') not in [usdc_mint_address, sol_mint_address, usdt_mint_address, relevant_mint_address]:
            account_index = post_entry.get('accountIndex')
            pre_entry = next((item for item in pre_balances if item.get('accountIndex') == account_index), None)
            post_amount = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
            if pre_entry:
                pre_amount = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                change = post_amount - pre_amount
            else:
                # Si no hay un balance previo, asume que todos los tokens en post son recibidos.
                change = post_amount
            
            if change != 0:  # Se asegura de que haya habido un cambio o un balance recién creado.
                received_tokens = change
                token_mint_address = post_entry.get('mint')
                break  # Termina después de encontrar el primer cambio relevante o cuenta nueva relevante.

    # Si no encontró en la primera pasada, intenta de nuevo con la condición alternativa
    if received_tokens is None or received_tokens == 0:
        for post_entry in reversed(post_balances):
            if post_entry.get('owner') == raydium_address and post_entry.get('mint') not in [usdc_mint_address, sol_mint_address, usdt_mint_address]:
                account_index = post_entry.get('accountIndex')
                pre_entry = next((item for item in pre_balances if item.get('accountIndex') == account_index), None)
                post_amount = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                if pre_entry:
                    pre_amount = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                    change = max(post_amount, pre_amount) - min(post_amount, pre_amount)
                    received_tokens = change
                    token_mint_address = post_entry.get('mint')
                    break
    
    # Verificación de relevant_mint_address para ventas
    if received_tokens < 0 and token_mint_address == relevant_mint_address:
        received_tokens = None  # Descarta este bloque si es una venta y coincide el mint

    return received_tokens, token_mint_address


def extract_transaction_details(transaction_meta, user_wallet, block_time_unix, transaction_instructions, transaction_data): # Actualizado para incluir transaction_data
    fee_lamports = transaction_meta.get('fee', 0)
    fee_sol = fee_lamports / 1_000_000_000
    swap_sol_used_list = []  # Almacena temporalmente todos los valores SOL utilizados
    transaction_type = 'compra'
    block_time_formatted = datetime.fromtimestamp(block_time_unix, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + ' UTC'

  
    received_tokens, token_mint_address = extract_received_tokens(transaction_meta, user_wallet, transaction_data)
    
    # Verificar si received_tokens es None y terminar la ejecución si es así
    if received_tokens is None:
        #print("Transacción omitida")
        # Retornar valores predeterminados aquí para asegurar la consistencia
        return (fee_sol, 0, 0, None, transaction_type, block_time_formatted, 'No aplicable', 'No aplicable',True)


    # Intentar encontrar SOL utilizado con la condición original
    for post_entry in transaction_meta.get('postTokenBalances', []):
        if post_entry.get('owner') == RAYDIUM_POOL_ADDRESS and post_entry.get('mint') == SOL_MINT_ADDRESS:
            account_index = post_entry.get('accountIndex')
            pre_entry = next((item for item in transaction_meta.get('preTokenBalances', []) if item.get('accountIndex') == account_index), None)

            post_balance = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
            if pre_entry:
                pre_balance = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                swap_sol_used_list.append(post_balance - pre_balance)
            else:
                swap_sol_used_list.append(post_balance)
            break  # Este break hace que solo se considere la primera coincidencia encontrada en postTokenBalances

    # Si no se encuentra con la condición original, buscar en innerInstructions
    if not swap_sol_used_list:
        # Se extraen los tokens recibidos y su dirección mint para la comparación antes de iterar sobre las instrucciones
        received_tokens_amount, received_token_mint = extract_received_tokens(transaction_meta, user_wallet, transaction_data)
        
        for inner_instruction in transaction_meta.get('innerInstructions', []):
            for instruction in inner_instruction['instructions']:
                if 'parsed' in instruction:
                    info = instruction['parsed']['info']
                    # Añade aquí la comprobación del 'mint' junto con 'authority'
                    if info.get('authority') == user_wallet and info.get('mint') != received_token_mint:
                        destination = info.get('destination')
                        for post_instruction in transaction_instructions:
                            if 'parsed' in post_instruction:
                                post_info = post_instruction['parsed']['info']
                                if post_info.get('source') == destination:
                                    if 'mint' in post_info and post_info['mint'] == SOL_MINT_ADDRESS:
                                        swap_sol_used_list.append(float(post_info['tokenAmount']['uiAmountString']))
                                    elif 'amount' in post_info:  # Para transferencias directas de SOL que usan 'amount'
                                        swap_sol_used_list.append(float(post_info['amount']) / 1_000_000_000)


    # Elegir el último valor SOL utilizado si hay múltiples
    if swap_sol_used_list:
        swap_sol_used = swap_sol_used_list[-1]
    else:
        # Si aún no se ha encontrado el monto de SOL utilizado, comenzamos la búsqueda en 'innerInstructions'
        swap_sol_used = None
        received_tokens, token_mint_address = extract_received_tokens(transaction_meta, user_wallet, transaction_data) # Usando el nuevo argumento

        received_tokens_str = ''.join(filter(str.isdigit, str(received_tokens)))  # Convertimos received_tokens a una cadena limpia de números
        
        
        for inner_instruction in transaction_meta.get('innerInstructions', []):
            for instruction in inner_instruction['instructions']:
                if 'parsed' in instruction:
                    parsed_info = instruction['parsed']
                    info = parsed_info['info']
                    # Primera condición: buscamos por 'authority' y descartamos si 'mint' es igual a 'token_mint_address'
                    if parsed_info['type'] in ['transfer', 'transferChecked'] and info.get('authority') == user_wallet:
                        if not token_mint_address or info.get('mint') != token_mint_address:
                            amount = float(info.get('amount') or info.get('tokenAmount', {}).get('amount', 0))
                            swap_sol_used = amount / 1_000_000_000
                            swap_sol_used_str = ''.join(filter(str.isdigit, str(swap_sol_used)))
                            
                            # Aseguramos que la cantidad de SOL no sea esencialmente la misma que la de tokens recibidos, ignorando las últimas cifras
                            if swap_sol_used_str[:-2] == received_tokens_str[:-2]:  # Compara ignorando el último dígito
                                
                                return (fee_sol, 0, received_tokens, get_token_name(token_mint_address), transaction_type, block_time_formatted, 'No aplicable', 'No aplicable', True)
                            else:
                                # Si los valores son efectivamente iguales (ajustando por pequeñas diferencias), descartamos este valor de SOL y seguimos buscando
                                swap_sol_used = None
            
                # Si encontramos un valor adecuado o necesitamos seguir buscando
                if swap_sol_used is not None:
                    break

        # Si aún no hemos encontrado un valor adecuado, buscamos cualquier transferencia de SOL sin considerar el 'authority'
        if swap_sol_used is None:
            for inner_instruction in transaction_meta.get('innerInstructions', []):
                for instruction in inner_instruction['instructions']:
                    if 'parsed' in instruction:
                        parsed_info = instruction['parsed']
                        info = parsed_info['info']
                        # Añadiendo la verificación del tipo de instrucción aquí
                        if parsed_info['type'] in ['transfer', 'transferChecked'] and info.get('mint') == SOL_MINT_ADDRESS:
                            amount = info.get('amount') or info.get('tokenAmount', {}).get('amount', 0)
                            swap_sol_used = float(amount) / 1_000_000_000
                            break

                if swap_sol_used is not None:
                    break
                
        if swap_sol_used is None:
            for post_entry in reversed(transaction_meta.get('postTokenBalances', [])):
                if post_entry.get('mint') == SOL_MINT_ADDRESS:
                    account_index = post_entry.get('accountIndex')
                    pre_entry = next((item for item in transaction_meta.get('preTokenBalances', []) if item.get('accountIndex') == account_index), None)
                    
                    post_amount = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                    if pre_entry:
                        pre_amount = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                        swap_sol_used = post_amount - pre_amount
                        break  # Salir del bucle una vez que se encuentra el valor y se calcula el SOL utilizado
                    
        if swap_sol_used is None:
            for inner_instruction in transaction_meta.get('innerInstructions', []):
                for instruction in inner_instruction['instructions']:
                    if ('parsed' in instruction and
                        instruction['parsed'].get('type') == 'transfer' and
                        instruction['parsed']['info'].get('source') == user_wallet and
                        instruction['programId'] == '11111111111111111111111111111111'):
                        # Convertir lamports a SOL
                        swap_sol_used = instruction['parsed']['info']['lamports'] / 1_000_000_000
                        break  # Rompe el ciclo si se encuentra una coincidencia
                    if swap_sol_used is not None:
                        break
                if swap_sol_used is not None:
                    break                  
                
    received_tokens, token_mint_address = extract_received_tokens(transaction_meta, user_wallet, transaction_data) # Usando el nuevo argumento
    token_name = get_token_name(token_mint_address)  # Obtiene el nombre del token
    
    # Nueva comprobación después de determinar swap_sol_used

    swap_sol_used_alternative = None
    
    if swap_sol_used is not None and received_tokens is not None:
        swap_sol_used_str = ''.join(filter(str.isdigit, str(swap_sol_used)))
        received_tokens_str = ''.join(filter(str.isdigit, str(received_tokens)))

        if swap_sol_used_str == received_tokens_str:
            # Inicializa swap_sol_used_alternative como None para buscar una alternativa
            

            # Buscar en innerInstructions para uso alternativo de SOL
            for inner_instruction in transaction_meta.get('innerInstructions', []):
                for instruction in inner_instruction['instructions']:
                    if 'parsed' in instruction and instruction['parsed']['type'] == 'transfer':
                        info = instruction['parsed']['info']
                        info_amount_str = ''.join(filter(str.isdigit, str(info.get('amount', '0'))))
                        if info_amount_str != received_tokens_str and info.get('mint', '') != token_mint_address:
                            amount = info.get('amount', 0)  # Esto devuelve 0 si 'amount' no está presente
                            swap_sol_used_alternative = float(amount) / 1_000_000_000
                            if info_amount_str[:-2] == received_tokens_str[:-2]:
                                    
                                break
                            else:
                                # Si los valores son efectivamente iguales (ajustando por pequeñas diferencias), descartamos este valor de SOL y seguimos buscando
                                
                                swap_sol_used = None

                if swap_sol_used_alternative is not None:
                    swap_sol_used = swap_sol_used_alternative
                    break
        else:
            # Si las cifras son distintas, continúa con el swap_sol_used ya calculado
            pass

        # Si se encontró un swap_sol_used alternativo válido, úsalo; de lo contrario, mantiene el valor original de swap_sol_used
        swap_sol_used = swap_sol_used_alternative if swap_sol_used_alternative is not None else swap_sol_used
    
    if swap_sol_used is None or swap_sol_used == 0:
        #print("No se encontró SOL utilizado en la transacción, se omitirá.")
        return (fee_sol, None, received_tokens, get_token_name(token_mint_address), transaction_type, block_time_formatted, 'No aplicable', 'No aplicable', True)

    
    if swap_sol_used is not None and received_tokens > 0 and swap_sol_used < 0:
        swap_sol_used = abs(swap_sol_used)

    # Correction for the sign of SOL used in purchases
    if received_tokens > 0 and swap_sol_used < 0:
        swap_sol_used = abs(swap_sol_used)
        
    if received_tokens < 0 and swap_sol_used > 0:
    # Esto indica una venta, por lo tanto, hacemos negativo el swap_sol_used para reflejar SOL "recibido"
        swap_sol_used = -swap_sol_used
    
    # Decide on labels based on transaction type
    if received_tokens < 0:  # Assuming negative received tokens indicate a sale
        sol_label = 'Sol received'
        tokens_label = 'Tokens sold'
    else:
        sol_label = 'Sol used'
        tokens_label = 'Tokens received'
    
    block_time_formatted = datetime.fromtimestamp(block_time_unix, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + ' UTC'
    
    # Retornamos los valores calculados junto con los nuevos labels para SOL y tokens
    return (fee_sol, swap_sol_used, received_tokens, token_name, transaction_type, block_time_formatted, token_mint_address, sol_label, tokens_label, False)


def get_solana_balance(user_wallet, endpoint_url):
    if not endpoint_url:
        print("No hay endpoints disponibles para el balance.")
        return None

    solana_client = Client(endpoint_url)
    pubkey = Pubkey.from_string(user_wallet)
    response = solana_client.get_balance(pubkey)
    balance = response.value / 1_000_000_000
    balance_formatted = "{:.4f}".format(balance)
    return balance_formatted



def process_transaction(signature, user_wallet, fecha_inicio, endpoint_url, attempt=1, max_attempts=3):
    global mint_address_cache  # Asegúrate de utilizar la caché global
    
    # Asumimos que el endpoint_url ya es proporcionado y no necesitamos obtenerlo aquí
    if not endpoint_url:
        print("No se proporcionó un endpoint válido.")
        return None

    # Crear un cliente de Solana con el endpoint proporcionado
    solana_client = Client(endpoint_url)
    
    try:
        response = solana_client.get_transaction(signature, "jsonParsed", max_supported_transaction_version=0)
        response_json = json.loads(response.to_json())
        
        if 'result' not in response_json or not response_json['result']:
            return None

        transaction_meta = response_json['result']['meta']
        block_time_unix = response_json['result']['blockTime']
        fecha_transaccion = datetime.fromtimestamp(block_time_unix, timezone.utc)
        
        if fecha_transaccion < fecha_inicio:
            return "Transacción omitida por fecha."
        
        if transaction_meta.get('err') is not None or any("failed" in log for log in transaction_meta.get('logMessages', [])):
            return None
        
        inner_instructions = transaction_meta.get('innerInstructions', [])
        unique_indexes = len(set(i['index'] for i in inner_instructions))
        total_parsed_instructions = sum(len(i['instructions']) for i in inner_instructions)

        # Si hay 1 o menos índices y menos de 2 instrucciones parsed, omitir
        if unique_indexes <= 1 and total_parsed_instructions < 2:
            return None
        
        inner_instructions_list = []
        for inner_instruction in transaction_meta.get('innerInstructions', []):
            for instruction in inner_instruction['instructions']:
                inner_instructions_list.append(instruction)

        fee_sol, swap_sol_used, received_tokens, token_name, transaction_type, block_time_formatted, token_mint_address, sol_label, tokens_label, omit_transaction = extract_transaction_details(transaction_meta, user_wallet, block_time_unix, inner_instructions_list, response_json['result'])
        
        if token_mint_address:
            if token_mint_address in mint_address_cache:
                token_name = mint_address_cache[token_mint_address]
            else:
                token_name = get_token_name(token_mint_address)
                mint_address_cache[token_mint_address] = token_name

        if not omit_transaction:
            return {
                'token_name': token_name,
                'amount_tokens': received_tokens,
                'sol_amount': swap_sol_used,
                'fee': fee_sol,
                'date': block_time_formatted,
                'type': 'buy' if received_tokens > 0 else 'sell',
                'timestamp': block_time_unix,
                'mint_address': token_mint_address
            }
    except Exception as e:
        if attempt < max_attempts:
            sleep_time = min(2 ** attempt, 60) + (uniform(0, 1) * 0.1)
            sleep(sleep_time)
            return process_transaction(signature, user_wallet, fecha_inicio, endpoint_url, attempt + 1, max_attempts)
    
    return None

from celery import current_task


@app.task(bind=True)
def analizar_wallet(self, user_wallet, dias_a_analizar):
    try:
        endpoint_url = endpoint_manager.acquire_endpoint_with_retry()
    except Exception as e:
        logger.error(f"Failed to acquire endpoint: {str(e)}")
        raise self.retry(countdown=10)
    
    self.update_state(state='PROGRESS', meta={'endpoint_url': endpoint_url})
    logger.debug(f"Task ID: {self.request.id}, Endpoint seleccionado: {endpoint_url}")

    try:
        fecha_inicio = datetime.now(timezone.utc) - timedelta(days=dias_a_analizar)
        balance = get_solana_balance(user_wallet, endpoint_url)
        firmas = obtener_transacciones(user_wallet, limite=3000, endpoint_url=endpoint_url)
        transactions_summary = {}

        contador_omisiones = 0
        max_omisiones = 6

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_signature = {
                executor.submit(process_transaction, firma.signature, user_wallet, fecha_inicio, endpoint_url): firma
                for firma in firmas
            }
            for future in as_completed(future_to_signature):
                if redis_client.get(f"task_revoked:{self.request.id}"):
                    logger.info(f"Tarea {self.request.id} revocada, terminando ejecución.")
                    for f in future_to_signature:
                        if not f.done():
                            f.cancel()
                    return "Tarea revocada."

                try:
                    transaction = future.result()
                    if transaction == "Transacción omitida por fecha.":
                        contador_omisiones += 1
                        if contador_omisiones > max_omisiones:
                            for f in future_to_signature:
                                if not f.done():
                                    f.cancel()
                            break
                        continue
                    contador_omisiones = 0

                    if transaction:
                        token_name = transaction['token_name']
                        token_mint_address = transaction['mint_address']
                        tran_type = transaction['type']

                        token_key = f"{token_name}_{token_mint_address}"

                        if token_key not in transactions_summary:
                            transactions_summary[token_key] = {
                                'buy_count': 0,
                                'sell_count': 0,
                                'buy': {'sol_amount': 0, 'tokens': 0, 'fee': 0, 'first_date': None, 'last_date': None},
                                'sell': {'sol_amount': 0, 'tokens': 0, 'fee': 0, 'first_date': None, 'last_date': None},
                                'mint_address': token_mint_address
                            }

                        summary = transactions_summary[token_key][tran_type]

                        summary['sol_amount'] += transaction['sol_amount']
                        summary['tokens'] += transaction['amount_tokens']
                        summary['fee'] += transaction['fee']

                        if tran_type == 'buy':
                            if summary['first_date'] is None or transaction['timestamp'] < summary['first_date']:
                                summary['first_date'] = transaction['timestamp']
                        if tran_type == 'sell':
                            if summary['last_date'] is None or transaction['timestamp'] > summary['last_date']:
                                summary['last_date'] = transaction['timestamp']

                        if tran_type == 'buy':
                            transactions_summary[token_key]['buy_count'] += 1
                        elif tran_type == 'sell':
                            transactions_summary[token_key]['sell_count'] += 1
                
                except Exception as e:
                    logger.error(f"Error processing transaction: {e}")
                    continue

        for token, data in transactions_summary.items():
            total_sold_sol = abs(data['sell']['sol_amount'])  # Convierte el SOL de ventas a positivo
            total_bought_sol = data['buy']['sol_amount']  # Ya es positivo, SOL gastado en compras
            profit_sol = total_sold_sol - total_bought_sol  # Calcula el beneficio
            data['profit_sol'] = profit_sol  # Guarda el beneficio en el resumen

        sol_price_usd = get_current_sol_price_usd()
        
        if sol_price_usd is not None:
            for token, data in transactions_summary.items():
                if 'profit_sol' in data:
                    profit_usd = data['profit_sol'] * sol_price_usd  # Calcula el "Profit USD"
                    data['profit_usd'] = profit_usd  # Guarda el "Profit USD" en el resumen del token

        for token, data in transactions_summary.items():
            if 'profit_sol' in data:
                total_bought_sol = data['buy']['sol_amount']
                if total_bought_sol != 0:
                    profit_percentage = (data['profit_sol'] / total_bought_sol) * 100
                else:
                    profit_percentage = 0
                data['profit_percentage'] = profit_percentage

        for token, data in list(transactions_summary.items()):
            if data['buy_count'] == 0 and data['sell_count'] > 0:
                del transactions_summary[token]

        total_profit_sol = 0
        total_profit_usd = 0

        for token, data in transactions_summary.items():
            if 'profit_sol' in data:
                total_profit_sol += data['profit_sol']
                if 'profit_usd' in data:
                    total_profit_usd += data['profit_usd']

        for token, data in list(transactions_summary.items()):
            if data['buy_count'] == 0 and data['sell_count'] > 0:
                del transactions_summary[token]
                
        total_final_sol_used_in_buys = 0
        total_tokens_with_buys = 0

        for token, data in transactions_summary.items():
            if data['buy_count'] > 0:
                total_final_sol_used_in_buys += data['buy']['sol_amount']
                total_tokens_with_buys += 1

        average_final_sol_per_buy = total_final_sol_used_in_buys / total_tokens_with_buys if total_tokens_with_buys > 0 else 0
        
        resultados = {
            'balance_SOL': "{:.2f}".format(float(balance)),
            'total_profit_SOL': total_profit_sol,
            'total_profit_USD': total_profit_usd,
            'average_final_SOL_per_buy': f"{average_final_sol_per_buy:.2f} SOL",
            'tokens_summary': [],
            'formatted_total_profit_SOL': f"{total_profit_sol:.2f}",
            'formatted_total_profit_USD': f"${total_profit_usd:.2f}",
        }

        for token_key, data in transactions_summary.items():
            token_name, token_mint_address = token_key.split('_')
            token_data = {
                'name': token_name,
                'buy_sell': f"{data['buy_count']}/{data['sell_count']}",
                'details': [],
                'profit_SOL': data['profit_sol'] if 'profit_sol' in data else None,
                'profit_USD': data['profit_usd'] if 'profit_usd' in data else None,
                'profit_percentage': data['profit_percentage'] if 'profit_percentage' in data else None,
                'formatted_profit_SOL': f"{data['profit_sol']:.2f}" if 'profit_sol' in data else "N/A",
                'formatted_profit_USD': f"${data['profit_usd']:.2f}" if 'profit_usd' in data else "N/A",
                'formatted_profit_percentage': f"{data['profit_percentage']:.2f}%" if 'profit_percentage' in data else "N/A",
                'chart_url': f"https://dexscreener.com/solana/{data['mint_address']}?maker={user_wallet}" if 'mint_address' in data else "#"
            }

            for tran_type in ['buy', 'sell']:
                if tran_type in data and data[tran_type]['tokens'] != 0:
                    tran_details = {
                        'type': tran_type,
                        'first_date': datetime.fromtimestamp(data[tran_type]['first_date'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + ' UTC' if data[tran_type]['first_date'] is not None else "N/A",
                        'last_date': datetime.fromtimestamp(data[tran_type]['last_date'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + ' UTC' if data[tran_type]['last_date'] is not None else "N/A",
                        'SOL': f"{abs(data[tran_type]['sol_amount']):.2f}",
                        'tokens': f"{abs(data[tran_type]['tokens']):,.2f}",
                        'fee': f"{data[tran_type]['fee']:.6f}"
                    }
                    token_data['details'].append(tran_details)

            resultados['tokens_summary'].append(token_data)

        return resultados
        
    finally:
        try:
            if endpoint_url:
                endpoint_manager.release_endpoint(endpoint_url)
                logger.debug(f"Endpoint released in finally: {endpoint_url}")
                redis_client.delete(f"task_revoked:{self.request.id}")
                logger.debug(f"Revocation mark deleted for Task ID: {self.request.id}")
        except Exception as e:
            logger.error(f"Error releasing endpoint or deleting revocation mark: {e}")