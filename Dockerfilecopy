# Usa una imagen base de Python
FROM python:3.12.1

# Establece el directorio de trabajo en el contenedor
WORKDIR /solanalyzer

# Copia los archivos de requisitos primero para aprovechar la caché de la capa de Docker
COPY requirements.txt ./

# Instala las dependencias del sistema y limpia archivos temporales
RUN apt-get update && apt-get install -y sqlite3 && rm -rf /var/lib/apt/lists/*

# Instala las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install eventlet

# Copia el resto del código fuente del proyecto al contenedor
COPY . .

# Establece la variable de entorno para Flask
ENV FLASK_APP=analyzer.py
ENV FLASK_RUN_HOST=0.0.0.0

# Indicar el puerto que va a exponer el contenedor
EXPOSE 5000

# Copia el script de entrada para aplicar migraciones
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Comando para ejecutar el script de entrada
ENTRYPOINT ["/entrypoint.sh"]

# Comando para ejecutar la aplicación
CMD ["python", "./analyzer.py"]
