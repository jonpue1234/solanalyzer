#!/bin/sh

# Verificar si la carpeta de migraciones existe, si no, inicializarla
if [ ! -d "migrations" ]; then
  flask db init
fi

# Ejecutar las migraciones de la base de datos
flask db upgrade

# Iniciar la aplicación
exec "$@"
