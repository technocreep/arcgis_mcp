FROM python:3.11-slim

# Установка системных зависимостей для GDAL/Geopandas
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Настройка переменных окружения для GDAL
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

WORKDIR /app

# Копируем requirements
COPY requirements.txt .

# Установка Python зависимостей
# Сначала numpy, так как он часто нужен для сборки других пакетов
RUN pip install --no-cache-dir numpy
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код проекта
COPY arcgis_mcp/ ./arcgis_mcp/

# Создаем директорию для проектов
RUN mkdir -p /app/projects

# Переменные окружения
ENV PYTHONPATH=/app

CMD ["uvicorn", "arcgis_mcp.ingestion.app:app", "--host", "0.0.0.0", "--port", "8000"]