# Usa una imagen base de Python
FROM apache/airflow:2.10.1

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia todos los archivos y carpetas del directorio actual al contenedor
COPY . /app

# Crea un entorno virtual y activa el entorno
RUN python -m venv venv

# Agrega el entorno virtual al PATH
ENV PATH="/app/venv/bin:$PATH"

# Establece el PYTHONPATH
ENV PYTHONPATH=/app

# Instala las dependencias del archivo requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

