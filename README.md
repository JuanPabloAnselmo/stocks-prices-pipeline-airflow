# 📈 Stock Prices Pipeline

Este repositorio contiene un pipeline ETL (Extract, Transform, Load) diseñado para procesar datos financieros de acciones, desde la obtención de datos en bruto hasta el cálculo de métricas y atributos avanzados. Utilizando herramientas como Apache Airflow, Redshift, Docker y APIs financieras, el pipeline automatiza la creación de tablas y la actualización de datos históricos de precios de acciones, facilitando su análisis y manipulación. Además de la creación de un grafico para observar los valores de cada acción.

## 🚀 Características

- **Integración con APIs**: Recupera datos de precios de acciones desde fuentes como Alpha Vantage y Finnhub.
- **Pipeline ETL**: Organizado en tres fases (bronze, silver y gold) para obtener, procesar y enriquecer datos financieros.
- **Creación automática de tablas**: Genera tablas en Amazon Redshift para almacenar los datos de stock.
- **Parquet Storage**: Exporta los datos procesados a archivos Parquet para optimizar el almacenamiento y la lectura.
- **Pruebas unitarias**: Pruebas de linting y dependencias para garantizar la calidad y consistencia del código.
- **Contenerización con Docker**: Configurado para ejecutarse en un entorno Dockerizado, aprovechando Airflow, PostgreSQL y Redis.
- **Gráfico**: Utilizando Streamlit se realiza un grafico para observar el precio de apertura, mayor precio, menor precio, precio de cierre, volumen de cada stock

## 📁 Estructura del Proyecto

```plaintext
.
├── bronze                  # Módulo para extraer y almacenar los datos en bruto
├── silver                  # Módulo para limpiar y transformar los datos
├── gold                    # Módulo para calcular atributos
├── dags                    # Definición de DAGs para Apache Airflow
├── config                  # Configuración del proyecto
├── logs                    # Logs generados por Airflow
├── plugins                 # Plugins personalizados de Airflow
├── tasks                   # Funciones individuales del pipeline
├── tests                   # Pruebas unitarias
├── utils                   # Funciones auxiliares
├── Dockerfile              # Definición de la imagen Docker
├── docker-compose.yaml     # Configuración de Docker Compose
├── requirements.txt        # Dependencias del proyecto
├── `app.py`                # Script para graficar
├── makefile                # Archivo Makefile para automatizar implementación
└── `README.md`             # Documentación del proyecto
```

## Descripción de la Base de Datos
La base de datos está diseñada con el siguiente formato:

#### Tabla de Hechos (fact):

- **daily_stock_prices_table**: Almacena los precios diarios de las acciones, incluyendo los precios de apertura, cierre, máximos, mínimos y volumen de transacciones.

#### Tablas de Dimensiones (dimension):

- **stock_table**: Contiene información estática sobre las acciones, como el símbolo, nombre de la empresa, industria, y otros detalles relevantes.
- **date_table**: Almacena las fechas y metadatos asociados, permitiendo análisis temporales.
  
#### Tabla de Atributos Calculados:

- **atributes_stock_prices_table**: Almacena atributos calculados derivados de la tabla de hechos daily_stock_prices_table, como cambios porcentuales de precios y volatilidad. Aunque esta tabla contiene datos calculados, puede considerarse parte de una capa analítica debido a su dependencia de la tabla de hechos principal

## 🛠️ Instalación y Configuración

### Prerrequisitos
- Docker
- AWS Redshift
- Airflow
- Python

### Paso a Paso

#### 1. Clonar repositorio

```bash
git clone https://github.com/JuanPabloAnselmo/stocks-prices-pipeline-airflow.git
cd stocks-price-pipeline  
```

#### 2. Configurar variables de entorno
Define las variables necesarias en el archivo .env o directamente en config/config.py para la conexión a Redshift y las APIs de stock:

```bash
AIRFLOW_UID=50000 # Colacar siempre mismo valor
API_KEY_ALPHA=your_alpha_vantage_api_key
API_KEY_FINHUB=your_finnhub_api_key
USER_REDSHIFT=your_username
PASSWORD_REDSHIFT=your_password
```

#### 3. Correr Makefile

El Makefile automatiza la creación de directorios necesarios y la configuración del entorno para utilizar Airflow y Streamlit. Incluye comandos para construir las imágenes de Docker si no existen y para levantar los servicios con Docker Compose. 

Simplemente ejecuta el siguiente comando para realizar todas estas tareas de manera secuencial:

```bash
make all
```

#### 4. Acceder a la interfaz de Airflow

Visita http://localhost:8080 en tu navegador. El usuario y la contraseña predeterminados son airflow para ambos.

#### 5. Acceder a la interfaz de Streamlit

Visita http://localhost:8501 en tu navegador. Se vera el grafico con los datos correspondientes a la tabla daily_stock_prices_table

> **Nota**: Los stocks por default se realizaron al azar. Si se quiere la información de algun stock en particular, hay que entrar a utils/config.py y cambiar los valores de la variables STOCKS_SYMBOLS_LIST.

## 📊 Estructura del Pipeline

El pipeline está dividido en tres capas principales, siguiendo el modelo de ETL:

### Bronze Layer:

Obtiene datos en bruto desde las APIs y los almacena en archivos Parquet.
**DAG**: run_bronze

**Scripts:**

- `api_data_downloader.py`: Recupera los precios diarios de una acción específica desde la API de Alpha Vantage para una fecha determinada. Devuelve un DataFrame con los precios de apertura, máximo, mínimo, cierre y volumen, o un DataFrame vacío si no hay datos disponibles. Además se obtiene el perfil de una acción, incluyendo el nombre, industria y otros atributos, desde la API de Finnhub. Devuelve un DataFrame con la información de perfil, o un DataFrame vacío si no se pueden obtener datos.
- `parquet_create.py`:  Crea archivos en formato Parquet para los precios diarios de acciones y los perfiles de las mismas. Recupera los datos de las APIs de Alpha Vantage y Finnhub, y los guarda en archivos Parquet organizados por fecha. Si no se pueden obtener datos válidos, lanza una excepción de Airflow para cancelar la ejecución del DAG.


### Silver Layer:

Limpia y transforma los datos, cargándolos en tablas Redshift.
**DAG**: run_silver

**Scripts:**

- `create_tables.py`: Verifica si las tablas necesarias para el esquema en Redshift (stock_table, date_table, daily_stock_prices_table y atributes_stock_prices_table) ya existen. Si no, las crea. Las tablas almacenan información sobre acciones, fechas, precios diarios y atributos derivados de los precios de las acciones.
- `load_parquet_files.py`:  Carga archivos Parquet de precios diarios de acciones, perfiles de acciones y fechas, actualiza los archivos Silver correspondientes si es necesario, y genera un DataFrame con las fechas. También asegura que los nuevos datos se concatenen con los archivos existentes si ya existen datos previos.
- `table_insert_sql.py`:  Gestiona la tabla stock_table utilizando SCD Tipo 2, lo que implica actualizar registros existentes desactivando el anterior y creando uno nuevo con los cambios, o insertar nuevos registros si no existen. Ademas, actualiza la tabla date_table insertando nuevas fechas solo si estas aún no están presentes. Y por ultimo, actualiza la tabla daily_stock_prices_table insertando nuevos precios de acciones únicamente para fechas más recientes que el último registro existente, evitando así la duplicación de datos y asegurando que solo se añada información nueva y relevante.

### Gold Layer:

Calcula atributos financieros clave y genera tablas finales para análisis avanzados.
**DAG**: run_gold

**Scripts:**

- `calculate_stock_attributes.py`:  Calcula atributos financieros basados en los precios de acciones para una fecha dada y los inserta en la tabla atributes_stock_prices_table

## 🔍 Pruebas

La carpeta `tests` contiene cuatro pruebas:

#### Pruebas de funcionalidad
1. `test_create_daily_stock_prices_table.py`: Evalúa la función que obtiene información de la API.
2. `test_daily_stock_price_dtypes.py`: Verifica si los tipos de datos del parquet en silver corresponden a los requeridos por la tabla `daily_stock_prices_table` en la base de datos.

#### Pruebas de calidad de código
3. `test_dependencies.py`: Comprueba si hay problemas de dependencias.
4. `test_linting.py`: Asegura que el estilo del código cumple con PEP8.

> **Nota**: Las pruebas de calidad de código (3 y 4) se ejecutan automáticamente en cada push o pull request mediante GitHub Actions.

## ✨ Futuras Mejoras

- Notificaciones y alertas automáticas en caso de fallos.
- Optimización de la ingestión de datos históricos con procesamiento distribuido.
