# Python Data Applications
Alumno: Scasso, Facundo M.

Repositorio del TP para ITBA Python Data Applications.

## Temática seleccionada

Tenemos un negocio de *ridesharing* llamado **Mentre**.
Nuestra base de datos se encuentra en Redshift, y queremos desarrollar un ETL basado en Airflow.

## Utilización del código

Empezaremos por la utilización de código por un tema de eficiencia, pero más abajo en la sección **Composición del repositorio** ahondaremos más en los archivos del proyecto.

### API key para la API pública

https://developer.accuweather.com/accuweather_custom/package/purchase/free/free

- Ingresar los datos que pide AccuWeather.
- Verificar la casilla de correo haciendo click en el link que llega por email.
- Hacer login inmediatamente y configurar una contraseña personal para AccuWeather.
- Crear una app en la sección MY APPS.
- Una vez creada la app, copiar su API key. AccuWeather permite hasta _50 llamadas diarias_ con el plan gratuito.
- Poner esta API key en el archivo `.env` como una variable de entorno de nombre **ACWT_API_KEY**.

### Levantar Airflow

Poner el archivo `.env` -provisto al profesor- en la carpeta `mentre/`.

Luego, ubicarse en dicha carpeta `mentre/` y levantar Airflow mediante los siguientes comandos de Docker Compose:
```bash
cd mentre
docker compose up airflow-init
docker compose up
```

### Airflow dags

Para acceder a Airflow, utilizar el usuario y contraseña provisto al profesor.

Se encontrarán con los siguientes DAGs:
- `create_database`: Crea las tablas del proyecto con sus correspondientes esquemas de tipo de datos en el schema `DB_SCHEMA` del archivo `.env`.
- `drop_database`: Elimina completamente las tablas del proyecto.
- `mock_data_redshift`: Crea de forma aleatoria la información falsa _(mock)_ del proyecto, tomando hipótesis varias para dicha creación, de tal forma que haya correspondencia y cierta correlación entre las variables de las tablas.
    - Este DAG está preparado para detectar si ya existe una tabla (else error) y si está vacía (else saltea todos los cálculos del task correspondiente).
- `get_clima` (c/hora): Llama a la API de AccuWeather para pedir el detalle meteorológico actual de la Ciudad Autónoma de Buenos Aires (Argentina), lo transforma a tabla, filtra las columnas necesarias y lo sube a Redshift.

El camino usual es:
1. `create_database`
2. `mock_data_redshift`

Para el desarrollador, ante cualquier inconveniente que no pueda ser resuelto por debuguear `mock_data_redshift`, usar `drop_database` y volver a correr los DAGs 1 y 2.

## Stack tecnológico

- 🐍 Python
    - airflow
    - pytest
    - requests (para la API de AccuWeather)
    - sqlalchemy (para el cluster de RedShift provisto por la universidad)
- 🐋 Docker & Docker Compose
- 🐙 GitHub Actions

## Composición del repositorio

### Airflow

...

### Tests

...

# GitHub Actions

...

## Recursos utilizados

- [Spanish Names](https://github.com/marcboquet/spanish-names)
