# Python Data Applications
Alumno: Scasso, Facundo M.

Repositorio del TP para ITBA Python Data Applications.

## Temática seleccionada

Tenemos un negocio de *ridesharing* llamado **Mentre**.
Nuestra base de datos se encuentra en Redshift, y queremos desarrollar un ETL basado en Airflow.

## Utilización del código

Empezaremos por la utilización de código por un tema de eficiencia, pero más abajo en la sección **Composición del repositorio** ahondaremos más en los archivos del proyecto.

### Seteos adicionales para la exploración de código

Para la exploración de código y un correcto linting en VSCode, deberá crearse el _virtual environment_ con los siguientes comandos:

```bash
make venv
source .venv/bin/activate  # o activarlo mediante el pop-up de VSCode
make install
```

### API key para la API pública

https://developer.accuweather.com/accuweather_custom/package/purchase/free/free

1. Ingresar los datos que pide AccuWeather.
2. Verificar la casilla de correo haciendo click en el link que llega por email.
3. Hacer login inmediatamente y configurar una contraseña personal para AccuWeather.
4. Crear una app en la sección MY APPS.
5. Una vez creada la app, copiar su API key. AccuWeather permite hasta _50 llamadas diarias_ con el plan gratuito.
6. Poner esta API key en el archivo `.env` como una variable de entorno de nombre **ACWT_API_KEY**.

### Levantar Airflow

Para levantar Airflow se necesita utilizar Docker. Para Windows es necesario contar con WSL y Docker Desktop abierto y corriendo.

Poner el archivo `.env` -provisto al profesor- en la carpeta `mentre/`.

Luego, ubicarse en dicha carpeta `mentre/` y levantar Airflow mediante los siguientes comandos de Docker Compose:
```bash
cd mentre
docker compose up airflow-init
docker compose up
```

### Airflow DAGs

Para acceder a Airflow, ir a http://localhost:8080/home desde un explorador y utilizar el usuario y contraseña provisto al profesor.

Se encontrarán con los siguientes DAGs:
- `create_database`: Crea las tablas del proyecto con sus correspondientes esquemas de tipo de datos en el schema `DB_SCHEMA` del archivo `.env`.
- `drop_database`: Elimina completamente las tablas del proyecto.
- `mock_data_redshift`: Crea de forma aleatoria la información falsa _(mock)_ del proyecto, tomando hipótesis varias para dicha creación, de tal forma que haya correspondencia y cierta correlación entre las variables de las tablas. Este DAG está preparado para detectar 2 situaciones de interés:
    - Si no existe una tabla requerida, levanta un error.
    - Si existe la tabla requerieda pero no está vacía, saltea todos los cálculos del task correspondiente.
- `get_clima` (c/hora): Llama a la API de AccuWeather para pedir el detalle meteorológico actual de la Ciudad Autónoma de Buenos Aires (Argentina), lo transforma a tabla, filtra las columnas necesarias y lo sube a Redshift.

El camino usual es:
1. `create_database`
2. `mock_data_redshift`

Para el desarrollador: ante cualquier inconveniente que no pueda ser resuelto por debuguear `mock_data_redshift`, usar `drop_database` y volver a correr los DAGs 1 y 2.

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

Nuestra implementación de Airflow es mediante Docker Compose, habiendo partido del [archivo oficial](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) que provee Airflow.

La carpeta principal `mentre/` contiene todos los archivos relevantes a Airflow, véase:
- Las carpetas `config`, `dags`, `logs` y `plugins`
- El archivo `docker-compose.yaml`
- El archivo `Dockerfile` (que es tomado por Docker Compose para levantar la imagen de Mentre)
- Los archivos de requerimientos para Python `requirements_test.txt` y `requirements.txt`

La totalidad del código de Airflow se encuentra en la carpeta `dags`. Los DAGs están sueltos en dicha carpeta, y contamos con varias carpetas más:
- `tasks`: Contiene algunas de las funciones utilizadas en los tasks para los DAGs.
- `code`: El resto de código Python, separado por competencias.
- `options`: Archivos útiles para el correcto uso de valores del proyecto, por ejemplo, strings conocidos. Reduce typos al escribir y referenciar código.
- `tables`: Información estática del proyecto, donde residen las tablas que no dependen de la aleatoriedad.
- `mock`: Archivos útiles para la creación aleatoria de datos falsos _(mock)_.
- `queries`: Queries para su utilización en el código mediante sqlalchemy. Se fuerza su utilización mediante funciones centralizadas, con el objetivo de estandarizar las llamadas a la base de datos. Caso contrario, podrían generarse errores que afectarían la base de forma permanente, o incluso abrir la puerta a casos maliciosos como lo puede ser la inyección de código SQL.
- `local`: Donde residen los archivos de forma local que generan las tasks de nuestros DAGs de Airflow. Se utiliza para levantar resultados intermedios en tasks posteriores, pasando únicamente los caminos _(paths)_ de los archivos y no el objeto en sí.

### Tests

La carpeta `tests` contiene los tests del proyecto. Adicionalmente, tiene un archivo especial llamado `conftest.py` que lo utilizamos para que los imports relativos del proyecto se comporten de manera equivalente a como lo hacen en Airflow, para que no se genere un error al correr los tests.

Para correr los tests puede utilizarse el siguiente comando:
```bash
make test
```

No obstante, en el siguiente ítem veremos su utilización automática, sin necesidad de correr el comando de forma manual.

### GitHub Actions

El proyecto cuenta con 2 workflows para GitHub Actions, localizados en la carpeta `.github/workflows/`.

#### `test.yaml`

Setea todo lo necesario en un Ubuntu con Python 3.10, y prueba correr tanto ruff (linting) como pytest (testing). El build en sí y estos 2 checks deben correrse exitosamente para considerar la corrida del workflow como exitosa.

El check se hace tanto en PR a develop y main, como cuando ya está hecho el merge (que cuenta como un push). Esto representa un flujo normal en el ámbito laboral, ya que se suele "duplicar" este check antes y después de mergear para minimizar la cantidad de errores y estar seguros de que el código que llega a develop y a main cumple nuestros estándares de calidad.

#### `enforcer.yaml`

Este workflow adicional sólo corre cuando se hace un PR a main. Genera un status check que falla si se intenta hacer un PR desde una branch que no sea develop.

## Recursos utilizados

- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Starter Workflows](https://github.com/actions/starter-workflows)
- [Spanish Names](https://github.com/marcboquet/spanish-names)

## Licencia

Todo el código desarrollado se encuentra bajo la licencia **GPL-3.0**. Pueden encontrar su contenido en el archivo `LICENSE`, pero recomendamos leer el resumen de la misma [aquí](https://choosealicense.com/licenses/gpl-3.0/).

Disclaimer: Este es un proyecto personal, para el curso de ITBA Python Data Applications y en categoría de alumno.
