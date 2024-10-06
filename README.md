# Python Data Applications
Alumno: Scasso, Facundo M.

Repositorio del TP para ITBA Python Data Applications.

## Temática seleccionada

Tenemos un negocio de *ridesharing* llamado **Mentre**.
Nuestra base de datos se encuentra en Redshift, y queremos desarrollar un ETL basado en Airflow.

## Utilización del código

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

...

## Stack tecnológico

- Docker & Docker Compose
- Python
    - Airflow
    - Pytest
    - SQLAlchemy

## Composición del repositorio

...

## Recursos utilizados

- [Spanish Names](https://github.com/marcboquet/spanish-names)
