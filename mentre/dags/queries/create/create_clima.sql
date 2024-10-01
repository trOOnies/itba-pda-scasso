CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".clima (
    id              INTEGER NOT NULL,
    tiempo_trunc_h  TIMESTAMP NOT NULL UNIQUE,
    dt_anio         SMALLINT,
    dt_mes          SMALLINT,
    dt_dia          SMALLINT,
    dt_dow          SMALLINT,
    dt_hora         SMALLINT,
    temperatura_c   SMALLINT,
    clima_id        SMALLINT,  -- despejado, parcialmente nublado, nublado, ventoso, lluvias leves, lluvias fuertes, tormenta electrica, granizo, nevado
    humedad_rel     DECIMAL(5, 4),
    humedad_abs     SMALLINT,
    indice_uv       SMALLINT,
    PRIMARY KEY(id)
);