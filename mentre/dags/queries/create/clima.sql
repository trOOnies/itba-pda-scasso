CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".clima (
    id                   INTEGER NOT NULL,
    tiempo_round_h       TIMESTAMP NOT NULL UNIQUE,
    dt_anio              SMALLINT,
    dt_mes               SMALLINT,
    dt_dia               SMALLINT,
    dt_dow               SMALLINT,
    dt_hora              SMALLINT,
    clima_id             SMALLINT,  -- 4: Intermittent Clouds
    humedad_relativa_pp  SMALLINT,
    indice_uv            SMALLINT,
    nubes_pp             SMALLINT,
    temperatura_c        SMALLINT,
    sensacion_termica_c  SMALLINT,
    velocidad_viento_kmh SMALLINT,
    visibilidad_km       SMALLINT,
    presion_mb           SMALLINT,
    precipitacion_mm     SMALLINT,
    PRIMARY KEY(id),
    FOREIGN KEY(clima_id) references "{DB_SCHEMA}".clima_id(id)
);