CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".usuarios (
    id             INTEGER NOT NULL,
    nombre         VARCHAR(100) NOT NULL,
    apellido       VARCHAR(100) NOT NULL,
    genero         VARCHAR(1) NOT NULL,
    fecha_registro DATE NOT NULL,
    fecha_bloqueo  DATE,
    tipo_premium   VARCHAR(50) NOT NULL,
    PRIMARY KEY(id)
);