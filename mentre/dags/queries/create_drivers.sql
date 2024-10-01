CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".drivers (
    id                  INTEGER NOT NULL,
    nombre              VARCHAR(100) NOT NULL,
    apellido            VARCHAR(100) NOT NULL,
    genero              VARCHAR(1) NOT NULL,
    fecha_registro      DATE NOT NULL,
    fecha_bloqueo       DATE,
    direccion_provincia VARCHAR(50) NOT NULL,
    direccion_ciudad    VARCHAR(150) NOT NULL,
    direccion_calle     VARCHAR(150) NOT NULL,
    direccion_altura    SMALLINT NOT NULL,
    categoria           VARCHAR(50) NOT NULL,
    PRIMARY KEY(id)
);