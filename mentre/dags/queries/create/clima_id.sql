CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".clima_id (
    id SMALLINT NOT NULL,
    dia BOOLEAN NOT NULL,
    noche BOOLEAN NOT NULL,
    texto VARCHAR(50) NOT NULL,
    PRIMARY KEY(id)
);