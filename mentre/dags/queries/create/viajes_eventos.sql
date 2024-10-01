CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".viajes_eventos (
    id_viaje      INTEGER NOT NULL,
    id_ord        SMALLINT NOT NULL,
    evento_id     SMALLINT NOT NULL,  -- abierto, end_cerrado, facturado, end_cancelado_usuario, end_cancelado_driver, end_cancelado_mentre, end_otros
    tiempo_evento TIMESTAMP NOT NULL,
    PRIMARY KEY(id_viaje, id_ord),
    FOREIGN KEY(id_viaje) references "{DB_SCHEMA}".viajes(id)
);