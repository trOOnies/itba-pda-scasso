CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".viajes (
    id         INTEGER NOT NULL,
    driver_id  INTEGER NOT NULL,
    usuario_id INTEGER NOT NULL,
    origen_lat       DECIMAL(10, 6) NOT NULL,
    origen_long      DECIMAL(10, 6) NOT NULL,
    destino_lat      DECIMAL(10, 6) NOT NULL,
    destino_long     DECIMAL(10, 6) NOT NULL,
    distancia_metros INTEGER NOT NULL,
    end_cerrado           BOOLEAN NOT NULL,
    end_cancelado_usuario BOOLEAN NOT NULL,
    end_cancelado_driver  BOOLEAN NOT NULL,
    end_cancelado_mentre  BOOLEAN NOT NULL,
    end_otros             BOOLEAN NOT NULL,
    fue_facturado         BOOLEAN NOT NULL,
    tiempo_inicio         TIMESTAMP NOT NULL,
    tiempo_fin            TIMESTAMP,
    duracion_viaje_seg    SMALLINT,
    precio_bruto_usuario DECIMAL(10, 2),
    descuento            DECIMAL(10, 2),
    precio_neto_usuario  DECIMAL(10, 2),
    comision_driver      DECIMAL(10, 2),
    margen_mentre        DECIMAL(10, 2),
    PRIMARY KEY(id)
);