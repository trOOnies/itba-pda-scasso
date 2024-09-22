CREATE TABLE drivers (
    id                  SERIAL PRIMARY KEY,
    nombre              VARCHAR(100) NOT NULL,
    apellido            VARCHAR(100) NOT NULL,
    genero              VARCHAR(1) NOT NULL,
    fecha_registro      DATE NOT NULL,
    fecha_bloqueo       DATE,
    direccion_provincia VARCHAR(50) NOT NULL,
    direccion_ciudad    VARCHAR(150) NOT NULL,
    direccion_calle     VARCHAR(150) NOT NULL,
    direccion_altura    SMALLINT NOT NULL,
    categoria           VARCHAR(50) NOT NULL
);

CREATE TABLE usuarios (
    id             SERIAL PRIMARY KEY,
    nombre         VARCHAR(100) NOT NULL,
    apellido       VARCHAR(100) NOT NULL,
    genero         VARCHAR(1) NOT NULL,
    fecha_registro DATE NOT NULL,
    fecha_bloqueo  DATE,
    tipo_premium   VARCHAR(50) NOT NULL
);

CREATE TABLE viajes_eventos (
    id            SERIAL PRIMARY KEY,
    id_viaje      SERIAL PRIMARY KEY,
    id_evento     SMALLINT NOT NULL,  -- abierto, end_cerrado, facturado, end_cancelado_usuario, end_cancelado_driver, end_cancelado_mentre, end_otros
    tiempo_evento TIMESTAMP NOT NULL
);

CREATE TABLE viajes (
    id_viaje   SERIAL NOT NULL PRIMARY KEY,
    id_driver  SERIAL NOT NULL,
    id_usuario SERIAL NOT NULL,
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
    margen_mentre        DECIMAL(10, 2)
);

CREATE TABLE clima (
    id              SERIAL PRIMARY KEY,
    tiempo_trunc_h  TIMESTAMP NOT NULL UNIQUE,
    dt_anio         SMALLINT,
    dt_mes          SMALLINT,
    dt_dia          SMALLINT,
    dt_dow          SMALLINT,
    dt_hora         SMALLINT,
    temperatura_c   SMALLINT,
    id_clima        SMALLINT,  -- despejado, parcialmente nublado, nublado, ventoso, lluvias leves, lluvias fuertes, tormenta electrica, granizo, nevado
    humedad_rel     DECIMAL(5, 4),
    humedad_abs     SMALLINT,
    indice_uv       SMALLINT
);
