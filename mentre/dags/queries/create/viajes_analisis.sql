CREATE TABLE "{DB_SCHEMA}".viajes_analisis
AS 
WITH t_0 AS (
    SELECT
        c.id as clima_row_id,
        tiempo_round_h,
        dt_anio,
        dt_mes,
        dt_dia,
        dt_dow,
        dt_hora,
        clima_id,
        temperatura_c,
        sensacion_termica_c,
        precipitacion_mm,
        count(1) as total_viajes,
        sum(distancia_metros) as total_distancia_metros,
        sum(margen_mentre) as total_margen_mentre,
        sum(precio_neto_usuario) as total_recaudacion
    FROM "{DB_SCHEMA}".clima c LEFT JOIN "{DB_SCHEMA}".viajes v
        ON c.tiempo_round_h = date_trunc('hour', v.tiempo_inicio)
    GROUP BY
        clima_row_id,
        tiempo_round_h,
        dt_anio,
        dt_mes,
        dt_dia,
        dt_dow,
        dt_hora,
        clima_id,
        temperatura_c,
        sensacion_termica_c,
        precipitacion_mm
)
SELECT
    clima_row_id,
    tiempo_round_h,
    dt_anio,
    dt_mes,
    dt_dia,
    dt_dow,
    dt_hora,
    total_viajes,
    total_distancia_metros,
    case
        when total_distancia_metros > 0
        then cast(total_viajes as float) / total_distancia_metros
        else 0.0
    end as distancia_metros_prom,
    total_margen_mentre,
    case
        when total_recaudacion > 0
        then 100.0 * total_margen_mentre / total_recaudacion
        else 0.0
    end as margen_mentre_pp,
    clima_id,
    temperatura_c,
    sensacion_termica_c,
    precipitacion_mm
FROM t_0
ORDER BY clima_row_id;