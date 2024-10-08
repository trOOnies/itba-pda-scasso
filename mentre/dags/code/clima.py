import pandas as pd


def parse_accuweather_api_data(data: dict) -> dict:
    new_data = {
        "tiempo_round_h": pd.to_datetime(
            data["LocalObservationDateTime"],
            utc=True,
        ).round("60min"),
        "clima_id":             data["WeatherIcon"],
        "humedad_relativa_pp":  data["RelativeHumidity"],
        "indice_uv":            data["UVIndex"],
        "nubes_pp":             data["CloudCover"],
        "temperatura_c":        data["Temperature"]["Metric"]["Value"],
        "sensacion_termica_c":  data["RealFeelTemperature"]["Metric"]["Value"],
        "velocidad_viento_kmh": data["Wind"]["Speed"]["Metric"]["Value"],
        "visibilidad_km":       data["Visibility"]["Metric"]["Value"],
        "presion_mb":           data["Pressure"]["Metric"]["Value"],
        "precipitacion_mm":     data["Precip1hr"]["Metric"]["Value"],
    }
    new_data.update(
        {
            "dt_anio": new_data["tiempo_round_h"].year,
            "dt_mes": new_data["tiempo_round_h"].month,
            "dt_dia": new_data["tiempo_round_h"].day,
            "dt_dow": new_data["tiempo_round_h"].dayofweek,
            "dt_hora": new_data["tiempo_round_h"].hour,
        }
    )
    return new_data
