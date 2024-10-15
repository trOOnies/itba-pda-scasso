import json
import pandas as pd

from code.database_funcs import get_max_id


def parse_accuweather_api_data(data: dict) -> dict:
    new_data = {
        "tiempo_round_h": pd.to_datetime(
            data["LocalObservationDateTime"],
            utc=True,
        ).round("60min"),
        "clima_id": data["WeatherIcon"],
        "humedad_relativa_pp": data["RelativeHumidity"],
        "indice_uv": data["UVIndex"],
        "nubes_pp": data["CloudCover"],
        "temperatura_c": data["Temperature"]["Metric"]["Value"],
        "sensacion_termica_c": data["RealFeelTemperature"]["Metric"]["Value"],
        "velocidad_viento_kmh": data["Wind"]["Speed"]["Metric"]["Value"],
        "visibilidad_km": data["Visibility"]["Metric"]["Value"],
        "presion_mb": data["Pressure"]["Metric"]["Value"],
        "precipitacion_mm": data["Precip1hr"]["Metric"]["Value"],
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


def mock_clima_hlf(path_clima_id: str) -> pd.DataFrame:
    path_data = "tables/data/acwt_buenos_aires_1.json"
    with open(path_data, "r") as f:
        data = json.load(f)
    data = data[0]

    parsed_data = parse_accuweather_api_data(data)
    df = pd.DataFrame({k: 100 * [v] for k, v in parsed_data.items()})
    df = df.reset_index(drop=False)
    df = df.rename({"index": "id"}, axis=1)
    return df


def transform_function(data: dict, engine) -> pd.DataFrame:
    """Transform function for the clima hourly ETL."""
    parsed_data = parse_accuweather_api_data(data)
    parsed_data["id"] = get_max_id(engine, table_name="clima") + 1
    df = pd.DataFrame([parsed_data])
    assert df.shape[0] == 1
    return df
