import json
import numpy as np
import pandas as pd

from code.database_funcs import get_max_id
from options import CLIMA_ID


def parse_accuweather_api_data(data: dict) -> dict:
    """Parse AccuWeather API raw data."""
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


def get_dates(viajes: pd.DataFrame):
    dates = pd.concat(
        (viajes["tiempo_inicio"], viajes["tiempo_fin"][viajes["tiempo_fin"].notnull()]),
        axis=0,
    )
    dates = pd.to_datetime(dates)
    date_min = dates.min().round("60min")
    date_max = dates.max().round("60min")
    del dates
    return date_min, date_max


def mock_temp(df: pd.DataFrame, is_day: bool) -> pd.DataFrame:
    """Mock temperature."""
    df["temperatura_c"] = (
        np.random.uniform(18.0, 35.0, size=df.shape[0])
        if is_day
        else np.random.uniform(10.0, 17.0, size=df.shape[0])
    )
    df["indice_uv"] = (df["temperatura_c"] - 10.0) * (11.0 / 25.0)
    df["indice_uv"] = df["indice_uv"].astype(int) + np.random.randint(-1, 4)  # centered around 1
    df["indice_uv"] = df["indice_uv"].map(lambda v: min(v, 12))
    return df


def mock_cid(df: pd.DataFrame, is_day: bool, clima_id: pd.DataFrame) -> pd.DataFrame:
    """Mock clima_id values.
    Take into account that some clima_id are present both at day and night.
    """
    ids = (
        clima_id["id"][clima_id["dia"]].values
        if is_day
        else clima_id["id"][clima_id["noche"]].values
    )
    cids = np.random.randint(0, ids.size, size=df.shape[0])
    df["clima_id"] = ids[cids]
    return df


def mock_clima_id_dependent(
    df: pd.DataFrame,
    col: str,
    min_val: int | float,
    max_val: int | float,
    ids: list[int],
    is_int: bool,
) -> pd.DataFrame:
    """Mock a clima_id dependent variable.

    NOTE: `max_val` is exclusive for ints, but it uses numpy's `uniform` for floats.
    """
    zero_val = 0 if is_int else 0.0

    df[f"{col}_aux"] = (
        np.random.randint(min_val, max_val, size=df.shape[0])
        if is_int
        else np.random.uniform(min_val, max_val, size=df.shape[0])
    )

    df[col] = df[[f"{col}_aux", "clima_id"]].apply(
        lambda row: row[f"{col}_aux"] if row["clima_id"] in ids else zero_val,
        axis=1,
    )

    df = df.drop(f"{col}_aux", axis=1)
    return df


def mock_humidity(df: pd.DataFrame) -> pd.DataFrame:
    """Mock humidity."""
    col = "humedad_relativa_pp"
    df[f"{col}_rainy"] = np.random.uniform(95.0, 100.0, size=df.shape[0])
    df[f"{col}_not_rainy"] = np.random.uniform(2.0, 95.0, size=df.shape[0])
    df[col] = df[[f"{col}_rainy", f"{col}_not_rainy", "clima_id"]].apply(
        lambda row: (
            row[f"{col}_rainy"]
            if row["clima_id"] in CLIMA_ID.RAINY
            else row[f"{col}_not_rainy"]
        ),
        axis=1,
    )
    df = df.drop([f"{col}_rainy", f"{col}_not_rainy"], axis=1)
    return df


def mock_day_part(df: pd.DataFrame, clima_id: pd.DataFrame, is_day: bool) -> pd.DataFrame:
    """Mock data for day or night."""
    df = mock_temp(df, is_day)
    df = mock_cid(df, is_day, clima_id)

    df = mock_clima_id_dependent(df, "nubes_pp", 5, 101, ids=CLIMA_ID.CLOUDY, is_int=True)
    df = mock_clima_id_dependent(df, "velocidad_viento_kmh", 2.0, 31.0, ids=CLIMA_ID.WINDY, is_int=False)

    df["visibilidad_km"] = df["clima_id"].map(lambda cid: 6.0 if cid in CLIMA_ID.FOG else 16.1)
    df["presion_mb"] = np.random.uniform(900.0, 1100.0, size=df.shape[0])

    df = mock_clima_id_dependent(df, "precipitacion_mm", 2.0, 100.0, ids=CLIMA_ID.RAINY, is_int=False)
    df = mock_humidity(df)

    df["sensacion_termica_c"] = df["temperatura_c"] + ((df["humedad_relativa_pp"] - 65) * (2.0 / 35.0))

    return df


def mock_clima_f(clima_id: pd.DataFrame, viajes: pd.DataFrame) -> pd.DataFrame:
    """Function for mocking clima Redshift data."""
    date_min, date_max = get_dates(viajes)

    df = pd.DataFrame(pd.date_range(date_min, date_max, freq="h", inclusive="both", name="tiempo_round_h"))
    df["is_day"] = df["tiempo_round_h"].dt.hour.map(lambda h: (9 <= h) and (h <= 21))  # eq. to 6 to 18 UTC-3

    df = pd.concat(
        (
            mock_day_part(df[df["is_day"]], clima_id, is_day=True),
            mock_day_part(df[~df["is_day"]], clima_id, is_day=False),
        ),
        axis=0,
    )
    df = df.sort_values("tiempo_round_h", ignore_index=True)
    df = df.drop("is_day", axis=1)

    df["dt_anio"] = df["tiempo_round_h"].dt.year
    df["dt_mes"] = df["tiempo_round_h"].dt.month
    df["dt_dia"] = df["tiempo_round_h"].dt.day
    df["dt_dow"] = df["tiempo_round_h"].dt.dayofweek
    df["dt_hora"] = df["tiempo_round_h"].dt.hour

    return df


def transform_function(data: dict, engine) -> pd.DataFrame:
    """Transform function for the clima hourly ETL."""
    parsed_data = parse_accuweather_api_data(data)
    parsed_data["id"] = get_max_id(engine, table_name="clima") + 1
    df = pd.DataFrame([parsed_data])
    assert df.shape[0] == 1
    return df


# * High level functions


def mock_clima_hlf(path_clima_id: str, path_viajes: str) -> pd.DataFrame:
    """High level function for Airflow's mock_clima task."""
    path_data = "tables/data/acwt_buenos_aires_1.json"
    with open(path_data, "r") as f:
        data = json.load(f)
    data = data[0]

    clima_id = pd.read_csv(path_clima_id, usecols=["id", "dia", "noche"])

    viajes = pd.read_csv(path_viajes, usecols=["tiempo_inicio", "tiempo_fin"])

    df = mock_clima_f(clima_id, viajes)
    df = df.reset_index(drop=False)
    df = df.rename({"index": "id"}, axis=1)
    return df
