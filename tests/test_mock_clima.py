import json
import pandas as pd

from code.mock_clima import parse_accuweather_api_data


class TestMockClima:
    def test_parse_accuweather_api_data(self):
        path = "mentre/dags/tables/data/acwt_buenos_aires_1.json"
        with open(path, "r") as f:
            data = json.load(f)
        data = data[0]

        assert data["WeatherIcon"] == 4
        assert isinstance(data["Temperature"], dict)

        parsed_data = parse_accuweather_api_data(data)

        assert parsed_data == {
            "tiempo_round_h": pd.to_datetime(
                "2024-10-08T16:00:00+00:00",
                utc=True,
            ),
            "clima_id": 4,
            "humedad_relativa_pp": 72,
            "indice_uv": 5,
            "nubes_pp": 70,
            "temperatura_c": 20,
            "sensacion_termica_c": 20.6,
            "velocidad_viento_kmh": 24.1,
            "visibilidad_km": 16.1,
            "presion_mb": 1020,
            "precipitacion_mm": 0,
            "dt_anio": 2024,
            "dt_mes": 10,
            "dt_dia": 8,
            "dt_dow": 1,  # Tuesday
            "dt_hora": 16,
        }
