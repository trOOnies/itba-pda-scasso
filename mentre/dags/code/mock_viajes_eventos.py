import logging
import numpy as np
import pandas as pd

from code.utils import random_categories_array
from options import TRIP_END


# * Middle level functions


def get_viajes(path: str) -> pd.DataFrame:
    viajes = pd.read_csv(
        path,
        usecols=(
            ["id"]
            + TRIP_END.ALL_CLOSED
            + ["tiempo_inicio", "duracion_viaje_seg", "tiempo_fin"]
        ),
    )
    assert not viajes.empty, "'viajes' local CSV is empty."

    viajes["last_evento_id"] = pd.from_dummies(
        viajes[TRIP_END.ALL_CLOSED],
        default_category=TRIP_END.ABIERTO,
    )
    viajes = viajes.drop(TRIP_END.ALL_CLOSED, axis=1)
    viajes["last_evento_id"] = (
        viajes["last_evento_id"].map(TRIP_END.TO_EVENTO_ID).astype(int)
    )
    viajes["tiempo_inicio"] = pd.to_datetime(viajes["tiempo_inicio"])
    viajes["tiempo_fin"] = pd.to_datetime(viajes["tiempo_fin"])

    return viajes


def get_eventos_start(viajes: pd.DataFrame) -> pd.DataFrame:
    """Get ride start events."""
    viajes_len = viajes.shape[0]
    assert viajes_len > 0
    df = pd.concat(
        (
            viajes["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.zeros(viajes_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array(viajes_len, TRIP_END.ABIERTO),
                name="evento_id",
            ),
            viajes["tiempo_inicio"].rename("tiempo_evento").reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df


def get_end_canceled_rides(viajes: pd.DataFrame) -> pd.DataFrame:
    """Get ride cancelation events."""
    eventos_not_ok_final = {}
    for event in TRIP_END.ALL_CLOSED_NOT_OK:
        viajes_i = viajes[viajes["last_evento_id"] == TRIP_END.TO_EVENTO_ID[event]]

        viajes_i_len = viajes_i.shape[0]
        if viajes_i_len == 0:
            continue

        eventos_not_ok_final[event] = pd.concat(
            (
                viajes_i["id"].rename("id_viaje").reset_index(drop=True),
                pd.Series(
                    np.ones(viajes_i_len, dtype=int),
                    name="id_ord",
                ),
                pd.Series(
                    TRIP_END.event_array((viajes_i_len,), event),
                    name="evento_id",
                ),
                (
                    viajes_i["tiempo_inicio"]
                    + pd.to_timedelta(
                        np.random.randint(15, 1200, viajes_i_len).reshape((-1,)),
                        unit="sec",
                    )
                )
                .rename("tiempo_evento")
                .reset_index(drop=True),
            ),
            axis=1,
        )
        del viajes_i

    end_canceled_rides = pd.concat(
        list(eventos_not_ok_final.values()),
        axis=0,
    )
    assert not end_canceled_rides.empty
    assert end_canceled_rides.shape[1] == 4
    assert end_canceled_rides.notnull().all(None)
    return end_canceled_rides


def split_viajes_cerrado(viajes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Closed rides: They can either be corrected as closed, or directly closed (the usual)."""
    mask = viajes["last_evento_id"] == TRIP_END.TO_EVENTO_ID[TRIP_END.CERRADO]
    viajes_cerrado = viajes[mask]
    assert not viajes_cerrado.empty, "No closed trips were found."

    mask_corrected_as_cerrado = np.random.random(size=viajes_cerrado.shape[0]) < 0.10
    return (
        viajes_cerrado[mask_corrected_as_cerrado],
        viajes_cerrado[~mask_corrected_as_cerrado],
    )


def get_eventos_uncorrected(viajes_cerrado_corrected: pd.DataFrame) -> pd.DataFrame:
    """Get ride cancelation events that later ended up in closed rides."""
    viajes_cerrado_corrected_len = viajes_cerrado_corrected.shape[0]
    assert viajes_cerrado_corrected_len > 0
    df = pd.concat(
        (
            viajes_cerrado_corrected["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.ones(viajes_cerrado_corrected_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                random_categories_array(
                    viajes_cerrado_corrected_len,
                    {
                        TRIP_END.CANCELADO_USUARIO: 0.25,
                        TRIP_END.CANCELADO_DRIVER: 0.50,
                        TRIP_END.CANCELADO_MENTRE: 0.75,
                        TRIP_END.OTROS: 1.00,
                    },
                    cat_to_id=TRIP_END.TO_EVENTO_ID,
                ),
                name="evento_id",
            ),
            (
                viajes_cerrado_corrected["tiempo_inicio"]
                + pd.to_timedelta(
                    viajes_cerrado_corrected["duracion_viaje_seg"] / 2, unit="sec"
                )
            )
            .rename("tiempo_evento")
            .reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df


def get_eventos_corrected(viajes_cerrado_corrected: pd.DataFrame) -> pd.DataFrame:
    """Get ride end events that are corrections after a ride cancelation."""
    viajes_cerrado_corrected_len = viajes_cerrado_corrected.shape[0]
    assert viajes_cerrado_corrected_len > 0
    df = pd.concat(
        (
            viajes_cerrado_corrected["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.full(viajes_cerrado_corrected_len, 2, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array((viajes_cerrado_corrected_len,), TRIP_END.CERRADO),
                name="evento_id",
            ),
            viajes_cerrado_corrected["tiempo_fin"]
            .rename("tiempo_evento")
            .reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df


def get_eventos_end(viajes_cerrado: pd.DataFrame) -> pd.DataFrame:
    """Get ride end normal events, that is, straight from ride start to ride end
    without any other event.
    """
    viajes_cerrado_len = viajes_cerrado.shape[0]
    assert viajes_cerrado_len > 0
    df = pd.concat(
        (
            viajes_cerrado["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.ones(viajes_cerrado_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array((viajes_cerrado_len,), TRIP_END.CERRADO),
                name="evento_id",
            ),
            viajes_cerrado["tiempo_fin"].rename("tiempo_evento").reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df


# * High level function


def mock_viajes_eventos_hlf(path_viajes: str) -> pd.DataFrame:
    """High level function for the mocking of viajes_eventos."""
    viajes = get_viajes(path_viajes)

    eventos_start = get_eventos_start(viajes)
    eventos_not_ok_final = get_end_canceled_rides(viajes)
    viajes_cerrado_corrected, viajes_cerrado = split_viajes_cerrado(viajes)

    eventos_uncorrected = get_eventos_uncorrected(viajes_cerrado_corrected)
    eventos_corrected = get_eventos_corrected(viajes_cerrado_corrected)

    eventos_end = get_eventos_end(viajes_cerrado)

    eventos = [
        eventos_start,
        eventos_not_ok_final,
        eventos_uncorrected,
        eventos_corrected,
        eventos_end,
    ]
    logging.info(f"Event DataFrames shapes: {[df.shape for df in eventos]}")
    eventos = pd.concat(eventos, axis=0).sort_values("tiempo_evento", ignore_index=True)
    del (
        eventos_start,
        eventos_not_ok_final,
        eventos_uncorrected,
        eventos_corrected,
        eventos_end,
    )

    logging.info("Events formed and ready to be saved and loaded.")
    return eventos
