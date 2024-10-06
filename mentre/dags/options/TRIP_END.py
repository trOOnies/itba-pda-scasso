import numpy as np

ABIERTO           = "abierto"

CERRADO           = "end_cerrado"
CANCELADO_USUARIO = "end_cancelado_usuario"
CANCELADO_DRIVER  = "end_cancelado_driver"
CANCELADO_MENTRE  = "end_cancelado_mentre"
OTROS             = "end_otros"

ALL_CLOSED_NOT_OK = [
    CANCELADO_USUARIO,
    CANCELADO_DRIVER,
    CANCELADO_MENTRE,
    OTROS,
]  # ! maintain same order
ALL_CLOSED = [CERRADO] + ALL_CLOSED_NOT_OK
ALL = [ABIERTO] + ALL_CLOSED

TO_EVENTO_ID = {
    ABIERTO: 0,
    CERRADO: 1,
    CANCELADO_USUARIO: 2,
    CANCELADO_DRIVER: 3,
    CANCELADO_MENTRE: 4,
    OTROS: 999,
}


def event_array(shape, event_id: int) -> np.ndarray:
    return np.full(
        shape,
        TO_EVENTO_ID[event_id],
        dtype=int,
    )
