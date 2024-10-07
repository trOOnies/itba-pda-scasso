import numpy as np
from typing import Any


def random_categories_array(
    size: int,
    cat_dict: dict[Any, float],
    cat_to_id: dict[Any, int] | None,
) -> np.ndarray:
    assert size > 0
    cats = (
        np.array(list(cat_dict.keys()))
        if cat_to_id is None
        else np.array([cat_to_id[cat] for cat in cat_dict.keys()], dtype=int)
    )

    ths = np.array(list(cat_dict.values()), dtype=float)

    arr = np.random.random(size)
    cats_ixs = np.searchsorted(ths, arr)

    return cats[cats_ixs]
