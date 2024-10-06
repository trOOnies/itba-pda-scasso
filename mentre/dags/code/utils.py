from math import isclose
import numpy as np
from random import random
from typing import Any


def random_category(cat_dict: dict[Any, float]) -> Any:
    """Random category based on cummulative probabilities.

    NOTE: Validation has to be done before using this function.
    """
    rand_val = random()

    for cat, cumm_proba in cat_dict.items():
        if rand_val < cumm_proba:
            return cat

    if isclose(rand_val, 1.00):
        return list(cat_dict.values())[-1]

    raise ValueError("Cummulative probabilites are wrong.")


def random_categories_array(
    size: int,
    cat_dict: dict[Any, float],
    cat_to_id: dict | None,
) -> np.ndarray:
    assert size > 0
    cats = (
        np.array(list(cat_dict.keys()), dtype=int)
        if cat_to_id is None
        else np.array([cat_to_id[cat] for cat in cat_dict.keys()], dtype=int)
    )

    ths = np.array(list(cat_dict.values()), dtype=float)

    arr = np.random.random(size)
    cats_ixs = np.searchsorted(ths, arr)

    return cats[cats_ixs]
