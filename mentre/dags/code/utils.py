from math import isclose
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
