"""Utils functions."""

from functools import wraps
import logging
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


def start_end_log(task_name: str):
    """Decorator for logging corresponding [START] and [END] in Airflow."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logging.info(f"[START] {task_name}")
            result = func(*args, **kwargs)
            logging.info(f"[END] {task_name}")
            return result
        return wrapper
    return decorator
