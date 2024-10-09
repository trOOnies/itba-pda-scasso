import numpy as np
import pandas as pd

from code.mock_people import get_mask_man


class TestMockPeople:
    def test_get_mask_man(self):
        gender_ser = np.hstack(
            (
                np.full(10_000, "M", dtype=str),
                np.full(10_000, "F", dtype=str),
                np.full(10_000, "X", dtype=str),
            )
        )
        assert gender_ser.shape == (30_000,)
        gender_ser = pd.Series(gender_ser, name="gender")

        assert get_mask_man(gender_ser, 1.00).sum() == 20_000
        assert get_mask_man(gender_ser, 0.00).sum() == 10_000
        assert (
            get_mask_man(gender_ser, 0.001).sum() < get_mask_man(gender_ser, 0.999).sum()
        )
        mask_man_sum = get_mask_man(gender_ser, 0.50).sum()
        assert (10_000 < mask_man_sum) and (mask_man_sum < 20_000)
