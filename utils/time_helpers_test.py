import pytest
from time_helpers import *

FLOAT_EPSILON = 0.00001

def test_epoch_ms_to_datetime():
    epoch_ms = 1742432791940
    assert epoch_ms_to_datetime(epoch_ms) == "2025-03-20 01:06:31.940000"

def test_hours_to_ms():
    hrs = 1.3
    assert abs(hours_to_ms(hrs) - 3600000 * 1.3) < FLOAT_EPSILON

def test_ms_to_hours():
    ms1 = int(3600000 * 1.5)
    assert abs(ms_to_hours(ms1) - 1.5) < FLOAT_EPSILON

    ms2 = int(3600000 * -2.3)
    assert abs(ms_to_hours(ms2) - (-2.3)) < FLOAT_EPSILON


