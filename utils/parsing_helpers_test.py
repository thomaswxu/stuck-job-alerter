import pytest
from parsing_helpers import *

def test_get_counts_in_dict_list(): 
    dict_list = {"A": [1], "B": [1, 2, 3], "C": []}
    counts = get_counts_in_dict_list(dict_list)
    assert counts == {'A': 1, 'B': 3, 'C': 0}