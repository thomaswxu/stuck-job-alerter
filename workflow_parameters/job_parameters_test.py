import pytest
from job_parameters import JobParams

def test_parse_workspaces():
    assert JobParams.parse_workspaces("[workspace1, workspace2, workspace3]") \
        == ['workspace1', 'workspace2', 'workspace3']
    assert JobParams.parse_workspaces("[]") \
        == []
    assert JobParams.parse_workspaces("[,,]") \
        == []

def test_parse_secret_names():
    assert JobParams.parse_secret_names("[secret1, secret2, secret3]") \
        == ['secret1', 'secret2', 'secret3']
    assert JobParams.parse_secret_names("[]") \
        == []
    assert JobParams.parse_secret_names("[,,]") \
        == []
    assert JobParams.parse_secret_names(" [token]") \
        == ["token"]
    assert JobParams.parse_secret_names("[token]  ") \
        == ["token"]
    assert JobParams.parse_secret_names("     [token]  ") \
        == ["token"]
    assert JobParams.parse_secret_names("") \
        == []
    assert JobParams.parse_secret_names("[]") \
        == []