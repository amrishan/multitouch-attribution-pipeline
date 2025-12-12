
import pytest
from unittest.mock import patch, mock_open
from src.multitouch.data_generation import generate_synthetic_data

def test_generate_synthetic_data():
    # Patch builtins.open to avoid actual file IO
    with patch("builtins.open", mock_open()) as mocked_file:
        # Patch random to be deterministic if needed, but for now just checking it runs
        generate_synthetic_data("dummy_path.csv", unique_id_count=5)
        
        # Check file opened
        mocked_file.assert_called_with("dummy_path.csv", "w")
        
        # Check some writes happened
        handle = mocked_file()
        # Expect header write
        handle.write.assert_any_call("uid,time,interaction,channel,conversion\n")
        # Expect some data writes
        assert handle.write.call_count > 1
