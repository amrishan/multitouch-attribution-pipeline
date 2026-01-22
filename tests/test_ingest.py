
import os
import pytest
from unittest.mock import MagicMock, patch
from src.multitouch.ingest import ingest_data, register_bronze_table

@pytest.fixture
def mock_spark():
    return MagicMock()


@patch("src.multitouch.ingest.col")
@patch("src.multitouch.ingest.to_timestamp")
def test_ingest_data_flow(mock_to_timestamp, mock_col, mock_spark):
    # Setup mocks
    mock_schema = MagicMock()
    # Mock schema inference
    mock_spark.read.parquet.return_value.schema = mock_schema
    
    # Mock table creation buffer
    mock_df_writer = MagicMock()
    mock_spark.createDataFrame.return_value.write.format.return_value.mode.return_value = mock_df_writer
    
    # Run
    ingest_data(mock_spark, "dummy_raw", "dummy_bronze", table_name="my_catalog.my_schema.my_table")
    
    # Assert
    # Check that schema was inferred from parquet
    mock_spark.read.parquet.assert_called_with("dummy_raw")
    
    # Check that saveAsTable was called (Managed Table path)
    mock_df_writer.saveAsTable.assert_called_with("my_catalog.my_schema.my_table")
    
    # Check that COPY INTO was executed via spark.sql
    args = mock_spark.sql.call_args[0][0]
    assert "COPY INTO" in args
    assert "delta.`dummy_bronze`" not in args # Should use table name or target logic
    assert "my_catalog.my_schema.my_table" in args

def test_register_bronze_table(mock_spark):
    register_bronze_table(mock_spark, "db_name", "path/to/bronze", reset=True)
    
    # Verify SQL calls
    calls = mock_spark.sql.call_args_list
    assert len(calls) >= 3
    assert "DROP DATABASE" in calls[0][0][0]
    assert "CREATE DATABASE" in calls[1][0][0]
    assert "CREATE TABLE" in calls[2][0][0]
