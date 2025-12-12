
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
    mock_spark.read.csv.return_value.schema = mock_schema
    
    mock_df = MagicMock()
    # Mock the chain for readStream
    mock_reader = mock_spark.readStream.format.return_value
    for _ in range(5): # 5 options
        mock_reader = mock_reader.option.return_value
    
    mock_reader.schema.return_value.load.return_value = mock_df
    
    # Mock transform
    mock_transformed_df = MagicMock()
    # Mock chain: df.withColumn().withColumn()
    mock_df.withColumn.return_value.withColumn.return_value = mock_transformed_df
    
    # Run
    ingest_data(mock_spark, "dummy_raw", "dummy_bronze")
    
    # Assert
    mock_spark.readStream.format.assert_called_with("cloudFiles")
    mock_spark.read.csv.assert_called_with("dummy_raw", header=True)
    # Check write stream started
    mock_transformed_df.writeStream.format.assert_called_with("delta")

def test_register_bronze_table(mock_spark):
    register_bronze_table(mock_spark, "db_name", "path/to/bronze", reset=True)
    
    # Verify SQL calls
    calls = mock_spark.sql.call_args_list
    assert len(calls) >= 3
    assert "DROP DATABASE" in calls[0][0][0]
    assert "CREATE DATABASE" in calls[1][0][0]
    assert "CREATE TABLE" in calls[2][0][0]
