import pytest
import pandas as pd
from sqlalchemy import text, create_engine
from db import DatabaseConnector, ResultSet

def test_create_engine():
    
    inmemory_env = DatabaseConnector("sqlite:///:memory:")
        
    assert type(inmemory_env).__name__ == "DatabaseConnector"
    assert type(inmemory_env.engine).__name__ == "Engine"
    
def test_execute_query():
    
    inmemory_env = DatabaseConnector("sqlite:///:memory:")
            
    with inmemory_env.engine.connect() as conn:
        # Create a sample table and insert data
        conn.execute(text("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO test_table (name) VALUES ('Alice'), ('Bob')"))

    inmemory_env.execute_query("SELECT * FROM test_table;")

    assert type(inmemory_env.result_set).__name__ == "ResultSet"

def test_as_dataframe():
    inmemory_env = DatabaseConnector("sqlite:///:memory:")

    # Set up the database and insert data
    with inmemory_env.engine.connect() as conn:
        conn.execute(text("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO test_table (name) VALUES ('Alice'), ('Bob')"))
        conn.commit()

    # Run the SELECT query and check if data is fetched correctly
    inmemory_env.execute_query("SELECT * FROM test_table;")
    df = inmemory_env.result_set.as_dataframe()

    assert type(df).__name__ == "DataFrame"
    assert df.columns.tolist() == ['id', 'name']
    assert df['name'].tolist() == ['Alice', 'Bob']
