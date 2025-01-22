# Built-Ins
import argparse
import pathlib

# Third Party
import pandas as pd
import sqlalchemy


def access_to_df(engine:sqlalchemy.Engine, table_name:str) -> pd.DataFrame:
    """Accesses a table in the database and returns it as a pandas DataFrame.
    
    Parameters
    ----------
    engine : sqlalchemy.Engine
        The engine to use to access the database.
    table_name : str
        The name of the table to access.

    returns
    -------
    pd.DataFrame
        The entire table as a pandas DataFrame.
    """
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)

def process_data(dir: pathlib.Path, scenario: str):
    """Processes the data."""
    for file in dir.glob("*.mdb"):
        url = f"access+pyodbc:///?odbc_connect=DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={file.resolve()}"
        engine = sqlalchemy.create_engine(url)
        
        planning_data = access_to_df(engine, "Planning")
        #connect 
        #insert
        #del planning_data
        print("staop")
    # return df



def build(dir:pathlib.Path, out_path:pathlib.Path):


    process_data(dir, "Core")

