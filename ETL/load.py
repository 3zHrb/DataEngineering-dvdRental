import pandas as pd
import transform
import os

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer

from dotenv import load_dotenv

load_dotenv()


os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/Users/abdulazizalharbi/Downloads/dvdrental-391308-6a9a9c94a55b.json"

project_id = "dvdrental-391308"
database_name = "dvdrental_database"


# snowflake_connection = URL(
#     user = os.getenv('sf_user'),
#     password = os.getenv('sf_password'),
#     account = os.getenv('sf_account'),
#     database = os.getenv('sf_database')
# )

# engine = create_engine()


def load():
    dictOfDataframes = transform.transform()

    for dfName, df in dictOfDataframes.items():
        print(f"loadding df: {dfName}")
        print(df)
        print(df.info())
        df.to_gbq(
            destination_table=f"{database_name}.{dfName}",
            project_id=project_id,
            if_exists="replace",
        )


load()
