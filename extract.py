from sqlalchemy import create_engine
import pandas as pd
import os

# db_host = os.environ.get("host")
# db_username = os.environ.get("user")
# db_password = os.environ.get("password")

host = "localhost"
user = "postgres"
password = "Abdulaziz1993"

print(f"db host : {host},  db username: {user},  db password: {password}")


def extract():

    database_access = create_engine(
        f"postgresql://{user}:{password}@{host}:5432/dvdrental"
    )

    # print(
    #     database_access.connect().execute(
    #         """SELECT first_name, last_name FROM actor
    # WHERE last_name LIKE 'G%%';"""
    #     )
    # )

    query = """SELECT first_name, last_name FROM actor
            WHERE last_name LIKE 'G%%';"""

    print(
        pd.read_sql_query(
            "USE dvdrental",
            database_access,
        )
    )

    # query = database_access.connect().execute(
    #     """SELECT first_name, last_name FROM actor
    #         WHERE last_name LIKE 'G%%';"""
    # )
    # rows = query.fetchall()
    # for row in rows:
    #     print(row)


extract()
