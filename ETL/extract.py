from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
import os

db_host = os.getenv("host")
db_username = os.getenv("user")
db_password = os.getenv("password")

engine = create_engine(
    f"postgresql://{db_username}:{db_password}@{db_host}:5432/dvdrental"
)


### Note: This database has a flaw, because rental table or payment table one of them should have had a film_id that was rented, now fact table is hard to be created

### but we can only get how much each film made based on the link between film -> inventory -> rental -> payment

def getSqlQuery(query):
    return pd.read_sql_query(query, engine)


def extract():

    # getting all sql tables
    actor = getSqlQuery("SELECT * FROM actor;")
    address = getSqlQuery("SELECT * FROM address;").drop("address2", axis=1)
    category = getSqlQuery("SELECT * FROM category;")
    city = getSqlQuery("SELECT * FROM city;")
    country = getSqlQuery("SELECT * FROM country;")
    customer = getSqlQuery("SELECT * FROM customer;")
    film = getSqlQuery("SELECT * FROM film;")
    film_actor = getSqlQuery("SELECT * FROM film_actor;")
    film_category = getSqlQuery("SELECT * FROM film_category;")
    inventory = getSqlQuery("SELECT * FROM inventory;")
    language = getSqlQuery("SELECT * FROM language;")
    payment = getSqlQuery("SELECT * FROM payment;")
    rental = getSqlQuery("SELECT * FROM rental;")
    staff = getSqlQuery("SELECT * FROM staff;")
    store = getSqlQuery("SELECT * FROM store;")

    ## This code will do the exact same thing:

    # tables = pd.read_sql_query("""SELECT table_name FROM INFORMATION_SCHEMA.TABLES
    # WHERE is_insertable_into = 'YES' AND table_schema = 'public';""", engine)['table_name']

    # for table in tables.values:
    # exec(f"{table} = getSqlQuery(f'SELECT * FROM {table}')")
    # =============================================================

    # tables = pd.read_sql_query("""SELECT table_name FROM INFORMATION_SCHEMA.TABLES
    # WHERE is_insertable_into = 'YES' AND table_schema = 'public';""", engine)['table_name']

    ## or as a dictionary:
    # dictOfTables = {}
    # for table in tables.values:
    # dictOfTables[table] = getSqlQuery(f'SELECT * FROM {table}')
    tableDict = {
        "actor": actor,
        "address": address,
        "category": category,
        "city": city,
        "country": country,
        "customer": customer,
        "film": film,
        "film_actor": film_actor,
        "film_category": film_category,
        "inventory": inventory,
        "language": language,
        "payment": payment,
        "rental": rental,
        "staff": staff,
        "store": store,
    }

    return tableDict


extract()
