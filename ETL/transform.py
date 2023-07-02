import pandas as pd

import extract


def transform():
    tableDict = extract.extract()

    actor = tableDict["actor"]
    address = tableDict["address"]
    category = tableDict["category"]
    city = tableDict["city"]
    country = tableDict["country"]
    customer = tableDict["customer"]
    film = tableDict["film"].drop("special_features", axis=1)
    film_actor = tableDict["film_actor"]
    film_category = tableDict["film_category"]
    inventory = tableDict["inventory"]
    language = tableDict["language"]
    payment = tableDict["payment"]
    rental = tableDict["rental"]
    staff = tableDict["staff"]
    store = tableDict["store"]

    # creating film_dim
    film_dim = (
        pd.merge(
            film,
            film_category,
            on="film_id",
            how="left",
            suffixes=("_film", "_filmCategory"),
        )
        .merge(category, on="category_id", how="left")
        .merge(
            language, on="language_id", how="left", suffixes=("_category", "_language")
        )
        .merge(film_actor, on="film_id", how="left")
        .merge(actor, on="actor_id", how="left", suffixes=("_filmActor", "_actor"))
    )

    # creating address_dim
    address_dim = pd.merge(address, city, on="city_id", how="left").merge(
        country, on="country_id", how="left"
    )

    address_dim.rename(
        columns={
            "last_update_x": "last_update_address",
            "last_update_y": "last_update_city",
            "last_update": "last_update_country",
        },
        inplace=True,
    )

    # creating rental_dim
    paymentsUniqueColumns = payment.columns.difference(
        rental.columns[rental.columns != "rental_id"]
    )
    rental_dim = pd.merge(
        rental, payment.loc[:, paymentsUniqueColumns], on="rental_id", how="left"
    )

    # creating inventory_dim
    inventory_dim = pd.merge(
        inventory, store, on="store_id", how="left", suffixes=("_inventory", "_store")
    )

    # create customer_dim
    customer_dim = customer.drop(["active", "last_update", "create_date"], axis=1)
    # customer_dim["create_date"] = pd.to_datetime(customer_dim["create_date"])

    # create staff_dim
    staff_dim = staff.drop("picture", axis=1)

    # creating fact_table
    rentalColumns = rental.columns[~rental.columns.isin(["customer_id", "staff_id"])]
    fact_table = (
        pd.merge(payment, rental[rentalColumns], on="rental_id", how="left")
        .merge(customer, on="customer_id", how="left", suffixes=("", "_customer"))
        .merge(address, on="address_id")
        .merge(store["store_id"], on="store_id")
        .merge(inventory, on="inventory_id", how="left")
        .merge(film, on="film_id", how="left")[
            [
                "rental_id",
                "customer_id",
                "staff_id",
                "film_id",
                "inventory_id",
                "address_id",
                "rental_id",
                "store_id_x",
                "amount",
            ]
        ]
    )
    fact_table.rename(columns={"store_id_x": "store_id"}, inplace=True)
    fact_table = fact_table.loc[:, ~fact_table.columns.duplicated()].copy()

    dictOfDataFrames = {
        "address_dim": address_dim,
        "rental_dim": rental_dim,
        "inventory_dim": inventory_dim,
        "customer_dim": customer_dim,
        "staff_dim": staff_dim,
        "film_dim": film_dim,
        "fact_table": fact_table,
    }

    print(film_dim)
    return dictOfDataFrames


transform()
