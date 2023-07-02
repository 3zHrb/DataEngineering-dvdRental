from diagrams import Diagram, Cluster
from diagrams.custom import Custom

from urllib.request import urlretrieve

urlretrieve(
    "https://www.postgresqltutorial.com//wp-content/uploads/2021/04/postgresql-tutorial-homepage.svg",
    "./postgresql.png",
)


with Diagram("DVD Rental Pipeline"):
    with Cluster("Orchestration"):
        airflow = Custom("Airflow", "./AirflowLogo.png")
    with Cluster("ETL", direction="LR"):
        with Cluster("extract"):
            extract = Custom("PostgreSQL", "./postgresql.png")
        with Cluster(label="Transform"):
            transform = Custom("Pandas", "./pandas.png")
        with Cluster(label="Load"):
            load = Custom("BigQuery", "./bigQuery.png")
    with Cluster("Analysis"):
        lookerStudio = Custom("Looker Studio", "./looker_studio.png")
        etl = extract >> transform >> load
        extract - airflow
        airflow - load
        etl >> lookerStudio
