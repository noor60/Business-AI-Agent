import pandas as pd
import os
from dotenv import load_dotenv
from psycopg import connect
from prefect import task


# Load environment variables
load_dotenv()


@task
def fetch_data():
    db_url = os.getenv("DATABASE_URL")

    query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name;
            """

    try:
        conn = connect(db_url)
        print("Connected to the database")

        query = """
            SELECT *
            FROM pos_customers
            """

        customers_data = pd.read_sql(query, conn)
        customers_data.to_csv("../outputs/customers_data.csv", index=False)
        print("Data exported to CSV files successfully.")

        query = """
            SELECT *
            FROM pos_orders
            """

        orders_data = pd.read_sql(query, conn)
        orders_data.to_csv("../outputs/orders_data.csv", index=False)
        print("Data exported to CSV files successfully.")
        return customers_data, orders_data

    except Exception as e:
        print(f"How exceptional! {e}")


@task
def data_ingestion():
    customer_path = (
        Path(__file__).resolve().parents[1] / "outputs" / "customers_data.csv"
    )
    order_path = Path(__file__).resolve().parents[1] / "outputs" / "orders_data.csv"

    if customer_path and order_path:
        customer_data = pd.read_csv(customer_path)
        orders = pd.read_csv(order_path)
        print("Data files already exist. Skipping fetch.")

    else:
        customer_data, customer_data = fetch_data()

    return customer_data, customer_data
