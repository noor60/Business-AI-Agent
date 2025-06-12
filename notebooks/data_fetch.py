import pandas as pd
import os
from dotenv import load_dotenv
ghu
from psycopg import connect

# Load environment variables
load_dotenv()

def fetch_data():
    # get url
    db_url = os.getenv("DATABASE_URL")

    print(db_url)

    # with just url  us connect
    conn = connect(db_url)
    print("Connected to the database")

    # Connect and list all table names
    query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
        """

    all_tables = pd.read_sql(query, conn)

    # Connect and list all table names
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
