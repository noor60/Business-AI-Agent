import pandas as pd
from tasks.data_fetch import fetch_data
from tasks.pre_processor import data_pre_processing



def data_ingestion():
    # check in data file for the occurance of the file if not fetch it
    try:
        customer_data = pd.read_csv("../outputs/customers_data.csv")
        orders = pd.read_csv("../outputs/orders_data.csv")
        print("Data files already exist. Skipping fetch.")
    except Exception as e:
        print(f"How exceptional! {e}")
        fetch_data()
        customer_data = pd.read_csv("../outputs/customers_data.csv")
        orders = pd.read_csv("../outputs/orders_data.csv")
        print("Data inputed after fetching from DB.")
        
    return customer_data, orders
        
def data_processer(customer_data, orders):
    # check in data file for the occurance of the file if not fetch it
    try:
        customer_data = pd.read_csv("../outputs/customers_data_processed.csv")
        orders_data = pd.read_csv("../outputs/orders_data_processed.csv")
        print("Data files already exist. Skipping process.")
        
    except Exception as e:
        print(f"How exceptional! {e}")
        data_pre_processing()
        orders_data = pd.read_csv("../outputs/orders_data_processed.csv")
        customer_data = pd.read_csv("../outputs/customers_data_processed.csv")
        print("Data processed after fetching from DB.")
        
    return customer_data, orders




def pipline():
    customer_data, orders = data_ingestion()
    customer_data, orders = data_processer(customer_data, orders)
    
    print("Data ingestion and processing completed successfully.")
    print(f"Customer Data Shape: {customer_data.shape}")
    print(f"Orders Data Shape: {orders.shape}")
    
    return customer_data, orders



customer_data, orders=pipline()
