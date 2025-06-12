import pandas as pd
import os
from data_fetch import fetch_data

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


add