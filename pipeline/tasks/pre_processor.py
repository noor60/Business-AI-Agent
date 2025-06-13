import pandas as pd
import ast



def data_pre_processing():
    customer_data = pd.read_csv("../outputs/customers_data.csv")
    orders = pd.read_csv("../outputs/orders_data.csv")

    print(customer_data.shape)
    print(customer_data.name.nunique())
    print(customer_data.street.nunique())
    print(customer_data.street.value_counts())

    print(customer_data.zip.nunique())
    print(customer_data.zip.value_counts())
    customer_data["zip"] = customer_data["zip"].astype(str).str.replace(",", "")
    customer_data[["name", "street", "district", "city"]] = customer_data[
        ["name", "street", "district", "city"]
    ].apply(lambda col: col.astype(str).str.lower())

    customer_data.isna().sum()
    customer_data.drop(
        columns=[
            'company',
            "original_shipping",
            "weborder",
            "email",
            "floor",
            "street2",
            "firstname",
            "customer_group",
            "customer_number",
        ],
        inplace=True,
    )
    customer_data.to_csv("../outputs/customers_data_processed.csv", index=False)
    orders["order_items_json"].unique()
    df=orders.copy()
   
    # Step 2: Convert string of list-of-dicts into actual Python objects
    df['parsed_items'] = df['order_items_json'].apply(ast.literal_eval)

    # Step 3: Explode the list so each item becomes its own row
    df_exploded = df.explode('parsed_items')

    # Step 4: Extract quantity, item_code, and item_name from the dict
    df_exploded['quantity'] = df_exploded['parsed_items'].apply(lambda x: x['quantity'])
    df_exploded['item_code'] = df_exploded['parsed_items'].apply(lambda x: x['item_code'])
    df_exploded['item_name'] = df_exploded['parsed_items'].apply(lambda x: x['item_name'])
    df_exploded['item_price'] = df_exploded['parsed_items'].apply(lambda x: x['item_price'])
    df_exploded['is_discount'] = df_exploded['parsed_items'].apply(lambda x: x['is_discount'])
    df_exploded['total_price'] = df_exploded['parsed_items'].apply(lambda x: x['total_price'])
    df_exploded['item_description'] = df_exploded['parsed_items'].apply(lambda x: x['item_description'])


    
    df_exploded[["customer_name", "payment_method", "item_name", "item_description"]] = df_exploded[["customer_name", "payment_method", "item_name", "item_description"]].apply(lambda col: col.astype(str).str.lower())


    df_exploded.isna().sum()
    df_exploded.drop(
        columns=[
            "order_items_json",
            "note",
            "note_status",
            "delivery_type",
            'paid_amount',
            'session_id',
            'customer_phone',
            'mytime1',
            'raw_file_name',
        ],
        inplace=True,
    )
    df_exploded.to_csv("../outputs/orders_data_processed.csv", index=False)
    
