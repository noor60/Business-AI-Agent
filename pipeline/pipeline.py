
import tasks.data_fetch
import tasks.pre_processor
from prefect import flow



@flow
def data_pipeline():
    customer_data, orders = data_ingestion()
    processed_customer_data, processed_orders_data = data_processer()


if __name__ == "__main__":
    data_pipeline()
