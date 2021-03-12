import pandas as pd
import numpy as np

from env import host, user, password

def get_db_url(db_name):
    return f"mysql+pymysql://{user}:{password}@{host}/{db_name}"


def get_data_from_sql():
    query = """
    SELECT customer_id, monthly_charges, tenure, total_charges, phone_service, internet_service_type_id
    FROM customers;
    """
    df = pd.read_sql(query, get_db_url('telco_churn'))
    return df

def wrangle_telco():
    """
    Queries the telco_churn database
    Returns a clean df with six columns:
    customer_id(object), monthly_charges(float), 
    tenure(int), total_charges(float),
    phone_service(object), internet_service_type_id(int)
    """
    df = get_data_from_sql()
    df['tenure'] = df.tenure.replace(0, 1)
    df['total_charges'] = df.total_charges.replace(' ', df.monthly_charges)
    df['total_charges'] = df.total_charges.astype('float')
    return df