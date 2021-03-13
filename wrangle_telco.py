import pandas as pd
import numpy as np

from env import host, user, password

def get_db_url(db_name):
    return f"mysql+pymysql://{user}:{password}@{host}/{db_name}"


def get_data_from_sql():
    query = """
    SELECT c.*, ist.internet_service_type, ct.contract_type, pt.payment_type
    FROM customers as c
    JOIN internet_service_types as ist USING(internet_service_type_id)
    JOIN contract_types as ct USING(contract_type_id)
    JOIN payment_types as pt USING(payment_type_id);
    """
    df = pd.read_sql(query, get_db_url('telco_churn'))
    return df

def wrangle_telco():
    """
    Queries the telco_churn database
    Returns a df with 24 columns and conversion of service columns to 0,1 binary cols.
    """
    df = get_data_from_sql()
    df['tenure'] = df.tenure.replace(0, 1)
    df['churn'] = df.churn.replace({'Yes': 1, 'No': 0})
    df['partner'] = df.partner.replace({'Yes': 1, 'No': 0})
    df['dependents'] = df.dependents.replace({'Yes': 1, 'No': 0})
    df['paperless_billing'] = df.paperless_billing.replace({'Yes': 1, 'No': 0})
    df['phone_service'] = df.phone_service.replace({'Yes': 1, 'No': 0})
    df['multiple_lines'] = df.multiple_lines.replace({'Yes': 1, 'No': 0, 'No phone service': 0})
    df['online_security'] = df.online_security.replace({'Yes': 1, 'No': 0, 'No internet service': 0})
    df['streaming_movies'] = df.streaming_movies.replace({'Yes': 1, 'No': 0, 'No internet service': 0})
    df['streaming_tv'] = df.streaming_tv.replace({'Yes': 1, 'No': 0, 'No internet service': 0})
    df['online_backup'] = df.online_backup.replace({'Yes': 1, 'No': 0, 'No internet service': 0})
    df['device_protection'] = df.device_protection.replace({'Yes': 1, 'No': 0, 'No internet service': 0})
    df['tech_support'] = df.tech_support.replace({'Yes': 1, 'No': 0, 'No internet service': 0})
    df['total_charges'] = df.total_charges.replace(' ', df.monthly_charges)
    df['total_charges'] = df.total_charges.astype('float')
    df['is_autopay'] = df.payment_type.str.contains('automatic')
    return df