import random
import pandas as pd
from prefect import task, flow


# python3 run_prefect.py

@task
def load_data():
    data = pd.read_csv("Prefect/data/original.csv")
    return data


@task
def add_column(data: pd.DataFrame):
    # df = pd.read_csv("../data/original.csv")
    data['random_number'] = random.randint(0, 100)
    data.to_csv("Prefect/data/result_add_column.csv")
    return data['random_number']


@task
def split_url(data: pd.DataFrame):
    # df = pd.read_csv("Prefect/data/original.csv")
    data['domain_of_url'] = data['url'].replace(to_replace='^https?:\/\/', value='', regex=True)
    data.to_csv("Prefect/data/result_split_url.csv")
    return data['domain_of_url']


@task
def merge_second_df(col1, col2):
    data = pd.DataFrame()
    data["col1"] = col1
    data["col2"] = col2
    data.to_csv("Prefect/data/final.csv")


@flow
def meeting_prefect():
    df = load_data()
    column_random = add_column(df)
    column_domain = split_url(df)
    merge_second_df(column_domain, column_random)


def start():
    print(meeting_prefect())