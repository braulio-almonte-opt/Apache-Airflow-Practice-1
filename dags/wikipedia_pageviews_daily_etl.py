from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pendulum
import pandas as pd
from google.cloud import bigquery
import datetime

def queryData():
    client = bigquery.Client(project="mindful-braid-514312-p3")
    query = """
        SELECT
        DATE(datehour) AS event_date,
        wiki,
        title,
        SUM(views) AS total_views
        FROM `bigquery-public-data.wikipedia.pageviews_2026`
        WHERE DATE(datehour) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND wiki IN ('en', 'es')
        GROUP BY 1,2,3
        LIMIT 10;
        """

    query_job = client.query(query=query)
    rows = query_job.result()
    rowsAsDicts = [dict(row) for row in rows]
    df = pd.DataFrame(rowsAsDicts)

    return df

def transformData(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="extract_task")
    df = pd.DataFrame(data, columns=["event_date", "wiki", "title", "total_views"])
    loadDateList = [datetime.datetime.today().strftime("%Y-%m-%d") for i in range(len(df))]
    df.insert(4, "load_date", loadDateList, allow_duplicates=True)
    df.dropna(inplace=True)
    df["wiki"] = df["wiki"].str.lower()
    df["title"] = df["title"].str.strip()

    print(df.to_string())

    # return df

def loadData(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="extract_task")
    df = pd.DataFrame(data, columns=["event_date", "wiki", "title", "total_views", "load_date"])
    
    pass

with DAG(
    'wikipedia_dag',
    description='A simple tutorial DAG',
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 2, 4, tz="EST"),
    catchup=False,
    tags=['example'],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=queryData
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transformData
    )

    # load_task = PythonOperator(
    #     task_id="python_task",
    #     python_callable=loadData,
    # )

    # BigQueryCheckOperator_task = BigQueryCheckOperator(
    # task_id='BigQueryCheckOperator_task',
    # sql,
    # gcp_conn_id='google_cloud_default',
    # use_legacy_sql=True,
    # location=None,
    # impersonation_chain=None,
    # labels=None,
    # deferrable=False,
    # )

extract_task >> transform_task
# extract_task >> transform_task >> load_task >> BigQueryCheckOperator_task 