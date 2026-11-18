from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pendulum
import pandas as pd
from google.cloud import bigquery
import datetime
import dotenv
import os

dotenv.load_dotenv()
projectID = os.getenv("GOOGLE_CLOUD_PROJECT_ID")

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
    df.dropna(inplace=True) #running this first ensures that loadDateList iterates less times
    loadDateList = [datetime.datetime.today().strftime("%Y-%m-%d") for i in range(len(df))]
    # df["event_date"] = df["event_date"].astype(str)
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    df.insert(4, "load_date", loadDateList, allow_duplicates=True)
    df["wiki"] = df["wiki"].str.lower()
    df["title"] = df["title"].str.strip()

    return df

def loadData(**kwargs):
    df = kwargs["ti"].xcom_pull(task_ids="transform_task")
    datasetID = f"{projectID}.training"
    client = bigquery.Client(project=projectID)
    dataset = bigquery.Dataset(datasetID)
    dataset.location = "US"
    wikipediaTableID = f"{datasetID}.wikipedia_pageviews_daily"

    tableSchema = [
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("wiki", "STRING"),  
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("total_views", "INTEGER"),
        bigquery.SchemaField("load_date", "STRING")
    ]

    tableJobConfig = bigquery.LoadJobConfig(
        schema = tableSchema,
        write_disposition = "WRITE_APPEND"
    )
    try:
        client.create_dataset(dataset=dataset, exists_ok=True)
        loadJob = client.load_table_from_dataframe(dataframe=df, destination=wikipediaTableID, job_config=tableJobConfig)
        loadJob.result()
    except Exception as e:
        return {"error": e}
    else:
        return "ok"

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

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=loadData,
    )

    check_table_has_rows = BigQueryCheckOperator(
        task_id="check_table_has_rows",
        sql=f"SELECT COUNT(*) > 0 FROM `{projectID}.training.wikipedia_pageviews_daily` WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
        use_legacy_sql=False,
        project_id=projectID
    )
    """
    Note: the task requested for all fields to be string except for total_views but query
    provided for BigQueryCheckOperator could not calculate date using string format,
    so I changed to schema to use DATE on event_date column
    """
    
extract_task >> transform_task >> load_task >> check_table_has_rows