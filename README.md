### Wikipedia Airflow Practice Task

This task is divided into 3 subtasks

1. Extract yesterday's wikipedia articles.
2. Transform to discard rows with null values.
3. Transform to add a `load_date` column to keep note when the dag successfuly ran.
4. Load all data into BigQuery project under `training.wikipedia_pageviews_daily`



## Instructions:

1. Install Apache Airflow
2. Set dags folder with command `export AIRFLOW__CORE__DAGS_FOLDER="/Path/To/Your/Dags/Folder"`
3. Run project with command `apache standalone`
4. ???
5. Profit