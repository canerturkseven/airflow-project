import datetime as dt
from pathlib import Path
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow import DAG


def _wait_for_supermarket(supermarket_id):
    supermarket_path = Path("./data/supermarket_" + supermarket_id)
    data_files = supermarket_path.glob("data-*.csv")
    success_file = supermarket_path / "_SUCCESS"
    return data_files and success_file.exists()

dag = DAG(
    dag_id="supermarket_data_cleanse",
    start_date=dt.datetime(year=2019, month=1, day=1),
    end_date=dt.datetime(year=2019, month=1, day=15),
    schedule_interval=dt.timedelta(minutes=10),
    catchup=True,
)
wait_for_supermarket_1 = FileSensor(
    task_id="wait_for_supermarket_1",
    filepath="/data/supermarket1/data.csv",
    dag=dag,
)
wait_for_supermarket_2 = FileSensor(
    task_id="wait_for_supermarket_2",
    filepath="/data/supermarket2/data.csv",
    dag=dag,
)
cleanse_supermarket_1 = BashOperator(
    task_id="cleanse_supermarket_1", bash_command="echo Cleanse supermarket1", dag=dag
)
cleanse_supermarket_2 = BashOperator(
    task_id="cleanse_supermarket_2", bash_command="echo Cleanse supermarket2", dag=dag
)

wait_for_supermarket_1 >> cleanse_supermarket_1
wait_for_supermarket_2 >> cleanse_supermarket_2
