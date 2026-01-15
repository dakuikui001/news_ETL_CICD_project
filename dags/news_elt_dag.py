from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import pendulum

# 定义基础路径
INCLUDE_DIR = "/usr/local/airflow/include"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Shanghai") # 或者 "Asia/Singapore"

with DAG(
    dag_id='notebook_etl_pipeline',
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule='0 6 * * *',
    catchup=False
) as dag:

    # 1. 运行爬虫 Notebook
    # cd 进入目录确保 'data/' 相对路径可用
    run_scrapy = BashOperator(
    task_id='run_scrapy_notebook',
    # 1. 切换到绝对路径
    # 2. 加上 --log-output 实时看进度
    # 3. 加上 --no-progress-bar 让 Log 更干净
    bash_command=(
        'cd /usr/local/airflow/include && '
        'papermill scrapy.ipynb out_scrapy.ipynb '
        '--log-output --no-progress-bar'
    ),
    # 设置 10 分钟超时，防止无限卡死
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

    # 2. 运行 Spark ETL Notebook
    # 核心：设置 PYTHONPATH，让 Notebook 能找到同级目录下的 .py 模块
    run_spark_main = BashOperator(
        task_id='run_spark_etl_notebook',
        bash_command=(
            f"export PYTHONPATH=$PYTHONPATH:{INCLUDE_DIR}/pyspark && "
            f"cd {INCLUDE_DIR}/pyspark && "
            f"papermill run.ipynb out_run.ipynb"
        )
    )

    run_scrapy >> run_spark_main