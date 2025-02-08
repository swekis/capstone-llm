from datetime import datetime, timedelta
from airflow import DAG
from conveyor.operators import ConveyorSparkSubmitOperatorV2, ConveyorContainerOperatorV2

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "docker_operator_dag_peter",
    default_args=default_args,
    catchup=False,
) as dag:
    ConveyorContainerOperatorV2(
        task_id="clean",
        instance_type="mx.medium",
        aws_role="capstone_conveyor_llm",
        cmds=["python3","src/capstonellm/tasks/clean.py"],
        arguments=["-e","online"],
    )
