from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

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
    "peters_dag",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:

    t1 = DockerOperator(
        task_id="task_clean",
        image="peter",
        container_name="task_clean",
        api_version="auto",
        auto_remove=True,
        command="python3 -m src.capstonellm.tasks.clean", # code to be executed in this instance of 'docker run' in the bash shell in this case
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment = {'AWS_ACCESS_KEY_ID':Variable.get("ACCESS_KEY_ID"),
            'AWS_SECRET_ACCESS_KEY':Variable.get("SECRET_ACCESS_KEY")
        }
    )