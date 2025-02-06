FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.2-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN pip install .

CMD uv run python3 -m src.capstonellm.tasks.clean