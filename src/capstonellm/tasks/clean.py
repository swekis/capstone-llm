import argparse
import logging
from pyspark.sql import SparkSession
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
import pyarrow as pa
import polars as pl
import pandas

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):

    df_questions_main = (pl.from_arrow(
        pa.Table.from_batches(
            spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/dbt/questions.json")
                ._collect_as_arrow()))
                .explode("items")
                .unnest("items")
                .explode("tags")
    )

    df_answers_main = (pl.from_arrow(
        pa.Table.from_batches(
            spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/dbt/answers.json")
                ._collect_as_arrow()))
                .explode("items")
                .unnest("items")
    )

    df_questions_filter = df_questions_main.filter(
            pl.col('tags') == 'dbt'
        ).select(
            pl.col('title'),
            pl.col('body').alias('question'),
            pl.col('question_id')
        )

    df_answers_filter = df_answers_main.select(
            pl.col('body').alias('answer'),
            pl.col('question_id')
        )

    df_qna = df_questions_filter.join(df_answers_filter, on='question_id').select(['title','question','answer'])

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    df_cleaned = spark.createDataFrame(df_qna.to_pandas())

    df_cleaned.write.json("s3a://dataminded-academy-capstone-llm-data-us/cleaned/peter/dbt", mode='overwrite')



def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
