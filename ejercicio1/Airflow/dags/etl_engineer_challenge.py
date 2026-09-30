import logging
import datetime
import polars as pl
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

dag_name = __file__.removesuffix(".py").split("/")[-1]

bucket = "bck-landing"
object_name = "data/data_prueba_tecnica.csv"

sink_object = "master/data_prueba_tecnica.parquet"
sink_bucket = "bck-bronce"


def process_file(df:pl.DataFrame):
    def clean_empty_str(col_name:str):
        return pl.when(
            pl.col(col_name) == ""
            ).then(
                pl.lit(None).alias(col_name)
            ).otherwise(
                pl.col(col_name)
            )

    def count_null_values(dataframe: pl.DataFrame):
        size = len(dataframe)
        return dataframe.select([
        (size - pl.col(col).count()).alias(col + "_nulls")
        for col in dataframe.columns
        ]).select(pl.selectors.all(), pl.lit(size).alias("sample_size"))

    def parse_date_column(
        column: str,
    ) -> pl.DataFrame:
        return pl.coalesce(
                [
                    pl.col(column).str.strptime(
                        pl.Date,
                        format="%Y-%m-%d",
                        strict=False,
                    ),
                    pl.col(column).str.strptime(
                        pl.Date,
                        format="%Y%m%d",
                        strict=False,
                    ),
                    pl.col(column).str.strptime(
                        pl.Date,
                        format="%Y-%m-%dT%H:%M:%S",
                        strict=False
                    ),
                    pl.col(column).str.strptime(
                        pl.Date,
                        format="%Y-%m-%d %H:%M:%S",
                        strict=False
                    )
                ]
            )

    def valide_empty_str(col_name:str):
        return pl.when(
            pl.col(col_name) == ""
            ).then(
                pl.col(col_name)
            ).otherwise(
                pl.lit(None).alias(col_name)
            ).count()

    #clean column names
    df = df.rename({name:name.strip() for name in df.columns})
    #count null values by column
    nulls_by_col = count_null_values(df)
    logging.info(
        "null summary on source: \n %s", str(nulls_by_col.head())
    )

    dlq = df.filter(
        pl.col("id").is_null() | pl.col("company_id").is_null()
        ).with_columns(
            pl.when(
                pl.coalesce(pl.col("id"),pl.col("company_id")).is_null()
            ).then(pl.lit("id and company_id are null")).when(
                pl.col("id").is_null()
            ).then(pl.lit("id is null")).when(
                pl.col("company_id").is_null()
            ).then(pl.lit("company_id is null")).alias("error")
        )
    if len(dlq) >0:
        logging.info("se han encontrado registros con id nulos. Se guardaran el dlq para su revision")
        #TODO: save to dlq
    # se eliminan registros con id's nulos
    df = df.filter(pl.col("id").is_not_null() & pl.col("company_id").is_not_null())

    pre_clean_dates = df.with_columns(
        pl.selectors.ends_with("_at").str.strip_chars()
        )

    count_empty_string_dates = pre_clean_dates.select(
    valide_empty_str("created_at"),
    valide_empty_str("paid_at"),
    pl.lit(len(pre_clean_dates)).alias("total_rows")
    )
    logging.info(
            "empty string on date columns: \n %s", str(count_empty_string_dates.head())
        )

    final_df = pre_clean_dates.with_columns(
    clean_empty_str("created_at"),
    clean_empty_str("paid_at")
    ).with_columns(
        parse_date_column("created_at").alias("created_at_cleaned"),
            parse_date_column("paid_at").alias("paid_at_cleaned")
    )
    nulls_by_col_final = count_null_values(final_df)

    logging.info(
                "empty string on date columns: \n %s", str(nulls_by_col_final.head())
            )
    return final_df


def process():
    import io

    bytes_ = io.BytesIO()
    minio = S3Hook(aws_conn_id="minio_conn")
    content = minio.get_key(key= object_name, bucket_name = bucket)
    response = content.get()["Body"]
    df = pl.read_csv(response.read(), has_header=True, separator=",")
    df_final = process_file(df)
    df_final.write_parquet(bytes_)
    minio.load_bytes(
        bytes_data = bytes_.getvalue(),
        key = sink_object,
        bucket_name = sink_bucket,
        replace = True)


with DAG(
    dag_id=dag_name,
    schedule= None,
    start_date=datetime.datetime(year = 2026, month=1, day=1)
    ) as dag:

    transform = PythonOperator(
        task_id = "process",
        python_callable= process
    )
