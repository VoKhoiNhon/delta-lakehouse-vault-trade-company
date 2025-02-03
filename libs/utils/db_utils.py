from typing import Iterable, Any
import os

import psycopg2
from elasticsearch import Elasticsearch
from utils.delta_utils import timing_decorator

# es = Elasticsearch(['http://10.1.0.88:9200'])
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT")


def create_index(es, index_name, settings=None) -> None:
    """Create new index with custom or default settings"""

    default_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "45s",
            "flush_after_merge": "1gb",
        }
    }

    final_settings = default_settings
    if settings:
        final_settings = default_settings

    if es.indices.exists(index=index_name):
        es.indices.put_settings(body=final_settings, index=index_name)
        print(f"Index {index_name} already exists, update config")
    else:
        es.indices.create(index=index_name, body=final_settings)
        print(f"Created : Index {index_name}")


def write_to_els(df, index_name, mode="append") -> None:
    try:
        es = Elasticsearch(
            hosts=[{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}]
        )
        create_index(es, index_name)
        (
            df.write.format("org.elasticsearch.spark.sql")
            .option("es.nodes", f"{ELASTICSEARCH_HOST}")
            .option("es.port", f"{ELASTICSEARCH_PORT}")
            .option("es.nodes.wan.only", "true")
            .option("es.resource", index_name)
            .option("es.mapping.id", "id")
            .option("es.write.operation", "upsert")
            .option("es.batch.size.entries", "5000")
            .option("es.batch.size.bytes", "30mb")
            .option("es.batch.write.retry.count", "5")
            .option("es.batch.write.retry.wait", "10s")
            .option("es.http.timeout", "1m")
            .mode(mode)
            .save()
        )
    except Exception as e:
        print(f"An error occurred: {e}")


# def timing_decorator(func: Callable):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         result = func(*args, **kwargs)
#         end_time = time.time()
#         execution_time = (end_time - start_time) / 60
#         print(f"Function {func.__name__} took {execution_time:.2f} minutes to execute")
#         return result

#     return wrapper


@timing_decorator
def write_to_db(df, table, mode) -> None:
    url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    df.write.jdbc(url, table, mode, properties)


@timing_decorator
def update_to_db(
    iterator: Iterable[Any],
    table: str,
    columns_to_update: list[str],
    where_conditions: list[str],
) -> None:
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
        )
        cursor = conn.cursor()
        for row in iterator:
            set_statement = ", ".join(
                [f"{column} = '{row[column]}'" for column in columns_to_update]
            )
            where_statement = " and ".join(
                [f"{column} = '{row[column]}'" for column in where_conditions]
            )
            update_query = (
                f"""UPDATE {table} SET {set_statement} WHERE {where_statement}"""
            )
            cursor.execute(update_query)
            conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@timing_decorator
def merge_to_db(
    target_table: str,
    source_table: str,
    columns: list[str],
) -> None:
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
        )
        cursor = conn.cursor()
        merge_query = f"""
        MERGE INTO {target_table} AS target
        USING {source_table} AS source
        ON target.id = source.id
        WHEN MATCHED THEN
        UPDATE SET
            {", ".join(f"target.{column} = source.{column}" for column in columns)}
        WHEN NOT MATCHED THEN
        INSERT (
            {", ".join(columns)}
        )
        VALUES (
            {", ".join(f"source.{column}" for column in columns)}
        );
        """
        cursor.execute(merge_query)
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@timing_decorator
def delete_to_db(
    iterator: Iterable[Any],
    table: str,
    where_conditions: list[str],
) -> None:
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
        )
        cursor = conn.cursor()
        for row in iterator:
            where_statement = " and ".join(
                [f"{column} = '{row[column]}'" for column in where_conditions]
            )
            delete_query = f"""DELETE FROM {table} WHERE {where_statement}"""
            cursor.execute(delete_query)
            conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
