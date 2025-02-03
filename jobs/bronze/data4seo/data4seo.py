from concurrent.futures import ProcessPoolExecutor, as_completed
from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
    convert_country_code_to_country_name,
    add_pure_company_name,
)
from libs.utils.delta_utils import delta_insert
from libs.utils.connection import S3Storage
from libs.utils.logger import DriverLogger


from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import time


def get_data_files_from_path(
    spark,
    s3_path="s3a://lakehouse-raw/data4seo/company-20241205/data",
):
    endpoint_url = "https://" + spark.sparkContext.getConf().get(
        "spark.hadoop.fs.s3a.endpoint"
    )
    aws_access_key_id = spark.sparkContext.getConf().get(
        "spark.hadoop.fs.s3a.access.key"
    )
    aws_secret_access_key = spark.sparkContext.getConf().get(
        "spark.hadoop.fs.s3a.secret.key"
    )
    import boto3
    import re

    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )

    parts = s3_path.replace("s3a://", "").split("/", 1)
    bucket, prefix = parts[0], parts[1]
    bucket = s3_resource.Bucket(bucket)
    objects = list(bucket.objects.filter(Prefix=prefix))
    datas = []
    for obj in objects:
        if ".csv" in obj.key:
            datas.append(obj.key)

    sorted_files = sorted(
        datas, key=lambda x: int(re.search(r"/(\d+)\.csv$", x).group(1))
    )
    return sorted_files


def clean_jurisdiction(df):
    import pandas as pd

    spark = df.sparkSession
    spark.createDataFrame(
        pd.read_csv("resources/lookup_jurisdiction.csv", na_filter=False)
    ).createOrReplaceTempView("lookup_jurisdiction")
    df.createOrReplaceTempView("company")

    company_jurisdiction_cleaned = (
        spark.sql(
            """
    with tmp as (
        select c.*,
        case
            when c.jurisdiction is null or c.jurisdiction = '' then 'unspecified'
            when c.jurisdiction = '__HIVE_DEFAULT_PARTITION__' then 'unspecified'
            when c.country_code = 'US' and c.jurisdiction = 'USA' then 'United States'
            when c.country_code = 'US' and c.jurisdiction = 'AL' then 'US.Alabama'
            when c.country_code = 'US' and c.jurisdiction = 'AK' then 'US.Alaska'
            when c.country_code = 'US' and c.jurisdiction = 'AZ' then 'US.Arizona'
            when c.country_code = 'US' and c.jurisdiction = 'AR' then 'US.Arkansas'
            when c.country_code = 'US' and c.jurisdiction = 'CA' then 'US.California'
            when c.country_code = 'US' and c.jurisdiction = 'CO' then 'US.Colorado'
            when c.country_code = 'US' and c.jurisdiction = 'CT' then 'US.Connecticut'
            when c.country_code = 'US' and c.jurisdiction = 'DE' then 'US.Delaware'
            when c.country_code = 'US' and c.jurisdiction = 'DC' then 'US.District of Columbia'
            when c.country_code = 'US' and c.jurisdiction = 'FL' then 'US.Florida'
            when c.country_code = 'US' and c.jurisdiction = 'GA' then 'US.Georgia'
            when c.country_code = 'US' and c.jurisdiction = 'HI' then 'US.Hawaii'
            when c.country_code = 'US' and c.jurisdiction = 'ID' then 'US.Idaho'
            when c.country_code = 'US' and c.jurisdiction = 'IL' then 'US.Illinois'
            when c.country_code = 'US' and c.jurisdiction = 'IN' then 'US.Indiana'
            when c.country_code = 'US' and c.jurisdiction = 'IA' then 'US.Iowa'
            when c.country_code = 'US' and c.jurisdiction = 'KS' then 'US.Kansas'
            when c.country_code = 'US' and c.jurisdiction = 'KY' then 'US.Kentucky'
            when c.country_code = 'US' and c.jurisdiction = 'LA' then 'US.Louisiana'
            when c.country_code = 'US' and c.jurisdiction = 'ME' then 'US.Maine'
            when c.country_code = 'US' and c.jurisdiction = 'MD' then 'US.Maryland'
            when c.country_code = 'US' and c.jurisdiction = 'MA' then 'US.Massachusetts'
            when c.country_code = 'US' and c.jurisdiction = 'MI' then 'US.Michigan'
            when c.country_code = 'US' and c.jurisdiction = 'MN' then 'US.Minnesota'
            when c.country_code = 'US' and c.jurisdiction = 'MS' then 'US.Mississippi'
            when c.country_code = 'US' and c.jurisdiction = 'MO' then 'US.Missouri'
            when c.country_code = 'US' and c.jurisdiction = 'MT' then 'US.Montana'
            when c.country_code = 'US' and c.jurisdiction = 'NE' then 'US.Nebraska'
            when c.country_code = 'US' and c.jurisdiction = 'NV' then 'US.Nevada'
            when c.country_code = 'US' and c.jurisdiction = 'NH' then 'US.New Hampshire'
            when c.country_code = 'US' and c.jurisdiction = 'NJ' then 'US.New Jersey'
            when c.country_code = 'US' and c.jurisdiction = 'NM' then 'US.New Mexico'
            when c.country_code = 'US' and c.jurisdiction = 'NY' then 'US.New York'
            when c.country_code = 'US' and c.jurisdiction = 'NC' then 'US.North Carolina'
            when c.country_code = 'US' and c.jurisdiction = 'ND' then 'US.North Dakota'
            when c.country_code = 'US' and c.jurisdiction = 'OH' then 'US.Ohio'
            when c.country_code = 'US' and c.jurisdiction = 'OK' then 'US.Oklahoma'
            when c.country_code = 'US' and c.jurisdiction = 'OR' then 'US.Oregon'
            when c.country_code = 'US' and c.jurisdiction = 'PA' then 'US.Pennsylvania'
            when c.country_code = 'US' and c.jurisdiction = 'RI' then 'US.Rhode Island'
            when c.country_code = 'US' and c.jurisdiction = 'SC' then 'US.South Carolina'
            when c.country_code = 'US' and c.jurisdiction = 'SD' then 'US.South Dakota'
            when c.country_code = 'US' and c.jurisdiction = 'TN' then 'US.Tennessee'
            when c.country_code = 'US' and c.jurisdiction = 'TX' then 'US.Texas'
            when c.country_code = 'US' and c.jurisdiction = 'UT' then 'US.Utah'
            when c.country_code = 'US' and c.jurisdiction = 'VT' then 'US.Vermont'
            when c.country_code = 'US' and c.jurisdiction = 'VA' then 'US.Virginia'
            when c.country_code = 'US' and c.jurisdiction = 'WA' then 'US.Washington'
            when c.country_code = 'US' and c.jurisdiction = 'WV' then 'US.West Virginia'
            when c.country_code = 'US' and c.jurisdiction = 'WI' then 'US.Wisconsin'
            when c.country_code = 'US' and c.jurisdiction = 'WY' then 'US.Wyoming'
            when c.country_code = 'US' and c.jurisdiction = 'Fl' then 'US.Florida'
            when c.country_code = 'US' and c.jurisdiction = 'Madison County, New York' then 'US.New York'
            when c.country_code = 'US' then concat_ws('.', c.country_code, c.jurisdiction)
            when l.official_name is not null then l.jurisdiction
            when c.jurisdiction = 'Bolivia, Plurinational State of' then 'Bolivia'
            when c.jurisdiction = 'Palestine, State of' then 'Palestine'
            when c.jurisdiction = 'Moldova, Republic of' then 'Moldova'
            when c.jurisdiction = 'Tanzania, United Republic of' then 'Tanzania'
            when c.jurisdiction = 'Korea, Democratic People%27s Republic of' then 'North Korea'
            when c.jurisdiction = 'Micronesia, Federated States of' then 'Micronesia'
            when c.jurisdiction = 'Côte d%27Ivoire' then "Côte d'Ivoire"
            else c.jurisdiction
        end as new_jurisdiction
        from company c
        left join lookup_jurisdiction l on c.jurisdiction = l.official_name
    )
    select tmp.*
    from tmp
    inner join lookup_jurisdiction l on tmp.new_jurisdiction = l.jurisdiction
    """
        )
        .withColumn("jurisdiction", F.col("new_jurisdiction"))
        .drop("new_jurisdiction")
    )

    return company_jurisdiction_cleaned


class Executer(BronzeExecuter):
    def transform(self, df):
        rename_dict = {
            "title": "name",
            "is_claimed": "is_sanctioned",
            "address": "full_address",
            "address_info.address": "street",
            "address_info.city": "city",
            "address_info.zip": "postal_code",
            "address_info.region": "region",
            "address_info.country_code": "country_code",
            "main_image": "image_url",
        }
        convert_country_code_to_name_udf = udf(
            convert_country_code_to_country_name, StringType()
        )

        # START NEW LOGIC
        # Transform
        df = (
            df.withColumn(
                "contacts_parsed",
                F.from_json(
                    F.col("contacts"),
                    "array<struct<type:string,value:string,source:string>>",
                ),
            )
            .withColumn(
                "telephone_values",
                F.expr(
                    """
                    filter(contacts_parsed, x -> x.type = 'Telephone' AND x.source != 'backlinks')
                """
                ),
            )
            .withColumn(
                "mail_values",
                F.expr(
                    """
                    filter(contacts_parsed, x -> x.type = 'Mail' AND x.source != 'backlinks')
                """
                ),
            )
            .withColumn(
                "telephone",
                F.expr("concat_ws(', ', transform(telephone_values, x -> x.value))"),
            )
            .withColumn(
                "mail", F.expr("concat_ws(', ', transform(mail_values, x -> x.value))")
            )
            # Merge All Categories into List-String Categories
            .withColumn("category_ids", regexp_replace("category_ids", r"[\[\]]", ""))
            .withColumn(
                "additional_categories",
                regexp_replace("additional_categories", r"[\[\]]", ""),
            )
            .withColumn(
                "temp",
                F.when(
                    (F.col("category_ids") != "null")
                    & (F.col("additional_categories") != "null"),
                    F.concat(
                        F.col("category_ids"),
                        F.lit(" "),
                        F.col("additional_categories"),
                    ),
                ).otherwise(
                    F.coalesce(F.col("category_ids"), F.col("additional_categories"))
                ),
            )
            .withColumn("category_list", F.split(F.col("temp"), ","))
            # Filter URL -> domain, mail, ...
            .withColumn(
                "domain", regexp_extract(col("url"), r"https?://(www\.|m\.)?([^/]+)", 2)
            )
            .withColumn(
                "facebook_url",
                when(col("domain").contains("facebook"), col("url")).otherwise(None),
            )
            .withColumn(
                "twitter_url",
                when(
                    (col("domain").contains("twitter")) | (col("domain") == "x.com"),
                    col("url"),
                ).otherwise(None),
            )
            .withColumn(
                "linkedin_url",
                when(col("domain").contains("linkedin"), col("url")).otherwise(None),
            )
            .withColumn(
                "website",
                when(
                    col("facebook_url").isNull()
                    & col("twitter_url").isNull()
                    & col("linkedin_url").isNull(),
                    col("url"),
                ).otherwise(None),
            )
            # Remove newline  in description
            .withColumn("description", regexp_replace(col("description"), r"\n", " "))
        )
        # Rename Column
        for old_col, new_col in rename_dict.items():
            df = df.withColumnRenamed(old_col, new_col)
        df = df.filter(col("country_code").isNotNull()).withColumn(
            "country_name", convert_country_code_to_name_udf(df["country_code"])
        )
        df = df.filter(col("country_code").isNotNull()).withColumn(
            "country_name", convert_country_code_to_name_udf(df["country_code"])
        )
        df = df.withColumn(
            "jurisdiction",
            F.when(F.col("country_code") == "US", F.col("region")).otherwise(
                F.col("country_name")
            ),
        )
        # END NEW LOGIC
        df = add_pure_company_name(df=df, column_name="name")
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        # Add a column to capture the file names
        # df = df.withColumn("file_path", input_file_name())
        df = clean_column_names(df)
        df = clean_jurisdiction(df)

        # source = self.input_dataframe_dict["raw_data4seo"]
        # bucket = source.data_location.replace("s3a://", "").split("/", 1)[0]
        # tmp_path = source.data_location.replace(bucket, bucket + "/tmp", 1)
        # df = add_pure_company_name(df, "name", tmp_path)
        # df = df.repartition(1, *self.meta_table_model.partition_by)

        return df

    def execute(self, options=None):
        source = self.input_dataframe_dict["raw_data4seo"]
        s3_path = "s3a://lakehouse-raw/data4seo/company-20241205/data"
        data_files = get_data_files_from_path(self.spark, s3_path)
        print(data_files)
        for data_file in data_files:
            start_time = time.time()
            df = (
                self.spark.read.format(source.format)
                .options(**source.options)
                .load("s3a://lakehouse-raw/" + data_file)
            ).withColumn("source_file", F.lit(data_file))
            df = self.transform(df)

            delta_insert(
                spark=self.spark,
                df=df,
                data_location=self.meta_table_model.data_location,
                partition_by=self.meta_table_model.partition_by,
                options=self.meta_table_model.options if not options else options,
            )
            end_time = time.time()
            execution_time = (end_time - start_time) / 60
            print(f"source {data_file} took {execution_time:.2f} minutes to execute")


def simple_run(
    env="pro", params={}, spark=None, payload={"load_date": "20241205", "env": "prod"}
):
    import sys

    table_meta = TableMeta(
        from_files="metas/bronze/data4seo/data4seo.yaml", env=env, payload=payload
    )
    executer = Executer(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


def process_file(match_file, meta_file_path):
    # Create a new TableMeta object for each file
    individual_table_meta = TableMeta(from_files=meta_file_path)
    individual_table_meta.input_resources[0].data_location = match_file
    driver_logger = DriverLogger(name="data4seo", log_file="data4seo.log").get_logger()
    spark_app_name = f"data4seo_raw_to_bronze-{match_file}"

    # Start to Execute Main Function on each Processor
    try:
        executer = Executer(
            app_name=spark_app_name,
            meta_table_model=individual_table_meta.model,
            meta_input_resource=individual_table_meta.input_resources,
        )
        executer.execute()
        driver_logger.info(
            f"Successfully Processing {match_file} by SparkAppName {spark_app_name}"
        )
        executer.spark.stop()
        return True
    except Exception as error:
        error_msg = f"FAILED Processing {match_file} by SparkAppName {spark_app_name} due to {error}"
        driver_logger.error(error_msg)
        return False


def run(max_workers=8):
    meta_file_path = "metas/bronze/data4seo/data4seo.yaml"
    table_meta = TableMeta(from_files=meta_file_path)
    driver_logger = DriverLogger(name="data4seo", log_file="data4seo.log").get_logger()
    driver_logger.info(
        f"READ from data_location: {table_meta.input_resources[0].data_location}"
    )

    # Search Raw-files
    s3storage = S3Storage(bucket_name="lakehouse-raw")
    matching_files = s3storage.list_files_with_regex(
        path_regex=table_meta.input_resources[0].data_location
    )
    driver_logger.info(
        f"Matching {len(matching_files)} files from data_location : {matching_files}"
    )

    # Process files
    futures = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs at once
        for match_file in matching_files:
            futures.append(executor.submit(process_file, match_file, meta_file_path))

        # As each task completes, submit the next one
        for future in as_completed(futures):
            try:
                result = (
                    future.result()
                )  # Wait for the task to finish and retrieve its result
                driver_logger.info(
                    f"Task {future} completed successfully with result: {result}"
                )
            except Exception as e:
                driver_logger.error(
                    f"Error processing file: task {future}: {e}", exc_info=True
                )
            driver_logger.info(
                f"Task {future} completed, submitting next task if available."
            )


def run_once():
    meta_file_path = "metas/bronze/data4seo/data4seo.yaml"
    individual_table_meta = TableMeta(from_files=meta_file_path)
    spark_app_name = f"data4seo_raw_to_bronze"

    executer = Executer(
        app_name=spark_app_name,
        meta_table_model=individual_table_meta.model,
        meta_input_resource=individual_table_meta.input_resources,
    )
    executer.execute()
    executer.spark.stop()
    return True


if __name__ == "__main__":
    # run(max_workers = 10)
    run_once()
