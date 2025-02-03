POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_DB=""
POSTGRES_HOST=""
POSTGRES_PORT=""

def write_to_db(df, table, mode) -> None:
    url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    df.write.jdbc(url, table, mode, properties)

transaction = spark.read.format("delta").load("s3a://lakehouse-gold/trade_service/transaction")
write_to_db(transaction, "transaction", "append")
port = spark.read.format("delta").load("s3a://lakehouse-gold/trade_service/port")
write_to_db(port, "port", "append")
hscode = spark.read.format("delta").load("s3a://lakehouse-silver/lookup_hscode")
write_to_db(hscode, "hscode", "append")
entity = spark.read.format("delta").load("s3a://lakehouse-gold/trade_service/entity")
write_to_db(entity, "entity", "append")