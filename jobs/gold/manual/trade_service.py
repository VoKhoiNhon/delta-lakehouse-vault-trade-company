from delta.tables import DeltaTable

from libs.executers.gold_executer import DBTradeServiceExecuter
from libs.utils.db_utils import write_to_db, merge_to_db, delete_to_db


class Executer(DBTradeServiceExecuter):
    def transform(self):
        return

    def execute(self):
        temp_table_name = self.params.get("delta_source", "")
        table_name = self.params.get("delta_source", "")
        df = self.transform()
        df.createOrReplaceTempView("manual")
        delta_table_source = self.params.get("delta_source", "")
        delete_query = f"""
        DELETE from delta.`{delta_table_source}`
        where id in (select dv_hashkey_company from manual)
        """
        self.spark.sql(delete_query)


def run(env="pro", params={}, spark=None):
    executer_hub = Executer(
        "write_db",
        None,
        None,
        params,
        spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()

delete_query = """
DELETE from delta.`s3a://lakehouse-gold/trade_service/entity`
where id in (select dv_hashkey_company from manual)
"""
spark.sql(delete_query)


merge_query = """
MERGE INTO delta.`s3a://lakehouse-gold/trade_service/transaction` as target
USING (
    SELECT t.id,
           m.dv_hashkey_company as check_key,
           m.update_value,
           t.jurisdiction
    FROM transaction t
    INNER JOIN manual m
    ON t.jurisdiction = m.jurisdiction AND (t.supplier_id = m.dv_hashkey_company OR t.buyer_id = m.dv_hashkey_company)
) as source
ON target.id = source.id
WHEN MATCHED AND source.check_key = target.supplier_id AND source.jurisdiction = target.jurisdiction
    THEN UPDATE SET supplier_id = source.update_value
WHEN MATCHED AND source.check_key = target.buyer_id AND source.jurisdiction = target.jurisdiction
    THEN UPDATE SET buyer_id = source.update_value
"""
spark.sql(merge_query)


from elasticsearch import Elasticsearch

es = Elasticsearch(["http://10.1.0.88:9200"])
es.delete(index="company_trade", id="121193b92e351104dd1728d9d71c4ef2")
