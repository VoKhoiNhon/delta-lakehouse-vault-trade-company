from pyspark.sql import functions as F

from libs.executers.gold_executer import GoldExecuter
from libs.meta import TableMeta


class CompanyDetailExecuter(GoldExecuter):
    def transform(self):
        source = self.input_dataframe_dict[
            self.meta_input_resource.table_name
        ].dataframe
        return (
            source.dataframe.withColumnRenamed("dv_hashkey_company", "id")
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )


def run():
    import sys

    table_meta_hub = TableMeta(
        from_files=["metas/gold/company_service/company_detail.yaml"]
    )
    executer_hub = CompanyDetailExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
