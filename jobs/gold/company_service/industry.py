from libs.executers.gold_executer import GoldExecuter
from libs.meta import TableMeta


class IndustryExecuter(GoldExecuter):
    def transform(self):

        h_industry = self.input_dataframe_dict["silver.h_industry"].dataframe

        if self.query:
            h_industry = h_industry.where(self.query)
            print(
                f"filter {self.query} \n" f"h_industry.count(): {h_industry.count():,}"
            )

        industry = h_industry.withColumnRenamed("dv_hashkey_industry", "id")

        return industry


def run(env="env", params={}, spark=None, payload={}):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/industry.yaml"],
        env=env,
        payload=payload,
    )

    executer_industry = IndustryExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )

    executer_industry.execute()


if __name__ == "__main__":
    run()
