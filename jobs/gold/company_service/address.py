from pyspark.sql import functions as F

from libs.executers.gold_executer import GoldExecuter
from libs.utils.base_transforms import update_hashkey_company
from libs.meta import TableMeta


class AddressExecuter(GoldExecuter):
    def transform(self):

        company_address = self.input_dataframe_dict[
            "silver.s_company_address"
        ].dataframe
        person_address = self.input_dataframe_dict["silver.s_person_address"].dataframe

        if self.query:
            company_address = company_address.where(self.query)
            person_address = person_address.where(self.query)
            print(
                f"filter {self.query} \n"
                f"company_address.count(): {company_address.count():,}"
                f"person_address.count(): {person_address.count():,}"
            )

        mapping_df = (
            self.input_dataframe_dict["silver.bridge_company_key"]
            .dataframe.filter(F.col("from_key").isNotNull())
            .select("from_key", "to_key")
        )

        company_address = update_hashkey_company(
            company_address, mapping_df, id_col="dv_hashkey_company"
        ).drop("from_key", "to_key")

        address = (
            company_address.withColumnRenamed("dv_hashkey_company", "id")
            .unionByName(
                person_address.withColumnRenamed("dv_hashkey_person", "id"),
                allowMissingColumns=True,
            )
            .drop(
                "jurisdiction",
                "registration_number",
                "name",
                "pure_name",
                "country_name",
                "type",
            )
            .dropDuplicates(["id"])
        )

        return address


def run(env="env", params={}, spark=None, payload={}):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/address.yaml"],
        env=env,
        payload=payload,
    )

    executer_address = AddressExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )

    executer_address.execute()


if __name__ == "__main__":
    run()
