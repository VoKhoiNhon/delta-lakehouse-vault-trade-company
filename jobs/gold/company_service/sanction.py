from libs.executers.gold_executer import GoldExecuter
from libs.meta import TableMeta


class SanctionExecuter(GoldExecuter):
    def transform(self):
        s_sanction = self.input_dataframe_dict["silver.s_sanction"].dataframe
        l_sanction_company = self.input_dataframe_dict[
            "silver.l_sanction_company"
        ].dataframe
        l_sanction_person = self.input_dataframe_dict[
            "silver.l_sanction_person"
        ].dataframe

        l_sanction_entity = l_sanction_company.selectExpr(
            "dv_hashkey_sanction", "dv_hashkey_company AS entity_id"
        ).union(
            l_sanction_person.selectExpr(
                "dv_hashkey_sanction", "dv_hashkey_person AS entity_id"
            )
        )

        sanction = s_sanction.join(
            l_sanction_entity, ["dv_hashkey_sanction"], "inner"
        ).selectExpr(
            "sanction_id AS id",
            "entity_id",
            "dv_recsrc",
            "dv_source_version",
            "dv_loaddts",
            "country",
            "authority",
            "program",
            "reason",
            "start_date",
            "end_date",
            "source_link",
        )

        return sanction


def run(env="env", params={}, spark=None, payload={}):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/sanction.yaml"],
        env=env,
        payload=payload,
    )

    executer_sanction = SanctionExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )

    executer_sanction.execute()


if __name__ == "__main__":
    run()
