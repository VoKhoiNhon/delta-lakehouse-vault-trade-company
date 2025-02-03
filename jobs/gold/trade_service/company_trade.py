from libs.executers.gold_executer import GoldTradeExecuter
from libs.meta import TableMeta
from libs.utils.db_utils import write_to_els

# from libs.utils.commons import get_n_group_jurisdiction
# from libs.utils.delta_utils import delta_upsert_for_hubnlink


class CompanyTradeExecuter(GoldTradeExecuter):
    def transform(self):
        entity = self.input_dataframe_dict["gold.entity"].dataframe
        return entity.where("is_trade").select(
            "id", "name", "description", "jurisdiction", "country_code", "country_name"
        )

    def execute(self):
        df = self.transform()
        write_to_els(df, self.input_dataframe_dict["gold.entity"].table_name)


def run(env="pro", params={}, spark=None):
    import sys

    table_meta_hub = TableMeta(
        from_files=["metas/gold/trade_service/company_trade.yaml"]
    )
    executer_hub = CompanyTradeExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
        params,
        spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
