from jobs.gold.trade_service.entity import run as entity
from jobs.gold.trade_service.transaction import run as trade
from jobs.gold.trade_service.port import run as port
entity(
    env="pro",
    params={"dv_source_version": "1tm"},
    spark=spark,
)
trade(
    env="pro",
    params={"dv_source_version": "1tm"},
    spark=spark
)
port(
    env="pro",
    params={"dv_source_version": "1tm"},
    spark=spark
)
