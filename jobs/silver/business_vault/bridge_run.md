from jobs.silver.bridge_company_verified_reg.bridge_company_demographic import run as bridge_company
from jobs.silver.bridge_company_verified_reg.bridge_company_verified_reg import run as bridge_key

bridge_key(
    env="pro",
    params={"dv_source_version": "1tm"},
    spark=spark
)

bridge_company(
    env="pro",
    params={"dv_source_version": "1tm"},
    spark=spark
)