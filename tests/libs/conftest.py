import pytest
from libs.meta import TableMeta


@pytest.fixture(scope="session")
def table_meta():
    # table_meta = TableMeta(from_text=meta_txt)
    table_meta = TableMeta(from_files="tests/libs/hub_account.yaml")
    return table_meta
