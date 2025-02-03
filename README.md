
## II. Storage Layers
| Property             | Raw Layer                                                | Bronze Layer                                                                                | Silver Layer                                                                                                                                  | Gold Layer                                                                                   |
| -------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Storage Format       | Native source format (csv, txt, json, xml, mp4, mp3 ...) | delta                                                                                       | delta                                                                                                                                         | delta                                                                                        |
| Data Modeling        | None                                                     | None                                                                                        | Raw Vault, Business Vault                                                                                                                     | 1. Dimensional modeling: Fact, Dim (Information mart, data mart) 2. Flatten tables           |
| Transformation Level | None                                                     | sources as-is (1-1): schema enforce, add meta data columns (record-src, load_datetime, ...) | Classification + Hard logic: date-time validate, trim values, drop-duplicated, enriching data (fill null master data like customer name, ...) | Business logic                                                                               |
| Read Strategy        | Full load                                                | Full load, period load, delta load (CDC)                                                    | Full load, period load, delta load (CDC)                                                                                                      | Full load, period load, delta load (CDC)                                                     |
| Write Strategy       | Overwrite by separate daily folder                       | Overwrite by date partition                                                                 | Insert only                                                                                                                                   | Upsert                                                                                       |
| Consumers            | Bronze Layer, Data Scientist                             | Silver Layer, Data Analyst, Data Scientist                                                  | Gold Layer, Data Analyst, Data Scientist, Applications                                                                                        | Gold Layer, Data Analyst, Data Scientist, Applications, Business Intelligence, Business User |
| Catalog              | No                                                       | Yes                                                                                         | Yes                                                                                                                                           | Yes                                                                                          |
| Data Level           | Unstructured, Semi-Structured                            | Structured                                                                                  | Structured                                                                                                                                    | Structured                                                                                   |
| Challenges           |                                                          | Schema evolution                                                                            | Data classification                                                                                                                           | Ad-hoc logic                                                                                 |

## III. Main Compenents:


### 1. TableMeta
- Stores table-specific parameters and configurations.
- Data Contract: Acts as a clear and concise definition of the table's structure and content.
- Profiling and Data Lineage: Provides essential information for data profiling, lineage tracking, and impact analysis.
```yaml
-   name: meta
    type:
    type: map
    keyType: string
    valueType: string
    valueContainsNull: true
-   name: meta2
    type:
        type: array
        elementType: string
        containsNull: true
```

### 2. Executer
- Abstract class for executing table-specific operations.




## build-lib:
```sh
rm -rf /libs.zip
cd /libs && \
zip -r libs.zip \
sdb/ -x '*__pycache__*' '*ipynb_checkpoints*'
```


## black
python3 -m venv .venv
source .venv/bin/activate

pip install black
black libs
black tests



current flow:
- map company without registration_number with company has registration_number

  => drop fake company that has same name


# Hôm nay

## company data
1. join các bảng trên gold: => đẩy ra 2 table để  feed cho 3 bảng company:
- h_company - mapping done
- s_company_address - mapping done
- s_company_demographic - not done
2. đã add pure_name cho các cột company name 3 silver table trên, đổi DV_HASHKEY_COMPANY
```
DV_HASHKEY_COMPANY = (
    "md5(concat_ws(';', jurisdiction, coalesce(registration_number, null), pure_name))"
)
```

## jurisdiction
1. thêm file chuẩn hóa resources/jurisdictions.csv (from AN)

 - tất cả các jurisdiction phải theo chuẩn này
 - riêng các quốc gia có state: jurisdiction = US.Texas [country_code].[state]

### với Canada và Australia - federal company khác Mỹ
Canada và Australia có khái niệm federal company nghĩa là jurisdiction của nó sẽ là cả coutry chứ không phải state, cái này được phân loại khi các bạn làm source !!! >> detect manualy
```
country,jurisdiction,is_state,country_code
Canada,Canada,false,CA
Australia,Australia,false,AU
```


prod:
postgres trade service 10.1.0.61:5432
es 10.1.0.88:9200

# Run 10-01-2025
## Dat Data:
1. h_company : jobs/silver/h_company/6kUSD/h_company__src_172_Dat.py
2. s_demographic: jobs/silver/s_company_demographic/6kUSD/s_company_demographic__src_172_Dat.py
3. l_bol: jobs/silver/l_bol/l_bol__src_172_Dat.py
4. s_bol: jobs/silver/s_bol/s_bol__src_172_Dat.py

```py
from jobs.silver.h_company.h_company__src_172_Dat import run as h_company_run
from jobs.silver.s_company_demographic.s_company_demographic__src_172_Dat import run as s_demographic_run
from jobs.silver.l_bol.l_bol__src_172_Dat import run as l_bol_run
from jobs.silver.s_bol.s_bol__src_172_Dat import run as s_bol_run
import time

def measure_time(function, *args, **kwargs):
    start_time = time.time()
    function(*args, **kwargs)
    end_time = time.time()
    execution_time = (end_time - start_time) / 60
    print(f"Function took {execution_time:.2f} minutes to execute")

measure_time(h_company_run, env="pro", params={}, spark=spark)
measure_time(s_demographic_run, env="pro", params={}, spark=spark)
measure_time(l_bol_run, env="pro", params={}, spark=spark)
measure_time(s_bol_run, env="pro", params={}, spark=spark)
```

## Colombia Trade - Hoai

```py
from jobs.silver.h_company.h_company__src_colombia_import_2016_2024 import run as run_h_company
from jobs.silver.s_company_demographic.s_company_demographic__src_colombia_import_2016_2024 import run as run_s_company_demographic
from jobs.silver.s_company_address.s_company_address__src_colombia_import_2016_2024 import run as run_s_company_address
from jobs.silver.s_bol.s_bol__src_colombia_import_2016_2024 import run as run_s_bol
from jobs.silver.l_bol.l_bol__src_colombia_import_2016_2024 import run as run as run_l_bol

measure_time(run_h_company, env="pro", params={}, spark=spark)
measure_time(run_s_company_demographic, env="pro", params={}, spark=spark)
measure_time(run_s_company_address, env="pro", params={}, spark=spark)
measure_time(run_s_bol, env="pro", params={}, spark=spark)
measure_time(run_l_bol, env="pro", params={}, spark=spark)
```