# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Demographics from GDPPR
# MAGIC * As our cohort is ancohored to GDPPR we should be able to get all demographics from there
# MAGIC * ProductionDate = '2021-11-26 14:02:40.645948'
# MAGIC * Except from ethnicity which we take from project CCU037
# MAGIC 
# MAGIC The following procedure is used 
# MAGIC 1. Get most-recent non-missing value of each variable per patient in our population
# MAGIC 2. Save as temp table
# MAGIC 3. Inner join tables together to a final population demographics table

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as n, COUNT(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMD - LSOA

# COMMAND ----------

lsoas = spark.sql(f'''
SELECT
  person_id_deid,
  LSOA
FROM
  (
    SELECT
     *,
     row_number() OVER(PARTITION BY NHS_NUMBER_DEID ORDER BY date DESC) rn
    FROM
      dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive a
      INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
      ON a.NHS_NUMBER_DEID == b.person_id_deid
      WHERE ProductionDate = '2021-11-26 14:02:40.645948' AND LSOA is not null
    )
WHERE
  rn = 1
''')

# COMMAND ----------

deprivation = spark.sql("""
SELECT
  LSOA_CODE_2011,
  DECI_IMD
FROM
  dss_corporate.english_indices_of_dep_v02
WHERE
  IMD_YEAR = 2019
  """)

# COMMAND ----------

# Use LSOA to join with deprivation/geography
lsoas = lsoas.join(deprivation, lsoas.LSOA == deprivation.LSOA_CODE_2011, "LEFT")
lsoas = lsoas.drop('LSOA_CODE_2011')

# COMMAND ----------

from pyspark.sql.functions import col, create_map, lit
from itertools import chain

mapping = {
    '1': '1',
    '2': '1', 
    '3': '2', 
    '4': '2', 
    '5': '3', 
    '6': '3', 
    '7': '4', 
    '8': '4', 
    '9': '5', 
    '10': '5'}

mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])

lsoas = lsoas.withColumn("IMD_quintile", mapping_expr[col("DECI_IMD")])

# COMMAND ----------

lsoas.createOrReplaceGlobalTempView('ccu019_temp_pop_demo_imd')

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_imd") 
create_table("ccu019_temp_pop_demo_imd") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_imd

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gender

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent non-missing gender
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_temp_pop_demo_gender
# MAGIC AS
# MAGIC SELECT
# MAGIC   person_id_deid,
# MAGIC   SEX as sex
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC      *,
# MAGIC      row_number() OVER(PARTITION BY NHS_NUMBER_DEID ORDER BY date DESC) rn
# MAGIC     FROM
# MAGIC       dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive a
# MAGIC       INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC       ON a.NHS_NUMBER_DEID == b.person_id_deid
# MAGIC       WHERE ProductionDate = '2021-11-26 14:02:40.645948' AND SEX is not null
# MAGIC     )
# MAGIC WHERE
# MAGIC   rn = 1

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_gender") 
create_table("ccu019_temp_pop_demo_gender") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sex, count(SEX) FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_gender
# MAGIC group by sex

# COMMAND ----------

# MAGIC %md
# MAGIC ### DOB

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent non-missing date of birth
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_temp_pop_demo_dob
# MAGIC AS
# MAGIC SELECT
# MAGIC   person_id_deid,
# MAGIC   YEAR_MONTH_OF_BIRTH
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC      *,
# MAGIC      row_number() OVER(PARTITION BY NHS_NUMBER_DEID ORDER BY date DESC) rn
# MAGIC     FROM
# MAGIC       dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive a
# MAGIC       INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC       ON a.NHS_NUMBER_DEID == b.person_id_deid
# MAGIC       WHERE ProductionDate = '2021-11-26 14:02:40.645948' AND YEAR_MONTH_OF_BIRTH is not null
# MAGIC     )
# MAGIC WHERE
# MAGIC   rn = 1

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_dob") 
create_table("ccu019_temp_pop_demo_dob") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_dob LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ethnicity CCU037 - latest from Marta

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dars_nic_391419_j3w9t_collab.ccu037_cohort_gdppr_wp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ethnicity_5_group, count(ethnicity_5_group) / 58162316 FROM dars_nic_391419_j3w9t_collab.ccu037_cohort_gdppr_wp a
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.NHS_NUMBER_DEID == b.person_id_deid
# MAGIC group by ethnicity_5_group

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Ethnicity CCU037

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dars_nic_391419_j3w9t_collab.ccu037_ethnicity_assembled

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Rows: 85903191 - uniq ids 85903191
# MAGIC SELECT count(*) FROM dars_nic_391419_j3w9t_collab.ccu037_ethnicity_assembled a
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.NHS_NUMBER_DEID == b.person_id_deid LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Rows: 85903191 - uniq ids 85903191
# MAGIC SELECT ETHNIC_GROUP, count(ETHNIC_GROUP) FROM dars_nic_391419_j3w9t_collab.ccu037_ethnicity_assembled a
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.NHS_NUMBER_DEID == b.person_id_deid
# MAGIC group by ETHNIC_GROUP
# MAGIC sort by ETHNIC_GROUP

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent non-missing date of birth
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_temp_pop_demo_ethnicity
# MAGIC AS
# MAGIC SELECT
# MAGIC   person_id_deid,
# MAGIC   ETHNIC_GROUP as ethnic_group
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu037_ethnicity_assembled a
# MAGIC   INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC   ON a.NHS_NUMBER_DEID == b.person_id_deid

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_ethnicity") 
create_table("ccu019_temp_pop_demo_ethnicity") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ethnicity skinnyRecord

# COMMAND ----------

ethnic_skinny = spark.sql(f'''
SELECT
  person_id_deid,
  ETHNIC as ethnic_cat
    FROM
        dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record a
        INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
         ON a.NHS_NUMBER_DEID == b.person_id_deid
         ''')

# COMMAND ----------

# From HES APC Data Dictionary (ONS 2011 census categories)
dict_ethnic = """
Code,Group,Description
A,White,British (White)
B,White,Irish (White)
C,White,Any other White background
D,Mixed,White and Black Caribbean (Mixed)
E,Mixed,White and Black African (Mixed)
F,Mixed,White and Asian (Mixed)
G,Mixed,Any other Mixed background
H,Asian or Asian British,Indian (Asian or Asian British)
J,Asian or Asian British,Pakistani (Asian or Asian British)
K,Asian or Asian British,Bangladeshi (Asian or Asian British)
L,Asian or Asian British,Any other Asian background
M,Black or Black British,Caribbean (Black or Black British)
N,Black or Black British,African (Black or Black British)
P,Black or Black British,Any other Black background
R,Chinese,Chinese (other ethnic group)
S,Other,Any other ethnic group
Z,Unknown,Unknown
X,Unknown,Unknown
99,Unknown,Unknown
0,White,White
1,Black or Black British,Black - Caribbean
2,Black or Black British,Black - African
3,Black or Black British,Black - Other
4,Asian or Asian British,Indian
5,Asian or Asian British,Pakistani
6,Asian or Asian British,Bangladeshi
7,Chinese,Chinese
8,Other,Any other ethnic group
9,Unknown,Unknown
"""

import io
import pandas as pd

dict_ethnic = pd.read_csv(io.StringIO(dict_ethnic))
dict_ethnic = dict_ethnic.rename(columns={"Code" : "code",
                                          "Group" : "ethnic_group",
                                          "Description" : "ethnicity"})

# COMMAND ----------

# Convert to dictionary for mapping
mapping = dict(zip(dict_ethnic['code'], dict_ethnic['ethnic_group']))

# COMMAND ----------

# Map
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])

ethnic_skinny = ethnic_skinny.withColumn("ethnic_group", mapping_expr[col("ethnic_cat")]).drop("ethnic_cat")

# COMMAND ----------

ethnic_skinny.createOrReplaceGlobalTempView('ccu019_temp_pop_demo_ethnicity_skinny')
drop_table("ccu019_temp_pop_demo_ethnicity_skinny") 
create_table("ccu019_temp_pop_demo_ethnicity_skinny") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ethnic_group, count(ethnic_group), count(ethnic_group)/58162316 * 100 FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_ethnicity_skinny
# MAGIC group by ethnic_group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ethnic_group, count(ethnic_group), count(ethnic_group)/58162316 * 100 FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_ethnicity
# MAGIC group by ethnic_group

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ethnicity gdppr

# COMMAND ----------

ethnic_gdppr = spark.sql(f'''
SELECT
  person_id_deid,
  ETHNIC as ethnic_cat
FROM
  (
    SELECT
     *,
     row_number() OVER(PARTITION BY NHS_NUMBER_DEID ORDER BY date DESC) rn
    FROM
      dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive a
      INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
      ON a.NHS_NUMBER_DEID == b.person_id_deid
      WHERE ProductionDate = '2021-11-26 14:02:40.645948' AND YEAR_MONTH_OF_BIRTH is not null
    )
WHERE
rn = 1''')

# COMMAND ----------

# From HES APC Data Dictionary (ONS 2011 census categories)
dict_ethnic = """
Code,Group,Description
A,White,British (White)
B,White,Irish (White)
C,White,Any other White background
D,Mixed,White and Black Caribbean (Mixed)
E,Mixed,White and Black African (Mixed)
F,Mixed,White and Asian (Mixed)
G,Mixed,Any other Mixed background
H,Asian or Asian British,Indian (Asian or Asian British)
J,Asian or Asian British,Pakistani (Asian or Asian British)
K,Asian or Asian British,Bangladeshi (Asian or Asian British)
L,Asian or Asian British,Any other Asian background
M,Black or Black British,Caribbean (Black or Black British)
N,Black or Black British,African (Black or Black British)
P,Black or Black British,Any other Black background
R,Chinese,Chinese (other ethnic group)
S,Other,Any other ethnic group
Z,Unknown,Unknown
X,Unknown,Unknown
99,Unknown,Unknown
0,White,White
1,Black or Black British,Black - Caribbean
2,Black or Black British,Black - African
3,Black or Black British,Black - Other
4,Asian or Asian British,Indian
5,Asian or Asian British,Pakistani
6,Asian or Asian British,Bangladeshi
7,Chinese,Chinese
8,Other,Any other ethnic group
9,Unknown,Unknown
"""

import io
import pandas as pd

dict_ethnic = pd.read_csv(io.StringIO(dict_ethnic))
dict_ethnic = dict_ethnic.rename(columns={"Code" : "code",
                                          "Group" : "ethnic_group",
                                          "Description" : "ethnicity"})

# COMMAND ----------

# Convert to dictionary for mapping
mapping = dict(zip(dict_ethnic['code'], dict_ethnic['ethnic_group']))

# COMMAND ----------

# Map
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])

ethnic_gdppr = ethnic_gdppr.withColumn("ethnic_group", mapping_expr[col("ethnic_cat")]).drop("ethnic_cat")

# COMMAND ----------

ethnic_gdppr.createOrReplaceGlobalTempView('ccu019_temp_pop_demo_ethnicity_gdppr')

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_ethnicity_gdppr") 
create_table("ccu019_temp_pop_demo_ethnicity_gdppr") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), count( distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_ethnicity_gdppr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ethnic_group, count(ethnic_group), count(ethnic_group)/58162316 * 100 FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_ethnicity_gdppr
# MAGIC group by ethnic_group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) /58162316 * 100 FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_ethnicity_gdppr
# MAGIC WHERE ethnic_group is NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### High-risk

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent non-missing date of birth
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_temp_pop_demo_hr
# MAGIC AS
# MAGIC SELECT distinct person_id_deid, max(high_risk) as high_risk
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT
# MAGIC      person_id_deid,
# MAGIC      CASE WHEN CODE = 1300561000000107 THEN 1 
# MAGIC           WHEN CODE is NULL THEN 0
# MAGIC           ELSE 0 End as high_risk
# MAGIC     FROM
# MAGIC       dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive a
# MAGIC       INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC       ON a.NHS_NUMBER_DEID == b.person_id_deid
# MAGIC       WHERE ProductionDate = '2021-11-26 14:02:40.645948'
# MAGIC )
# MAGIC group by person_id_deid

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_hr") 
create_table("ccu019_temp_pop_demo_hr") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT high_risk, count(high_risk) FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_hr
# MAGIC group by high_risk

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_hr

# COMMAND ----------

# MAGIC %md
# MAGIC ### Death all

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC dars_nic_391419_j3w9t_collab.ccu019_tmp_deaths

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent non-missing date of birth
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_temp_pop_demo_death_all
# MAGIC AS
# MAGIC SELECT
# MAGIC   person_id_deid,
# MAGIC   death_date
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC      a.person_id_deid,
# MAGIC      death_date,
# MAGIC      row_number() OVER(PARTITION BY a.person_id_deid ORDER BY death_date DESC) rn
# MAGIC     FROM
# MAGIC       dars_nic_391419_j3w9t_collab.ccu019_tmp_deaths a
# MAGIC       INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC       ON a.person_id_deid == b.person_id_deid
# MAGIC       WHERE death_date <= "2021-30-11"
# MAGIC     )
# MAGIC WHERE
# MAGIC   rn = 1

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_death_all") 
create_table("ccu019_temp_pop_demo_death_all") 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 880041 rows and 
# MAGIC -- 880041 distinct IDs
# MAGIC SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_death_all 

# COMMAND ----------

# MAGIC %md
# MAGIC ### COVID phenotypes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All rows           13,754,403
# MAGIC -- Before 2021-11-30:  8,675,328 
# MAGIC SELECT count(*) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events
# MAGIC where date_first < "2021-11-30"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events a
# MAGIC ANTI JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC where date_first < "2021-11-30" and death_covid == 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All rows           13,754,403
# MAGIC -- Before 2021-11-30:  8,675,328 
# MAGIC 
# MAGIC -- COVID deaths whith INNER JOIN with our CCU019 population cohort = 141287
# MAGIC -- COVID deaths with NO INNER JOIN to our population cohort        = 158793
# MAGIC SELECT count(distinct c.person_id_deid) FROM (dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr) as c
# MAGIC INNER JOIN (
# MAGIC SELECT a.person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events a
# MAGIC ANTI JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC where date_first < "2021-11-30" and death_covid == 1) as d
# MAGIC ON c.person_id_deid == d.person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All rows           13,754,403
# MAGIC -- Before 2021-11-30:  8,675,328 
# MAGIC 
# MAGIC -- COVID deaths whith INNER JOIN with our CCU019 population cohort = 141287
# MAGIC -- COVID deaths with NO INNER JOIN to our population cohort        = 158793
# MAGIC SELECT distinct c.person_id_deid FROM (dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr) as c
# MAGIC INNER JOIN (
# MAGIC SELECT a.person_id_deid FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events a
# MAGIC ANTI JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC where date_first < "2021-11-30" and death_covid == 1) as d
# MAGIC ON c.person_id_deid == d.person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All rows           13,754,403
# MAGIC -- Before 2021-11-30:  8,675,328 
# MAGIC 
# MAGIC -- COVID deaths whith INNER JOIN with our CCU019 population cohort = 141287
# MAGIC -- COVID deaths with NO INNER JOIN to our population cohort        = 158793
# MAGIC SELECT severity, count(severity) FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events a
# MAGIC ANTI JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.person_id_deid == b.person_id_deid
# MAGIC where date_first < "2021-11-30" and death_covid == 1
# MAGIC group by severity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent non-missing date of birth
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_temp_pop_demo_covid
# MAGIC AS
# MAGIC SELECT
# MAGIC   a.person_id_deid,
# MAGIC   death_covid,
# MAGIC   severity
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_covid_events a
# MAGIC   INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC   ON a.person_id_deid == b.person_id_deid
# MAGIC   WHERE date_first < "2021-11-30"

# COMMAND ----------

drop_table("ccu019_temp_pop_demo_covid") 
create_table("ccu019_temp_pop_demo_covid") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_covid

# COMMAND ----------

# MAGIC %md
# MAGIC # Create merged demographics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_paper_population_cohort_demographics
# MAGIC AS
# MAGIC SELECT a.person_id_deid, death_covid, 
# MAGIC CASE WHEN death_date IS null THEN 0 ELSE 1 END as death_all,
# MAGIC death_date, severity, sex, 
# MAGIC round(datediff("2020-01-23", CONCAT(YEAR_MONTH_OF_BIRTH,'-01')) / 365.25, 0) as age,
# MAGIC ethnic_group, IMD_quintile, YEAR_MONTH_OF_BIRTH,
# MAGIC rare_disease,
# MAGIC high_risk
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort a
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_gender b ON a.person_id_deid == b.person_id_deid      --- 58162316
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_dob c ON a.person_id_deid == c.person_id_deid         --- 58162316
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_imd d ON a.person_id_deid == d.person_id_deid         --- 58133152
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_covid f ON a.person_id_deid == f.person_id_deid       --- 8086751
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_death_all g ON a.person_id_deid == g.person_id_deid   --- 880041
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_ethnicity_skinny h ON a.person_id_deid == h.person_id_deid   --- 58162316 
# MAGIC LEFT JOIN (select distinct person_id_deid, 1 AS rare_disease FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease) i ON a.person_id_deid == i.person_id_deid --- !!!
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu019_temp_pop_demo_hr j ON a.person_id_deid == j.person_id_deid          --- 58162316

# COMMAND ----------

drop_table("ccu019_paper_population_cohort_demographics")
create_table("ccu019_paper_population_cohort_demographics")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics --- 58162316
# MAGIC --- SELECT count(*), count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics WHERE rare_disease == 1 --- 894396

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT death_covid, count(death_covid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics
# MAGIC group by death_covid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ethnic_group, count(ethnic_group), count(ethnic_group)/58162316 * 100 FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics
# MAGIC group by ethnic_group

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up

# COMMAND ----------

#drop_table("ccu019_temp_pop_demo_imd")
#drop_table("ccu019_temp_pop_demo_gender") 
#drop_table("ccu019_temp_pop_demo_dob") 
#drop_table("ccu019_temp_pop_demo_ethnicity") 
#drop_table("ccu019_temp_pop_demo_hr")
#drop_table("ccu019_temp_pop_death_all")
#drop_table("ccu019_temp_pop_demo_covid") 
