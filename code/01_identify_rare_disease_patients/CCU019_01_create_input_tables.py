# Databricks notebook source
# MAGIC %md
# MAGIC  # CCU019 Create Table Input
# MAGIC  
# MAGIC **Description** 
# MAGIC   
# MAGIC This notebook creates subsetted version for each of the main datasets required for the identification of rare-disease patients
# MAGIC   
# MAGIC It can be sourced for another notebook using:  
# MAGIC   
# MAGIC   `dbutils.notebook.run("./ccu019_01_create_input_table_aliases", 30000) # timeout_seconds`  
# MAGIC   
# MAGIC This is advantageous to using `%run ./ccu019_01_create_input_table` because it doesn't include the markdown component (although you can just click "hide result" on the cell with `%run`) 
# MAGIC 
# MAGIC **Project(s)** CCU019
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen, Chris Tomlinson (inspired by Sam Hollings!)
# MAGIC  
# MAGIC **Data input:** This notebook uses the archive tables made by the data wranglers - selecting the latest data by `productionDate`. The `productionDate` variabel is carried forward to master_phenotype in the `ccu19_tmp_gdppr` table, and will be saved in the main output tables; trajectory, severity and events, to ensure the data for the produced phenotypes is back tracable to source, for reproducability. 
# MAGIC 
# MAGIC **Data output**
# MAGIC * `ccu019_tmp_sgss`
# MAGIC * `ccu019_tmp_gdppr`
# MAGIC * `ccu019_tmp_deaths`
# MAGIC * `ccu019_tmp_sus`
# MAGIC * `ccu019_tmp_apc`
# MAGIC * `ccu019_tmp_cc`
# MAGIC * `ccu019_snomed_codes_covid19` 
# MAGIC * `ccu019_tmp_chess`
# MAGIC * `ccu019_vaccine_status`
# MAGIC 
# MAGIC **Software and versions** `sql`, `python`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Subseting all source tables by dates

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find latest production date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct * FROM
# MAGIC dars_nic_391419_j3w9t_collab.wrang002b_data_version_batchids
# MAGIC order by ProductionDate DESC

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring, regexp_replace
from datetime import datetime
from pyspark.sql.types import DateType

production_date = '2021-11-26 14:02:40.645948'
start_date = '2020-01-01' # HW: For rare diseases, we might not want to set a constraint on start_date for our phenotype definitions - so ignored in selected tables below. See those inline comments
end_date = '2021-12-02' # The maximal date covered by all sources.

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 GDPPR

# COMMAND ----------

# GDPPR
gdppr = spark.sql(f'''SELECT DISTINCT NHS_NUMBER_DEID, DATE, CODE, ProductionDate FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
                     WHERE ProductionDate == "{production_date}"''')
gdppr = gdppr.withColumnRenamed('DATE', 'date').withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid').withColumnRenamed('CODE', 'code')
gdppr = gdppr.withColumn('date_is', lit('DATE'))
# the following is commented out by Honghan as we won't need start_date for rare diseases
# gdppr = gdppr.filter((gdppr['date'] >= start_date) & (gdppr['date'] <= end_date))
gdppr = gdppr.filter(gdppr['date'] <= end_date)

gdppr = gdppr.filter(gdppr['person_id_deid'].isNotNull())
gdppr.createOrReplaceGlobalTempView('ccu019_tmp_gdppr')
#display(gdppr)
drop_table("ccu019_tmp_gdppr")
create_table("ccu019_tmp_gdppr") 

# COMMAND ----------

display(gdppr)

# COMMAND ----------

# MAGIC %sql --- 2021-12-02
# MAGIC SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Deaths

# COMMAND ----------

# Deaths
death = spark.sql(f'''SELECT * FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
                      WHERE ProductionDate == "{production_date}"''')
death = death.withColumn("death_date", to_date(death['REG_DATE_OF_DEATH'], "yyyyMMdd"))
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'person_id_deid')
death = death.withColumn('date_is', lit('REG_DATE_OF_DEATH'))
death = death.filter((death['death_date'] >= start_date) & (death['death_date'] <= end_date))
death = death.filter(death['person_id_deid'].isNotNull())
death.createOrReplaceGlobalTempView('ccu019_tmp_deaths')

drop_table("ccu019_tmp_deaths")
create_table("ccu019_tmp_deaths")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(death_date) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_deaths

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu019_tmp_deaths

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 HES OP

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC                     dars_nic_391419_j3w9t_collab.hes_op_all_years_archive

# COMMAND ----------

# HES APC with suspected or confirmed COVID-19
op = spark.sql(f'''SELECT PERSON_ID_DEID, APPTDATE, DIAG_4_CONCAT, OPERTN_4_CONCAT FROM
                    dars_nic_391419_j3w9t_collab.hes_op_all_years_archive
                    WHERE ProductionDate == "{production_date}" '''
                    # HW: do NOT put a constraint on diag
                    # AND
                    # DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%"'''
              )
op = op.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('APPTDATE', 'date')
op = op.withColumn('date_is', lit('APPTDATE'))
# the following is commented out by Honghan as we won't need start_date for rare diseases
# op = op.filter((op['date'] >= start_date) & (op['date'] <= end_date))
op = op.filter(op['date'] <= end_date) # replace it with end_date constraint only

# op = op.filter(apc['person_id_deid'].isNotNull())
op.createOrReplaceGlobalTempView('ccu019_tmp_op')
#display(apc)
drop_table("ccu019_tmp_op")
create_table("ccu019_tmp_op")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_op

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu019_tmp_op

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 HES APC

# COMMAND ----------

# HES APC with suspected or confirmed COVID-19
apc = spark.sql(f'''SELECT PERSON_ID_DEID, EPISTART, DIAG_4_CONCAT, OPERTN_4_CONCAT, DISMETH, DISDEST, DISDATE, SUSRECID FROM
                    dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
                    WHERE ProductionDate == "{production_date}" '''
                    # HW: dot NOT put a constraint on COVID-19 diagnosis
                    # AND
                    # DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%"'''
               )
apc = apc.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('EPISTART', 'date')
apc = apc.withColumn('date_is', lit('EPISTART'))
# the following is commented out by Honghan as we won't need start_date for rare diseases
# apc = apc.filter((apc['date'] >= start_date) & (apc['date'] <= end_date))
apc = apc.filter(apc['date'] <= end_date) # replace it with end_date constraint only

apc = apc.filter(apc['person_id_deid'].isNotNull())
apc.createOrReplaceGlobalTempView('ccu019_tmp_apc')
#display(apc)
drop_table("ccu019_tmp_apc")
create_table("ccu019_tmp_apc")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_apc

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu019_tmp_apc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 HES CC

# COMMAND ----------

# HES CC
cc = spark.sql(f'''SELECT * FROM dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive
              WHERE ProductionDate == "{production_date}"''')
cc = cc.withColumnRenamed('CCSTARTDATE', 'date').withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
cc = cc.withColumn('date_is', lit('CCSTARTDATE'))
# reformat dates for hes_cc as currently strings
asDate = udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
cc = cc.filter(cc['person_id_deid'].isNotNull())
cc = cc.withColumn('date', asDate(col('date')))
cc = cc.filter((cc['date'] >= start_date) & (cc['date'] <= end_date))
cc.createOrReplaceGlobalTempView('ccu019_tmp_cc')
#display(cc)
drop_table("ccu019_tmp_cc")
create_table("ccu019_tmp_cc")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_cc

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC OPTIMIZE dars_nic_391419_j3w9t_collab.ccu019_tmp_cc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 Vaccination status

# COMMAND ----------

vaccine = spark.sql(f'''SELECT PERSON_ID_DEID, DOSE_SEQUENCE, DATE_AND_TIME, ProductionDate FROM dars_nic_391419_j3w9t_collab.vaccine_status_dars_nic_391419_j3w9t_archive 
                        WHERE ProductionDate == "{production_date}"''')
vaccine = vaccine.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
# pillar2 = pillar2.withColumn('date', substring('date', 0, 10))
vaccine = vaccine.withColumn('date',substring('DATE_AND_TIME', 0,8))
vaccine = vaccine.withColumn('date', to_date(vaccine['date'], "yyyyMMdd"))
vaccine = vaccine.filter((vaccine['date'] >= start_date) & (vaccine['date'] <= end_date))
vaccine = vaccine.withColumn('date_is', lit('DATE_AND_TIME'))
vaccine = vaccine.select('person_id_deid', 'date', 'DOSE_SEQUENCE', 'ProductionDate')
vaccine.createOrReplaceGlobalTempView('ccu019_vaccine_status')
drop_table("ccu019_vaccine_status")
create_table("ccu019_vaccine_status")
display(vaccine)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM dars_nic_391419_j3w9t_collab.ccu019_vaccine_status

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_deaths

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_op

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_apc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_cc
