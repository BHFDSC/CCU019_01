# Databricks notebook source
# MAGIC %md
# MAGIC # CCU019_HZ_Covid_Survival_Events
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * Create a table with time-to-event data for survival analysis
# MAGIC 
# MAGIC **Project(s)** CCU0019
# MAGIC  
# MAGIC **Author(s)** Huayu Zhang
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2022-05-27
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** See running record
# MAGIC  
# MAGIC **Data input**  
# MAGIC `ccu013_covid_trajectory`
# MAGIC `curr302_patient_skinny_record`
# MAGIC `ccu013_vaccine_status`
# MAGIC `ccu019_hz_01_cohort`
# MAGIC 
# MAGIC **Data output**
# MAGIC `ccu019_hz_01_event`
# MAGIC 
# MAGIC **Software and versions** SQL, python, pyspark
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/covid_survival_rev1/CCU019_HZ_Covid_Survival_Config

# COMMAND ----------

from pprint import pprint

config = pipelines['pipeline_exact']['02_event']

pprint(config)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("app") \
    .getOrCreate()

spark.sql("use %s" % config['params']['database_name'])

# COMMAND ----------

# Load pheno data, inner with cohort date 

pheno = spark.sql(sql_select_metacols(config['input']['pheno']))
censor = spark.sql(sql_select_metacols(config['input']['censor']))
cohort = spark.sql(sql_select_metacols(config['input']['cohort'])).filter(F.col('cohort')).drop('cohort')

# Ids in vacc are not unique, keep the earliest dates for double vaccination
from pyspark.sql.window import Window

window = Window.partitionBy('person_id_deid').orderBy('date_vacc')
vacc = spark.sql(sql_select_metacols(config['input']['vaccination'])).filter(F.col('DOSE_SEQUENCE') == 2).drop('DOSE_SEQUENCE')\
                 .withColumn('row', F.row_number().over(window)).filter(F.col('row') == 1).drop('row')


pheno = pheno.join(cohort, pheno.person_id_deid == cohort.NHS_NUMBER_DEID, 'inner').drop("NHS_NUMBER_DEID")

# COMMAND ----------

# latest_pheno = pheno.agg({"date": "max"}).collect()[0]
# latest_censor = censor.agg({"date_censor": "max"}).collect()[0]
# latest_vacc = vacc.agg({"date_vacc": "max"}).collect()[0]

# print("""
# Latest event: %s
# Latest censor: %s
# Latest vaccination: %s
# """ % (
#   latest_pheno,
#   latest_censor,
#   latest_vacc
# ))

# earliest_pheno = pheno.agg({"date": "min"}).collect()[0]
# earliest_censor = censor.agg({"date_censor": "min"}).collect()[0]
# earliest_vacc = vacc.agg({"date_vacc": "min"}).collect()[0]

# print("""
# Earliest event: %s
# Earliest censor: %s
# Earliest vaccination: %s
# """ % (
#   earliest_pheno,
#   earliest_censor,
#   earliest_vacc
# ))

# COMMAND ----------

pheno = pheno.filter(
      F.when((F.col('date') >= config['params']['study_start_date']) & (F.col('date') < config['params']['study_end_date']), 
             True).otherwise(False)
)

vacc = vacc.filter(
      F.when((F.col('date_vacc') >= config['params']['first_vaccination']) & (F.col('date_vacc') < config['params']['study_end_date']), 
             True).otherwise(False)
)



# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy('person_id_deid').orderBy('date')

# COMMAND ----------

# Create event table and first pheno date for all distinct individuals

event = pheno.withColumn('row', F.row_number().over(window)) \
  .filter(F.col('row') == 1).select('person_id_deid', pheno.covid_phenotype.alias('pheno_start'), pheno.date.alias('date_start'))

# Join the first event

event = event.join(
  pheno.filter(pheno.covid_phenotype.isin(config['params']['event_included'])) \
    .withColumn('row', F.row_number().over(window)) \
    .filter(F.col('row') == 1).select('person_id_deid', pheno.covid_phenotype.alias('pheno_event'), pheno.date.alias('date_event')), 
  'person_id_deid', 'left')

event = event.withColumn('time_diff_event', F.datediff('date_event', 'date_start'))

# Join the censor

event = event.join(
  censor, event.person_id_deid == censor.NHS_NUMBER_DEID, 'left'
).drop('NHS_NUMBER_DEID')

event = event.withColumn('time_diff_censor', F.datediff('date_censor', 'date_start'))

# Join the vacc

event = event.join(vacc, 'person_id_deid', 'left')

event = event.withColumn('time_diff_vacc2', F.datediff('date_vacc', 'date_start'))

event = event.withColumn('in_wave1', 
                         F.when((event.date_start >= config['params']['wave_1_start_date']) & (event.date_start < config['params']['wave_2_start_date']), 
                                True).otherwise(False))

event = event.withColumn('in_wave2', 
                         F.when((event.date_start >= config['params']['wave_2_start_date']) & (event.date_start <= config['params']['study_end_date']), 
                                True).otherwise(False))

event = event.withColumn('vacc2_before_event',
                        F.when((event.time_diff_vacc2 > -14) | F.isnull(event.time_diff_vacc2), False).otherwise(True))

event = event.withColumn('wave2_vacced',
                        F.when(event.in_wave2 & event.vacc2_before_event, True).otherwise(False)
                        )

event = event.withColumn('wave2_novac',
                        F.when(event.in_wave2 & ~event.vacc2_before_event, True).otherwise(False)
                        )

event = event.withColumn('event_group', 
                          F.when(event.in_wave1, 'wave1').otherwise(
                            F.when(event.wave2_novac, 'wave2_novac').otherwise('wave2_vacced')
                          )
                        )

# COMMAND ----------

# Calculate futime and save futime within the follow up period

from pyspark.sql.types import IntegerType

def get_futime(time_diff_event, time_diff_censor):
  if time_diff_event is not None:
    return time_diff_event
  elif time_diff_censor is not None:
    return time_diff_censor
  else:
    return 30 # Strange behaviour of pyspark here, have to manually set this
  
get_futime_udf = F.udf(get_futime, IntegerType())

event = event.withColumn('futime', get_futime_udf('time_diff_event', 'time_diff_censor'))

event = event.filter((event.futime >= 0) & (event.futime <=config['params']['follow_up_length']))

# Add has_event column

event = event.withColumn('has_event', event.date_event.isNotNull())

# COMMAND ----------

# Save the table

drop_table("ccu019_hz_01_event")

event.createOrReplaceGlobalTempView(config['output']['event']['name'])
create_table(config['output']['event']['name'])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from ccu019_hz_01_event
# MAGIC limit 10
