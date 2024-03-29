._CCU019_HZ_02_Covid_Survival_Events.py                                                             000644  000767  000024  00000000470 14430646447 020563  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2       8                                      ATTR      8   �   �                  �   H  com.apple.macl      �   <  com.apple.quarantine  �[�n~�LF��R���P                                                      q/0083;64634d27;Safari;588E1D89-B179-4DF1-A5B4-AF9AB072A86E                                                                                                                                                                                                         PaxHeader/CCU019_HZ_02_Covid_Survival_Events.py                                                     000644  000767  000024  00000000036 14430646447 022315  x                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         30 mtime=1684229415.054316299
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CCU019_HZ_02_Covid_Survival_Events.py                                                               000644  000767  000024  00000015375 14430646447 020360  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         # Databricks notebook source
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
                                                                                                                                                                                                                                                                   ._CCU019_HZ_03_Covid_Survival_RD-Matching_exact.py                                                  000644  000767  000024  00000000470 14430646451 022534  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2       8                                      ATTR      8   �   �                  �   H  com.apple.macl      �   <  com.apple.quarantine  �[�n~�LF��R���P                                                      q/0083;64634d29;Safari;86144A94-3668-412D-821B-20847C1C8579                                                                                                                                                                                                         PaxHeader/CCU019_HZ_03_Covid_Survival_RD-Matching_exact.py                                          000644  000767  000024  00000000036 14430646451 024266  x                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         30 mtime=1684229417.208637604
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CCU019_HZ_03_Covid_Survival_RD-Matching_exact.py                                                    000644  000767  000024  00000013647 14430646451 022331  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         # Databricks notebook source
# MAGIC %md
# MAGIC # CCU019_HZ_03_Covid_Survival_RD-Matching_exact
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * Make matched cohorts for rare diseases based on metadata
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
# MAGIC `ccu019_paper_cohort_rare_disease`
# MAGIC `ccu019_hz_01_cohort`
# MAGIC `curr302_patient_skinny_record`
# MAGIC `ccu019_hz_01_event`
# MAGIC `ccu037_ethnicity_assembled`
# MAGIC 
# MAGIC **Data output**
# MAGIC `ccu019_hz_01_metadata`
# MAGIC `ccu019_hz_01_match_exact`
# MAGIC 
# MAGIC **Software and versions** SQL, python, pyspark 3
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/covid_survival_rev1/CCU019_HZ_Covid_Survival_Config

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/CCU019_HZ_matching_function

# COMMAND ----------

from tqdm import tqdm
from pprint import pprint

config = pipelines['pipeline_exact']['03_match']

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

# Load rare disease identification table and attach event

rd = spark.sql(sql_select_metacols(config['input']['rd']))

# RD table might not be distinct, select the distinct rows
rd = rd.distinct()

cohort = spark.sql(sql_select_metacols(config['input']['cohort'])).filter(F.col('cohort'))

# Select those in cohort
rd = rd.join(cohort, rd.person_id_deid == cohort.NHS_NUMBER_DEID, 'inner')

# COMMAND ----------

# Get and join metatables

demo = spark.sql(sql_select_metacols(config['input']['demo']))
event = spark.sql(sql_select_metacols(config['input']['event']))
ethnic = spark.sql(sql_select_metacols(config['input']['ethnic']))


meta = event.join(demo, event.person_id_deid == demo.NHS_NUMBER_DEID, 'left').drop('NHS_NUMBER_DEID')
meta = meta.join(ethnic, meta.person_id_deid == ethnic.NHS_NUMBER_DEID, 'left').drop('NHS_NUMBER_DEID')
meta = meta.withColumn('age_start', F.floor(F.datediff('date_start', 'DATE_OF_BIRTH')/365.25))

# COMMAND ----------

meta_pdf = meta.select(['person_id_deid'] + config['params']['meta_cols_to_query']).toPandas()
rd_pdf = rd.select('person_id_deid', config['params']['rd_identifier']).toPandas()

# COMMAND ----------

meta_pdf['age_group'] = pd.cut(meta_pdf['age_start'], bins=config['params']['bins_age_group'], labels=config['params']['labels_age_group'])

meta_pdf.dropna(subset=['age_group'], inplace=True)
meta_pdf.drop(columns=['age_start'], inplace=True)

# COMMAND ----------

diseases = rd_pdf[config['params']['rd_identifier']].unique()

# COMMAND ----------

matched_dfs = []
not_successful = []


for disease in tqdm(diseases):
  try:
    pdf = pd.merge(left=meta_pdf, right=rd_pdf[rd_pdf[config['params']['rd_identifier']] == disease], on='person_id_deid', how='left')
    pdf[config['params']['rd_identifier']] = ~pd.isna(pdf[config['params']['rd_identifier']])

    matcher = ExactMatch(data=pdf, y_var=config['params']['rd_identifier'], id_var='person_id_deid')
    matcher.match(ratio=2)
    matched_df = matcher.get_id_group_mapping()
    matched_df['disease'] = disease

    matched_dfs.append(matched_df)
  except:
    not_successful.append(disease)

# COMMAND ----------

rd_pdf.phenotype[rd_pdf.phenotype.isin(not_successful)].value_counts()

# COMMAND ----------

pdf_all_matched = pd.concat(matched_dfs)

# COMMAND ----------

meta.createOrReplaceGlobalTempView(config['output']['meta']['name'])
meta = meta.withColumn('production_timestamp_hz', F.current_timestamp())

drop_table(config['output']['meta']['name'])
create_table(config['output']['meta']['name'])

# COMMAND ----------

df = spark.createDataFrame(pdf_all_matched)
df = df.withColumn('production_timestamp_hz', F.current_timestamp())

drop_table(config['output']['match']['name'])

df.createOrReplaceGlobalTempView(config['output']['match']['name'])
create_table(config['output']['match']['name'])

# COMMAND ----------

config['output']['match']['name']

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Confirm the results below

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from ccu019_hz_01_match_exact
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.disease, t1.case_count, t2.ctrl_count from (
# MAGIC (
# MAGIC   select disease, count(disease) as case_count
# MAGIC   from ccu019_hz_01_match_exact
# MAGIC   where matched_group
# MAGIC   group by disease
# MAGIC ) as t1
# MAGIC inner join 
# MAGIC (
# MAGIC   select disease, count(disease) as ctrl_count
# MAGIC   from ccu019_hz_01_match_exact
# MAGIC   where not matched_group
# MAGIC   group by disease
# MAGIC   order by ctrl_count desc
# MAGIC ) as t2
# MAGIC on t1.disease == t2.disease
# MAGIC )
# MAGIC order by case_count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.disease, t1.case_count, t2.ctrl_count from (
# MAGIC (
# MAGIC   select disease, count(disease) as case_count
# MAGIC   from ccu019_hz_01_match_exact
# MAGIC   where matched_group
# MAGIC   group by disease
# MAGIC ) as t1
# MAGIC inner join 
# MAGIC (
# MAGIC   select disease, count(disease) as ctrl_count
# MAGIC   from ccu019_hz_01_match_exact
# MAGIC   where not matched_group
# MAGIC   group by disease
# MAGIC   order by ctrl_count desc
# MAGIC ) as t2
# MAGIC on t1.disease == t2.disease
# MAGIC )
# MAGIC where t1.disease == "Congenital hydrocephalus"

# COMMAND ----------


                                                                                         ._CCU019_HZ_04_Covid_Survival_Analysis_exact.py                                                     000644  000767  000024  00000004110 14430646454 022261  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2       H                                      ATTR      H   �  d                  �   H  com.apple.macl     ,    %com.apple.metadata:kMDItemWhereFroms   0     com.apple.quarantine  �[�n~�LF��R���P                                                      bplist00�_4https://nhsd-dspp-dae-prod-data-out.s3.eu-west-2.amazonaws.com/AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/CCU019_HZ_04_Covid_Survival_Analysis_exact.py?response-content-disposition=attachment&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAIaCXVzLWVhc3QtMSJHMEUCIQCQGaQW354G6C33ZNSgG9IzndJtOrwYmDUifj5Ddc8PLgIgFFIiEWj28Vvue0NHNXIgKvBS8tVY4TqVvxUhoaFZYO0qnQMIKxADGgwyMTU4NjQ1MjE0NzAiDFGdRnjUyIh86wv15Cr6Ag0oyqnbX5M4IzQgNkY45jIAVWCmr%2FfbCDGfgpo6SxmxkrRl%2BrhIZAz5ywrls3BurlavK%2Fz7Z91eLICL%2BsP84OdH%2Bldb9Uic%2F7k7SHYyh91Les9N2tmeXGzdBw1iQq5GAbxXDe47a5c9pedBOG4tZYREGJjTOfpBDypO8c2gfQCvQYZ%2BHHANRnGY8g%2F3r5%2FzT6sR%2Ft20iiN13ozXg8u%2FJE3G3b%2B2qcIY%2BRfd88yu6uLZSseTP5LIHrjXXp%2BAqeR%2Bi3GpVMmqJerFI2b0HtW5kCuaGMU6jVmCgU2e%2FhPDOsU6BpbxdfejOue3S10fJItq4%2FRTEAEqK9WMrCBSB0679PJ8QjySLpWSTtoEaBK8F9mPODD0TUiWe7b4FBycgphNMbr4QbGMsWM5ezTLLsjrNrJdFc8c3tAvKfIcwVXyl%2F7rRp8Fgg3pnma%2BCRynZrmMeekhgAI9tgzc6lHvz%2Fandyq3HhLI6jFQ2CIOVGXddIQm2oIK40oR4sDNSDDsmI2jBjqMAh3bkesO1BCVxxgdbtN%2FzsQ%2Bg7xsvdHKIRC3nD4qxCpQoOK5p8BW3pr4LXTFJ1CTgBdLluErFXHprLwQVVOBIPn4dVm6LqbL%2FNG1TySgQSeN0CMV1WBad%2F%2B0pKOesyLunA%2BGt8c72Qjo77Pnt9Wu8kNxM0QK2LL2ODhswlRjRjjwZdkBUgmVVkQWph6yJJ3RQ44YS3agPKICotamfzOp8LDzE7AdCNKrDAM4jIHG0ZBDXsqRgzwxnr%2FpPehpogP7zz6fLERMxqGBhVndpqCFY2dn3qc6P%2B%2F59GvhFgEiRl3zX4aIrwGVxYfuF7jKuPNBlw2qEo6FKlmqgaw5ZQ40gNLj2fc8cGSMyzyTR94%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230516T093020Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIATEQUHGL7DUC663HH%2F20230516%2Feu-west-2%2Fs3%2Faws4_request&X-Amz-Signature=e09c5e62a89115971c95aa06594fd766943424cc6a0f2e021c647ffcb55b2944_�https://s3.console.aws.amazon.com/s3/buckets/nhsd-dspp-dae-prod-data-out?region=eu-west-2&prefix=AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/&showversions=false  C                           �q/0082;64634d2c;Safari;                                                                                                                                                                                                                                                                                                                                                                                                                                                         PaxHeader/CCU019_HZ_04_Covid_Survival_Analysis_exact.py                                             000644  000767  000024  00000000036 14430646454 024020  x                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         30 mtime=1684229420.817751166
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CCU019_HZ_04_Covid_Survival_Analysis_exact.py                                                       000644  000767  000024  00000010710 14430646454 022047  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         # Databricks notebook source
# MAGIC %md
# MAGIC # CCU019_HZ_Covid_Survival_Analysis_exact
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * The survival analysis
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
# MAGIC `ccu019_hz_01_match_exact`
# MAGIC `ccu019_hz_01_event`
# MAGIC 
# MAGIC **Data output**
# MAGIC `ccu019_hz_01_analysis`
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/covid_survival_rev1/CCU019_HZ_Covid_Survival_Config

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/CCU019_HZ_statsmodels_extension

# COMMAND ----------

from tqdm import tqdm
from pprint import pprint

config = pipelines['pipeline_exact']['04_analysis']

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

event = spark.sql(sql_select_metacols(config['input']['event']))
match = spark.sql(sql_select_metacols(config['input']['match']))

# COMMAND ----------

disease_counts = {row.disease: row.asDict()['count'] for row in match.filter('matched_group').groupBy('disease').count().collect() if row.asDict()['count'] >= config['params']['min_n_case']}
disease_counts = dict(sorted(disease_counts.items(), key=lambda item: item[1], reverse=True))

diseases = list(disease_counts.keys())

# COMMAND ----------

from statsmodels.duration.survfunc import survdiff
from statsmodels.duration.hazard_regression import PHReg


results = []
event_groups = ['all', 'wave1', 'wave2_novac', 'wave2_vacced']


for disease in tqdm(diseases):

  for event_group in event_groups:
    
    analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')
    
    if event_group != 'all':      
      analysis = analysis.filter(F.col('event_group') == event_group)

    data = analysis.toPandas()
    
    n_case = sum(data.matched_group == 1)
    n_ctrl = sum(data.matched_group == 0)

    result = {
      'event_group': event_group,
      'case': disease
    }
    
    try:
      result.update({
        'case_name': data.disease.iloc[0],
        'n_case': n_case,
        'n_ctrl': n_ctrl,
        'n': n_case + n_ctrl,
        'n_events': sum(data['has_event'])
      })
    except:
      pass

    try:
      stat, pv = survdiff(data.futime, data.has_event, data.matched_group)

      result.update({ 
        'logrank_stat': stat,
        'logrank_p_value': pv
      })
    except:
      pass

    try:
      ph_model = PHReg.from_formula('futime ~ matched_group', data, status=data['has_event'])
      ph_results = ph_model.fit()

      result.update({'ph_reg_%s' % k: v for k,v in ph_results.summary().tables[1].to_dict('records')[0].items()})
      result.update({'ph_test_%s' % k: v[0] for k,v in ph_results.cox_ph_test().items()})
    except:
      pass

    results.append(result)

# COMMAND ----------

# Create view to download data

import pandas as pd

results_pdf = pd.DataFrame.from_records(results)
results_pdf['case_name'] = results_pdf['case_name'].astype(str)


# COMMAND ----------

import re

# Save the results to databricks

results_pdf.columns = [re.sub(r'[^\w_]', '_', col) for col in results_pdf.columns] 

df = spark.createDataFrame(results_pdf)
df = df.withColumn('production_timestamp_hz', F.current_timestamp())

# drop_table(config['analysis']['output'])

df.createOrReplaceGlobalTempView(config['output']['analysis']['name'])
drop_table(config['output']['analysis']['name'])
create_table(config['output']['analysis']['name'])


# COMMAND ----------

# censored table for output


pdf = spark.sql("""
select *
from %s
""" % config['output']['analysis']['name']).toPandas()

results_pdf_censored = censoring_minor_case(pdf.copy(), columns=[ 'n_case', 'n_ctrl', 'n', 'n_events'])

results_pdf_censored = results_pdf_censored.astype(str)

display(results_pdf_censored)


                                                        ._CCU019_HZ_05_Covid_Survival_Visualization.py                                                      000644  000767  000024  00000004107 14430646461 022160  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2       G                                      ATTR      G   �  c                  �   H  com.apple.macl     ,    %com.apple.metadata:kMDItemWhereFroms   /     com.apple.quarantine  �[�n~�LF��R���P                                                      bplist00�_3https://nhsd-dspp-dae-prod-data-out.s3.eu-west-2.amazonaws.com/AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/CCU019_HZ_05_Covid_Survival_Visualization.py?response-content-disposition=attachment&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAIaCXVzLWVhc3QtMSJHMEUCIQCQGaQW354G6C33ZNSgG9IzndJtOrwYmDUifj5Ddc8PLgIgFFIiEWj28Vvue0NHNXIgKvBS8tVY4TqVvxUhoaFZYO0qnQMIKxADGgwyMTU4NjQ1MjE0NzAiDFGdRnjUyIh86wv15Cr6Ag0oyqnbX5M4IzQgNkY45jIAVWCmr%2FfbCDGfgpo6SxmxkrRl%2BrhIZAz5ywrls3BurlavK%2Fz7Z91eLICL%2BsP84OdH%2Bldb9Uic%2F7k7SHYyh91Les9N2tmeXGzdBw1iQq5GAbxXDe47a5c9pedBOG4tZYREGJjTOfpBDypO8c2gfQCvQYZ%2BHHANRnGY8g%2F3r5%2FzT6sR%2Ft20iiN13ozXg8u%2FJE3G3b%2B2qcIY%2BRfd88yu6uLZSseTP5LIHrjXXp%2BAqeR%2Bi3GpVMmqJerFI2b0HtW5kCuaGMU6jVmCgU2e%2FhPDOsU6BpbxdfejOue3S10fJItq4%2FRTEAEqK9WMrCBSB0679PJ8QjySLpWSTtoEaBK8F9mPODD0TUiWe7b4FBycgphNMbr4QbGMsWM5ezTLLsjrNrJdFc8c3tAvKfIcwVXyl%2F7rRp8Fgg3pnma%2BCRynZrmMeekhgAI9tgzc6lHvz%2Fandyq3HhLI6jFQ2CIOVGXddIQm2oIK40oR4sDNSDDsmI2jBjqMAh3bkesO1BCVxxgdbtN%2FzsQ%2Bg7xsvdHKIRC3nD4qxCpQoOK5p8BW3pr4LXTFJ1CTgBdLluErFXHprLwQVVOBIPn4dVm6LqbL%2FNG1TySgQSeN0CMV1WBad%2F%2B0pKOesyLunA%2BGt8c72Qjo77Pnt9Wu8kNxM0QK2LL2ODhswlRjRjjwZdkBUgmVVkQWph6yJJ3RQ44YS3agPKICotamfzOp8LDzE7AdCNKrDAM4jIHG0ZBDXsqRgzwxnr%2FpPehpogP7zz6fLERMxqGBhVndpqCFY2dn3qc6P%2B%2F59GvhFgEiRl3zX4aIrwGVxYfuF7jKuPNBlw2qEo6FKlmqgaw5ZQ40gNLj2fc8cGSMyzyTR94%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230516T093024Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIATEQUHGL7DUC663HH%2F20230516%2Feu-west-2%2Fs3%2Faws4_request&X-Amz-Signature=e53169b101be79d3ddfa3debfd83eca332f4b8b4c5415b243cfe8306a532b828_�https://s3.console.aws.amazon.com/s3/buckets/nhsd-dspp-dae-prod-data-out?region=eu-west-2&prefix=AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/&showversions=false  B                           �q/0082;64634d31;Safari;                                                                                                                                                                                                                                                                                                                                                                                                                                                          PaxHeader/CCU019_HZ_05_Covid_Survival_Visualization.py                                              000644  000767  000024  00000000036 14430646461 023711  x                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         30 mtime=1684229425.619494617
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CCU019_HZ_05_Covid_Survival_Visualization.py                                                        000644  000767  000024  00000036144 14430646461 021751  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         # Databricks notebook source
# MAGIC %md
# MAGIC # CCU019_HZ_Covid_Survival_Analysis
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * The survival analysis
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
# MAGIC `ccu019_hz_01_match_exact`
# MAGIC `ccu019_hz_01_event`
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/covid_survival_rev1/CCU019_HZ_Covid_Survival_Config

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/CCU019_HZ_statsmodels_extension

# COMMAND ----------

from tqdm import tqdm
from pprint import pprint

config = pipelines['pipeline_exact']['05_visualization']

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

event = spark.sql(sql_select_metacols(config['input']['event']))
match = spark.sql(sql_select_metacols(config['input']['match']))

# COMMAND ----------

# disease_counts = {row.disease: row.asDict()['count'] for row in match.filter('matched_group').groupBy('disease').count().collect() if row.asDict()['count'] >= config['params']['min_n_case']}
# disease_counts = dict(sorted(disease_counts.items(), key=lambda item: item[1], reverse=True))

# diseases = list(disease_counts.keys())

# COMMAND ----------

import matplotlib

print(matplotlib.__version__)

# COMMAND ----------

# Test Plot

import matplotlib.pyplot as plt
from statsmodels.api import SurvfuncRight
import numpy as np

diseases = ['Polymyalgia rheumatica', 'Progressive supranuclear palsy']

event_groups = ['all', 'wave1', 'wave2_novac', 'wave2_vacced']
event_labels = {
  'all': 'Overall',
  'wave1': 'Wave 1',
  'wave2_novac': 'Wave 2-Not Full Vac.',
  'wave2_vacced': 'Wave 2-Full Vac.'
}


n_fig = len(diseases) * 5

n_col = 5
n_row = len(diseases)

fig_width = 9
fig_height = 5

fig, axes = plt.subplots(n_row, n_col, figsize=(fig_width * n_col, fig_height * n_row))
fig.subplots_adjust(wspace=0.6, hspace=1.1)

follow_up_length = int(config['params']['follow_up_length'])

for i, disease in enumerate(diseases):
  
  for j, event_group in enumerate(event_groups):
  
    analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')
    
    if event_group != 'all':      
      analysis = analysis.filter(F.col('event_group') == event_group)
    
    data = analysis.toPandas()
    disease_name = disease

    ax = axes[i, j]

    try:
      for ii, group in data.groupby(by='matched_group'):
        label = disease if ii else 'Reference population'
        color = 'tab:red' if ii else 'tab:blue'

        sf = SurvfuncRight(group.futime, group.has_event)
        x = sf.surv_times
        y = sf.surv_prob
        e = sf.surv_prob_se

        if sf.surv_times[-1] < follow_up_length:
          x = np.append(x, follow_up_length)
          y = np.append(y, y[-1])
          e = np.append(e, e[-1])
                
        if sf.surv_times[0] > 0:
          x = np.insert(x, 0, 0)
          y = np.insert(y, 0, 1)
          e = np.insert(e, 0, 0)
        
        y = (1 - y) * 100
        e = e * 100
        
        ax.plot(x, y, label=label, color=color)
        ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

      ax.legend(loc='upper left')
      ax.set_xticks(ticks=config['params']['table_time_points'])

      follow_up_timepoints = config['params']['table_time_points']

      cellText = []

      cellText.append(['' for _ in range(len(follow_up_timepoints))])

      for ii, group in data.groupby(by='matched_group'):
        cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])

      cellText.append(['' for _ in range(len(follow_up_timepoints))])

      for ii, group in data.groupby(by='matched_group'):
        cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])

      cellText = [intersperse(row) for row in cellText]

      cellText = censor_table_text(cellText)

      ax.table(cellText=cellText,
               rowLabels=[make_bold('Numbers at risk'), 'Reference population', 'With the condition', make_bold('Cumulative number of events'), 'Reference population', 'With the condition'],
               rowLoc='right',
               cellLoc='center',
               loc='bottom', bbox=[0, -0.5, 0.95, 0.35],
               edges='open')

    except:
      pass

    ax.set_xlabel('Time since first event (Days)')
    ax.set_ylabel('Percentage with events')
    ax.set_title('\n'.join([make_bold(event_labels[event_group]), disease_name]), loc='left')
  
  # The combined figure for Wave 2 
  
  analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')
    
  analysis = analysis.filter((F.col('event_group') == 'wave2_novac') | (F.col('event_group') == 'wave2_vacced'))

  data = analysis.toPandas()
  disease_name = disease

  ax = axes[i, -1]

  try:
    colors = {
      False: {
        'wave2_novac': 'tab:blue',
        'wave2_vacced': 'tab:olive'
      },
      True: {
        'wave2_novac': 'tab:red',
        'wave2_vacced': 'tab:cyan'
      }
    }

    labels = {
      False: {
        'wave2_novac': 'CTRL-Not Full Vac.',
        'wave2_vacced': 'CTRL-Full Vac.'
      },
      True: {
        'wave2_novac': 'RD-Not Full Vac.',
        'wave2_vacced': 'RD-Full Vac.'
      }
    }

    for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):

      label = labels[ii][jj]
      color = colors[ii][jj]

      sf = SurvfuncRight(group.futime, group.has_event)
      x = sf.surv_times
      y = sf.surv_prob
      e = sf.surv_prob_se

      if sf.surv_times[-1] < follow_up_length:
        x = np.append(x, follow_up_length)
        y = np.append(y, y[-1])
        e = np.append(e, e[-1])
      
      if sf.surv_times[0] > 0:
        x = np.insert(x, 0, 0)
        y = np.insert(y, 0, 1)
        e = np.insert(e, 0, 0)
      
      y = (1 - y) * 100
      e = e * 100

      ax.plot(x, y, label=label, color=color)
      ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

    ax.legend(loc='upper left')
    ax.set_xticks(ticks=config['params']['table_time_points'])

    follow_up_timepoints = config['params']['table_time_points']

    cellText = []
    rowLabels = []

    cellText.append(['' for _ in range(len(follow_up_timepoints))])
    rowLabels.append(make_bold('Numbers at risk'))

    for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
      cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])
      rowLabels.append(labels[ii][jj])

    cellText.append(['' for _ in range(len(follow_up_timepoints))])
    rowLabels.append(make_bold('Cumulative number of events'))

    for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
      cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])
      rowLabels.append(labels[ii][jj])

    cellText = [intersperse(row) for row in cellText]

    cellText = censor_table_text(cellText)

    ax.table(cellText=cellText,
             rowLabels=rowLabels,
             rowLoc='right',
             cellLoc='center',
             loc='bottom', bbox=[0, -0.85, 0.95, 0.70],
             edges='open')


  except:
    pass

  ax.set_xlabel('Time since first event (Days)')
  ax.set_ylabel('Percentage with events')
  ax.set_title('\n'.join([make_bold('Wave 2'), disease_name]), loc='left')
  
  
display(fig)

# COMMAND ----------

# Plot

def generate_fig(diseases):

  import matplotlib.pyplot as plt
  from statsmodels.api import SurvfuncRight
  import numpy as np

  event_groups = ['all', 'wave1', 'wave2_novac', 'wave2_vacced']
  event_labels = {
    'all': 'Overall',
    'wave1': 'Wave 1',
    'wave2_novac': 'Wave 2-Not Full Vac.',
    'wave2_vacced': 'Wave 2-Full Vac.'
  }


  n_fig = len(diseases) * 5

  n_col = 5
  n_row = len(diseases)

  fig_width = 9
  fig_height = 6

  fig, axes = plt.subplots(n_row, n_col, figsize=(fig_width * n_col, fig_height * n_row))
  fig.subplots_adjust(wspace=0.6, hspace=1.1)

  follow_up_length = int(config['params']['follow_up_length'])

  for i, disease in enumerate(diseases):

    for j, event_group in enumerate(event_groups):

      analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')

      if event_group != 'all':      
        analysis = analysis.filter(F.col('event_group') == event_group)

      data = analysis.toPandas()
      disease_name = disease

      ax = axes[i, j]

      try:
        for ii, group in data.groupby(by='matched_group'):
          label = disease if ii else 'Reference population'
          color = 'tab:red' if ii else 'tab:blue'

          sf = SurvfuncRight(group.futime, group.has_event)
          x = sf.surv_times
          y = sf.surv_prob
          e = sf.surv_prob_se

          if sf.surv_times[-1] < follow_up_length:
            x = np.append(x, follow_up_length)
            y = np.append(y, y[-1])
            e = np.append(e, e[-1])
          
          if sf.surv_times[0] > 0:
            x = np.insert(x, 0, 0)
            y = np.insert(y, 0, 1)
            e = np.insert(e, 0, 0)
            
          y = (1 - y) * 100
          e = e * 100

          ax.plot(x, y, label=label, color=color)
          ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

        ax.legend(loc='upper left')
        ax.set_xticks(ticks=config['params']['table_time_points'])

        follow_up_timepoints = config['params']['table_time_points']

        cellText = []

        cellText.append(['' for _ in range(len(follow_up_timepoints))])

        for ii, group in data.groupby(by='matched_group'):
          cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])

        cellText.append(['' for _ in range(len(follow_up_timepoints))])

        for ii, group in data.groupby(by='matched_group'):
          cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])

        cellText = [intersperse(row) for row in cellText]

        cellText = censor_table_text(cellText)

        ax.table(cellText=cellText,
                 rowLabels=[make_bold('Numbers at risk'), 'Reference population', 'With the condition', make_bold('Cumulative number of events'), 'Reference population', 'With the condition'],
                 rowLoc='right',
                 cellLoc='center',
                 loc='bottom', bbox=[0, -0.5, 0.95, 0.35],
                 edges='open')


      except:
        pass

      ax.set_xlabel('Time since first event (Days)')
      ax.set_ylabel('Percentage with events')
      ax.set_title('\n'.join([make_bold(event_labels[event_group]), disease_name]), loc='left')

    # The combined figure for Wave 2 

    analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')

    analysis = analysis.filter((F.col('event_group') == 'wave2_novac') | (F.col('event_group') == 'wave2_vacced'))

    data = analysis.toPandas()
    disease_name = disease

    ax = axes[i, -1]

    try:
      colors = {
        False: {
          'wave2_novac': 'tab:blue',
          'wave2_vacced': 'tab:olive'
        },
        True: {
          'wave2_novac': 'tab:red',
          'wave2_vacced': 'tab:cyan'
        }
      }

      labels = {
        False: {
          'wave2_novac': 'CTRL-Not Full Vac.',
          'wave2_vacced': 'CTRL-Full Vac.'
        },
        True: {
          'wave2_novac': 'RD-Not Full Vac.',
          'wave2_vacced': 'RD-Full Vac.'
        }
      }

      for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):

        label = labels[ii][jj]
        color = colors[ii][jj]

        sf = SurvfuncRight(group.futime, group.has_event)
        x = sf.surv_times
        y = sf.surv_prob
        e = sf.surv_prob_se

        if sf.surv_times[-1] < follow_up_length:
          x = np.append(x, follow_up_length)
          y = np.append(y, y[-1])
          e = np.append(e, e[-1])

        if sf.surv_times[0] > 0:
          x = np.insert(x, 0, 0)
          y = np.insert(y, 0, 1)
          e = np.insert(e, 0, 0)
        
        y = (1 - y) * 100
        e = e * 100

        ax.plot(x, y, label=label, color=color)
        ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

      ax.legend(loc='upper left')
      ax.set_xticks(ticks=config['params']['table_time_points'])

      follow_up_timepoints = config['params']['table_time_points']

      cellText = []
      rowLabels = []

      cellText.append(['' for _ in range(len(follow_up_timepoints))])
      rowLabels.append(make_bold('Numbers at risk'))

      for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
        cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])
        rowLabels.append(labels[ii][jj])

      cellText.append(['' for _ in range(len(follow_up_timepoints))])
      rowLabels.append(make_bold('Cumulative number of events'))

      for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
        cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])
        rowLabels.append(labels[ii][jj])

      cellText = [intersperse(row) for row in cellText]

      cellText = censor_table_text(cellText)

      ax.table(cellText=cellText,
               rowLabels=rowLabels,
               rowLoc='right',
               cellLoc='center',
               loc='bottom', bbox=[0, -0.85, 0.95, 0.70],
               edges='open')


    except:
      pass

    ax.set_xlabel('Time since first event (Days)')
    ax.set_ylabel('Percentage with events')
    ax.set_title('\n'.join([make_bold('Wave 2'), disease_name]), loc='left')
  
  return fig

# COMMAND ----------

disease_counts = {row.disease: row.asDict()['count'] for row in match.filter('matched_group').groupBy('disease').count().collect() if row.asDict()['count'] >= config['params']['min_n_case']}
disease_counts = dict(sorted(disease_counts.items(), key=lambda item: item[1], reverse=True))

diseases = list(disease_counts.keys())

diseases_copy = diseases.copy()

disease_chunks = chunkize(diseases_copy, 40)

print(len(disease_chunks))

# COMMAND ----------

chunk = 0

fig = generate_fig(disease_chunks[chunk])

display(fig)

plt.close()

# COMMAND ----------

chunk = 1

fig = generate_fig(disease_chunks[chunk])

display(fig)

plt.close()

# COMMAND ----------

chunk = 2

fig = generate_fig(disease_chunks[chunk])

display(fig)

plt.close()

# COMMAND ----------

chunk = 3

fig = generate_fig(disease_chunks[chunk])

display(fig)

plt.close()

# COMMAND ----------

chunk = 4

fig = generate_fig(disease_chunks[chunk])

display(fig)

plt.close()
                                                                                                                                                                                                                                                                                                                                                                                                                            ._CCU019_HZ_07_Covid_Survival_Analysis_disease_group.py                                             000644  000767  000024  00000004120 14430646466 024015  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2       P                                      ATTR      P   �  l                  �   H  com.apple.macl     ,    %com.apple.metadata:kMDItemWhereFroms   8     com.apple.quarantine  �[�n~�LF��R���P                                                      bplist00�_<https://nhsd-dspp-dae-prod-data-out.s3.eu-west-2.amazonaws.com/AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/CCU019_HZ_07_Covid_Survival_Analysis_disease_group.py?response-content-disposition=attachment&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAIaCXVzLWVhc3QtMSJHMEUCIQCQGaQW354G6C33ZNSgG9IzndJtOrwYmDUifj5Ddc8PLgIgFFIiEWj28Vvue0NHNXIgKvBS8tVY4TqVvxUhoaFZYO0qnQMIKxADGgwyMTU4NjQ1MjE0NzAiDFGdRnjUyIh86wv15Cr6Ag0oyqnbX5M4IzQgNkY45jIAVWCmr%2FfbCDGfgpo6SxmxkrRl%2BrhIZAz5ywrls3BurlavK%2Fz7Z91eLICL%2BsP84OdH%2Bldb9Uic%2F7k7SHYyh91Les9N2tmeXGzdBw1iQq5GAbxXDe47a5c9pedBOG4tZYREGJjTOfpBDypO8c2gfQCvQYZ%2BHHANRnGY8g%2F3r5%2FzT6sR%2Ft20iiN13ozXg8u%2FJE3G3b%2B2qcIY%2BRfd88yu6uLZSseTP5LIHrjXXp%2BAqeR%2Bi3GpVMmqJerFI2b0HtW5kCuaGMU6jVmCgU2e%2FhPDOsU6BpbxdfejOue3S10fJItq4%2FRTEAEqK9WMrCBSB0679PJ8QjySLpWSTtoEaBK8F9mPODD0TUiWe7b4FBycgphNMbr4QbGMsWM5ezTLLsjrNrJdFc8c3tAvKfIcwVXyl%2F7rRp8Fgg3pnma%2BCRynZrmMeekhgAI9tgzc6lHvz%2Fandyq3HhLI6jFQ2CIOVGXddIQm2oIK40oR4sDNSDDsmI2jBjqMAh3bkesO1BCVxxgdbtN%2FzsQ%2Bg7xsvdHKIRC3nD4qxCpQoOK5p8BW3pr4LXTFJ1CTgBdLluErFXHprLwQVVOBIPn4dVm6LqbL%2FNG1TySgQSeN0CMV1WBad%2F%2B0pKOesyLunA%2BGt8c72Qjo77Pnt9Wu8kNxM0QK2LL2ODhswlRjRjjwZdkBUgmVVkQWph6yJJ3RQ44YS3agPKICotamfzOp8LDzE7AdCNKrDAM4jIHG0ZBDXsqRgzwxnr%2FpPehpogP7zz6fLERMxqGBhVndpqCFY2dn3qc6P%2B%2F59GvhFgEiRl3zX4aIrwGVxYfuF7jKuPNBlw2qEo6FKlmqgaw5ZQ40gNLj2fc8cGSMyzyTR94%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230516T093030Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIATEQUHGL7DUC663HH%2F20230516%2Feu-west-2%2Fs3%2Faws4_request&X-Amz-Signature=de5537ae9fd98c02fac392ae1e4230d9b71a79aabf486d82407683a81f4459fd_�https://s3.console.aws.amazon.com/s3/buckets/nhsd-dspp-dae-prod-data-out?region=eu-west-2&prefix=AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/&showversions=false  K                           �q/0082;64634d36;Safari;                                                                                                                                                                                                                                                                                                                                                                                                                                                 PaxHeader/CCU019_HZ_07_Covid_Survival_Analysis_disease_group.py                                     000644  000767  000024  00000000036 14430646466 025553  x                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         30 mtime=1684229430.499302211
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CCU019_HZ_07_Covid_Survival_Analysis_disease_group.py                                               000644  000767  000024  00000011302 14430646466 023600  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         # Databricks notebook source
# MAGIC %md
# MAGIC # CCU019_HZ_Covid_Survival_Analysis_disease_group
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * The survival analysis
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
# MAGIC `ccu019_hz_01_match_exact_by_category`
# MAGIC `ccu019_hz_01_event`
# MAGIC 
# MAGIC **Data output**
# MAGIC `ccu019_hz_01_analysis_by_category`
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/covid_survival_rev1/CCU019_HZ_Covid_Survival_Config

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/CCU019_HZ_statsmodels_extension

# COMMAND ----------

from tqdm import tqdm
from pprint import pprint

config = pipelines['pipeline_exact']['07_analysis_group']

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

event = spark.sql(sql_select_metacols(config['input']['event']))
match = spark.sql(sql_select_metacols(config['input']['match']))

# COMMAND ----------

disease_counts = {row.disease: row.asDict()['count'] for row in match.filter('matched_group').groupBy('disease').count().collect() if row.asDict()['count'] >= config['params']['min_n_case']}
disease_counts = dict(sorted(disease_counts.items(), key=lambda item: item[1], reverse=True))

diseases = list(disease_counts.keys())

# COMMAND ----------

from statsmodels.duration.survfunc import survdiff
from statsmodels.duration.hazard_regression import PHReg


results = []
event_groups = ['all', 'wave1', 'wave2_novac', 'wave2_vacced']


for disease in tqdm(diseases):

  for event_group in event_groups:
    
    analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')
    
    if event_group != 'all':      
      analysis = analysis.filter(F.col('event_group') == event_group)

    data = analysis.toPandas()
    
    n_case = sum(data.matched_group == 1)
    n_ctrl = sum(data.matched_group == 0)

    result = {
      'event_group': event_group,
      'case': disease
    }
    
    try:
      result.update({
        'case_name': data.disease.iloc[0],
        'n_case': n_case,
        'n_ctrl': n_ctrl,
        'n': n_case + n_ctrl,
        'n_events': sum(data['has_event'])
      })
    except:
      pass

    try:
      stat, pv = survdiff(data.futime, data.has_event, data.matched_group)

      result.update({ 
        'logrank_stat': stat,
        'logrank_p_value': pv
      })
    except:
      pass

    try:
      ph_model = PHReg.from_formula('futime ~ matched_group', data, status=data['has_event'])
      ph_results = ph_model.fit()

      result.update({'ph_reg_%s' % k: v for k,v in ph_results.summary().tables[1].to_dict('records')[0].items()})
      result.update({'ph_test_%s' % k: v[0] for k,v in ph_results.cox_ph_test().items()})
    except:
      pass

    results.append(result)

# COMMAND ----------

# # Create view to download data

# import pandas as pd

# results_pdf = pd.DataFrame.from_records(results)
# results_pdf['case_name'] = results_pdf['case_name'].astype(str)

# results_pdf_censored = censoring_minor_case(results_pdf.copy(), columns=[ 'n_case', 'n_ctrl', 'n', 'n_events'])

# display(spark.createDataFrame(results_pdf_censored))

# COMMAND ----------

import re

# Save the results to databricks

results_pdf.columns = [re.sub(r'[^\w_]', '_', col) for col in results_pdf.columns] 

df = spark.createDataFrame(results_pdf)
df = df.withColumn('production_timestamp_hz', F.current_timestamp())

# drop_table(config['analysis']['output'])

df.createOrReplaceGlobalTempView(config['output']['analysis']['name'])

drop_table(config['output']['analysis']['name'])
create_table(config['output']['analysis']['name'])


# COMMAND ----------

display(df)

# COMMAND ----------

# censored table for output


pdf = spark.sql("""
select *
from %s
""" % config['output']['analysis']['name']).toPandas()


results_pdf_censored = censoring_minor_case(pdf.copy(), columns=[ 'n_case', 'n_ctrl', 'n', 'n_events'])
results_pdf_censored = results_pdf_censored.astype(str)

display(results_pdf_censored)
                                                                                                                                                                                                                                                                                                                              ._CCU019_HZ_08_Covid_Survival_Visualization_group.py                                                000644  000767  000024  00000004115 14430646474 023402  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2       M                                      ATTR      M   �  i                  �   H  com.apple.macl     ,  	  %com.apple.metadata:kMDItemWhereFroms   5     com.apple.quarantine  �[�n~�LF��R���P                                                      bplist00�_9https://nhsd-dspp-dae-prod-data-out.s3.eu-west-2.amazonaws.com/AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/CCU019_HZ_08_Covid_Survival_Visualization_group.py?response-content-disposition=attachment&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAIaCXVzLWVhc3QtMSJHMEUCIQCQGaQW354G6C33ZNSgG9IzndJtOrwYmDUifj5Ddc8PLgIgFFIiEWj28Vvue0NHNXIgKvBS8tVY4TqVvxUhoaFZYO0qnQMIKxADGgwyMTU4NjQ1MjE0NzAiDFGdRnjUyIh86wv15Cr6Ag0oyqnbX5M4IzQgNkY45jIAVWCmr%2FfbCDGfgpo6SxmxkrRl%2BrhIZAz5ywrls3BurlavK%2Fz7Z91eLICL%2BsP84OdH%2Bldb9Uic%2F7k7SHYyh91Les9N2tmeXGzdBw1iQq5GAbxXDe47a5c9pedBOG4tZYREGJjTOfpBDypO8c2gfQCvQYZ%2BHHANRnGY8g%2F3r5%2FzT6sR%2Ft20iiN13ozXg8u%2FJE3G3b%2B2qcIY%2BRfd88yu6uLZSseTP5LIHrjXXp%2BAqeR%2Bi3GpVMmqJerFI2b0HtW5kCuaGMU6jVmCgU2e%2FhPDOsU6BpbxdfejOue3S10fJItq4%2FRTEAEqK9WMrCBSB0679PJ8QjySLpWSTtoEaBK8F9mPODD0TUiWe7b4FBycgphNMbr4QbGMsWM5ezTLLsjrNrJdFc8c3tAvKfIcwVXyl%2F7rRp8Fgg3pnma%2BCRynZrmMeekhgAI9tgzc6lHvz%2Fandyq3HhLI6jFQ2CIOVGXddIQm2oIK40oR4sDNSDDsmI2jBjqMAh3bkesO1BCVxxgdbtN%2FzsQ%2Bg7xsvdHKIRC3nD4qxCpQoOK5p8BW3pr4LXTFJ1CTgBdLluErFXHprLwQVVOBIPn4dVm6LqbL%2FNG1TySgQSeN0CMV1WBad%2F%2B0pKOesyLunA%2BGt8c72Qjo77Pnt9Wu8kNxM0QK2LL2ODhswlRjRjjwZdkBUgmVVkQWph6yJJ3RQ44YS3agPKICotamfzOp8LDzE7AdCNKrDAM4jIHG0ZBDXsqRgzwxnr%2FpPehpogP7zz6fLERMxqGBhVndpqCFY2dn3qc6P%2B%2F59GvhFgEiRl3zX4aIrwGVxYfuF7jKuPNBlw2qEo6FKlmqgaw5ZQ40gNLj2fc8cGSMyzyTR94%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230516T093035Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIATEQUHGL7DUC663HH%2F20230516%2Feu-west-2%2Fs3%2Faws4_request&X-Amz-Signature=e9f74e8f7eaf9edc2f60ccfa0d490950e03adc9d93c4bd7daf5308b220b64947_�https://s3.console.aws.amazon.com/s3/buckets/nhsd-dspp-dae-prod-data-out?region=eu-west-2&prefix=AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/&showversions=false  H                           �q/0082;64634d3c;Safari;                                                                                                                                                                                                                                                                                                                                                                                                                                                    PaxHeader/CCU019_HZ_08_Covid_Survival_Visualization_group.py                                        000644  000767  000024  00000000036 14430646474 025134  x                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         30 mtime=1684229436.108427148
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CCU019_HZ_08_Covid_Survival_Visualization_group.py                                                  000644  000767  000024  00000034012 14430646474 023164  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         # Databricks notebook source
# MAGIC %md
# MAGIC # CCU019_HZ_Covid_Survival_Analysis
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * The survival analysis
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
# MAGIC `ccu019_hz_01_match_exact_by_category`
# MAGIC `ccu019_hz_01_event`
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/covid_survival_rev1/CCU019_HZ_Covid_Survival_Config

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/HZ/CCU019_HZ_statsmodels_extension

# COMMAND ----------

from tqdm import tqdm
from pprint import pprint

config = pipelines['pipeline_exact']['08_visualization_group']

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

event = spark.sql(sql_select_metacols(config['input']['event']))
match = spark.sql(sql_select_metacols(config['input']['match']))

# COMMAND ----------

disease_counts = {row.disease: row.asDict()['count'] for row in match.filter('matched_group').groupBy('disease').count().collect() if row.asDict()['count'] >= config['params']['min_n_case']}
disease_counts = dict(sorted(disease_counts.items(), key=lambda item: item[1], reverse=True))

diseases = list(disease_counts.keys())
diseases_copy = diseases.copy()

# COMMAND ----------

pprint(disease_counts)

# COMMAND ----------

import matplotlib

print(matplotlib.__version__)

# COMMAND ----------

# Test Plot

import matplotlib.pyplot as plt
from statsmodels.api import SurvfuncRight
import numpy as np

diseases = diseases_copy[:2]

event_groups = ['all', 'wave1', 'wave2_novac', 'wave2_vacced']
event_labels = {
  'all': 'Overall',
  'wave1': 'Wave 1',
  'wave2_novac': 'Wave 2 Unvaccinated',
  'wave2_vacced': 'Wave 2 Vaccinated'
}


n_fig = len(diseases) * 5

n_col = 5
n_row = len(diseases)

fig_width = 9
fig_height = 5

fig, axes = plt.subplots(n_row, n_col, figsize=(fig_width * n_col, fig_height * n_row))
fig.subplots_adjust(wspace=0.6, hspace=1.1)

follow_up_length = int(config['params']['follow_up_length'])

for i, disease in enumerate(diseases):
  
  for j, event_group in enumerate(event_groups):
  
    analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')
    
    if event_group != 'all':      
      analysis = analysis.filter(F.col('event_group') == event_group)
    
    data = analysis.toPandas()
    disease_name = disease

    ax = axes[i, j]

    try:
      for ii, group in data.groupby(by='matched_group'):
        label = disease if ii else 'Reference population'
        color = 'tab:red' if ii else 'tab:blue'

        sf = SurvfuncRight(group.futime, group.has_event)
        x = sf.surv_times
        y = sf.surv_prob
        e = sf.surv_prob_se

        if sf.surv_times[-1] < follow_up_length:
          x = np.append(x, follow_up_length)
          y = np.append(y, y[-1])
          e = np.append(e, e[-1])

        y = (1 - y) * 100
        e = e * 100
        
        ax.plot(x, y, label=label, color=color)
        ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

      ax.legend(loc='upper left')
      ax.set_xticks(ticks=config['params']['table_time_points'])

      follow_up_timepoints = config['params']['table_time_points']

      cellText = []

      cellText.append(['' for _ in range(len(follow_up_timepoints))])

      for ii, group in data.groupby(by='matched_group'):
        cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])

      cellText.append(['' for _ in range(len(follow_up_timepoints))])

      for ii, group in data.groupby(by='matched_group'):
        cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])

      cellText = [intersperse(row) for row in cellText]

      cellText = censor_table_text(cellText)

      ax.table(cellText=cellText,
               rowLabels=[make_bold('Numbers at risk'), 'Reference population', 'With the condition', make_bold('Cumulative number of events'), 'Reference population', 'With the condition'],
               rowLoc='right',
               cellLoc='center',
               loc='bottom', bbox=[0, -0.5, 0.95, 0.35],
               edges='open')


    except:
      pass

    ax.set_xlabel('Time since first event (Days)')
    ax.set_ylabel('Percentage with event')
    ax.set_title('\n'.join([make_bold(event_labels[event_group]), disease_name]), loc='left')
  
  # The combined figure for Wave 2 
  
  analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')
    
  analysis = analysis.filter((F.col('event_group') == 'wave2_novac') | (F.col('event_group') == 'wave2_vacced'))

  data = analysis.toPandas()
  disease_name = disease

  ax = axes[i, -1]

  try:
    colors = {
      False: {
        'wave2_novac': 'tab:blue',
        'wave2_vacced': 'tab:olive'
      },
      True: {
        'wave2_novac': 'tab:red',
        'wave2_vacced': 'tab:cyan'
      }
    }

    labels = {
      False: {
        'wave2_novac': 'CTRL Unvaccinated',
        'wave2_vacced': 'CTRL Vaccinated'
      },
      True: {
        'wave2_novac': 'RD Unvaccinated',
        'wave2_vacced': 'RD Vaccinated'
      }
    }

    for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):

      label = labels[ii][jj]
      color = colors[ii][jj]

      sf = SurvfuncRight(group.futime, group.has_event)
      x = sf.surv_times
      y = sf.surv_prob
      e = sf.surv_prob_se

      if sf.surv_times[-1] < follow_up_length:
        x = np.append(x, follow_up_length)
        y = np.append(y, y[-1])
        e = np.append(e, e[-1])

      y = (1 - y) * 100
      e = e * 100

      ax.plot(x, y, label=label, color=color)
      ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

    ax.legend(loc='upper left')
    ax.set_xticks(ticks=config['params']['table_time_points'])

    follow_up_timepoints = config['params']['table_time_points']

    cellText = []
    rowLabels = []

    cellText.append(['' for _ in range(len(follow_up_timepoints))])
    rowLabels.append(make_bold('Numbers at risk'))

    for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
      cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])
      rowLabels.append(labels[ii][jj])

    cellText.append(['' for _ in range(len(follow_up_timepoints))])
    rowLabels.append(make_bold('Cumulative number of events'))

    for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
      cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])
      rowLabels.append(labels[ii][jj])

    cellText = [intersperse(row) for row in cellText]

    cellText = censor_table_text(cellText)

    ax.table(cellText=cellText,
             rowLabels=rowLabels,
             rowLoc='right',
             cellLoc='center',
             loc='bottom', bbox=[0, -0.85, 0.95, 0.70],
             edges='open')


  except:
    pass

  ax.set_xlabel('Time since first event (Days)')
  ax.set_ylabel('Percentage with event')
  ax.set_title('\n'.join([make_bold('Wave 2'), disease_name]), loc='left')
  
  
display(fig)

# COMMAND ----------

# Plot

def generate_fig(diseases):

  import matplotlib.pyplot as plt
  from statsmodels.api import SurvfuncRight
  import numpy as np

  event_groups = ['all', 'wave1', 'wave2_novac', 'wave2_vacced']
  event_labels = {
    'all': 'Overall',
    'wave1': 'Wave 1',
    'wave2_novac': 'Wave 2-Not Full Vac.',
    'wave2_vacced': 'Wave 2-Full Vac.'
  }


  n_fig = len(diseases) * 5

  n_col = 5
  n_row = len(diseases)

  fig_width = 9
  fig_height = 6

  fig, axes = plt.subplots(n_row, n_col, figsize=(fig_width * n_col, fig_height * n_row))
  fig.subplots_adjust(wspace=0.6, hspace=1.1)

  follow_up_length = int(config['params']['follow_up_length'])

  for i, disease in enumerate(diseases):

    for j, event_group in enumerate(event_groups):

      analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')

      if event_group != 'all':      
        analysis = analysis.filter(F.col('event_group') == event_group)

      data = analysis.toPandas()
      disease_name = disease

      ax = axes[i, j]

      try:
        for ii, group in data.groupby(by='matched_group'):
          label = disease if ii else 'Reference population'
          color = 'tab:red' if ii else 'tab:blue'

          sf = SurvfuncRight(group.futime, group.has_event)
          x = sf.surv_times
          y = sf.surv_prob
          e = sf.surv_prob_se

          if sf.surv_times[-1] < follow_up_length:
            x = np.append(x, follow_up_length)
            y = np.append(y, y[-1])
            e = np.append(e, e[-1])
          
          if sf.surv_times[0] > 0:
            x = np.insert(x, 0, 0)
            y = np.insert(y, 0, 1)
            e = np.insert(e, 0, 0)
            
          y = (1 - y) * 100
          e = e * 100

          ax.plot(x, y, label=label, color=color)
          ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

        ax.legend(loc='upper left')
        ax.set_xticks(ticks=config['params']['table_time_points'])

        follow_up_timepoints = config['params']['table_time_points']

        cellText = []

        cellText.append(['' for _ in range(len(follow_up_timepoints))])

        for ii, group in data.groupby(by='matched_group'):
          cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])

        cellText.append(['' for _ in range(len(follow_up_timepoints))])

        for ii, group in data.groupby(by='matched_group'):
          cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])

        cellText = [intersperse(row) for row in cellText]

        cellText = censor_table_text(cellText)

        ax.table(cellText=cellText,
                 rowLabels=[make_bold('Numbers at risk'), 'Reference population', 'With the condition', make_bold('Cumulative number of events'), 'Reference population', 'With the condition'],
                 rowLoc='right',
                 cellLoc='center',
                 loc='bottom', bbox=[0, -0.5, 0.95, 0.35],
                 edges='open')


      except:
        pass

      ax.set_xlabel('Time since first event (Days)')
      ax.set_ylabel('Percentage with events')
      ax.set_title('\n'.join([make_bold(event_labels[event_group]), disease_name]), loc='left')

    # The combined figure for Wave 2 

    analysis = match.filter(F.col('disease') == disease).join(event, 'person_id_deid', 'inner')

    analysis = analysis.filter((F.col('event_group') == 'wave2_novac') | (F.col('event_group') == 'wave2_vacced'))

    data = analysis.toPandas()
    disease_name = disease

    ax = axes[i, -1]

    try:
      colors = {
        False: {
          'wave2_novac': 'tab:blue',
          'wave2_vacced': 'tab:olive'
        },
        True: {
          'wave2_novac': 'tab:red',
          'wave2_vacced': 'tab:cyan'
        }
      }

      labels = {
        False: {
          'wave2_novac': 'CTRL-Not Full Vac.',
          'wave2_vacced': 'CTRL-Full Vac.'
        },
        True: {
          'wave2_novac': 'RD-Not Full Vac.',
          'wave2_vacced': 'RD-Full Vac.'
        }
      }

      for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):

        label = labels[ii][jj]
        color = colors[ii][jj]

        sf = SurvfuncRight(group.futime, group.has_event)
        x = sf.surv_times
        y = sf.surv_prob
        e = sf.surv_prob_se

        if sf.surv_times[-1] < follow_up_length:
          x = np.append(x, follow_up_length)
          y = np.append(y, y[-1])
          e = np.append(e, e[-1])

        if sf.surv_times[0] > 0:
          x = np.insert(x, 0, 0)
          y = np.insert(y, 0, 1)
          e = np.insert(e, 0, 0)
        
        y = (1 - y) * 100
        e = e * 100

        ax.plot(x, y, label=label, color=color)
        ax.fill_between(x, y - e, y + e, alpha=0.3, color=color)

      ax.legend(loc='upper left')
      ax.set_xticks(ticks=config['params']['table_time_points'])

      follow_up_timepoints = config['params']['table_time_points']

      cellText = []
      rowLabels = []

      cellText.append(['' for _ in range(len(follow_up_timepoints))])
      rowLabels.append(make_bold('Numbers at risk'))

      for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
        cellText.append([len(group) - sum(group.futime <= cut) for cut in follow_up_timepoints])
        rowLabels.append(labels[ii][jj])

      cellText.append(['' for _ in range(len(follow_up_timepoints))])
      rowLabels.append(make_bold('Cumulative number of events'))

      for (ii, jj), group in data.groupby(by=['matched_group', 'event_group']):
        cellText.append([sum((group.futime <= cut) & group.has_event) for cut in follow_up_timepoints])
        rowLabels.append(labels[ii][jj])

      cellText = [intersperse(row) for row in cellText]

      cellText = censor_table_text(cellText)

      ax.table(cellText=cellText,
               rowLabels=rowLabels,
               rowLoc='right',
               cellLoc='center',
               loc='bottom', bbox=[0, -0.85, 0.95, 0.70],
               edges='open')

    except:
      pass

    ax.set_xlabel('Time since first event (Days)')
    ax.set_ylabel('Percentage with events')
    ax.set_title('\n'.join([make_bold('Wave 2'), disease_name]), loc='left')
  
  return fig

# COMMAND ----------

diseases = diseases_copy

fig = generate_fig(diseases)

display(fig)

plt.close()
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ._CCU019_HZ_Covid_Survival_Config.py                                                                000644  000767  000024  00000004075 14430646506 020224  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2       =                                      ATTR      =   �  Y                  �   H  com.apple.macl     ,  �  %com.apple.metadata:kMDItemWhereFroms   %     com.apple.quarantine  �[�n~�LF��R���P                                                      bplist00�_)https://nhsd-dspp-dae-prod-data-out.s3.eu-west-2.amazonaws.com/AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/CCU019_HZ_Covid_Survival_Config.py?response-content-disposition=attachment&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAIaCXVzLWVhc3QtMSJHMEUCIQCQGaQW354G6C33ZNSgG9IzndJtOrwYmDUifj5Ddc8PLgIgFFIiEWj28Vvue0NHNXIgKvBS8tVY4TqVvxUhoaFZYO0qnQMIKxADGgwyMTU4NjQ1MjE0NzAiDFGdRnjUyIh86wv15Cr6Ag0oyqnbX5M4IzQgNkY45jIAVWCmr%2FfbCDGfgpo6SxmxkrRl%2BrhIZAz5ywrls3BurlavK%2Fz7Z91eLICL%2BsP84OdH%2Bldb9Uic%2F7k7SHYyh91Les9N2tmeXGzdBw1iQq5GAbxXDe47a5c9pedBOG4tZYREGJjTOfpBDypO8c2gfQCvQYZ%2BHHANRnGY8g%2F3r5%2FzT6sR%2Ft20iiN13ozXg8u%2FJE3G3b%2B2qcIY%2BRfd88yu6uLZSseTP5LIHrjXXp%2BAqeR%2Bi3GpVMmqJerFI2b0HtW5kCuaGMU6jVmCgU2e%2FhPDOsU6BpbxdfejOue3S10fJItq4%2FRTEAEqK9WMrCBSB0679PJ8QjySLpWSTtoEaBK8F9mPODD0TUiWe7b4FBycgphNMbr4QbGMsWM5ezTLLsjrNrJdFc8c3tAvKfIcwVXyl%2F7rRp8Fgg3pnma%2BCRynZrmMeekhgAI9tgzc6lHvz%2Fandyq3HhLI6jFQ2CIOVGXddIQm2oIK40oR4sDNSDDsmI2jBjqMAh3bkesO1BCVxxgdbtN%2FzsQ%2Bg7xsvdHKIRC3nD4qxCpQoOK5p8BW3pr4LXTFJ1CTgBdLluErFXHprLwQVVOBIPn4dVm6LqbL%2FNG1TySgQSeN0CMV1WBad%2F%2B0pKOesyLunA%2BGt8c72Qjo77Pnt9Wu8kNxM0QK2LL2ODhswlRjRjjwZdkBUgmVVkQWph6yJJ3RQ44YS3agPKICotamfzOp8LDzE7AdCNKrDAM4jIHG0ZBDXsqRgzwxnr%2FpPehpogP7zz6fLERMxqGBhVndpqCFY2dn3qc6P%2B%2F59GvhFgEiRl3zX4aIrwGVxYfuF7jKuPNBlw2qEo6FKlmqgaw5ZQ40gNLj2fc8cGSMyzyTR94%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230516T093046Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIATEQUHGL7DUC663HH%2F20230516%2Feu-west-2%2Fs3%2Faws4_request&X-Amz-Signature=8433d1fa8318566e415edd4db660051870cafc758cd1987a8228dd8767cf9158_�https://s3.console.aws.amazon.com/s3/buckets/nhsd-dspp-dae-prod-data-out?region=eu-west-2&prefix=AROATEQUHGL7NCPQSLQKQ%3Ahuayu.zhang/&showversions=false  8                           �q/0082;64634d46;Safari;                                                                                                                                                                                                                                                                                                                                                                                                                                                                    PaxHeader/CCU019_HZ_Covid_Survival_Config.py                                                        000644  000767  000024  00000000036 14430646506 021751  x                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         30 mtime=1684229446.197339114
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CCU019_HZ_Covid_Survival_Config.py                                                                  000644  000767  000024  00000024222 14430646506 020003  0                                                                                                    ustar 00hzhang13                        staff                           000000  000000                                                                                                                                                                         # Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # CCU019_HZ_Covid_Survival_Config
# MAGIC 
# MAGIC **Description** 
# MAGIC 
# MAGIC This is the master config file for the survival analysis
# MAGIC This file also contains helper functions for the project
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
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# Configs

pipelines = {
  "pipeline_exact": {
    "01_cohort": {
      "input": {
        "population": {
          "name": "ccu019_paper_population_cohort", 
          "cols": ["person_id_deid"]
        },
        "covid": {
          "name": "ccu013_covid_trajectory",
          "cols": ["person_id_deid"]
        }
      },
      "output": {
        "cohort": {
          "name": "ccu019_hz_01_cohort"
        }
      },
      "params": {
        "database_name": "dars_nic_391419_j3w9t_collab",
        "allow_skip": True
      }
    },
    "02_event": {
      "input": {
        "pheno": {
        "name": "ccu013_covid_trajectory", 
        "cols": ["person_id_deid", "covid_phenotype", "date"],        
      }, 
      "censor": {
        "name": "curr302_patient_skinny_record", 
        "cols": ["NHS_NUMBER_DEID", "DATE_OF_DEATH as date_censor"] 
      },
      "vaccination": {
        "name": "ccu013_vaccine_status",
        "cols": ["person_id_deid", "date as date_vacc", "DOSE_SEQUENCE"]
      },
      "cohort": {
        "name": "ccu019_hz_01_cohort",
        "cols": ["NHS_NUMBER_DEID", "cohort"]
      }
    },
    "output": {
      "event": {
        "name": "ccu019_hz_01_event"
      }
    },
    "params": {
      "database_name": "dars_nic_391419_j3w9t_collab",
      "allow_skip": True,
      "event_included": [
        '04_Covid_inpatient_death',
        '04_Fatal_with_covid_diagnosis',
        '04_Fatal_without_covid_diagnosis'
      ],
      "follow_up_length": 30,
      "study_start_date": "2020-01-23",
      "first_vaccination": "2020-12-08",
      "wave_1_start_date": "2020-01-23",
      "wave_2_start_date": "2020-09-01",
      "study_end_date": "2021-11-30"
      }
    },
    "03_match": {
      "input": {
        "rd": {
          "name": "ccu019_paper_cohort_rare_disease",
          "cols": ["person_id_deid", "phenotype"]
        },
        "cohort": {
          "name": "ccu019_hz_01_cohort",
          "cols": ["NHS_NUMBER_DEID", "cohort"] 
        },
        "demo": {
          "name": "curr302_patient_skinny_record",
          "cols": ["NHS_NUMBER_DEID", "DATE_OF_BIRTH", "SEX", "ETHNIC"]
        },
        "event": {
          "name": "ccu019_hz_01_event",
          "cols": ["person_id_deid", "date_start", "event_group"]
        },
        "ethnic": {
          "name": "ccu037_ethnicity_assembled",
          "cols": ["NHS_NUMBER_DEID", "ETHNIC_GROUP"]
        }
      },
      'output': {
        "meta": {
          "name": "ccu019_hz_01_metadata"
        },
        "match": {
          "name": "ccu019_hz_01_match_exact"
        } 
      },
      "params": {
        "database_name": "dars_nic_391419_j3w9t_collab",
        "allow_skip": True,
        "method": "exact", 
        "meta_cols_to_query": ['age_start', 'SEX', 'ETHNIC_GROUP', 'event_group'],
        "rd_identifier": 'phenotype',
        "bins_age_group": [17.5, 30, 40, 50, 60, 70, 120],
        "labels_age_group": ['<=30', '30-40', '40-50', '50-60', '60-70', '>70'],
        "meta_cols_to_match": ['age_group', 'SEX', 'ETHNIC_GROUP', 'event_group']
      }      
    },
    "04_analysis": {
      "input": {
        "match": {
          "name": "ccu019_hz_01_match_exact",
          "cols": ["*"]
        },
        "event": {
          "name": "ccu019_hz_01_event",
          "cols": ["person_id_deid", "futime", "has_event", "event_group"]
        }
      },
      "output": {
        "analysis": {
          "name": "ccu019_hz_01_analysis"
        }
      },
      "params": {
        "database_name": "dars_nic_391419_j3w9t_collab",
        "allow_skip": True,
        "min_n_case": 20
      }
    },
    "05_visualization": {
      "input": {
        "match": {
          "name": "ccu019_hz_01_match_exact",
          "cols": ["*"]
        },
        "event": {
          "name": "ccu019_hz_01_event",
          "cols": ["person_id_deid", "futime", "has_event", "event_group"]
        }
      },
      "output": {
      },
      "params": {
        "database_name": "dars_nic_391419_j3w9t_collab",
        "allow_skip": True,
        "table_time_points": [0, 7, 14, 21, 28],
        "min_n_case": 20,
        "follow_up_length": 30
      }
    },
    "06_match_group": {
      "input": {
        "rd": {
          "name": "ccu019_paper_cohort_rare_disease",
          "cols": ["person_id_deid", "phenotype"]
        },
        "cohort": {
          "name": "ccu019_hz_01_cohort",
          "cols": ["NHS_NUMBER_DEID", "cohort"] 
        },
        "demo": {
          "name": "curr302_patient_skinny_record",
          "cols": ["NHS_NUMBER_DEID", "DATE_OF_BIRTH", "SEX", "ETHNIC"]
        },
        "event": {
          "name": "ccu019_hz_01_event",
          "cols": ["person_id_deid", "date_start", "event_group"]
        },
        "ethnic": {
          "name": "ccu037_ethnicity_assembled",
          "cols": ["NHS_NUMBER_DEID", "ETHNIC_GROUP"]
        },
        "rd_cate": {
          "name": "ccu019_paper_cohort_rare_disease_category",
          "cols": ["*"]
        }
      },
      'output': {
        "meta": {
          "name": "ccu019_hz_01_metadata_by_category"
        },
        "match": {
          "name": "ccu019_hz_01_match_exact_by_category"
        } 
      },
      "params": {
        "database_name": "dars_nic_391419_j3w9t_collab",
        "allow_skip": True,
        "method": "exact", 
        "meta_cols_to_query": ['age_start', 'SEX', 'ETHNIC_GROUP', 'event_group'],
        "rd_identifier": 'category',
        "bins_age_group": [17.5, 30, 40, 50, 60, 70, 120],
        "labels_age_group": ['<=30', '30-40', '40-50', '50-60', '60-70', '>70'],
        "meta_cols_to_match": ['age_group', 'SEX', 'ETHNIC_GROUP', 'event_group']
      }
    },
    "07_analysis_group": {
      "input": {
        "match": {
          "name": "ccu019_hz_01_match_exact_by_category",
          "cols": ["*"]
        },
        "event": {
          "name": "ccu019_hz_01_event",
          "cols": ["person_id_deid", "futime", "has_event", "event_group"]
        }
      },
      "output": {
        "analysis": {
          "name": "ccu019_hz_01_analysis_by_category"
        }
      },
      "params": {
        "database_name": "dars_nic_391419_j3w9t_collab",
        "allow_skip": True,
        "min_n_case": 20
      }
    },
    "08_visualization_group": {
      "input": {
        "match": {
          "name": "ccu019_hz_01_match_exact_by_category",
          "cols": ["*"]
        },
        "event": {
          "name": "ccu019_hz_01_event",
          "cols": ["person_id_deid", "futime", "has_event", "event_group"]
        }
      },
      "output": {
      },
      "params": {
        "database_name": "dars_nic_391419_j3w9t_collab",
        "allow_skip": True,
        "table_time_points": [0, 7, 14, 21, 28],
        "min_n_case": 20,
        "follow_up_length": 30
      }
    }
  }
}

# COMMAND ----------

def sql_select_metacols(meta_config):
  return 'select %s from %s' % (','.join(meta_config['cols']), meta_config['name'])

# COMMAND ----------

def chunkize(l, size):
  chunks = []
  start = 0
  
  while (start + size) <= len(l):
    chunks.append(l[start:(start + size)])
    start += size
    
  chunks.append(l[start:len(l)])
  
  return chunks
  

def censoring_minor_case(table, columns, drop_original=True):
  """
  The function censors number less than 10 for data export
  """
  
  
  if str(type(table)) == "<class 'pandas.core.frame.DataFrame'>":
    
    for col in columns:
      loc_col = table.columns.get_loc(col)
      table.insert(loc=loc_col, 
                   column='%s_censored' % col, 
                   value=table[col].apply(lambda x: censor_str_or_int(x))
                  )
      
    if drop_original:
      table.drop(columns=columns, inplace=True)

    return table
  
  else:
    pass
  

# COMMAND ----------

def table_exist(table_name, database_name='dars_nic_391419_j3w9t_collab'):
  try:
    spark.sql('select * from %s.%s' % (database_name, table_name))
    return True
  except:
    return False

  
def check_input(config):

  for k, v in config['input'].items():
    assert table_exist(v['name']), 'Input table %s for %s not exist' % (v['name'], k) 

    
def output_exists(config):
  
  all_output_exists = True
  
  for k, v in config['output'].items():
    all_output_exists = all_output_exists and table_exist(v['name'])
    
  return all_output_exists


# COMMAND ----------

def intersperse(l, spersing=''):
  """
  Parameters
  ----------
  l : list
  spersing : any, default=''
  
  Returns
  -------
  list
    List with interspersing
  """
  
  sl = [spersing] * (len(l) * 2 - 1)
  sl[0::2] = l
  return sl


def round_to_five(x):
  
  mod_5 = x % 5
  
  if mod_5 < 2.5:
    return x - mod_5
  else:
    return x - mod_5 + 5
  


def censor_str_or_int(x):
  """
  Censor any number less than 10 to '<10'
  
  Parameters
  ----------
  x: str or int
  
  Returns
  -------
  str or int
    Censored number or text
  """
  
  if isinstance(x, int):
    x_int = x
  elif isinstance(x, float):
    try:
      x_int = int(x)
    except ValueError:
      return x
  elif isinstance(x, str):
    try:
      x_int = int(float(x))
    except ValueError:
      return x
  else:
    return x
  
  if (x_int < 10) and (x_int >= 0):
    return '<10'
  else:
    return round_to_five(x_int)
  
  
def censor_table_text(cellText):
  """
  Returns censored cellText table for table plot in matplotlib
  """
  for i, row in enumerate(cellText):
    cellText[i] = [censor_str_or_int(x) for x in row]
  return cellText

def make_bold(s):
  """
  Transform string to bold using tex
  for matplotlib
  """
  return '$\\bf{%s}$' % s.replace(' ', '\\ ')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              