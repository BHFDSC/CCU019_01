# Databricks notebook source
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


