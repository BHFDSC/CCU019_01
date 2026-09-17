# Databricks notebook source
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


