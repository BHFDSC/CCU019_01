# Databricks notebook source
# MAGIC %md
# MAGIC  # CCCU019 - Find rare disease patients
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * Identifies patients rare diseases
# MAGIC 
# MAGIC **Project(s)** CCU0019
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen, Huayu Zhang, Honghan Wu
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2021-12-07
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2021-12-07
# MAGIC  
# MAGIC **Data input**  
# MAGIC All inputs are via notebook `CCU019_01_create_table_input`
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load rare disease phenotype definition

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_06_phenotype_definitions

# COMMAND ----------

# MAGIC %md 
# MAGIC ## [Temp] query disease counts for manual assessment

# COMMAND ----------

# MAGIC %python
# MAGIC # construct query components automatically using phenotype definitions
# MAGIC # construct_query function is defined in the phenotype definitions notebook
# MAGIC 
# MAGIC # sql_init = """
# MAGIC # CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_rare_disease as
# MAGIC # SELECT person_id_deid, date, 
# MAGIC # (case when DIAG_4_CONCAT LIKE "%xxxx%" THEN 'xxxx'
# MAGIC #  Else '0' End) as clinical_code,
# MAGIC # (case when DIAG_4_CONCAT LIKE "%xxxx%" THEN 'xxxx'
# MAGIC #  Else '0' End) as phenotype,
# MAGIC # "HES APC" as source, 
# MAGIC # "ICD10" as codeing, date_is
# MAGIC # --, SUSRECID
# MAGIC # FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_apc
# MAGIC # WHERE DIAG_4_CONCAT LIKE "%xxxx%" 
# MAGIC # """
# MAGIC 
# MAGIC # sqlContext.sql(sql_init)
# MAGIC 
# MAGIC sql_cmd_template = """
# MAGIC SELECT person_id_deid, {phenotype_str}
# MAGIC FROM {table}
# MAGIC WHERE {condition_str}
# MAGIC """
# MAGIC 
# MAGIC data_tables = [
# MAGIC   {'table': 'dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr', 
# MAGIC    'code_columns': ['code'], 'code_scheme': 'SNOMED', 'source_label': 'GDPPR'},
# MAGIC   {'table': 'dars_nic_391419_j3w9t_collab.ccu019_tmp_apc', 
# MAGIC    'code_columns': ['DIAG_4_CONCAT'], 'code_scheme': 'ICD10', 'source_label': 'HES APC'},
# MAGIC   {'table': 'dars_nic_391419_j3w9t_collab.ccu019_tmp_op', 
# MAGIC    'code_columns': ['DIAG_4_CONCAT'], 
# MAGIC    'code_scheme': 'ICD10', 'source_label': 'HES OP'}
# MAGIC ] 
# MAGIC 
# MAGIC phenotypes = {}
# MAGIC # the following is for testing a few if all disease phenotypes sql fails
# MAGIC # for k in list(rare_disease_phenotypes.keys())[:3]:
# MAGIC #   phenotypes[k] = rare_disease_phenotypes[k]
# MAGIC # phenotypes = rare_disease_phenotypes
# MAGIC 
# MAGIC 
# MAGIC # rds = list(rare_disease_phenotypes.keys())
# MAGIC 
# MAGIC rd2freq = {}
# MAGIC batch = 300
# MAGIC offset = 0
# MAGIC cur_batch = 0
# MAGIC 
# MAGIC # rds = list(rare_disease_phenotypes.keys())
# MAGIC temp_rd_phenotypes = {'Mitochondrial DNA depletion syndrome, encephalomyopathic form with renal tubulopathy': {'ICD10': ['G318'], 'SNOMED': ['765100000'], 'categories': ['genetic diseases', 'inborn errors of metabolism', 'neurological diseases']},
# MAGIC                      'TEMPI syndrome': {'ICD10': ['D751'], 'SNOMED': ['718614004'], 'categories': ['bone diseases', 'developmental anomalies during embryogenesis', 'genetic diseases']}
# MAGIC                      }
# MAGIC rds = list(temp_rd_phenotypes.keys())
# MAGIC 
# MAGIC ret_pd = None
# MAGIC while offset < len(rds):
# MAGIC   print(f'working on {offset} to {offset + batch}, total {len(rds)}')
# MAGIC   sqls = []
# MAGIC   phenotypes = {}
# MAGIC   for rd in rds[offset:offset + batch]:
# MAGIC     phenotypes[rd] = rare_disease_phenotypes[rd]
# MAGIC   for t in data_tables:
# MAGIC     code_str, phenotype_str, condition_str = construct_query(t['code_columns'], phenotypes, t['code_scheme'])
# MAGIC     t.update({
# MAGIC       'code_str': code_str, 'phenotype_str': phenotype_str, 'condition_str': condition_str
# MAGIC     })
# MAGIC     sqls.append(sql_cmd_template.format(**t))
# MAGIC   sql_count = '\n UNION \n'.join(sqls)
# MAGIC   q = f"""
# MAGIC   select count(distinct person_id_deid) num, phenotype
# MAGIC   from (
# MAGIC     {sql_count}
# MAGIC   ) group by phenotype
# MAGIC   """
# MAGIC     
# MAGIC   if ret_pd is None:
# MAGIC     ret_pd = sqlContext.sql(q).toPandas()
# MAGIC   else:
# MAGIC     ret_pd = ret_pd.append(sqlContext.sql(q).toPandas(), ignore_index=True)
# MAGIC #   i += 1
# MAGIC   
# MAGIC   offset += batch
# MAGIC   cur_batch += 1
# MAGIC 
# MAGIC display(ret_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create temporary table of `ccu019_rare_disease` by automating SQL commands using 
# MAGIC - rare phenotype definitions
# MAGIC - source table configurations

# COMMAND ----------

# MAGIC %python
# MAGIC # construct query components automatically using phenotype definitions
# MAGIC # construct_query function is defined in the phenotype definitions notebook
# MAGIC 
# MAGIC # sql_init = """
# MAGIC # CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_rare_disease as
# MAGIC # SELECT person_id_deid, date, 
# MAGIC # (case when DIAG_4_CONCAT LIKE "%xxxx%" THEN 'xxxx'
# MAGIC #  Else '0' End) as clinical_code,
# MAGIC # (case when DIAG_4_CONCAT LIKE "%xxxx%" THEN 'xxxx'
# MAGIC #  Else '0' End) as phenotype,
# MAGIC # "HES APC" as source, 
# MAGIC # "ICD10" as codeing, date_is
# MAGIC # --, SUSRECID
# MAGIC # FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_apc
# MAGIC # WHERE DIAG_4_CONCAT LIKE "%xxxx%" 
# MAGIC # """
# MAGIC 
# MAGIC # sqlContext.sql(sql_init)
# MAGIC 
# MAGIC sql_cmd_template = """
# MAGIC SELECT person_id_deid, date, 
# MAGIC {code_str},
# MAGIC {phenotype_str},
# MAGIC '{source_label}' as source, 
# MAGIC "{code_scheme}" as codeing, date_is 
# MAGIC --, SUSRECID
# MAGIC FROM {table}
# MAGIC WHERE {condition_str}
# MAGIC """
# MAGIC 
# MAGIC data_tables = [
# MAGIC   {'table': 'dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr', 
# MAGIC    'code_columns': ['code'], 'code_scheme': 'SNOMED', 'source_label': 'GDPPR'},
# MAGIC   {'table': 'dars_nic_391419_j3w9t_collab.ccu019_tmp_apc', 
# MAGIC    'code_columns': ['DIAG_4_CONCAT'], 'code_scheme': 'ICD10', 'source_label': 'HES APC'},
# MAGIC   {'table': 'dars_nic_391419_j3w9t_collab.ccu019_tmp_op', 
# MAGIC    'code_columns': ['DIAG_4_CONCAT'], 
# MAGIC    'code_scheme': 'ICD10', 'source_label': 'HES OP'}
# MAGIC ] 
# MAGIC 
# MAGIC phenotypes = {}
# MAGIC # the following is for testing a few if all disease phenotypes sql fails
# MAGIC # for k in list(rare_disease_phenotypes.keys())[:3]:
# MAGIC #   phenotypes[k] = rare_disease_phenotypes[k]
# MAGIC # phenotypes = rare_disease_phenotypes
# MAGIC 
# MAGIC batch = 1
# MAGIC offset = 2
# MAGIC cur_batch = 0
# MAGIC 
# MAGIC rds = list(rare_disease_phenotypes.keys())
# MAGIC # temp_rd_phenotypes = {
# MAGIC #                      'TEMPI syndrome': {'ICD10': ['D751', 'D7512'], 'SNOMED': ['718614004'], 'categories': ['bone diseases', 'developmental anomalies during embryogenesis', 'genetic diseases']}
# MAGIC #                      }
# MAGIC # rds = list(temp_rd_phenotypes.keys())
# MAGIC 
# MAGIC # the block is for manually fixing missed diseases due to the mappings that contain */+ characters
# MAGIC missed_rd_phenotypes = {
# MAGIC   'dummy': {'ICD10': ['dummy']},
# MAGIC   'dummy1': {'ICD10': ['dummy']},
# MAGIC   'Meningococcal meningitis': {'ICD10': ['G01', 'A390'], 'SNOMED': ['192644005']},
# MAGIC   'Ocular cicatricial pemphigoid': {'ICD10': ['L12', 'H133'], 'SNOMED': ['314757003']},
# MAGIC   'Gonococcal conjunctivitis': {'ICD10': ['A543', 'H131'], 'SNOMED': ['231858009']},
# MAGIC }
# MAGIC rare_disease_phenotypes = missed_rd_phenotypes
# MAGIC rds = list(rare_disease_phenotypes.keys())
# MAGIC 
# MAGIC debug_mode = False
# MAGIC 
# MAGIC while offset < len(rds):
# MAGIC   print(f'working on {offset} to {offset + batch}, total {len(rds)}')
# MAGIC   sqls = []
# MAGIC   for t in data_tables:
# MAGIC     phenotypes = {}
# MAGIC     for rd in rds[offset:offset + batch]:
# MAGIC       phenotypes[rd] = rare_disease_phenotypes[rd]
# MAGIC   #   phenotypes = rare_disease_phenotypes
# MAGIC     code_str, phenotype_str, condition_str, not_empty = construct_query(t['code_columns'], phenotypes, t['code_scheme'])
# MAGIC     if not_empty:
# MAGIC       t.update({
# MAGIC         'code_str': code_str, 'phenotype_str': phenotype_str, 'condition_str': condition_str
# MAGIC       })
# MAGIC       sqls.append(sql_cmd_template.format(**t))
# MAGIC 
# MAGIC   sql_creation = """
# MAGIC   CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_rare_disease as
# MAGIC   """
# MAGIC   
# MAGIC   if offset > 0:
# MAGIC     sql_creation = """
# MAGIC     insert into dars_nic_391419_j3w9t_collab.ccu019_rare_disease
# MAGIC     """
# MAGIC 
# MAGIC   sql_creation += '\n UNION \n'.join(sqls)
# MAGIC   # print(sql_creation)
# MAGIC   # cols = [f'diag_4_{i:02d}' for i in range(1, 11)]
# MAGIC   # code_str, phenotype_str, condition_str = construct_query(cols, rare_disease_phenotypes, 'SNOMED') 
# MAGIC   # print(code_str)
# MAGIC   print('doing sql...')
# MAGIC   if debug_mode:
# MAGIC     print(sql_creation)
# MAGIC   else:
# MAGIC     sqlContext.sql(sql_creation)
# MAGIC   
# MAGIC   if offset == 0:
# MAGIC     # materialise the temp tables for rare disease phenotype provenance table
# MAGIC     print('creating table...')
# MAGIC     if not debug_mode:
# MAGIC       drop_table("ccu019_rare_disease")
# MAGIC       create_table("ccu019_rare_disease")
# MAGIC     
# MAGIC   offset += batch
# MAGIC   cur_batch += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## some checking

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dars_nic_391419_j3w9t_collab.ccu019_rare_disease limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct person_id_deid) as num, phenotype from dars_nic_391419_j3w9t_collab.ccu019_rare_disease where codeing='SNOMED' group by phenotype

# COMMAND ----------

# MAGIC %python
# MAGIC ebt_phenotypes = []
# MAGIC for p in list(e_bt_mapping.keys()):
# MAGIC   ebt_phenotypes.append(p.replace("'", "\\'"))
# MAGIC ebt_phenotypes_str = ','.join([f"'{p}'" for p in ebt_phenotypes])
# MAGIC sql = f"select count(distinct person_id_deid) as num, phenotype from dars_nic_391419_j3w9t_collab.ccu019_rare_disease where codeing='ICD10' and phenotype in ({ebt_phenotypes_str}) group by phenotype"
# MAGIC display(sqlContext.sql(sql))

# COMMAND ----------

# MAGIC %sql
# MAGIC select phenotype, count(distinct person_id_deid) num from dars_nic_391419_j3w9t_collab.ccu019_rare_disease  
# MAGIC -- where phenotype='Mitochondrial DNA depletion syndrome, encephalomyopathic form with renal tubulopathy' 
# MAGIC group by phenotype

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- All rare disease observations
# MAGIC select count(distinct person_id_deid) from dars_nic_391419_j3w9t_collab.ccu019_rare_disease

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- All rare disease observations - within our study population!
# MAGIC select count(distinct person_id_deid) from dars_nic_391419_j3w9t_collab.ccu019_rare_disease as a
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020 as b
# MAGIC on b.NHS_NUMBER_DEID == a.person_id_deid

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Number of rare diseases with patents
# MAGIC select count(distinct phenotype)
# MAGIC from dars_nic_391419_j3w9t_collab.ccu019_rare_disease

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct person_id_deid) num, phenotype
# MAGIC from dars_nic_391419_j3w9t_collab.ccu019_rare_disease
# MAGIC group by phenotype
# MAGIC order by num DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dars_nic_391419_j3w9t_collab.ccu019_rare_disease limit 10

# COMMAND ----------


