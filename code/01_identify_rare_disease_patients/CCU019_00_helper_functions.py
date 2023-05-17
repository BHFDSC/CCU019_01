# Databricks notebook source

##### Use
# %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

### By Sam Hollings
# Functions to create and drop (separate) a table from a pyspark data frame
# Builds delta tables to allow for optimisation
# Modifies owner to allow tables to be dropped

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None, if_not_exists=True) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  if if_not_exists is True:
    if_not_exists_script=' IF NOT EXISTS'
  else:
    if_not_exists_script=''
  
  spark.sql(f"""CREATE TABLE {if_not_exists_script} {database_name}.{table_name} USING DELTA AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

## 2. Chris Tomlinson
# Couple of useful functions for common tasks that I haven't found direct pyspark equivalents of
# Likely more efficient ways exist for those familiar with pyspark
# Code is likley to be heavily inspired by solutions seen on StackOverflow, unfortunately reference links not kept

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

asDate = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())

# Pyspark has no .shape() function therefore define own
import pyspark

def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape

# Checking for duplicates
def checkOneRowPerPt(table, id):
  n = table.select(id).count()
  n_distinct = table.select(id).dropDuplicates([id]).count()
  print("N. of rows:", n)
  print("N. distinct ids:", n_distinct)
  print("Duplicates = ", n - n_distinct)
  if n > n_distinct:
    raise ValueError('Data has duplicates!')

# COMMAND ----------

# By Huayu
# Helper funtions for query table columns
# Helper functions for joining tables with common ID

def get_columns(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab'):
  """
  This function queries column names of table from a pyspark session and return the column names as a list
  
  :param table_name: name of the table
  :param database_name: name of the database, default to 'dars_nic_391419_j3w9t_collab'
  :return: columns as a list
  """
  columns = [col.col_name for col in spark.sql("""
    SHOW COLUMNS IN %s IN %s
  """ % (table_name, database_name)).collect()]
  return columns


def get_table_columns_map(table_names:list, database_name:str='dars_nic_391419_j3w9t_collab'):
  """
  This function queries the column names of a list of tables and return a dict of {table:[list of column names]}
  
  :param table_names: list of table names
  :param database_name: name of the database, default to 'dars_nic_391419_j3w9t_collab'
  :return: {table_name:[list of column names]}
  """
  columns_of_tables = {
    table_name: get_columns(table_name)
    for table_name in table_names
  }
  return columns_of_tables


def comma_join(l:list):
  """
  This function joins list of strings by commas, which is useful for sql queries
  
  :param l: list of strings
  :return: comma-joined string
  """
  return ','.join(l)


def sql_for_multi_leftjoin(table_names:list, join_id:str='person_id_deid', database_name:str='dars_nic_391419_j3w9t_collab'):
  
  """
  This function construct the sql query for left joinning list of tables based on the same id
  
  :param table_names: list of table names
  :param join_id: the name of id column which is shared by all tables
  :param database_name: name of the database
  :return: str of the sql query
  """
  
  table_column_map = get_table_columns_map(table_names)
  selected_columns_for_join = ''
  
  for i, table in enumerate(table_column_map):
    if i == 0:
      selected_columns_for_join += comma_join(['.'.join([table, col]) for col in table_column_map[table]])
    else:
      tmp_cols = table_column_map[table].copy()
      tmp_cols.remove(join_id)
      selected_columns_for_join += ',' + comma_join(['.'.join([table, col]) for col in tmp_cols])

  sql = ''
  
  for i, table in enumerate(table_column_map):
    if i == 0:    
#       sql += "USE %s;\n" % database_name
      sql += "SELECT %s \nFROM %s\n" % (selected_columns_for_join, table)
      last_table = table
    else:
      sql += "LEFT JOIN %s\n ON %s.%s = %s.%s\n" % (table, last_table, join_id, table, join_id)
      last_table = table  
    
  return sql
