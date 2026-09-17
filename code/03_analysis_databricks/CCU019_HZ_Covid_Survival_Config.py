# Databricks notebook source
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
