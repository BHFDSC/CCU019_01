# Databricks notebook source
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
