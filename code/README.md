Analysis and curation code related to the analysis conducted for this project using the English data in the [NHS Digital Trusted Research Environment](https://digital.nhs.uk/coronavirus/coronavirus-data-services-updates/trusted-research-environment-service-for-england) for England.

## Within [Databricks in the Data Access Environment (DAE)](https://digital.nhs.uk/services/data-access-environment-dae/user-guides/using-databricks-in-the-data-access-environment)  

* `00_orphanet_rd_selection`  
	* See `readme` for description
* `01_identify_rare_disease_patients`  
    * `CCU019_00_helper_functions.py`  
    * `CCU019_01_create_input_tables.py`  
    * `CCU019_02_phenotype_definitions.py`  
    * `CCU019_03_find_rare_disease_patients.py`  
* `02_study_population`  
    * `CCU019_04_subset_to_paper_cohort.py`  
    * `CCU019_15_paper_rare_disease_demographics.py`  
    * `CCU019_16_paper_pop_demographics.py`  
* `03_analysis_databricks`  
    * `CCU019_01_Covid_Survival_Cohort.py`
    * `CCU019_02_Covid_Survival_Events.py`
    * `CCU019_03_Covid_Survival_RD-Matching_exact.py`
    * `CCU019_04_Covid_Survival_Analysis_exact.py`
    * `CCU019_05_Covid_Survival_Visualization.py`
    * `CCU019_07_Covid_Survival_Analysis_disease_group.py`
    * `CCU019_08_Covid_Survival_Visualization_group.py`
    * `CCU019_HZ_Covid_Survival_Config.py`	

## Within [RStudio in the DAE](https://digital.nhs.uk/services/data-access-environment-dae/user-guides/using-rstudio-in-the-data-access-environment)  

* `04_analysis_R`  
    * `00_ccu019_table2_cencus_prep.R`  
    * `00_save_full_pop_demographic.R`  
    * `01_ccu019_table1.Rmd`  
    * `02_ccu019_table2_ethnicity_and_sex_only.R`  
    * `02_ccu019_table2.R`  
    * `03_diff_in_gender_ethnicity_ratios.R`  



