source("/mnt/efs/j.thygesen/dbConnect.R")
setwd("/mnt/efs/j.thygesen/dars_nic_391419_j3w9t_collab/CCU019")

# First load in data 
rare <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease")
category <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease_category")
if(!exists("cohort")){load("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/ccu019_pop_cohort.Rdata")}
demo <- cohort[which(cohort$rare_disease == 1), ]
total_n <- dbGetQuery(con, "SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort")

# Load in the prepared census data which is formated and contains the gender-age group counts from our population.
# This data is made with the script: 02_ccu013_table2_cencus_prep.R
census <- read.table("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/english_census_2021_withPopData.txt", header = T)
by_popsize <- 1000000 # Population size used to calculate age and gender adjusted estimates

correct_small_n <- function(var, denominator){
  n <- sum(var, na.rm = T)
  n_denominator <- sum(denominator, na.rm = T)
  paste0(n,  "(", round(n / n_denominator  * 100, 1), ")")
}

rare_diseases <- unique(rare$phenotype)
d <- data.frame(rare_disease = rare_diseases)

for(i in 1:nrow(d)){
  x <- rare[which(rare$phenotype == d[i,1]), ]
  n <- length(unique(x$person_id_deid))
  d[i,"n"] <- n
 
  ## Genders
  gender <- unique(demo[which(demo[,"person_id_deid"] %in% x[,"person_id_deid"]),c('person_id_deid','sex')])
  gender$sex <- as.numeric(gender$sex)
  d[i, "female"] <- correct_small_n(gender$sex==2, !is.na(gender$sex))
  d[i, "male"] <- correct_small_n(gender$sex==1, !is.na(gender$sex))
  d[i, "unknown_gender"] <- correct_small_n(is.na(gender$sex), is.na(gender$sex))
  
  ethnicity <- unique(demo[which(demo[,"person_id_deid"] %in% x[,"person_id_deid"]),c('person_id_deid','ethnic_group')])
  ethnicity[which(is.na(ethnicity$ethnic_group)), "ethnic_group"] <- "Unknown"
  n_ethnicity <-  length(unique(ethnicity$person_id_deid))
  d[i, "white"] <- correct_small_n(ethnicity$ethnic_group == "White", n_ethnicity)
  d[i, "asian_or_asian_british"] <- correct_small_n(ethnicity$ethnic_group %in%  c("Asian or Asian British", "Chinese"), n_ethnicity)
  d[i, "black_or_black_british"] <- correct_small_n(ethnicity$ethnic_group == "Black or Black British", n_ethnicity)
  d[i, "mixed"] <- correct_small_n(ethnicity$ethnic_group == "Mixed", n_ethnicity)
  d[i, "other"] <- correct_small_n(ethnicity$ethnic_group == "Other", n_ethnicity)
  d[i, "unknown_ethnicity"] <- correct_small_n(ethnicity$ethnic_group == "Unknown", n_ethnicity)
  d[i, "category"] <- paste0(category[which(category[,"phenotype"]==d[i, 1]), "category"], collapse = ", ")
  print(i)
}

d <- d[order(d$n, decreasing = T), ]
d[1,]

write.table(d, "~/dars_nic_391419_j3w9t_collab/CCU019/output/tables/CCU019_table2_rare_disease_stats_ethnicity_and_sex_only.txt", sep = "\t", quote = F)

