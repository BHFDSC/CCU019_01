# Load libraries prior to sessionInfo() to ensure documented
library(dplyr)
library(tidyr)
library(magrittr)
library(stringr)
library(DBI)

source("/mnt/efs/j.thygesen/dbConnect.R")
setwd("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019")

cohort <- dbGetQuery(con, "SELECT person_id_deid, death_covid, 
       death_all,  
       severity,
       sex, 
       age, 
       ethnic_group,
       IMD_quintile,
       rare_disease,
       high_risk
FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics")

cohort[is.na(cohort[,"death_covid"]), "death_covid"] <- 0
cohort[which(is.na(cohort$severity)), "severity"] <- "no_covid"
cohort[is.na(cohort[,"ethnic_group"]), "ethnic_group"] <- "Unknown"
cohort[is.na(cohort[,"IMD_quintile"]), "IMD_quintile"] <- "Unknown"

table(cohort$death_covid, useNA = "always")
table(cohort$death_all, useNA = "always")
table(cohort$severity, useNA = "always")
any(is.na(cohort$sex))
any(is.na(cohort$age))
table(cohort$ethnic_group, useNA = "always")
table(cohort$ethnic_group, useNA = "always")/nrow(cohort) * 100
table(cohort$IMD_quintile, useNA = "always")
any(is.na(cohort$high_risk))

cohort_small <- cohort[,2:ncol(cohort)]

save(cohort_small, file = "/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/ccu019_pop_cohort_noID.Rdata")
save(cohort, file = "/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/ccu019_pop_cohort.Rdata")
