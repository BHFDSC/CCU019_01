source("/mnt/efs/j.thygesen/dbConnect.R")
setwd("/mnt/efs/j.thygesen/dars_nic_391419_j3w9t_collab/CCU019")

# First load in data 
rare <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease")
category <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease_category")
if(!exists("cohort")){load("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/ccu019_pop_cohort.Rdata")}
ageonset <- dbGetQuery(con, "SELECT person_id_deid, age FROM dars_nic_391419_j3w9t_collab.ccu019_rare_disease_demographics_cohort")
demo <- cohort[which(cohort$rare_disease == 1), ]
total_n <- dbGetQuery(con, "SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort")

# Load in the prepared census data which is formated and contains the gender-age group counts from our population.
# This data is made with the script: 02_ccu013_table2_cencus_prep.R
census <- read.table("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/english_census_2021_withPopData.txt", header = T)
by_popsize <- 1000000 # Population size used to calculate age and gender adjusted estimates

correct_small_n <- function(var, denominator){
  n <- sum(var, na.rm = T)
  n_denominator <- sum(denominator, na.rm = T)
  ifelse(n<10, ifelse(n==0, 0, "<10"), paste0(n,  "(", round(n / n_denominator  * 100, 1), ")"))
}

rare_diseases <- unique(rare$phenotype)
d <- data.frame(rare_disease = rare_diseases)

for(i in 1:nrow(d)){
  x <- rare[which(rare$phenotype == d[i,1]), ]
  n <- length(unique(x$person_id_deid))
  d[i,"n"] <- n
  d[i,"prev"] <- round(n / total_n * by_popsize,3)
  d[i,"n_unique_ids"] <- ifelse(n < 10, ifelse(n == 0, 0, "<10"), n)
  uni_codes = unique(x[,c("person_id_deid", "clinical_code")])
  d[i,"n_unique_codes"] <- paste0(names(table(uni_codes$clinical_code)), "(n=", sapply(table(uni_codes$clinical_code),function(X){ifelse(X<10, "<10", X)}), ")", collapse = "|")
  ## Frequency
  x[i,"pop_freq"] <- x[i,"n_unique_ids"] / total_n
  
  ## Age and gender adjusted freq
  ag <- unique(demo[which(demo[,"person_id_deid"] %in% x[,"person_id_deid"]),c('person_id_deid','sex','age')])
  for(j in 1:nrow(census)){
    census[j, "n_rare_x"] <- sum(ag$age>=census[j, "min_age"] & ag$age<=census[j, "max_age"] & ag$sex == census[j,"gender"])
    census[j, "age_sex_rate"] <- census[j, "n_rare_x"] / census[j, "n_pop"] * by_popsize
    census[j, "weighted_age_sex_rate"] <- census[j, "age_sex_rate"] * census[j,"percentage"]
  }
  age_gender_adjusted_rate <- sum(census[, "weighted_age_sex_rate"])
  SE <- age_gender_adjusted_rate / sqrt(n)
  ci_upper <- age_gender_adjusted_rate + 1.96 * SE
  ci_lower <- age_gender_adjusted_rate - 1.96 * SE
  d[i, "age_gender_prev"] <- round(age_gender_adjusted_rate,3)
  d[i, "age_gender_prev_CI_upper"] <- round(ci_upper,3)
  d[i, "age_gender_prev_CI_lower"] <- round(ci_lower,3)

  ## Genders
  gender <- unique(ag[,c('person_id_deid','sex')])
  gender$sex <- as.numeric(gender$sex)
  d[i, "female"] <- correct_small_n(gender$sex==2, !is.na(gender$sex))
  d[i, "male"] <- correct_small_n(gender$sex==1, !is.na(gender$sex))
  d[i, "unknown_gender"] <- correct_small_n(is.na(gender$sex), is.na(gender$sex))
  
  age <- unique(ag[,c('person_id_deid','age')])
  age[which(age$age > 130 | age$age < 0), "age"] <- NA
  d[i, "age_18"] <- correct_small_n(age$age<18, !is.na(age$age))
  d[i, "age_18_29"] <- correct_small_n(age$age>=18 & age$age <=29, !is.na(age$age))
  d[i, "age_30_49"] <- correct_small_n(age$age>=30 & age$age <=49, !is.na(age$age))
  d[i, "age_50_69"] <- correct_small_n(age$age>=50 & age$age <=69, !is.na(age$age))
  d[i, "age_70"] <- correct_small_n(age$age>=70, !is.na(age$age))
  d[i, "unknown_age"] <- correct_small_n(is.na(age$age), length(age))
  age_onset <-  ageonset[which(ageonset[,1] %in% x[,1]), ]
  age_onset$age <- as.numeric(age_onset$age)
  d[i, "age_of_onset"] <- paste(round(mean(age_onset$age),1), "(", round(sd(age_onset$age),1), ")")
  d[i, "age_of_onset_median"] <- median(age_onset$age)
  d[i, "age_of_onset_lower_quantile"] <- quantile(age_onset$age)[2]
  d[i, "age_of_onset_upper_quantile"] <- quantile(age_onset$age)[4]
  d[i, "age_of_onset_IQR"] <- IQR(age_onset$age)
  
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
head(d)
d[which(d$rare_disease == "Leprechaunism"), ]
d[which(d$rare_disease == "Polymyalgia rheumatica"), ]
d[which(d$rare_disease == "Lafora disease"), ]
nrow(d) - sum(d$n_unique_ids=="<10")
nrow(d[which(d$n>=10),])
nrow(d[which(d$n>5),])

png("output/ccu019_adjusted_and_unadjusted_prev.png")
plot(d$prev, d$age_gender_prev, xlim = range(0:300), ylim = range(0, 300),
     main = "Adjusted and unadjusted prevalence", xlab = "Prevalence", ylab = "Age & gender adjusted prevalence")
abline(coef = c(0,1), col = "red")
dev.off()

png("output/ccu019_adjusted_and_unadjusted_prev_ZOOM.png")
plot(d$prev, d$age_gender_prev, xlim = range(0:25), ylim = range(0, 25),
     main = "Adjusted and unadjusted prevalence", xlab = "Prevalence", ylab = "Age & gender adjusted prevalence")
abline(coef = c(0,1), col = "red")
  dev.off()

head(d[which(d$n < 10 ), ],2)
head(d[which(d$n == 10 ), ],2)
tail(d)

d[which(d$n < 10 ), "prev"] <- "<0.017"
updatecols <- c("female", "male", "age_18", "age_18_29", "age_30_49", "age_50_69", "age_70", "white", 
                "asian_or_asian_british", "black_or_black_british", "mixed", "other", "unknown_ethnicity")
for(i in 1:length(updatecols)){
  d[which(d$n < 10 ), updatecols[i]] <- "<10"
}

d[which(d$n <= 5 ), "age_gender_prev"] <- "<0.010"
d[which(d$n <= 5 ), "age_gender_prev_CI_lower"] <- NA
d[which(d$n <= 5 ), "age_gender_prev_CI_upper"] <- NA

names(d)
d$n <- NULL
d$unknown_age <- NULL
d$unknown_gender <- NULL

write.table(d, "~/dars_nic_391419_j3w9t_collab/CCU019/output/tables/CCU019_table2_rare_disease_stats.txt", sep = "\t", quote = F)

## Write age stander measures
age_summary <- d[which(d$n > 5),  c("rare_disease", "age_of_onset", "age_of_onset_median", "age_of_onset_lower_quantile", "age_of_onset_upper_quantile", "age_of_onset_IQR")]
nrow(age_summary)
write.table(age_summary, "~/dars_nic_391419_j3w9t_collab/CCU019/output/tables/CCU019_extended_age_of_onset_summary_error_fixed.txt", sep = "\t", quote = F, row.names = F)

## Per category count
ncat <- data.frame(category = sort(unique(category$category)), n_ids = NA)
for(i in 1:nrow(ncat)){
  diseases <- category[which(category$category == ncat[i,1]), "phenotype"]
  x_ids <- unique(rare[which(rare$phenotype %in% diseases), "person_id_deid"])
  ncat[i,"n_ids"] <- length(x_ids)
  age_onset <-  ageonset[which(ageonset[,1] %in% x_ids), ]
  age_onset$age <- as.numeric(age_onset$age)
  ncat[i, "age_of_onset"] <- paste(round(mean(age_onset$age),2), "(", round(sd(age_onset$age),2), ")")
  
  ag <- unique(demo[which(demo[,"person_id_deid"] %in% x_ids),c('person_id_deid','sex','age')])
  gender <- unique(ag[,c('person_id_deid','sex')])
  gender$sex <- as.numeric(gender$sex)
  ncat[i, "female"] <- correct_small_n(gender$sex==2, !is.na(gender$sex))
  ncat[i, "male"] <- correct_small_n(gender$sex==1, !is.na(gender$sex))
}
ncat <- ncat[order(ncat[,2], decreasing = T), ]
ncat[,"percent"] <- round(ncat[,"n_ids"] / 58162316 * 100, 3)
write.table(ncat, "~/dars_nic_391419_j3w9t_collab/CCU019/output/tables/CCU019_rare_category_count.txt", sep = "\t", quote = F)



## Some counts
if(!exists("cohort")){load("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/ccu019_pop_cohort.Rdata")}

## Number of rare disease patients COVID events
head(cohort[which(cohort$severity!="no_covid" & cohort$rare_disease == 1), ])
sum(cohort$severity!="no_covid" & cohort$rare_disease == 1,na.rm = T)
sum(cohort$severity!="no_covid" & cohort$rare_disease == 1,na.rm = T) / sum(cohort$rare_disease == 1,na.rm = T) * 100 #  14.37931

## Number of cohort with COVID events
head(cohort[which(cohort$severity!="no_covid"), ])
sum(cohort$severity!="no_covid",na.rm = T)
sum(cohort$severity!="no_covid") / nrow(cohort) * 100 # 13.90376

## Number of individuals with specific codes
table(rare$source)
table(rare$source)/nrow(rare) * 100


