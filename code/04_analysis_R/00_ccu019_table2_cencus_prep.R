census <- read.table("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/english_census_2021.txt", header = T)

## Some minor census alignments
census[, "min_age"] <- as.numeric(sapply(strsplit(as.character(census$age_group),'_'), "[", 2))
census[, "max_age"] <- as.numeric(sapply(strsplit(as.character(census$age_group),'_'), "[", 3)) 
# NAs OK - fixed below!
census[which(is.na(census$max_age)), "max_age"] <- 200
census[which(census$sex == "Male"), "gender"] <- 1
census[which(census$sex == "Female"), "gender"] <- 2
#load("/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/ccu019_pop_cohort.Rdata")

for(x in 1:nrow(census)){
  census[x, "n_pop"] <- sum(cohort$age>=census[x, "min_age"] & cohort$age<=census[x, "max_age"] & cohort$sex == census[x,"gender"])
  print(x)
}

write.table(census, "/mnt/efs/dars_nic_391419_j3w9t_collab/CCU019/data/english_census_2021_withPopData.txt", row.names = F, sep = "\t", quote = F)
