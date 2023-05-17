source("/mnt/efs/j.thygesen/dbConnect.R")
setwd("/mnt/efs/j.thygesen/dars_nic_391419_j3w9t_collab/CCU019")


## Load disease data
d <- read.table("output/tables/CCU019_table2_rare_disease_stats_ethnicity_and_sex_only.txt", header = T, sep = "\t")

# Load in gender ratios 
gender <- dbGetQuery(con, "SELECT sex, count(*) as n FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics group by sex")

ethnic <- dbGetQuery(con, "SELECT ethnic_group, count(*) as n FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort_demographics 
group by ethnic_group")


d <- d[which(d$n>=100),]
stats <- data.frame(rare_disease = d[,"rare_disease"])

options(warn=1) # Treat warnings as warnings
#options(warn=2) # Treat warnings as erros


for(i in 1:nrow(d)){
  print(paste0(i, " ",   d[i,"rare_disease"]))
  stats[i,"rare_disease"] <- d[i,"rare_disease"]
  stats[i,"n"] <- d[i,"n"]

  ## Sex
  rare_male <- as.numeric(unlist(strsplit(d[i,"male"],"\\("))[1])
  rare_female <- as.numeric(unlist(strsplit(d[i,"female"],"\\("))[1])
  unaffected_male <- as.numeric(gender[which(gender$sex == 1), "n"] - rare_male)
  unaffected_female <- as.numeric(gender[which(gender$sex == 2), "n"] - rare_female)

  rare <- c(rare_male, rare_female) 
  unaffected <- c(unaffected_male, unaffected_female)
  gender_table <- rbind(rare, unaffected)
  
  if(sum(gender[which(gender$sex %in% c(0,9)), "n"]) + sum(unaffected)+ sum(rare)!= 58162316){"error in sum"}
  
  stats[i,"sex_rare_ratio"] <- round(rare_male / rare_female,2)
  #stats[i,"female_rare_ratio"] <- round(rare_female / rare_male,2)
  stats[i,"sex_unaffected_ratio"] <- round(unaffected_male / unaffected_female,6)
  stats[i, "sex_ratio_diff"] <-  stats[i,"sex_rare_ratio"] - stats[i,"sex_unaffected_ratio"]
  
  if(all(gender_table>5)){
    
    test <- chisq.test(gender_table)
    stats[i,"sex_p-value"] <-  test$p.value
    stats[i,"sex_method"] <- "chisq"
    
  }else{
    test <- fisher.test(gender_table)
    stats[i,"sex_p-value"] <- test$p.value
    stats[i,"sex_method"] <- "fisher"
  }
  
  ## Ethnicity
  rare_white <- as.numeric(unlist(strsplit(d[i,"white"],"\\("))[1])
  rare_asian <- as.numeric(unlist(strsplit(d[i,"asian_or_asian_british"],"\\("))[1])
  rare_black <- as.numeric(unlist(strsplit(d[i,"black_or_black_british"],"\\("))[1])
  unaffected_white <- as.numeric(ethnic[which(ethnic$ethnic_group == "White"), "n"] - rare_white)
  unaffected_asian <- as.numeric(ethnic[which(ethnic$ethnic_group == "Asian or Asian British"), "n"] - rare_white)
  unaffected_black <- as.numeric(ethnic[which(ethnic$ethnic_group == "Black or Black British"), "n"] - rare_white)

  rare <- c(rare_white, rare_asian) 
  unaffected <- c(unaffected_white, unaffected_asian)
  ethnic_asian_table <- rbind(rare, unaffected)
  
  #stats[i,"asian_white_rare_ratio"] <- round(rare_asian / rare_white, 2)
  stats[i,"asian_rare_ratio"] <- round(rare_asian / rare_white, 2)
  stats[i,"asian_unaffected_ratio"] <- round(unaffected_asian / unaffected_white, 6)
  stats[i, "asian_ratio_diff"] <-  stats[i,"asian_rare_ratio"] - stats[i,"asian_unaffected_ratio"]
  
  if(all(ethnic_asian_table>5)){
    
    test <- chisq.test(ethnic_asian_table)
    stats[i,"asian_p-value"] <-  test$p.value
    stats[i,"asian_method"] <- "chisq"
    
  }else{
    test <- fisher.test(ethnic_asian_table)
    stats[i,"asian_p-value"] <- test$p.value
    stats[i,"asian_method"] <- "fisher"
  }
  
  rare <- c(rare_white, rare_black) 
  unaffected <- c(unaffected_white, unaffected_black)
  ethnic_black_table <- rbind(rare, unaffected)
  
  #stats[i,"black_white_rare_ratio"] <- round(rare_black / rare_white, 2)
  stats[i,"black_rare_ratio"] <- round(rare_black / rare_white, 2)
  stats[i,"black_unaffected_ratio"] <- round(unaffected_black / unaffected_white, 6)
  stats[i, "black_ratio_diff"] <-  stats[i,"black_rare_ratio"] - stats[i,"black_unaffected_ratio"]
  
  if(all(ethnic_black_table>5)){
    test <- chisq.test(ethnic_black_table)
    stats[i,"black_p-value"] <-  test$p.value
    stats[i,"black_method"] <- "chisq"
    
  }else{
    test <- fisher.test(ethnic_black_table)
    stats[i,"black_p-value"] <- test$p.value
    stats[i,"black_method"] <- "fisher"
  }
  
}

i <- which(d$rare_disease == "Maple syrup urine disease")

## Gender  Nominal significance
nrow(stats[which(stats[,"sex_p-value"] <= 0.05),])
## Corrected significance
nrow(stats[which(stats[,"sex_p-value"] <= (0.05/nrow(stats))),]) # 111

sort(stats[which(stats[,"sex_p-value"] <= (0.05/nrow(stats))),"sex_ratio_diff"])
nrow(stats[which(stats[,"sex_p-value"] <= (0.05/nrow(stats)) & stats$sex_ratio_diff>0),]) # 49
nrow(stats[which(stats[,"sex_p-value"] <= (0.05/nrow(stats)) & stats$sex_ratio_diff<0),]) # 62

## Asian
#--------------
nrow(stats[which(stats[,"asian_p-value"] <= 0.05/nrow(stats)),]) # 100
sort(stats[which(stats[,"asian_p-value"] <= 0.05/nrow(stats)),"asian_ratio_diff"])
nrow(stats[which(stats[,"asian_p-value"] <= (0.05/nrow(stats)) & stats$asian_ratio_diff>0),]) # 47 over representated
nrow(stats[which(stats[,"asian_p-value"] <= (0.05/nrow(stats)) & stats$asian_ratio_diff<0),]) # 53 under representated
asian_test <- stats[which(stats[,"asian_p-value"] <= (0.05/nrow(stats))),]
head(asian_test[order(asian_test$asian_ratio_diff, decreasing = T), c("rare_disease", "asian_rare_ratio", "asian_unaffected_ratio", "asian_ratio_diff")],3) # over represented
head(asian_test[order(asian_test$asian_ratio_diff),c("rare_disease", "asian_rare_ratio", "asian_unaffected_ratio", "asian_ratio_diff")],3) # Under represented

## black
#--------------
nrow(stats[which(stats[,"black_p-value"] <= 0.05/nrow(stats)),]) # 75
sort(stats[which(stats[,"black_p-value"] <= 0.05/nrow(stats)),"black_ratio_diff"])
nrow(stats[which(stats[,"black_p-value"] <= (0.05/nrow(stats)) & stats$black_ratio_diff>0),]) # 22 over represented
nrow(stats[which(stats[,"black_p-value"] <= (0.05/nrow(stats)) & stats$black_ratio_diff<0),]) # 53 under represented

black_test <- stats[which(stats[,"black_p-value"] <= (0.05/nrow(stats))),]
head(black_test[order(black_test$black_ratio_diff, decreasing = T),c("rare_disease", "black_rare_ratio", "black_unaffected_ratio", "black_ratio_diff")],3) # over represented
head(black_test[order(black_test$black_ratio_diff),c("rare_disease", "black_rare_ratio", "black_unaffected_ratio", "black_ratio_diff")],3) # Under represented

asian_under <- stats[which(stats[,"asian_p-value"] <= (0.05/nrow(stats)) & stats$asian_ratio_diff<0),"rare_disease"]
black_under <- stats[which(stats[,"black_p-value"] <= (0.05/nrow(stats)) & stats$black_ratio_diff<0),"rare_disease"]

## Write out important columns
stats$n <- NULL
write.table(stats, "output/tables/CCU019_table3_gender_ethnic_diff.txt", row.names = F, sep = "\t", quote = F)
