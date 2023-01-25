################################################################################
### Script description: 
### This R Script matches the firms from the Nielsen/GS1 data to the firm information from the Orbis database  
#### based on standardized firmnames (using the stnd_compname command in Stata) and the Zipcode.
### Data input: Orbis "population20_us" file, Orbis "names_native" file, "nielsen_product_gs1.csv" 
### Data output: "Nielsen_Orbis_Match.csv", "Nielsen_Orbis_doubles_unidentified.csv", "Nielsen_Orbis_doubles_best_matches.csv", "Nielsen_unmatched.csv".
################################################################################  

library(data.table)
library(dplyr)
library(tidyr)
library(stringr)
library(haven)


### Step 1: Prepare Orbis data for standardization: Join Orbis "population20_us" file with Orbis "names_native" file. ###
pop20_us <- fread("/INSPIRE_data2/nwich/Orbis/pop/population20_us_bvdid_postcode_city.csv")

#This loop stepwise reads 10 Mio rows and joins them with the address data to prevent memory overload. (Takes <= 1h to run) 
join_list <- list()

for (i in 1:36) {
  firm_names <- fread("/INSPIRE_data2/nwich/Orbis/names_native.csv", skip = (i-1)*10000000  , nrows = 10000000, header = F)
  names(firm_names) <- c("bvdidnumber", "firm_name")
  join <- left_join(pop20_us, firm_names, by = "bvdidnumber") %>% filter(is.na(firm_name) == F) #only save entries where matching was successfull 
  join_list[[i]] <- join 
}

orbis_bvdid_name <- bind_rows(join_list) #join all chunks

write.csv(orbis_bvdid_name, "/INSPIRE_data2/nwich/Orbis/pop/population20_us_name.csv", row.names = F)
write_dta(orbis_bvdid_name, "/INSPIRE_data2/nwich/Orbis/pop/population20_us_name.dta")

#orbis_bvdid_name <- fread("/INSPIRE_data2/nwich/Orbis/pop/population20_us_name.csv") 


### Step 2: Prepare "nielsen_product_gs1.csv" file for standardization. ###
nielsen_product_gs1 <- fread("/INSPIRE_data2/nwich/Output/nielsen_product_gs1.csv", colClasses = c("upc" = "character"))
upc_freq <- nielsen_product_gs1 %>% filter(dataset_found_uc != "HMS") %>% group_by(upc_prefix) %>% summarise(upc_freq = n()) #Remove UPCs that only appear in Consumer Panel data.
nielsen_gs1_firms <- nielsen_product_gs1 %>% filter(dataset_found_uc != "HMS") %>% distinct(upc_prefix, .keep_all = T) %>% select(upc_prefix, CompanyName, City, ZipCode, StateCode, AddressLine1, AddressLine2, AddressLine3) %>% 
  left_join(upc_freq, by = "upc_prefix")
write.csv(nielsen_gs1_firms, "/INSPIRE_data2/nwich/Output/Nielsen_Orbis_Matching/nielsen_RS_gs1_firms.csv", row.names = F)


### Step 3: Run "stnd_firmnames.do" in Stata, to standardize the Nielsen and Orbis firm names with the "stnd_compname" command. ###


### Step 4: Read and preprocess standardized files. ###
Nielsen_stnd <- fread("/INSPIRE_data2/nwich/Output/Nielsen_Orbis_Matching/nielsen_RS_gs1_firms_stnd.csv") %>% select(-c(addressline2, addressline3)) %>% arrange(desc(upc_freq)) %>% slice(-1)
US_statecodes <- c("AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY")
Nielsen_stnd <- Nielsen_stnd %>% filter(statecode %in% US_statecodes) #Remove companies outside the US.
Nielsen_stnd <- Nielsen_stnd[which(nchar(Nielsen_stnd$zipcode) %in% c(5,10)),] #Remove companies with Zipcode not 5 or 10 digits long. (Most likely outside of US)
Nielsen_stnd <- Nielsen_stnd %>% mutate(zipcode_5 = str_sub(zipcode, 1, 5), zipcode_4 = str_sub(zipcode, 1, 4), zipcode_3 = str_sub(zipcode, 1, 3), zipcode_2 = str_sub(zipcode, 1, 2), zipcode_1 = str_sub(zipcode, 1, 1))

Orbis_stnd_files <- list.files("/INSPIRE_data2/nwich/Orbis/pop/population20_us_name_stnd") %>% paste0("/INSPIRE_data2/nwich/Orbis/pop/population20_us_name_stnd/",.)
Orbis_stnd <- lapply(Orbis_stnd_files, fread, colClasses = c("postcode" = "character")) %>% bind_rows()
Orbis_guo <- fread("/INSPIRE_data2/nwich/Orbis/pop/orbis_guo.csv") #Orbis GUO info
Orbis_stnd <- Orbis_stnd %>% left_join(Orbis_guo, by = c("bvdidnumber" = "subsidiarybvdid")) %>% select(-c(guo50cid_c2020))
Orbis_stnd <- Orbis_stnd %>% mutate(postcode_5 = str_sub(postcode, 1, 5), postcode_4 = str_sub(postcode, 1, 4), postcode_3 = str_sub(postcode, 1, 3), postcode_2 = str_sub(postcode, 1, 2), postcode_1 = str_sub(postcode, 1, 1))

### Step 5: Match Nielsen and Orbis files ### 

#Function to determine the number of matches for a prefix from the Nielsen/GS1 data in the Orbis data.
n_match_fun <- function(prefix, prefix_pool) {
  return(length(which(prefix == prefix_pool)))
}


#5.1 Match using standardized firm name and complete zipcode.
Match_name_zip_9 <- Nielsen_stnd %>% left_join(Orbis_stnd, by = c("firm_name_stnd" = "firm_name_stnd", "zipcode" = "postcode"), keep = T) %>% filter(!is.na(bvdidnumber))
Match_name_zip_9$n_match <- sapply(Match_name_zip_9$upc_prefix, n_match_fun, prefix_pool = Match_name_zip_9$upc_prefix) 
Match_name_zip_9$match_digits <- 9
Match_name_zip_9_exact <- Match_name_zip_9 %>% filter(n_match == 1)
Match_name_zip_9_doubles <- Match_name_zip_9 %>% filter(n_match > 1)

#5.2 Match using standardized firm name and first 5 digits of zipcode
Nielsen_stnd_5 <- Nielsen_stnd %>% anti_join(Match_name_zip_9, by = "upc_prefix") 
Match_name_zip_5 <- Nielsen_stnd_5 %>% left_join(Orbis_stnd, by = c("firm_name_stnd" = "firm_name_stnd", "zipcode_5" = "postcode_5"), keep = T) %>% filter(!is.na(bvdidnumber))
Match_name_zip_5$n_match <- sapply(Match_name_zip_5$upc_prefix, n_match_fun, prefix_pool = Match_name_zip_5$upc_prefix)
Match_name_zip_5$match_digits <- 5
Match_name_zip_5_exact <- Match_name_zip_5 %>% filter(n_match == 1)
Match_name_zip_5_doubles <- Match_name_zip_5 %>% filter(n_match > 1)

#5.3 Match using standardized firm name and first 4 digits of zipcode
Nielsen_stnd_4 <- Nielsen_stnd_5  %>% anti_join(Match_name_zip_5, by = "upc_prefix") 
Match_name_zip_4 <- Nielsen_stnd_4  %>% left_join(Orbis_stnd, by = c("firm_name_stnd" = "firm_name_stnd", "zipcode_4" = "postcode_4"), keep = T) %>% filter(!is.na(bvdidnumber))
Match_name_zip_4$n_match <- sapply(Match_name_zip_4$upc_prefix, n_match_fun, prefix_pool = Match_name_zip_4$upc_prefix)
Match_name_zip_4$match_digits <- 4
Match_name_zip_4_exact <- Match_name_zip_4 %>% filter(n_match == 1)
Match_name_zip_4_doubles <- Match_name_zip_4 %>% filter(n_match > 1)

#5.4 ... first 3 digits
Nielsen_stnd_3 <- Nielsen_stnd_4 %>% anti_join(Match_name_zip_4, by = "upc_prefix") 
Match_name_zip_3 <- Nielsen_stnd_3 %>% left_join(Orbis_stnd, by = c("firm_name_stnd" = "firm_name_stnd", "zipcode_3" = "postcode_3"), keep = T) %>% filter(!is.na(bvdidnumber))
Match_name_zip_3$n_match <- sapply(Match_name_zip_3$upc_prefix, n_match_fun, prefix_pool = Match_name_zip_3$upc_prefix)
Match_name_zip_3$match_digits <- 3
Match_name_zip_3_exact <- Match_name_zip_3 %>% filter(n_match == 1)
Match_name_zip_3_doubles <- Match_name_zip_3 %>% filter(n_match > 1)

#5.5 ... first 2 digits
Nielsen_stnd_2 <- Nielsen_stnd_3 %>% anti_join(Match_name_zip_3, by = "upc_prefix") 
Match_name_zip_2 <- Nielsen_stnd_2  %>% left_join(Orbis_stnd, by = c("firm_name_stnd" = "firm_name_stnd", "zipcode_2" = "postcode_2"), keep = T) %>% filter(!is.na(bvdidnumber))
Match_name_zip_2$n_match <- sapply(Match_name_zip_2$upc_prefix, n_match_fun, prefix_pool = Match_name_zip_2$upc_prefix)
Match_name_zip_2$match_digits <- 2
Match_name_zip_2_exact <- Match_name_zip_2 %>% filter(n_match == 1)
Match_name_zip_2_doubles <- Match_name_zip_2 %>% filter(n_match > 1)

#5.6 ... first digit
Nielsen_stnd_1 <- Nielsen_stnd_2 %>% anti_join(Match_name_zip_2, by = "upc_prefix") 
Match_name_zip_1 <- Nielsen_stnd_1 %>% left_join(Orbis_stnd, by = c("firm_name_stnd" = "firm_name_stnd", "zipcode_1" = "postcode_1"), keep = T) %>% filter(!is.na(bvdidnumber))
Match_name_zip_1$n_match <- sapply(Match_name_zip_1$upc_prefix, n_match_fun, prefix_pool = Match_name_zip_1$upc_prefix)
Match_name_zip_1$match_digits <- 1
Match_name_zip_1_exact <- Match_name_zip_1 %>% filter(n_match == 1)
Match_name_zip_1_doubles <- Match_name_zip_1 %>% filter(n_match > 1)

#5.7 ... only by firm name
Nielsen_stnd_0 <- Nielsen_stnd_1 %>% anti_join(Match_name_zip_1, by = "upc_prefix") 
Match_name_zip_0 <- Nielsen_stnd_0 %>% left_join(Orbis_stnd, by = c("firm_name_stnd" = "firm_name_stnd"), keep = T) %>% filter(!is.na(bvdidnumber))
Match_name_zip_0$n_match <- sapply(Match_name_zip_0$upc_prefix, n_match_fun, prefix_pool = Match_name_zip_0$upc_prefix)
Match_name_zip_0$match_digits <- 0
Match_name_zip_0_exact <- Match_name_zip_0 %>% filter(n_match == 1)
Match_name_zip_0_doubles <- Match_name_zip_0 %>% filter(n_match > 1)

Match_name_zip_exact <- bind_rows(Match_name_zip_9_exact, Match_name_zip_5_exact, Match_name_zip_4_exact, Match_name_zip_3_exact, Match_name_zip_2_exact, Match_name_zip_1_exact, Match_name_zip_0_exact)
Match_name_zip_doubles <- bind_rows(Match_name_zip_9_doubles, Match_name_zip_5_doubles, Match_name_zip_4_doubles, Match_name_zip_3_doubles, Match_name_zip_2_doubles, Match_name_zip_1_doubles, Match_name_zip_0_doubles)

#Remaining firms, which couldn't be matched so far
Nielsen_stnd_remaining <- Nielsen_stnd_0 %>% anti_join(Match_name_zip_0, by = "upc_prefix") %>% select(-c(zipcode_5:zipcode_1))

### Step 6: For double matches: If Orbis GUO is identical for all matches pick just one of the matches. ###
prefix_doubles <- Match_name_zip_doubles$upc_prefix %>% unique()
prefix_identified <- c()
guo_n <- c()

# Ignore guo-NA matches, if there are at least two non guo-NA matches
for (i in 1:length(prefix_doubles)) {
  b <- Match_name_zip_doubles$guo50id_c2020[Match_name_zip_doubles$upc_prefix == prefix_doubles[i]] %>% na.omit() 
  if(length(b) >= 2) {
    if(length(unique(b)) == 1) {
      prefix_identified <- append(prefix_identified, prefix_doubles[i])
      guo_n <- append(guo_n, length(b)) #number of non guo-NA matches.
    }
  }
}

### Step 7: Create output files ###
prefix_identified_guo_n <- tibble(prefix_identified, guo_n)

Match_name_zip_doubles_identified <- Match_name_zip_doubles %>% filter(upc_prefix %in% prefix_identified, !is.na(guo50id_c2020)) %>% distinct(upc_prefix, .keep_all = T) %>%  
  left_join(prefix_identified_guo_n, by = c("upc_prefix" = "prefix_identified"))  #identified doubles
Match_name_zip_exact <- Match_name_zip_exact %>% mutate(guo_n = if_else(is.na(guo50id_c2020), 0, 1))
Match_name_zip <- bind_rows(Match_name_zip_exact, Match_name_zip_doubles_identified) %>% select(-c(zipcode_5:zipcode_1, postcode_5:postcode_1)) %>% 
   relocate(guo_n, .after = n_match) %>% arrange(desc(upc_freq)) #exact matches and identified doubles

Match_name_zip_doubles_unidentified <- Match_name_zip_doubles %>% filter((upc_prefix %in% prefix_identified) == F) %>% select(-c(zipcode_5:zipcode_1, postcode_5:postcode_1)) #unidentified doubles
Match_name_zip_doubles_unidentified <- Match_name_zip_doubles_unidentified %>% group_by(upc_prefix) %>% mutate(unique_guo = length(unique(na.omit(guo50id_c2020)))) %>% 
  mutate(guo_n = length(na.omit(guo50id_c2020))) %>% ungroup() %>% relocate(guo_n, .after = n_match) %>% relocate(unique_guo, .after = guo_n)

Match_name_zip_doubles_best_matches <- Match_name_zip_doubles_unidentified %>% filter(match_digits %in% c(9,5)) %>% filter(unique_guo %in% c(0,1)) %>% 
  distinct(upc_prefix, .keep_all = T) %>% select(-unique_guo) %>% mutate(guo50id_c2020 = NA) #best_doubles: match_digits in (9,5) and at most 1 guo.


### Step 8: Save output files ###
write.csv2(Match_name_zip, "/INSPIRE_data2/nwich/Output/Nielsen_Orbis_Matching/Nielsen_Orbis_Match.csv", row.names = F)
write.csv2(Match_name_zip_doubles_unidentified, "/INSPIRE_data2/nwich/Output/Nielsen_Orbis_Matching/Nielsen_Orbis_doubles_unidentified.csv", row.names = F)
write.csv2(Match_name_zip_doubles_best_matches, "/INSPIRE_data2/nwich/Output/Nielsen_Orbis_Matching/Nielsen_Orbis_doubles_best_matches.csv", row.names = F)
write.csv2(Nielsen_stnd_remaining, "/INSPIRE_data2/nwich/Output/Nielsen_Orbis_Matching/Nielsen_unmatched.csv", row.names = F)




