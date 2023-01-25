################################################################################
### Script description: This R Script performs the following tasks: 
### Identify the firm behind each UPC Code in the Nielsen products file from the GS1 file  
### by using the unique UPC Prefix (first 6-9/11 digits of the UPC Code) as an identifier. 
### The Nielsen products file (products.tsv) contains all UPC Codes (~ 5.6 Mio) from all Nielsen Datasets (Consumer Panel and Retail Scanner Data).
### The GS1 file (PrefixListAll.csv) contains UPC prefixes of firms and firm information like address, state etc.
### Output: The Nielsen products file augmented by firm prefixes and the associated firm information from the GS1 file ("nielsen_product_gs1.csv").
################################################################################

library(data.table)
library(dplyr)
library(stringr)
library(readxl)

#Read and prepare nielsen products file
nielsen_product <- fread("/INSPIRE_data2/nwich/Retail Scanner Data/reference_docs_and_masterFiles_neu/RMS/Master_Files/Latest/products.tsv", colClasses = c("upc" = "character"))
nielsen_upc <- nielsen_product$upc

#Read and prepare GS1 data 
gs1 <- fread("/INSPIRE_data2/nwich/Retail Scanner Data/GS1/PrefixListAll.csv")
gs1 <- gs1 %>% select(PrefixString, CompanyName, City, ZipCode, StateCode, AddressLine1, AddressLine2, AddressLine3)
gs1 <- apply(gs1, 2, str_remove_all, "\"") 
gs1 <- apply(gs1, 2, str_remove_all, "=")
gs1 <- apply(gs1, 2, str_trim)
gs1 <- as_tibble(gs1)
gs1_prefix <- gs1$PrefixString 

#For each UPC code, there are 6 prefix candidates (first 6-11 digits of UPC code)
cand_6 <- sapply(nielsen_upc, str_sub, 1, 6) #6 digits
cand_7 <- sapply(nielsen_upc, str_sub, 1, 7) #7 digits
cand_8 <- sapply(nielsen_upc, str_sub, 1, 8) #8 digits
cand_9 <- sapply(nielsen_upc, str_sub, 1, 9) #9 digits
cand_10 <- sapply(nielsen_upc, str_sub, 1, 10) #10 digits
cand_11 <- sapply(nielsen_upc, str_sub, 1, 11) #11 digits

cand_df <- tibble(cand_6, cand_7, cand_8, cand_9, cand_10, cand_11) 

#Function that checks which of the prefix candidates is correct and returns the correct one. If there is no match: Return "NA[Number of candidates]" (should be "NA0")
ind_fun  <- function(c, vec) {
  ind_6 <- which(c[1] == vec)
  ind_7 <- which(c[2] == vec)
  ind_8 <- which(c[3] == vec)
  ind_9 <- which(c[4] == vec)
  ind_10 <- which(c[5] == vec)
  ind_11 <- which(c[6] == vec)
  ind <- c(ind_6, ind_7, ind_8, ind_9, ind_10, ind_11)
  n_cand <- as.character(length(ind))
  if(length(ind) == 1){return(vec[ind])}
  else{return(paste0("NA",n_cand))}
}

#Find all Prefixes for all UPC codes from the Nielsen data in the gs1 data - takes ~ 48h to run
upcpre_vec <- apply(cand_df, 1, ind_fun, gs1_prefix)

#Add UPC prefixes to the nielsen products file and also add the associated company information from the gs1 file
nielsen_product_prefix <- data.frame(upc_prefix = upcpre_vec, nielsen_product)
nielsen_product_gs1 <- left_join(nielsen_product_prefix, gs1, by = c("upc_prefix" = "PrefixString"))

#Save output
write.csv2(nielsen_product_gs1, "/INSPIRE_data2/nwich/Output/nielsen_product_gs1.csv", row.names = F)



