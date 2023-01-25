################################################################################
# Script description: This R Script performs the following tasks:
### Aggregate the complete Nielsen Retail Scanner Data (2006-2020) on a UPC x Retailer x State x Year level by 
### computing weighted means, weighted medians and the number of observations, following Baker et al. (2020).
### Input files: All Nielsen Retail Scanner Movement files, Nielsen Retail Scanner Store files and Nielsen Retail Scanner UPC Version files. 
### Output file: "RS_upc_retailer_state_year_w_mean_median.csv"
################################################################################

library(data.table)
library(dplyr)
library(tidyr)
library(stringr)
library(stringi)
library(lubridate)
library(purrr)
library(readxl)
library(haven)
library(DescTools)
options(dplyr.summarise.inform = FALSE)

################################################################################

### Part 1: Read and preprocess Nielsen Retail Scanner Stores files. ###

################################################################################

# Read Retail Scanner Stores files from 2006-2020
RS_store_files_part1 <- list.files(path = "/INSPIRE_data2/nwich/Retail Scanner Data/", recursive = T) %>% 
  .[str_which(., "stores_")] %>% paste0("/INSPIRE_data2/nwich/Retail Scanner Data/", .)
RS_store_files_part2 <- list.files(path = "/hdd/Nielsen_Retail_Scanner_Part2/", recursive = T) %>% 
  .[str_which(., "stores_")] %>% paste0("/hdd/Nielsen_Retail_Scanner_Part2/", .)
RS_store_files <- c(RS_store_files_part1, RS_store_files_part2)

Nielsen_stores <- lapply(RS_store_files, fread) %>% bind_rows() %>% select(store_code_uc, year, retailer_code, fips_state_code, parent_code, dma_code) # Don't change the variable order

#Impute missing fips_state_code information in Stores files from previous years 
a <- Nielsen_stores %>% filter(is.na(fips_state_code)) %>% pull(store_code_uc) %>% unique() #store_code_uc s where fips_state_code information is missing for at least one year
b <- Nielsen_stores %>% filter(store_code_uc %in% a & !is.na(fips_state_code)) %>% distinct(store_code_uc, fips_state_code, dma_code) #Imputation data

for (i in 1:dim(b)[1]) {
  Nielsen_stores[Nielsen_stores$store_code_uc == b$store_code_uc[i] & is.na(Nielsen_stores$fips_state_code),4] <- b$fips_state_code[i] #Impute state code information
  Nielsen_stores[Nielsen_stores$store_code_uc == b$store_code_uc[i] & is.na(Nielsen_stores$dma_code),6] <- b$dma_code[i] #Impute dma information
}

#Impute missing retailer code information in Stores files
## Note: There are ~ 88.000 Year x Store observations (from ~ 540.000 obs.), where the retailer_code is missing. Almost all missings are prior to 2013.
## Imputation strategy: Impute missing retailer information from observations of the same store from other years. Only impute the retailer, when there were no changes in the retailer_code and in the parent_code.
## This reduces missing observations to ~ 13.000.

store_code_impute <- Nielsen_stores %>% filter(is.na(retailer_code)) %>% distinct(store_code_uc) %>% pull(store_code_uc) #stores with missing retailer information

store_code_list <- list()

for (i in 1:length(store_code_impute)) {
  y <- Nielsen_stores %>% filter(store_code_uc == store_code_impute[i]) #filter for the respective store
  if(any(is.na(unique(y$retailer_code)) == F) & length(unique(y$retailer_code)) <= 2 & length(unique(na.omit(y$parent_code))) == 1) {
    y[is.na(y$retailer_code),3] <- unique(na.omit(y$retailer_code))  #impute retailer information
  }
  store_code_list[[i]] <- y 
}

#Rebuild Nielsen_stores table with imputed retailer information
Nielsen_stores_imputed <- store_code_list %>% bind_rows() %>% bind_rows((Nielsen_stores %>% filter(store_code_uc %in% store_code_impute == F))) %>% select(-c(parent_code))



################################################################################

### Part 2: Read and preprocess Nielsen Retail Scanner Version Files. ###

################################################################################

# Read Retail Scanner Version Files (for UPC versions)
RMS_version_files_part1 <- list.files(path = "/INSPIRE_data2/nwich/Retail Scanner Data/", recursive = T, "rms") %>% paste0("/INSPIRE_data2/nwich/Retail Scanner Data/", .)
RMS_version_files_part2 <- list.files(path = "/hdd/Nielsen_Retail_Scanner_Part2/", recursive = T, "rms") %>% paste0("/hdd/Nielsen_Retail_Scanner_Part2/", .)
RMS_version_files <- c(RMS_version_files_part2, RMS_version_files_part1)
RMS_version <- lapply(RMS_version_files, fread, colClasses = c("upc" = "character", "upc_ver_uc" = "character")) 
names(RMS_version) <- 2006:2020


################################################################################

### Part 3: Create a meta dataset of all Nielsen Retail Scanner Movement files ###

#Notes:
## Each file contains all observations of a given Product Module in a given year. Overall, there are 15,419 files.  
## Files are arranged according to file size in ascending order.

################################################################################

# List all Retail Scanner Movement Files from 2006-2020 and create an overview table of all files with file size (in GB), year and file categories
RS_files_part1 <- list.files(path = "/INSPIRE_data2/nwich/Retail Scanner Data/", recursive = T) %>% .[str_which(., "Movement")] %>% paste0("/INSPIRE_data2/nwich/Retail Scanner Data/", .)
RS_files_part2 <- list.files(path = "/hdd/Nielsen_Retail_Scanner_Part2/", recursive = T) %>% .[str_which(., "Movement")] %>% paste0("/hdd/Nielsen_Retail_Scanner_Part2/", .)
RS_files <- c(RS_files_part1, RS_files_part2)

RS_files_info <- file.info(RS_files) %>% mutate(size_gb = size/(10^9)) %>% select(size_gb) %>% tibble(file = row.names(.), .)
row.names(RS_files_info) <- NULL
marker_year <- RS_files_info$file %>% str_locate("\\.tsv") %>% .[,1]
RS_files_info <- RS_files_info %>% tibble(year = str_sub(RS_files_info$file, marker_year-4, marker_year-1), 
                                          category_l = str_sub(RS_files_info$file, marker_year-9, marker_year-6), 
                                          category_h = str_sub(RS_files_info$file, marker_year-19, marker_year-16)) 
Categories <- read_excel("/INSPIRE_data2/nwich/Retail Scanner Data/reference_docs_and_masterFiles_neu/RMS/reference_documentation/Product_Hierarchy_2022.xlsx", col_types = "text")
RS_files_info <- Categories %>% select(product_module_code, product_module_descr, product_group_descr) %>% distinct(product_module_code, .keep_all = T) %>% 
  right_join(., RS_files_info, by = c("product_module_code" = "category_l")) %>% 
  select(file, size_gb, year, product_group_code  = category_h, product_group_descr, product_module_code, product_module_descr)

RS_files_info <- RS_files_info %>% arrange(size_gb)
RS_files_info$year <- as.numeric(RS_files_info$year)


################################################################################

### Part 4: Calculate weighted mean and weighted median on the UPC x Retailer x State x Year level  ###

# Notes: 
## Loops through all 15,419 Module files
## Takes ~ 4 days to run

# Processing steps:
## 1) Remove duplicate observations
## 2) Remove observations where retailer_code or fips_state_code or dma_code is not availabe
## 3) Remove all featured and displayed observations, after imputation based on retailer and designated market area.
## 4) Remove all observations with prmult > 1, remove all observations with price == 0.01 (cf. p. 16 in Retail Scanner data manual), and remove all obs. with units == 0.

################################################################################

for (i in 1:length(RS_files_info$file)) {
  
  #Read module file, add year information and Store information
  module <- fread(RS_files_info$file[i], colClasses = c("upc" = "character", "week_end" = "character")) %>% mutate(year = RS_files_info$year[i]) %>%
    left_join(Nielsen_stores_imputed, by = c("store_code_uc", "year"))
  
  #Remove duplicate observations (same store_code, same upc and same week)
  c <- duplicated(module %>% select(store_code_uc, upc, week_end))
  d <- duplicated(module %>% select(store_code_uc, upc, week_end), fromLast = T)
  module <- module[!(c | d),]
  
  #Remove observations where retailer_code or fips_state_code or dma_code is not availabe (necessary to impute feature and display variables)
  module <- module %>% filter(!is.na(retailer_code) & !is.na(fips_state_code) & !is.na(dma_code))
  
  #Remove all featured and displayed observations (with imputation based on retailer and DMA)
  feature_display <- module %>% filter(feature == 1 | display == 1)
  module <- module %>% anti_join(feature_display, by = c("upc", "week_end", "retailer_code", "dma_code")) %>% select(-c(feature, display))
  
  invisible(gc(full = T))
  
  #Remove all observations with prmult > 1, remove all observations with price == 0.01 (cf. p. 16 in Retail Scanner data manual), and remove all obs. with units == 0.
  module <- module %>% filter(prmult == 1, price > 0.01, units > 0) %>% select(-prmult) 
  
  invisible(gc(full = T))
  
  #Calculate weighted mean and weighted median of prices on the UPC x State x Year level
  module <- module %>% group_by(upc, retailer_code, fips_state_code, year) %>% 
     summarise(p_w_mean = weighted.mean(price, w = units), p_w_median = Quantile(price, weights = units, probs = 0.5), 
               p_w_q10 = Quantile(price, weights = units, probs = 0.1), p_w_q90 = Quantile(price, weights = units, probs = 0.9), n_obs = n(), n_sales = sum(units)) %>%
               ungroup() %>% left_join(., RMS_version[[RS_files_info$year[[i]]-2005]], by = "upc") %>% relocate(upc_ver_uc, .after = upc) %>% select(-c(panel_year)) 
   
  
   saveRDS(module, paste0("/INSPIRE_data2/nwich/Output/freq_ls/",i,".RDS"))
  
  #Keep track of progress
  print(sum(RS_files_info$size_gb[1:i])/sum(RS_files_info$size_gb))
  print(i)
  
  rm(list = c("module"), inherits = T)
  invisible(gc(full = T))
}



################################################################################

### Part 5: Create output file

################################################################################
    
# Concatenate all files and save output 
files <- list.files("/INSPIRE_data2/nwich/Output/freq_ls/") %>% paste0("/INSPIRE_data2/nwich/Output/freq_ls/",.) 
Nielsen_RS_Dataset <- lapply(files, readRDS) %>% rbindlist() %>% drop_na()
write.csv(Nielsen_RS_Dataset,"/INSPIRE_data2/nwich/Output/RS_upc_retailer_state_year_w_mean_median.csv", row.names = F)

