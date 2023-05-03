# devtools::install_github("16EAGLE/getSpatialData")

library(getSpatialData)
library(stringr)
library(dplyr)

set_archive("test/")
data("aoi_data")

aoi <- matrix(c(-87.09, -87.09, -86.65, -86.65, -87.09, 20.5, 20.2, 20.2, 20.5, 20.5), ncol = 2, byrow = F)

set_aoi(aoi)
view_aoi()

login_USGS(
 username = 'acturio',
 password = 'Pringlebasketmaniaco505',
 n_retry = 3,
 verbose = TRUE
 )

# services()
get_products() %>% str_subset("landsat")

products <- c("landsat_8_c1")
records <- get_records(
 #time_range = c("2015-07-21", "2015-07-21"),
 time_range = c("2021-12-28", "2022-10-12"),
 products = products
 )
records %>% as_tibble() %>%
    filter(level == "l1") %>%
    tail()

records1 <- getLandsat_records(
 time_range = c("2022-09-15", "2022-10-12"),
 products = "landsat_8_c1"
)

records_sub <- records #%>% filter(level == "l1") # subset

# get preview images
records_previews <- get_previews(records_sub, force = T)

# view previews with basemap
view_previews(records_previews)
plot_previews(records_previews)

# calc cloudcov in your aoi
records_cloudcov <- calc_cloudcov(records_previews)

#records_check <- check_availability(records_previews)
records_check2 <- check_availability(records_cloudcov)

records_ordered_ndvi <- records_check %>%
  filter(level == "sr_ndvi") %>%
  order_data(wait_to_complete = T, verbose = T)
#records_ordered2 <- order_data(records_check2)

get_ordered_records <- get_data(
  records_ordered_ndvi, force=T, wait_to_complete = T, md5_check = T
  )
get_ordered_records2 <- get_data(records_ordered2, force=T)

