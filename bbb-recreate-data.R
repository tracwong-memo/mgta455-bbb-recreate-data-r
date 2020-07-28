## Re-create the BBB data (individual assignment)

## A dataset like BBB doesn't exist in companies in its raw form
## Someone has to create it first ... likely from different data sources!
##
## The goal of this assignment is to re-create the tibble in bbb.rds data EXACTLY
## from its components. Follow the steps outlined below:
##
## 1. Determine how to load the different file types (use readr, readxl, and DBI)
## 2. Determine what data transformations are needed and how the data should be
##    combined into a tibble (aka data.frame). Use dplyr 'verbs' as much as possible.
##    tidyr may also be useful. You must name your re-created tibble 'bbb_rec'
## 3. Your work should be completely reproducible (i.e., generate the same results on
##    another computer). Think about the 'paths' you are using to load the data. Will
##    I or the TA have access to those same directories? Of course you cannot 'copy'
##    any data from bbb into bbb_rec. You can copy the data description however.
##    Read the following post for some great suggestions:
##    https://www.tidyverse.org/articles/2017/12/workflow-vs-script/
## 4. The final coding step will be to check that your code produces a tibble
##    identical to the tibble in the bbb.rds file, using the all.equal command
##    shown below. If the test passes, write bbb_rec to "data/bbb_rec.rds". Do
##    NOT change the test as this will be used in grading/evaluation
## 5. Use "Style active file" from the Addins dropdown to ensure your code is decently
##    formated and readable. See the post linked below for details
##    https://www.tidyverse.org/articles/2017/12/styler-1.0.0/
## 6. When you are done, save your, code and commit and push your work to GitLab
##    using GitGadget or the the Git tab in Rstudio. Of course you can commit and push
##    as often as you like, but only before the due date. Late assignments will not
##    be accepted
## 7. When testing your (final) code make sure to restart the R-process regularly using
##    Session > Restart R. Hint: Learn the keyboard shortcut for your OS to do this quickly
##    Restarting the R process ensures that all packages and variables your code needs
##    are actually available in your code. Do *not* use rm(list = ls()) in your code as it
##    will break our evaluation code! Again, read the post linked below
##    https://www.tidyverse.org/articles/2017/12/workflow-vs-script/
## 8. Load key libraries or use :: You may use libraries other than the ones shown below
##    but ONLY use R-packages that are part of the rsm-msba-spark docker container

library(dplyr)
library(tidyr)
library(lubridate)
library(DBI)

## load the original tibble from data/bbb.rds
bbb_file <- tempfile()
curl::curl_download(
  "https://www.dropbox.com/s/i6athvk5m4t822m/bbb.rds?dl=1",
  destfile = bbb_file
)
bbb <- readr::read_rds(bbb_file)

## view the data description of the original data to determine what
## needs to be created (result will pop-up in the Viewer tab in Rstudio)
radiant.data::describe(bbb)

## load demographics data from bbb_demographics.tsv
Demographics <- readr::read_tsv(file = "data/bbb_demographics.tsv", col_names = TRUE)

## load nonbook aggregate spending from bbb_nonbook.xlsx
library(readxl)
NonbookSpending <- read_excel("data/bbb_nonbook.xlsx")

## load purchase and buy-no-buy information from bbb.sqlite
BBBSqlite <- dbConnect(RSQLite::SQLite(), "data/bbb.sqlite")
dbListTables(BBBSqlite)
buyer <- dbGetQuery(BBBSqlite, "SELECT * FROM buyer")
purchase <- dbGetQuery(BBBSqlite, "SELECT * FROM purchase")
dbDisconnect(BBBSqlite)

## hint: what data type is "date" in the database?
## hint: most systems record dates internally as the number
## of days since some origin. You can use the lubridate::as_date
## function to convert the number to a date with argument: origin = "1970-01-01"
purchase$date <- as_date(purchase$date, origin = "1970-01-01")

## add the zip3 variable
Demographics["zip3"] <- substr(Demographics$zip, 1, 3)

## use the following reference date (i.e., "today" for the analysis)
start_date <- lubridate::ymd("2010-3-8")

## call this function to calculate the difference in months between "today" and
## the first (last) date on which a customer purchased a product
diff_months <- function(date1, date2) {
  y <- year(date1) - year(date2)
  m <- month(date1) - month(date2)
  return(y * 12 + m)
}

## generate the required dplyr code below for `first`, `last`, `book`, `purch`,
## and add the purchase frequencies for the different book types
## hint: you can use tidyr::spread to calculate the purchase frequencies
## hint: check the help for ?dplyr::first and ?dplyr::last
PurFreq <- purchase %>%
  group_by(acctnum, purchase) %>%
  summarize(n = n())

PurFreq <- spread(PurFreq, purchase, n, fill = 0)
PurFreq$purch <- rowSums(PurFreq[, 2:8])
PurFreq <- PurFreq[, c(1, 9, 3, 8, 4, 5, 7, 2, 6)]

PurFreq2 <- purchase %>%
  group_by(acctnum) %>%
  summarize(
    first = diff_months(start_date, first(date)),
    last = diff_months(start_date, last(date)),
    book = sum(price)
  )
## combine the different tibbles you loaded and `mutated`
Demographics$acctnum <- as.character(Demographics$acctnum)
Demographics[, 2:3] <- lapply(Demographics[, 2:3], as.factor)
PurFreq[, 2:9] <- lapply(PurFreq[, 2:9], as.integer)
PurFreq2[, 2:4] <- lapply(PurFreq2[, 2:4], as.integer)
buyer$buyer <- factor(buyer$buyer, levels = c("yes", "no"), labels = c("yes", "no"))

bbb_rec <- Demographics %>%
  left_join(PurFreq2, by = "acctnum") %>%
  left_join(NonbookSpending, by = "acctnum")

bbb_rec$total <- rowSums(bbb_rec[, 8:9])

bbb_rec <- bbb_rec %>%
  left_join(PurFreq, by = "acctnum") %>%
  left_join(buyer, by = "acctnum")

bbb_rec[, 9:10] <- lapply(bbb_rec[, 9:10], as.integer)

## check if the columns in bbb and bbb_rec are in the same order
## and are of the same type - fix as needed

# add the description as an attribute to bbb_rec (see data/bbb_description.txt)
mystring <- readr::read_file("data/bbb_description.txt")
attr(bbb_rec, "description") <- mystring

#############################################
## DO NOT EDIT CODE BELOW THIS LINE
## YOUR CODE MUST PASS BOTH TESTS

#############################################
test1 <- all_equal(bbb_rec, bbb)
test2 <- all_equal(attr(bbb_rec, "description"), attr(bbb, "description"))

if (isTRUE(test1) && isTRUE(test2)) {
  message("Well done! Both tests passed so you can write bbb_rec to the data/ directory")
  readr::write_rds(bbb_rec, path = "data/bbb_rec.rds")
} else {
  if (!isTRUE(test1)) {
    message(paste0("Test of equality of tibbles (data.frames) failed\n\n", paste0(test1, collapse = "\n"), "\n\nUse str(bbb) and str(bbb_rec) and check for differences\n"))
  }
  if (!isTRUE(test2)) {
    message(paste0("Test of equality of attributes failed\n\n", paste0(test2, collapse = "\n"), "\n\nUse str(bbb) and str(bbb_rec) and check for differences\n"))
  }
  stop("Tests to compare bbb and bbb_rec failed. Check your R-code carefully for errors.")
}
