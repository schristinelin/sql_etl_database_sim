#### CS 5200 Summer 2022
### Practicum 2 - Part 1
### Shanshan Lin
### 6/28/2022


# import libraries
library(RSQLite)
library(XML)
library(dplyr)

# retrieve the directory this script was saved in
fpath <- dirname(rstudioapi::getSourceEditorContext()$path)
# XML data file saved in the same directory
xmlname <- '/pubmed-tfm-xml/pubmed22n0001-tf.xml'
fpn = paste0(fpath, xmlname)
# read XML file
xmlObj <- xmlParse(paste(fpath, xmlname, sep='/'))
# retrieve roots and length of XML file
r <- xmlRoot(xmlObj)
xml_length <- xmlSize(r)

# create empty data frame to store results
df_all <- data.frame()
# loop through individual nodes
# and use XPath expressions to extract attributes for each row in data frame
for (i in seq(1, xml_length)) {
  # display status
  print(i)
  # get single node
  row <- r[[i]]
  
  # pmid of node
  pmid <- as.numeric(xmlAttrs(row)[1])
  
  # first - journal
  # issn
  xpathEx <- "*/Journal/ISSN"
  ISSN <- unlist(xpathApply(row, xpathEx, xmlValue))
  # if there is no attribute value, then return NA
  # same for the rest of other attributes
  ISSN <- ifelse(is.null(ISSN) == TRUE, NA, ISSN)
  # volume
  xpathEx <- "*/Journal/JournalIssue/Volume"
  volume <- unlist(xpathApply(row, xpathEx, xmlValue))
  volume <- ifelse(is.null(volume) == TRUE, NA, volume)
  # issue
  xpathEx <- "*/Journal/JournalIssue/Issue"
  issue <- unlist(xpathApply(row, xpathEx, xmlValue))
  issue <- ifelse(is.null(issue) == TRUE, NA, issue)
  # pub year
  xpathEx <- "*/Journal/JournalIssue/PubDate/Year"
  pubdate_year <- unlist(xpathApply(row, xpathEx, xmlValue))
  pubdate_year <- ifelse(is.null(pubdate_year) == TRUE, NA, pubdate_year)
  # pub month
  xpathEx <- "*/Journal/JournalIssue/PubDate/Month"
  pubdate_month <- unlist(xpathApply(row, xpathEx, xmlValue))
  pubdate_month <- ifelse(is.null(pubdate_month) == TRUE, NA, pubdate_month)
  # pub day
  xpathEx <- "*/Journal/JournalIssue/PubDate/Day"
  pubdate_day <- unlist(xpathApply(row, xpathEx, xmlValue))
  pubdate_day <- ifelse(is.null(pubdate_day) == TRUE, NA, pubdate_day)
  # title
  xpathEx <- "*/Journal/Title"
  Title <- unlist(xpathApply(row, xpathEx, xmlValue))
  Title <- ifelse(is.null(Title) == TRUE, NA, Title)
  # iso abbreviation
  xpathEx <- "*/Journal/ISOAbbreviation"
  ISOAbbreviation <- unlist(xpathApply(row, xpathEx, xmlValue))
  ISOAbbreviation <- ifelse(is.null(ISOAbbreviation) == TRUE, NA, ISOAbbreviation)
  
  # second - article title
  xpathEx <- "*/ArticleTitle"
  ArticleTitle <- unlist(xpathApply(row, xpathEx, xmlValue))
  ArticleTitle <- ifelse(is.null(ArticleTitle) == TRUE, NA, ArticleTitle)
  
  # store the attributes so far as one data frame row
  df <- data.frame(PMID = pmid, ISSN = ISSN, Volume = volume, Issue = issue, PubDate_Year = pubdate_year, PubDate_Month = pubdate_month, PubDate_Day = pubdate_day,
                   Title = Title, ISOAbbreviation = ISOAbbreviation, ArticleTitle = ArticleTitle)
  
  # third - authorlist
  authors_node <- row[[1]][[3]]
  # how many authors are there in this node?
  authors_len <- as.numeric(xmlSize(authors_node))
  # copy the row, df, for as many times as the number of authors present
  df <- df[rep(seq_len(nrow(df)), each = authors_len), ]
  
  # empty dataframe to store author results
  df_authors <- data.frame()
  # get results for each author
  for (i in seq(1, authors_len)) {
    LastName <- xmlValue(authors_node[[i]][[1]])
    ForeName <- xmlValue(authors_node[[i]][[2]])
    Initials <- xmlValue(authors_node[[i]][[3]])
    # bind each author onto each according row
    df_temp <- data.frame(LastName = LastName, ForeName = ForeName, Initials = Initials)
    df_authors <- rbind(df_authors, df_temp)
  }
  
  # construct the big dataframe
  df_node <- cbind(df, df_authors)
  df_all <- rbind(df_node, df_all)
  
}

# set primary key, 'rid' - record id
rownames(df_all) <- seq(1:nrow(df_all))
# drop duplicates
df_all <- unique(df_all)
# uncomment to save the document
#write.csv(df_all, paste0(fpath, '/xml_pubmed.csv'))

# create and connect to database
dbfile = '/pubmedDB.sqlite'
dbcon <- dbConnect(SQLite() ,paste0(fpath, dbfile))

# drop table exists for housekeeping purposes
dbSendQuery(dbcon, "DROP TABLE IF EXISTS Journals")
dbSendQuery(dbcon, "DROP TABLE IF EXISTS Articles")
dbSendQuery(dbcon, "DROP TABLE IF EXISTS Authors")

### create the tables
# articles table
dbSendQuery(dbcon, "CREATE TABLE Articles 
(article_id int NOT NULL,
  ArticleTitle TEXT,
  PRIMARY KEY (article_id)
);")

# Authors table
dbSendQuery(dbcon, "CREATE TABLE Authors 
(author_id int NOT NULL,
  LastName TEXT,
  ForeName TEXT,
  Initials TEXT,
  PRIMARY KEY (author_id)
);")

# Journals table
dbSendQuery(dbcon, "CREATE TABLE Journals 
(rid int NOT NULL, 
  PMID TEXT,
  ISSN TEXT,
  Title TEXT,
  ISOAbbreviation TEXT,
  Volume INT,
  Issue INT,
  PubDate_Year INT,
  PubDate_Month TEXT,
  PubDate_Day INT,
  article_id int NOT NULL,
  author_id int NOT NULL,
  PRIMARY KEY (rid),
  FOREIGN KEY(article_id) REFERENCES Articles(article_id),
  FOREIGN KEY(author_id) REFERENCES Authors(author_id)
);")

# Populate articles table
article_titles <- unique(df_all$ArticleTitle)
# surrogate keys for articles table
article_id <- seq(10000, 10000+length(article_titles)-1)
articles <- data.frame(article_id = article_id, ArticleTitle = article_titles)
# store in database
dbWriteTable(dbcon, "Articles", articles, append = T)
dbReadTable(dbcon, "Articles")


# Populate authors table
authors <- df_all %>% select(LastName, ForeName, Initials)
authors <- unique(authors)
authors$author_id <- seq(100, 100+nrow(authors)-1)
# store in database
dbWriteTable(dbcon, "Authors", authors, append = T)
dbReadTable(dbcon, "Authors")


# populate the journals table
df_all <- merge(df_all, authors, by  = c("LastName", "ForeName", "Initials"))
journals <- df_all %>% select(PMID, ISSN, Volume, Issue, PubDate_Year, PubDate_Month, PubDate_Day, Title, ISOAbbreviation, author_id)
journals$rid <- rownames(journals)
# link foreign keys
journals$article_id <- with(articles, article_id[match(df_all$ArticleTitle, ArticleTitle)])
# store in database
dbWriteTable(dbcon, "Journals", journals, append = T)
dbReadTable(dbcon, "Journals")

# disconnect database
dbDisconnect(dbcon)
