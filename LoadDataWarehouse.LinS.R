#### CS 5200 Summer 2022
### Practicum 2 - Part 2
### Shanshan Lin
### 6/28/2022


### Environment set-up
### connect to AWS MySQL database
# library
library(RMySQL)

# connection params
db_user <- 'linshans'
db_password <- 'cs5200summer'
# reusing the instance created for practicum 1
db_name <- 'birdstrikedb' # database name was set to this and can't be changed...
db_host <- 'cs5200dbs.cvgbgxakq0th.us-west-2.rds.amazonaws.com'
db_port <- 3306
# connect to database
pubmedDB <- dbConnect(MySQL(), user = db_user, password = db_password,
                           dbname = db_name, host = db_host, port = db_port)

### Also connect to the SQLite database created in Part 1
# retrieve the directory this script was saved in
fpath <- dirname(rstudioapi::getSourceEditorContext()$path)
dbfile = '/pubmedDB.sqlite'
dbcon <- dbConnect(SQLite() ,paste0(fpath, dbfile))

# make sure the tables do not already exist
dbFetch(dbSendQuery(pubmedDB, "DROP TABLE IF EXISTS Author_facts"))
dbFetch(dbSendQuery(pubmedDB, "DROP TABLE IF EXISTS Journal_facts"))

# create schema
dbSendQuery(pubmedDB, "CREATE SCHEMA IF NOT EXISTS pubmed_starschema;")
dbSendQuery(pubmedDB, "USE pubmed_starschema;")

### Task 3 - create author facts table in the pubmedDB database
dbSendQuery(pubmedDB, "CREATE TABLE Author_facts
(author_id INT NOT NULL,
  LastName VARCHAR(255),
  ForeName VARCHAR(255),
  Initials VARCHAR(255),
  Num_Article_Per_Author VARCHAR(255) NOT NULL,
  Total_Num_Coauthors_Across_All VARCHAR(255) NOT NULL,
  PRIMARY KEY (author_id)
);")

# pmid is per publication. there could be multiple pmids associated with a ISSN
author_facts <- dbFetch(dbSendQuery(dbcon, "SELECT DISTINCT temp.author_id, temp.ForeName, temp.LastName, temp.Initials, temp.Total_Num_Coauthors_Across_All, temp3.Num_Article_Per_Author FROM
                    (SELECT temp1.author_id, temp1.ForeName, temp1.LastName, temp1.Initials, SUM(temp2.num_of_coauthors) Total_Num_Coauthors_Across_All FROM
                    (SELECT j.pmid, a.author_id, a.ForeName, a.LastName, a.Initials FROM Journals j JOIN Authors a ON j.author_id = a.author_id) temp1
                    JOIN (SELECT j.pmid, count(a.author_id)-1 num_of_coauthors FROM Journals j JOIN Authors a ON j.author_id = a.author_id group by j.pmid) temp2
                    ON temp1.pmid = temp2.pmid
                    GROUP BY temp1.author_id) temp
                    JOIN (SELECT DISTINCT a.author_id, count(j.pmid) Num_Article_Per_Author 
                    FROM Journals j JOIN Authors a ON j.author_id = a.author_id 
                    group by a.author_id) temp3
                    ON temp.author_id = temp3.author_id"))

# task 3 - populate the author fact table
dbWriteTable(pubmedDB, "Author_facts", author_facts, row.names = FALSE, append = TRUE)
# read the table to make sure it works
dbReadTable(pubmedDB, "Author_facts")




### task 4 - create journal facts table in the pubmedDB database
dbSendQuery(pubmedDB, "CREATE TABLE Journal_facts
(rid INT NOT NULL,
  Title VARCHAR(255),
  Num_Article_Per_year VARCHAR(255) NOT NULL,
  Num_Article_Per_quarter VARCHAR(255) NOT NULL,
  Num_Article_Per_month VARCHAR(255) NOT NULL,
  PRIMARY KEY (rid)
);")

