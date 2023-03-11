### Data Processing in R and Python 2022Z
### Homework Assignment no. 1
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
### Report should include:
### * source() of this file at the beginning,
### * reading the data, 
### * attaching the libraries, 
### * execution time measurements (with microbenchmark),
### * and comparing the equivalence of the results,
### * interpretation of queries.

# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#

sqldf_1 <- function(Posts){
  
# sqldf functions need no comment
  return(sqldf('SELECT STRFTIME("%Y", CreationDate) AS Year, COUNT(*) AS TotalNumber
FROM Posts
GROUP BY Year'))
}

base_1 <- function(Posts){
  
  # count posts' ID's, grouped by the creation date year
    result<-aggregate(Posts$Id,list(format(as.Date(Posts$CreationDate),"%Y")),length)
  # simply formatting columns names
    colnames(result)<-c("Year","TotalNumber")
    
    return(result)
}

dplyr_1 <- function(Posts){
  
  # the n() function is doing the same thing as the "length" function in the base function
  result <- as.data.frame(Posts %>%
    group_by(Year = (format(as.Date(CreationDate),"%Y"))) %>%
    summarize(TotalNumber = n()))
  
  return(result)
}

data.table_1 <- function(Posts){
  
  # Here, the .N serves as the counting function 
  # Since the main tables are saved in data.frame format, they need to be transformed into data.table in order to work with data.table package
  result <- as.data.frame(as.data.table(Posts)[,.(TotalNumber=.N),by = list(Year = format(as.Date(Posts$CreationDate),"%Y"))])
  
  return(result)
}

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

sqldf_2 <- function(Users, Posts){
  
  return(sqldf("SELECT Id, DisplayName, SUM(ViewCount) AS TotalViews
FROM Users
JOIN (
SELECT OwnerUserId, ViewCount FROM Posts WHERE PostTypeId = 1
) AS Questions
ON Users.Id = Questions.OwnerUserId
GROUP BY Id
ORDER BY TotalViews DESC
LIMIT 10"))
  
}

base_2 <- function(Users, Posts){
  
# "u" serves as the Users table with only "Id" and "DisplayName" columns selected
  u<-Users[c("Id","DisplayName")]
# here, the function used for aggregate is sum. We need to provide a parameter "na.rm=TRUE" because a potential sum of a number and NA would be equal to NA.
# The name "tv" comes from "TotalViews" because this is the only column that we are going to need from this table
  tv<-aggregate(Posts$ViewCount,by=list(Posts$OwnerUserId),FUN=sum,na.rm=TRUE)
  colnames(tv) <- c("Id","TotalViews")
# merging by Id
  result<-merge(u,tv,by="Id")
# providing "-" before the column sets the descending order - by default it's ascending
  result<-result[order(-result$TotalViews),]
# changing rownames so that they match the sqldf() output
  rownames(result) <- 1:nrow(result)
  result <- head(result,10)
  
  return(result)
}

dplyr_2 <- function(Users, Posts){
  
  # the code organisation is very much similar to the base function
  tv<-Posts %>%
    group_by(OwnerUserId) %>%
    summarise(TotalViews = sum(ViewCount, na.rm=TRUE))
    
  result <- Users %>%
    select(Id,DisplayName) %>%
    inner_join(tv,by=c("Id"="OwnerUserId")) %>%
    arrange(desc(TotalViews)) %>%
    top_n(10)
    
  return(result)
}

data.table_2 <- function(Users, Posts){
  
   u<- as.data.table(Users)[,.(Id,DisplayName)]
   setkey(u,Id)
   tv<-as.data.table(Posts)[,.(TotalViews=sum(ViewCount, na.rm=TRUE)),by=list(OwnerUserId)]
   setkey(tv,OwnerUserId)
 # to merge two tables in data.table using inner join, we need to provide argument "nomatch=0" 
   result<-u[tv,nomatch=0]
   setorder(result, cols = -"TotalViews")
   result <- as.data.frame(result[1:10, ])
   
   return(result)
}

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

sqldf_3 <- function(Badges){
  
  return(sqldf("SELECT Year, Name, MAX((Count * 1.0) / CountTotal) AS MaxPercentage
FROM (
SELECT BadgesNames.Year, BadgesNames.Name, BadgesNames.Count, BadgesYearly.CountTotal
FROM (
SELECT Name, COUNT(*) AS Count, STRFTIME('%Y', Badges.Date) AS Year
FROM Badges
GROUP BY Name, Year
) AS BadgesNames
JOIN (
SELECT COUNT(*) AS CountTotal, STRFTIME('%Y', Badges.Date) AS Year
FROM Badges
GROUP BY YEAR
) AS BadgesYearly
ON BadgesNames.Year = BadgesYearly.Year
)
GROUP BY Year"))
}

base_3 <- function(Badges){
 # "bn" table comes from "BadgesNames", as this table is called in the original SQL code
   bn <- aggregate(Badges$Id,list(Badges$Name, format(as.Date(Badges$Date),"%Y")),length)
   colnames(bn)<-c("Name", "Year", "Count")
 # "badgesyearly" and "badgesmerge" names are pretty self-explanatory
   badgesyearly <- aggregate(Badges$Id,list(format(as.Date(Badges$Date),"%Y")),length)
   colnames(badgesyearly) <- c("Year", "CountTotal")
   badgesmerge <- merge(bn, badgesyearly, on="Year")
 # here the transform() function is adding a new row called x to badgesmerge, which represents the fraction of all badges
   badgesmerge <- transform(badgesmerge, x = (badgesmerge$Count*1.0)/badgesmerge$CountTotal)
 # grouping by the year, we're choosing the biggest value of all x's in a particular year
   result <- aggregate(badgesmerge$x, list(badgesmerge$Year),max)
 # we need to merge by these two columns, in case two different badges happen to have the same fraction in two different years
   result <- merge(result,badgesmerge[c("x","Year","Name")], on=c("x","Year"))
   result<-result[order(result$Year),]
 # reordering columns
   result<-result[c(3,4,1)]
   colnames(result)<-c("Year","Name","MaxPercentage")
   rownames(result) <- 1:nrow(result)
   
   return(result)
}

dplyr_3 <- function(Badges){
  
   bn<-as.data.frame(Badges %>%
                   group_by(Name,Year = (format(as.Date(Date),"%Y"))) %>%
                   summarize(Count = n()))
   #in dplyr, the mutate() function is the equivalence of the transform() function from base functions
   #here, the order of operations may differ from the base function, yet the resources used and the result are the same
   badgesmerge<-as.data.frame(Badges%>%
                                group_by(Year = (format(as.Date(Date),"%Y"))) %>%
                                summarize(CountTotal = n())) %>%
                                inner_join(bn,by=c("Year"="Year")) %>%
                                mutate(x = (Count*1.0)/CountTotal)
   #here, filter does the job of the max() function from base functions implementation
   #functions like "rename" should be pretty obvious
   result<-as.data.frame(badgesmerge%>%
     group_by(Year) %>%
     filter(x==max(x)) %>%
     select(-Count,-CountTotal)%>%
     rename(MaxPercentage=x)) 
   
   return(result)
}

data.table_3 <- function(Badges){
  
  bn<- as.data.table(Badges)[,.(Count=.N),by = list(Name,Year = format(as.Date(Date),"%Y"))]
  badgesyearly<-as.data.table(Badges)[,.(CountTotal=.N),by = list(Year = format(as.Date(Date),"%Y"))]
  setkey(bn,Year)
  setkey(badgesyearly,Year)
  badgesmerge<-bn[badgesyearly,nomatch=0]
# in data.table, structure ":=" is input by reference; it's used here to add a new column
  badgesmerge<-badgesmerge[,MaxPercentage:=(Count*1.0)/CountTotal]
# "setDT" function converts lists, data frames or data tables accordingly to arguments. ".SD" structure is a reference to a subset of a certain table.
  result<-setDT(badgesmerge)[,.SD[which.max(MaxPercentage)],by=Year]
# this syntax removes all columns mentioned in the LHS of :=
  result[,c("Count","CountTotal"):=list(NULL,NULL)]
  result<-as.data.frame(result)
  
  return(result)
}

# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

sqldf_4 <- function(Comments, Posts, Users){
  
  return(sqldf("SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
FROM (
SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
CmtTotScr.CommentsTotalScore
FROM (
SELECT PostId, SUM(Score) AS CommentsTotalScore
FROM Comments
GROUP BY PostId
) AS CmtTotScr
JOIN Posts ON Posts.Id = CmtTotScr.PostId
WHERE Posts.PostTypeId=1
) AS PostsBestComments
JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
ORDER BY CommentsTotalScore DESC
LIMIT 10"))
}

base_4 <- function(Comments, Posts, Users){
  
# "cts" = CmtTotScr from SQL
  cts <- aggregate(Comments$Score,by=list(Comments$PostId),FUN=sum,na.rm=TRUE)
  colnames(cts) <- c("PostId","CommentsTotalScore")
  
# "pbc" = PostsBestComments from SQL
  pbc <- merge(cts,Posts,by.x="PostId",by.y="Id")
# that is one way to choose a subset of set in first argument, filtered by the condition in second argument
  pbc <- subset(pbc,pbc$PostTypeId==1L)
  pbc <- pbc[c("OwnerUserId","Title","CommentCount","ViewCount","CommentsTotalScore")]
  
# "by.x" and "by.y" refer to columns to join by in first and second table, respectively
  result<- merge(pbc,Users,by.x=c("OwnerUserId"),by.y=c("Id"))
  result<-result[order(-result$CommentsTotalScore),]
  result <- result[c("Title","CommentCount","ViewCount","CommentsTotalScore","DisplayName","Reputation","Location")]
  rownames(result)=1:nrow(result)
  result <- head(result,10)
  
  return(result)
}

dplyr_4 <- function(Comments, Posts, Users){
  
  cts <- Comments %>%
    group_by(PostId) %>%
    summarise(CommentsTotalScore = sum(Score, na.rm=TRUE))
  
  pbc <- Posts %>%
    inner_join(cts,by=c("Id"="PostId")) %>%
    filter(PostTypeId==1L) %>%
    select(OwnerUserId,Title,CommentCount,ViewCount,CommentsTotalScore)
  
  result <- Users %>%
    inner_join(pbc,by=c("Id"="OwnerUserId")) %>%
    select(Title,CommentCount,ViewCount,DisplayName,Reputation,Location,CommentsTotalScore) %>%
    arrange(desc(CommentsTotalScore)) %>%
    top_n(10)
  
# relocate() function is changing the position of a chosen column relatively to how it has been specified in the second argument
  result <- as.data.frame(tibble(result) %>%
    relocate(CommentsTotalScore, .after=ViewCount))
    
  return(result)
}

data.table_4 <- function(Comments, Posts, Users){
  
  #nihil novi
  cts <- as.data.table(Comments)[,.(CommentsTotalScore=sum(Score, na.rm=TRUE)),by=list(PostId)]
  setkey(cts,PostId)
  
  setkey(as.data.table(Posts),Id)
  pbc <- cts[Posts, nomatch=0]
  pbc <- pbc[PostTypeId == 1L]
  pbc <- pbc[,.(OwnerUserId,Title,CommentCount,ViewCount,CommentsTotalScore)]
  setkey(pbc,OwnerUserId)
  
  setkey(as.data.table(Users),Id)
  result <- pbc[Users,nomatch=0]
  result <- result[,.(Title,CommentCount,ViewCount,CommentsTotalScore,DisplayName,Reputation,Location)]
  setorder(result, cols=-"CommentsTotalScore")
  result <- as.data.frame(result[1:10, ])
  
  return(result)
}

# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#

sqldf_5 <- function(Posts, Votes){
  
 return(sqldf("SELECT Posts.Title, STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date, VotesByAge.*
FROM Posts
JOIN (
SELECT PostId,
MAX(CASE WHEN VoteDate = 'before' THEN Total ELSE 0 END) BeforeCOVIDVotes,
MAX(CASE WHEN VoteDate = 'during' THEN Total ELSE 0 END) DuringCOVIDVotes,
MAX(CASE WHEN VoteDate = 'after' THEN Total ELSE 0 END) AfterCOVIDVotes,
SUM(Total) AS Votes
FROM (
SELECT PostId,
CASE STRFTIME('%Y', CreationDate)
WHEN '2022' THEN 'after'
WHEN '2021' THEN 'during'
WHEN '2020' THEN 'during'
WHEN '2019' THEN 'during'
ELSE 'before'
END VoteDate, COUNT(*) AS Total
FROM Votes
WHERE VoteTypeId IN (3, 4, 12)
GROUP BY PostId, VoteDate
) AS VotesDates
GROUP BY VotesDates.PostId
) AS VotesByAge ON Posts.Id = VotesByAge.PostId
WHERE Title NOT IN ('') AND DuringCOVIDVotes > 0
ORDER BY DuringCOVIDVotes DESC, Votes DESC
LIMIT 20")) 
}

base_5 <- function(Posts, Votes){
 # "vd" table stands for VotesDates in SQL code
 # the process of creating it has been split - first the "vd1" table formats the Votes table accordingly and later is being used for "vd"'s creation
   vd1 <- Votes[c("PostId","VoteTypeId","CreationDate")]
   vd1 <- transform(vd1,VDate = (format(as.Date(Votes$CreationDate),"%Y")))
 # the sapply() function iterates through all rows in column vd1$Date, and the result of the switch() function applied on it is saved in another column called VoteDate
   vd1$VoteDate <- sapply(vd1$VDate, switch,
                          "2022" = "after",
                          "2021" = "during",
                          "2020" = "during",
                          "2019" = "during",
                          "before")
 # this is a way to filter a table by list in base functions
   vd1 <- vd1[vd1$VoteTypeId %in% c(3L,4L,12L),]
   vd <- aggregate(vd1$VoteDate,list(vd1$PostId,vd1$VoteDate),length)
 # the column "Total" temporarily saves the amount of votes depending on the period of time
   colnames(vd) <- c("PostId","VoteDate","Total")
   vd <- vd[order(vd$PostId),]
   rownames(vd) <- 1:nrow(vd)
   
   
 # process of creating "vba" table (VotesByAge in SQL) has been split on 3 tables, where each one saves the votes which they are designed for
 # table "vba_b" includes only rows with VoteDate value equal to "before", same with "vba_d" saving "during" etc.
   vba_b <- vd[vd$VoteDate=="before",]
   vba_d <- vd[vd$VoteDate=="during",]
   vba_a <- vd[vd$VoteDate=="after",]
   
 # each table receives a designed column [B/D/A]COVIDVotes collecting the amount of votes depending on their destination
 # the value of the votes is being extracted from the value previously saved in Total column 
   vba_b$BeforeCOVIDVotes <- vba_b$Total
   vba_d$DuringCOVIDVotes <- vba_d$Total
   vba_a$AfterCOVIDVotes <- vba_a$Total
   
 # Merging with "all=TRUE" parameter provides a full outer join
   vba <- merge(vba_b,vba_d,by="PostId",all=TRUE)
   vba <- merge(vba, vba_a,by="PostId",all=TRUE)
   vba <- vba[c("PostId","BeforeCOVIDVotes","DuringCOVIDVotes","AfterCOVIDVotes")]
 # changing all NA values to 0
   vba[is.na(vba)] <- 0L
   vba$BeforeCOVIDVotes <- vba$BeforeCOVIDVotes
   vba$DuringCOVIDVotes <- vba$DuringCOVIDVotes
   vba$AfterCOVIDVotes <- vba$AfterCOVIDVotes
 # The sum of all votes is saved in Votes column
   vba$Votes <- vba$BeforeCOVIDVotes + vba$DuringCOVIDVotes + vba$AfterCOVIDVotes
   
   
   
   res <- merge(Posts,vba,by.x="Id",by.y="PostId")
 # filtering by conditions given in the task
   res <- res[!(res$Title==""),]
   res <- res[res$DuringCOVIDVotes>0,]
   res <- res[order(-res$DuringCOVIDVotes, -res$Votes),]
   rownames(res) <- 1:nrow(res)
   res <- res[c("Title","CreationDate","Id","BeforeCOVIDVotes","DuringCOVIDVotes","AfterCOVIDVotes","Votes")]
   Date <- format(as.Date(res$CreationDate),"%Y-%m-%d")
   res["CreationDate"] <- Date
   colnames(res) <- c("Title","Date","PostId","BeforeCOVIDVotes","DuringCOVIDVotes","AfterCOVIDVotes","Votes")
   result<-head(res,20)
   
   return(result)
}

dplyr_5 <- function(Posts, Votes){
  
  # the case_when() function is the equivalent of switch in dplyr
 vd <- Votes %>%
   select(PostId,VoteTypeId,CreationDate) %>%
   mutate(Year = format(as.Date(CreationDate),"%Y")) %>%
   mutate(VoteDate = case_when(
     Year == "2022" ~ "after",
     Year == "2021" ~ "during",
     Year == "2020" ~ "during",
     Year == "2019" ~ "during",
     TRUE ~ "before"
   )) %>%
   filter(VoteTypeId %in% c(3L,4L,12L)) %>%
   group_by(PostId,VoteDate) %>%
   summarize(Total = n()) %>%
   arrange(PostId)
 
 # the creation procedure of "vba" is pretty much the same as in the base functions
 vba_b <- vd %>%
   filter(VoteDate == "before") %>%
   mutate(BeforeCOVIDVotes = Total)
 vba_d <- vd %>%
   filter(VoteDate == "during") %>%
   mutate(DuringCOVIDVotes = Total)
 vba_a <- vd %>%
   filter(VoteDate == "after") %>%
   mutate(AfterCOVIDVotes = Total)
 
 # to full join in dplyr, you simply use full_join()
 result <- as.data.frame(vba_b %>%
   full_join(vba_d, by=c("PostId"="PostId")) %>%
   full_join(vba_a, by=c("PostId"="PostId")) %>%
   select(PostId,BeforeCOVIDVotes,DuringCOVIDVotes,AfterCOVIDVotes) %>%
   replace(is.na(.), 0L) %>%
   mutate(Votes = BeforeCOVIDVotes + DuringCOVIDVotes + AfterCOVIDVotes) %>%
   inner_join(Posts, by=c("PostId"="Id")) %>%
   filter(!(Title=="")) %>%
   filter(DuringCOVIDVotes>0) %>%
   arrange(desc(DuringCOVIDVotes),desc(Votes)) %>%
   select(Title,CreationDate,PostId,BeforeCOVIDVotes,DuringCOVIDVotes,AfterCOVIDVotes,Votes) %>%
   mutate(Date = format(as.Date(CreationDate),"%Y-%m-%d")) %>%
   mutate(CreationDate = NULL) %>%
   relocate(Date, .after = Title) %>%
     head(20))
 
  return(result)
}

data.table_5 <- function(Posts, Votes){
  
  vd <- as.data.table(Votes)[,.(PostId,VoteTypeId,CreationDate)]
  vd <- vd[,Year:=format(as.Date(CreationDate),"%Y")]
# in data.table, the switch here is fcase()
  vd<-vd[,VoteDate:=fcase(
    Year == "2022" , "after",
    Year == "2021" , "during",
    Year == "2020" , "during",
    Year == "2019" , "during",
    default = "before"
  )]
  vd<-vd[VoteTypeId==3L | VoteTypeId==4L | VoteTypeId==12L]
  vd <- vd[,.(Total=.N),by = list(PostId,VoteDate)]
  setorder(vd, cols="PostId")
  
  
  vba_b <- vd[VoteDate=="before"]
  vba_b <- vba_b[,BeforeCOVIDVotes:=Total]
  vba_d <- vd[VoteDate=="during"]
  vba_d <- vba_d[,DuringCOVIDVotes:=Total]
  vba_a <- vd[VoteDate=="after"]
  vba_a <- vba_a[,AfterCOVIDVotes:=Total]
  
  #the full join in data.table
  vba2 <- merge.data.table(vba_b, vba_d, by = "PostId", all = TRUE)
  vba <- merge.data.table(vba2, vba_a, by = "PostId", all = TRUE)
  
  
  vba <- vba[,.(PostId,BeforeCOVIDVotes,DuringCOVIDVotes,AfterCOVIDVotes)]
  vba[is.na(vba), ] <- 0L
  vba <- vba[,Votes:=BeforeCOVIDVotes+DuringCOVIDVotes+AfterCOVIDVotes]
  setkey(vba,PostId)
  
  setkey(as.data.table(Posts),Id)
  result <- vba[Posts,nomatch=0]
  result <- result[!(Title=="")]
  result <- result[DuringCOVIDVotes>0]
# setting order by a vector of columns requires using "setorderv" rather than "setorder"
  setorderv(result, c("DuringCOVIDVotes","Votes"),c(-1, -1))
  result <- result[,.(Title,CreationDate,PostId,BeforeCOVIDVotes,DuringCOVIDVotes,AfterCOVIDVotes,Votes)]
  result <- result[,Date:=format(as.Date(CreationDate),"%Y-%m-%d")]
  result <- result[,CreationDate:=NULL]
  setcolorder(result, c("Title", "Date", "PostId","BeforeCOVIDVotes","DuringCOVIDVotes","AfterCOVIDVotes","Votes"))
  result <- as.data.frame(result[1:20, ])
  
  return(result)
}

