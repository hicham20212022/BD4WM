#######################
#probleme on arrive pas a select ou filter qd il s'agit de lire tout le repertoire



# voir ici http://sgsong.blogspot.com/2015/06/how-to-use-sparkr-within-rstudio.html
Sys.setenv(SPARK_HOME="/home/user/spark")
Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.1.0" "sparkr-shell"')
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

library(SparkR)
library(magrittr)

# Initialize SparkContext and SQLContext
sc <- sparkR.init(appName="SparkR-Example")
sqlContext <- sparkRSQL.init(sc)


# Créer dataframe à partir d'un fichier: Ok.
 df <- read.df(sqlContext, "MySpark/SparkR/T.txt", source = "com.databricks.spark.csv", header="false")

# Créer dataframe à partir d'un repertoire: ????????????????????????????????????
 df <- read.df(sqlContext, "MySpark/SparkR", source = "com.databricks.spark.csv", header="false")


# créer RDD pour lire un fichier texte! Ok.
 rdd<-SparkR:::textFile(sc,"MySpark/SparkR/T.txt")



###########################################
# convertir RDD to DF! Ok.
# créer RDD pour lire un répéetoire texte! Ok.
rdd<-SparkR:::textFile(sc,"MySpark/WaterSpark/T.txt")

lines <- SparkR:::flatMap(rdd,
                 function(line) {
                   list(strsplit(line, split = ",")[[1]])
                 })

dfR2<-SparkR:::createDataFrame(sqlContext, lines)

col_headings <- c('A','B','C','D')
names(dfR2) <- col_headings

first(dfR2)
printSchema(dfR2)
row.names(dfR2)
collect(dfR2)


# essai requête
df22r <- filter(dfR2, dfR2$D == "22")
collect(df22r)
#affiche le contenu du RDD: avec un probleme il affiche pas tt; normal c R qui limite
#[ reached getOption("max.print") -- omitted 473840 entries ]
#collect(rdd)

