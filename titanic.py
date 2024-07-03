from databricks.connect import DatabricksSession 
from pyspark.sql.functions import when
from pyspark.sql.functions import count
from pyspark.sql.functions import col, asc, desc

spark = DatabricksSession.builder.remote(
    host="adb-5854717212043428.8.azuredatabricks.net",
    cluster_id="0324-195708-nlv2jrsc",
).getOrCreate()


df = spark.table("ggw.titanic.titanic_train")
tf = (df.withColumn("Age_tf", when(df.Age<26, "Young")
          .when(df.Age >= 26, "Old"))
        .withColumn("Status", when(df.Survived==1, "Alive")
          .when(df.Survived==0, "Dead"))
        .select("PassengerId", "Status", "Pclass", "Sex", "Age_tf")
)

(tf.write.format("delta")
         .mode("overwrite")
         .saveAsTable("ggw.titanic.titanic_filter")
)
print("Step 1 Done")

df1 = spark.table("ggw.titanic.titanic_filter")

df2 = (df1.select("PClass", "Status", "Age_tf") 
          .groupby("Pclass", "Status", "Age_tf")
          .agg(count("Status").alias("Nbr_survivors"))
          .orderBy("Pclass")
      )

(df2.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ggw.titanic.titanic_agg")
)
print ("Step 2 Done")
