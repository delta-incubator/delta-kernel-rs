import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sql("DROP TABLE IF EXISTS table_with_cdf_dv");

# commit 0
spark.sql("CREATE TABLE table_with_cdf_dv (id INT, name STRING, age INT) \
        USING DELTA \
        PARTITIONED by (age) \
        TBLPROPERTIES \
            (delta.enableChangeDataFeed = true, delta.enableDeletionVectors = true) \
        ")

# commit 1
spark.sql("INSERT INTO \
          table_with_cdf_dv \
          VALUES \
              (1, 'george', 24), \
              (2, 'john', 21), \
              (3, 'thomas', 22), \
              (4, 'james', 18) \
          ")

# commit 2
spark.sql("INSERT INTO \
          table_with_cdf_dv \
          VALUES \
              (5, 'james', 24), \
              (6, 'john', 22), \
              (7, 'andrew', 22) \
          ")

# commit 3
spark.sql("DELETE FROM \
          table_with_cdf_dv \
          WHERE \
              name == 'james' \
          ")

# commit 4
spark.sql("UPDATE \
          table_with_cdf_dv \
          SET \
              name = 'adams' \
          WHERE \
              age = 21 \
          ")

# commit 5
spark.sql("MERGE INTO \
          table_with_cdf_dv AS A \
          USING ( \
              SELECT MAX(age) AS max_age \
              FROM table_with_cdf_dv \
          ) AS B \
          ON A.age = B.max_age \
          WHEN MATCHED THEN \
              UPDATE SET \
                  A.name = CONCAT(A.name, \" the elder\") \
        ")

# commit 6
spark.sql("INSERT INTO \
          table_with_cdf_dv \
          VALUES \
              (10, 'martin', 21), \
              (11, 'william', 22), \
              (12, 'john', 24) \
          ")

# commit 7
spark.sql("DELETE FROM \
          table_with_cdf_dv \
          WHERE \
              name = 'martin' OR \
              name = 'william' \
          ")

# commit 8
spark.sql("INSERT INTO \
          table_with_cdf_dv \
          SELECT * FROM table_with_cdf_dv VERSION AS OF 6 \
          WHERE name = 'martin'\
          ")

# commit 9
spark.sql("ALTER TABLE \
          table_with_cdf_dv  \
          SET TBLPROPERTIES (delta.enableChangeDataFeed = false) \
          ")

# commit 10
spark.sql("INSERT INTO \
          table_with_cdf_dv \
          VALUES \
              (8, 'pretzel', 22)")
# commit 11
spark.sql("ALTER TABLE \
          table_with_cdf_dv  \
          SET TBLPROPERTIES (delta.enableChangeDataFeed = true) \
          ")

print("Table as of version 8")
spark.sql("SELECT * FROM table_with_cdf_dv VERSION AS OF 8").show()

print("Table changes from versions 0 to 8")
spark.sql("SELECT * FROM table_changes('table_with_cdf_dv', 0, 8) ORDER BY _commit_version").show()

# print("Table changes fails at version 9")
# spark.sql("SELECT * FROM table_changes('table_with_cdf_dv', 0, 9)").show()
