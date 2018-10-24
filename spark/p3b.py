from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession.builder.getOrCreate()
school = spark.read.csv('/home/alejandra/escuelasPR.csv')
school.createOrReplaceTempView("school")
schools = spark.sql("select _c1 as district, _c2 as city, count(*) "
                    "from school "
                    "where _c0 == 'Arecibo' "
                    "group by district, city ")

schools.show()
