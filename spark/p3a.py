from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession.builder.getOrCreate()
students = spark.read.csv('/home/alejandra/studentsPR.csv')
school = spark.read.csv('/home/alejandra/escuelasPR.csv')
students.createOrReplaceTempView("students")
school.createOrReplaceTempView("school")
result = spark.sql("Select students._c6 from students, school where students._c2 = school._c3 "
                   "and school._c2 ='Ponce' or school._c2 = 'San Juan' and school._c4 ='Superior' and students._c5 ='M'")
result.show()

