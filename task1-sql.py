import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Spark SQL").config("spark.some.config.option", "some-value").getOrCreate()
from pyspark.sql import Row
sc = spark.sparkContext
op1 = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
op2 = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
op1.createOrReplaceTempView("parking")
op2.createOrReplaceTempView("openv")

result=spark.sql('''SELECT parking.summons_number,parking.plate_id,parking.violation_precinct, parking.violation_code, parking.issue_date from parking LEFT JOIN openv on parking.summons_number=openv.summons_number WHERE openv.summons_number IS NULL''')



result.select(format_string('%d\t%s, %d, %d, %s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")

