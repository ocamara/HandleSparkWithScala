#import des packages 
import json
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import datetime
sqlContext=SQLContext(sc)

def convertJsonList(string):
    data=json.loads(string)
    return [data[key] for key in data.keys()]

def convertDate(string):
    return datetime.strptime(string.strip(),"%d/%m/%Y").date()


schema=StructType([
             (StructField("name",StringType(),True)),
	     (StructField("age",IntegerType(),True)),
	     (StructField("date",DateType(),True)),
             (StructField("three",StringType(),True)),
             (StructField("two",StringType(),True)),
             (StructField("one",StringType(),True))
          ])

file=sc.textFile("examples/src/main/resources/people.txt")

parts=file.map(lambda line:line.split(";"))

data=parts.map(lambda line:[line[0],line[1].strip(),line[2],convertJsonList(line[3])])

axa=data.map(lambda line:(line[0],int(line[1]),convertDate(line[2]),line[3][0],line[3][1],line[3][2]))

df=sqlContext.createDataFrame(axa,schema)

df.show()


