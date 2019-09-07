>>> from pyspark.sql.functions import udf
>>> def generateTxId(storeid, lane):
...   f_storeid = str(storeid).zfill(5)
...   f_lane = str(lane).zfill(2)
...   return f_storeid+f_lane
... 
>>> generateTxId_udf = udf(generateTxId)

>>> inputfile = sc.textFile("/temp")

>>> csv = inputfile.map(lambda x: x.split(","))                                    

>>> df1=sqlContext.createDataFrame(csv, ['val1','val2'])

>>> df1.select(generateTxId_udf(df1.val1,df1.val2))


>>> df1.select(generateTxId_udf(df1.val1,df1.val2).alias("TxId")).show()
+-------+
|   TxId|
+-------+
|0000102|
|0000304|
|0000506|
+-------+

>>> 
