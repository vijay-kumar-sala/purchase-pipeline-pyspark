from pyspark.sql import SparkSession

mongodbURI = "mongodb://127.0.0.1:27017"
spark = SparkSession\
            .builder\
            .appName("sales_analysis")\
            .config("spark.mongodb.write.connection.uri",mongodbURI+"/xyzSalesDB.sales_analysis")\
            .getOrCreate()
            
# read data
users_df = spark\
                .read\
                .json("""hdfs://localhost:9000/vijay/xyzSales/usersData.json""")
purchase_df = spark\
                .read\
                .json("""hdfs://localhost:9000/vijay/xyzSales/itemPurchaseData.json""")
# transform
users_df.printSchema()
purchase_df.printSchema()

purchase_df.createTempView("purchase_tbl")
queryString = "SELECT user_id, sum(Quantity) AS quantity FROM purchase_tbl GROUP BY user_id"
# print(type(queryString))
users_quantity_df = spark.sql(queryString)
users_quantity_df.show()

joined_df = users_df\
                .join(users_quantity_df,users_df.user_id==users_quantity_df.user_id,"inner")
joined_df.createTempView("user_sales_add")

res_df = spark\
    .sql("SELECT user_address, sum(quantity) FROM user_sales_add GROUP BY user_address")

# load data
res_df\
    .write\
    .format("mongodb")\
    .mode("append")\
    .save()
# res_df.write.json("""hdfs://localhost:9000/vijay/xyzSales/resSalesFile.json""")

spark.stop()
