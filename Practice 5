val df7 = spark.read.option("header",true).csv("/new_files_3_spark/h7")
val df15 = spark.read.option("header",true).csv("/new_files_3_spark/h15")
val df85 = spark.read.option("header",true).csv("/new_files_3_spark/h85")
val df96 = spark.read.option("header",true).csv("/new_files_3_spark/h96")

val df7_new = df7.withColumn("h", lit("h7"))
val df15_new = df15.withColumn("h", lit("h15"))
val df85_new = df85.withColumn("h", lit("h85"))
val df96_new = df96.withColumn("h", lit("h96"))
val df7_15 = df7_new.union(df15_new)
val df85_96 = df85_new.union(df96_new)
val df_general = df7_15.union(df85_96)

z.show(df_general)
