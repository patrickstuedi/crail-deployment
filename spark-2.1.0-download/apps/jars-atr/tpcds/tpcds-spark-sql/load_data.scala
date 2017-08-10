import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import java.sql.Date

:load /home/demo/zac-deployment/spark-2.0.0/apps/tpcds/tpcds-spark-sql/config.scala
:load /home/demo/zac-deployment/spark-2.0.0/apps/tpcds/tpcds-spark-sql/TpcdsSchema.scala

//val ctx = SQLContext //new org.apache.spark.sql.SQLContext(sc)
val ctx = SparkSession.builder().appName("TPCDS").getOrCreate()

var tables_df = scala.collection.mutable.Map[String, DataFrame]()
val tpcds = new TpcdsSchema()

def convertValue(value : Any, colType : StructField, index : Int, line : Seq[Any]) : Any = {
	val strrepr = value.toString
	if (strrepr == "") {
		 // null values have a special identifier
		println("ERROR: Could not convert column: #" + index + 
			"of line: " + line + 
			", value: '" + strrepr + 
			"' to type " + colType.dataType.getClass
		)
		throw new Exception("Empty column")
	} else if (strrepr == "<null>") {
		return null
	}
	try {
		colType.dataType match {
			case  _ : IntegerType => return strrepr.toInt
			case  _ : FloatType => return strrepr.toFloat
			case  _ : StringType => return strrepr
			case  _ : DateType =>  {
						val Array(y, m, d) = strrepr.split("-").map(_.toInt)
						return new Date(y, m, d)
					  }
			case  _ : DecimalType => return BigDecimal(strrepr)
			case _ => throw new IllegalArgumentException("Invalid Type")
		}
	} catch {
		case e : Exception => {
			println("ERROR: Could not convert column: #" + index + 
				"of line: " + line + 
				", value: '" + strrepr + 
				"' to type " + colType.dataType.getClass
			)
			println(e)
			sys.exit
		}
	}
}

def convertLine(line : Row, schema : StructType) : Row = {
	val lineAsSeq = line.toSeq
	assert(lineAsSeq.length == schema.length, 
		"The number of values provided (" + lineAsSeq.length + ") " +
	        "does not match the number of columns (" + schema.length + "). " +
	        "Row is: '" + line + "'"
        )
	val tuple =(lineAsSeq, schema, Stream from 1).zipped.
				map((l,st, idx) => {convertValue(l,st, idx, lineAsSeq)}
	           )
	return Row.fromSeq(tuple)
}

def csv2parquet(tableName : String, filePath : String, schema : StructType, delimiter : String = "\\|") : DataFrame = {
	val file = sc.textFile(filePath)
	println(" -- Reading text file: " + filePath)
	val lines = file.flatMap(x=> x.split("\n"))
	println(" -- Splitting lines and creating an RDD from file")
	val stringRDD = lines.map(line => {Row.fromSeq(line.replaceAll("^\\|","|<null>|")
	                                                  .replace("||", "|<null>|")
	                                                  .replace("||", "|<null>|")
	                                                  .split(delimiter)
	                                                  .filter(!_.isEmpty())
	                                                  .map(i => i.toString())
						      )
	                                   }
	                         )
//      stringRDD.foreach(println)
	val convertedRDD = stringRDD.map(line => convertLine(line, schema))
	println(" -- Printing RDD contents")
//      convertedRDD.foreach(i => println(i))
	println(" -- Applying schema")
//	val rddWithSchema = ctx.applySchema(convertedRDD, schema)
	val rddWithSchema = ctx.createDataFrame(convertedRDD, schema)
	println(" -- Converting to a DataFrame")
	val df = rddWithSchema.toDF()
	df.registerTempTable(tableName)
	df.printSchema()
	val writer = df.write
	var dffile= hdfs + tpcds_common_dir + "/" + tableName + tpcds_parquet_suffix
	writer.mode("overwrite")
	try {
		println(" -- Writing DataFrame to " + dffile)
		writer.save(dffile)
	} catch {
		case e: Exception => {
			println("could not write DF for table " + tableName)
			println(e)
			throw e
			sys.exit
		}
	}

	dffile= rdfs + "/" + tpcds_common_dir + "/" + tableName + tpcds_parquet_suffix
	//writer.mode("overwrite")
	try {
		println(" -- Writing DataFrame to " + dffile)
		writer.save(dffile)
	} catch {
		case e: Exception => {
			println("could not write DF for table " + tableName)
			println(e)
			throw e
			sys.exit
		}
	}
	return df
}


println("  -- Converting CSV data format to parquet")
val allTables = tpcds.schema
allTables.foreach(kv => {
	val tableName = kv._1
	val path = tpcds_csv_dir + "/"+ kv._1 + tpcds_csv_suffix
	println("  -- Converting table " + kv._1 + " (" + path + ") from CSV format to parquet")
	val schema = kv._2
	println(tableName)
	println(schema)
	try {
		tables_df(kv._1) = csv2parquet(tableName, path, schema)
	}
}
)

