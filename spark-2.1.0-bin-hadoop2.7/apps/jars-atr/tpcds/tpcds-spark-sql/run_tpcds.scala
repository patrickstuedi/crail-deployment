import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}

import java.time
import java.sql.Date
import java.io.{File, IOException}
import java.net.URI

:load /home/demo/zac-deployment/spark-2.0.0/apps/tpcds/tpcds-spark-sql/config.scala
:load /home/demo/zac-deployment/spark-2.0.0/apps/tpcds/tpcds-spark-sql/TpcdsSchema.scala

//val ctx = SQLContext //new org.apache.spark.sql.SQLContext(sc)
val ctx = SparkSession.builder().appName("TPCDS").getOrCreate()

var tpcds = new TpcdsSchema()
var tables_df = scala.collection.mutable.Map[String, DataFrame]()
var queries = scala.collection.mutable.Map[String, DataFrame]()
var explainPlan = true

def dir(obj : Any) = {
	obj.getClass.getMethods.map(_.getName).sorted.foreach(println)
}


//def copyTablesToRDFS() : Boolean = {
//	val hdfsFS = FileSystem.get(new URI(hdfs), sc.hadoopConfiguration)
//	val localFS = FileSystem.get(new URI(localfs), sc.hadoopConfiguration)
//	val rdfsFS = FileSystem.get(new URI(rdfs), sc.hadoopConfiguration)
//        try {
//		localFS.mkdirs(new Path(localfs + tpcds_data_dir))
//	} catch {
//		case e : java.io.IOException =>  {
//			println(e)
//			dir(e)
//		}
//	}
//
//	try {
//		rdfsFS.mkdirs(new Path(tpcds_data_dir))
//	} catch {
//		case e : java.io.IOException =>  {
//			println(e)
//			dir(e)
//		}
//	}
//
//       	val files = hdfsFS.listFiles(new Path(tpcds_data_dir), true)
//        while (files.hasNext){
//                val f = files.next()
//                val fn = f.getPath.toString
//                if (f.isFile && fn.matches(".*" + tpcds_filename_suffix + "/_SUCCESS")) {
//                        val pf = fn.replace("/_SUCCESS", "").replace(hdfs, "").replace(tpcds_data_dir, "").replace("/","")
//                        println("Moving directory: " + hdfs + tpcds_data_dir + "/" + pf + " to " + rdfs + tpcds_data_dir + "/"+ pf)
//                        try {
//				hdfsFS.copyToLocalFile(new Path(hdfs    + tpcds_data_dir + "/" + pf), 
//				                       new Path(localfs + tpcds_data_dir + "/" + pf)
//				)
//                        } catch {
//                                case e : java.io.IOException => {
//					println("WARN TPCDS: Skipping parquet file copy to " + localfs + tpcds_data_dir + "/")
//                                        println("WARN TPCDS: Reason: " + e)
//                                }
//                        }
//                        try {
//				rdfsFS.copyFromLocalFile(new Path(localfs + tpcds_data_dir + "/" + pf), 
//				                         new Path(rdfs    + tpcds_data_dir + "/" + pf)
//				)
//                        } catch {
//                                case e : java.io.IOException => {
//					println("WARN TPCDS: Skipping parquet file copy to " + rdfs + tpcds_data_dir + "/")
//                                        println("WARN TPCDS: Reason: " + e)
//                                }
//                        }
//                }
//        }
//	return true
//}

def loadTables(filesystem : String) : Boolean = {
	val fs = FileSystem.get(new URI(filesystem), sc.hadoopConfiguration)
	//val fs = FileSystem.get(sc.hadoopConfiguration)
        var d = tpcds_hdfs_dir
        if (filesystem == "rdfs") {
	     val d = tpcds_rdfs_dir 
        }
	println("INFO TPCDS: Searching for parquet files in " + d)
	val files = fs.listFiles(new Path(d+"/"), true)
	var cnt = 0
	while (files.hasNext){
		val f = files.next()
		val fn = f.getPath.toString
		if (f.isFile && fn.matches(".*" + tpcds_parquet_suffix + "/_SUCCESS")) {
			val dn = fn.replace("/_SUCCESS", "")
			val tn = dn.replace(d, "")
 			           .replace(tpcds_parquet_suffix, "")
			           .replace(filesystem, "")
			       	   .replace(tpcds_common_dir, "")
			       	   .replace("tpcds-", "")
			       	   .replace(tpcds_SF, "")
			           .replace("/", "")
			           .trim()
			try {
				println("INFO TPCDS: Loading file: " + dn + " as table: " + tn)
				tables_df(tn) = ctx.read.parquet(dn)
				tables_df(tn).registerTempTable(tn)
				cnt += 1
			} catch {
				case e : Exception => {
					println(e)
				}
			}
		}
      	}
	println ("INFO TPCDS: Loaded " + cnt + " tables")
	return true
}

def loadQueries() : Boolean = {
	val d = new File(tpcds_query_dir)
	assert(d.isDirectory)
	val sqlFiles = d.listFiles().filter(x => x.toString.matches(".*" + tpcds_query_suffix) )
	println("SQL files found: " + sqlFiles)
	sqlFiles.foreach(
		f => {
			var queryString = scala.io.Source.fromFile(f).mkString
			var queryName = f.toString.replace(tpcds_query_dir, "")
			       	                  .replace(tpcds_query_suffix, "")
			                          .replace("/", "")
			try {		
				queries(queryName) = ctx.sql(queryString)
				println("INFO TPCDS: Loaded query " + queryName + " from file " + f.toString)
			} catch {
				case e : Exception => {
					println("ERROR TPCDS: Could not load query " + queryName + " from file " + f.toString)
					println("ERROR TPCDS: Got exception: " + e)
				}
			}
		}
	)
	return true
}

def runQuery(queryName : String, sqlQuery : DataFrame, explain : Boolean) : Boolean = {
	val startTimeQuery = System.currentTimeMillis
	if (explain)
		sqlQuery.explain()
	try {
		sqlQuery.collect()
	} catch {
		case e : Exception => {
			println("ERROR TPCDS: Could not execute query " + queryName)
                        println("ERROR TPCDS: Got exception: " + e)
		}
	}
	val endTimeQuery = System.currentTimeMillis
	val runTimeQuery = endTimeQuery - startTimeQuery
	println("Query " + queryName + " runtime (s): " + runTimeQuery / 1000.0)
	return true
}


def printCommands() = {
	println("------------------")
	println("Available commands:")
	println("    Explain query         \t : \t explain <query> (e <query>)")
	println("    Load tables from hdfs \t : \t hdfs")
	println("    Load tables from rdfs \t : \t rdfs ")
//	println("    Copy tables to rdfs   \t : \t rdfs init")
	println("    Return to REPL        \t : \t shell (s)")
	queries.toSeq.sortBy(_._1).foreach(kv => {
	println("    Run query             \t : \t " + kv._1) 
	})
	println("    All queries           \t : \t all (a) ")
	println("    Exit                  \t : \t exit | quit")
	println("------------------")
}

def notDone(line: String) : Boolean = {
        if (line == null)
            return true
	line.trim() != "quit" || line != "exit"
}

def readCommand() : Any = {
	printCommands()
	print("$ ")
	Iterator.continually(Console.readLine).takeWhile(notDone(_)).foreach(
		line => {
			val opt = line.trim().toLowerCase()
			opt match {
				case null => null
				case _ if (opt == "exit" || opt == "quit") =>  {
					sys.exit
				}
				case _ if (opt == "hdfs") => {
					loadTables(hdfs)
					println("Loading queries")
					loadQueries()
				}
				case _ if (opt == "rdfs") => {
					//copyTablesToRDFS()
					loadTables(rdfs)
					loadQueries()
				}
				case _ if (opt == "rdfs init" || opt == "init rdfs") => {
                                        //copyTablesToRDFS()
                                        loadTables(rdfs)
                                        loadQueries()
                                }

				case _ if (opt == "shell" || opt == "s") => {
					println("Returning to scala REPL shell")
					return
				}
				case _ if (opt == "all" || opt == "a") => {
					println("Running all queries")
					queries.foreach(kv => runQuery(kv._1, kv._2, explainPlan))
				}
				case qn if queries.contains(qn) => runQuery(qn, queries(qn), explainPlan)
				case opt if (opt.startsWith("e ") || opt.startsWith("explain")) => {
					val words = opt.split(" |\t")
					if (words.length > 1 && queries.contains(words(1))) {
						queries(words(1)).explain()
					} else {
						println("Invalid query name")
					}
				}
				case all => { 
					println("Invalid command " + all)
				}
			}
			printCommands()			
			print("$ ")
		}
	)
}

readCommand()
