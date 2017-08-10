import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Dataset

spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

def _processDF(data: Dataset[_], tabName: String, doNullOutput: Boolean) = {
	if(doNullOutput){
		data.write.format("org.apache.spark.sql.execution.datasources.atr.AtrFileFormat").mode(SaveMode.Overwrite).save(tabName)
	} else {
		data.write.format("parquet").mode(SaveMode.Overwrite).save(tabName)
	}
}

def saveDF(data: Dataset[_], tabName: String) = _processDF(data, tabName, false) 
def discardDF(data: Dataset[_], tabName: String) = _processDF(data, tabName, true) 
