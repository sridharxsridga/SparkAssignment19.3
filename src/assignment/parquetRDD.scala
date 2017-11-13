//Creating a datafram with 1 to 100 and saving as parquet file

package assignment

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._


object parquetRDD {
  def main(args: Array[String]): Unit = {
 
    //specify the configuration for the spark application using instance of SparkConf
    val config = new SparkConf().setAppName("Assignment 19.2").setMaster("local")
    
    //setting the configuration and creating an instance of SparkContext 
    val sc = new SparkContext(config)
    
    //Entry point of our sqlContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    //to use toDF method 
    import sqlContext.implicits._
   
   //use toDF method to directly convert the RDD into dataframe 
    val listDF = sc.parallelize(1 to 100).toDF
    
    //Save the dataframe as parquet file 
    listDF.saveAsParquetFile("/home/acadgild/sridhar_scala/assignment/parquetFileOutput")
  
  }
}