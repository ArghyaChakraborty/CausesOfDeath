/*
 * This code accomplishes following :
 * 1) year vs death count in USA
 * 2) year vs causes of death vs total count of dead
 * 3) causes of death vs location
 * 
 * Input : https://catalog.data.gov/dataset/leading-causes-of-death-by-zip-code-1999-2013
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CausesOfDeathReport {
  def main(arr : Array[String]) {
    if(arr.size < 2) {
      println("Usage : spark-submit --class CausesOfDeathReport causes_of_death_<version>.jar <input file path> <input file name> [<output file path>]")
    } else {
      println("Processing Started")
      var startTime = System.currentTimeMillis()
      
      val ipPath = arr(0)
      val ipFile = arr(1)
      val opPath = if(arr.size >=3) {arr(2)} else {arr(1)}
      
      val sConf = new SparkConf().setAppName("Causes of Death App").setMaster("local[*]")
      val sc = new SparkContext(sConf)
      
      val fileRdd = sc.textFile(ipPath+"/"+ipFile)
      val fileRddSplittedNFiltered = fileRdd.map(line => line.split(",")).filter(arr => isNumeric(arr(3)))
      
      println("Pre-processing done in "+((System.currentTimeMillis()-startTime)/1000)+" seconds")
      startTime = System.currentTimeMillis()
      // Solution for 1)
      fileRddSplittedNFiltered.map(arr => (arr(0),arr(3).toInt)).reduceByKey(_+_).map(tup => new String(tup._1+","+tup._2)).saveAsTextFile(opPath+"/Causes_Of_Death_Year_vs_Total_Death")
      println("Job 1 Done in "+((System.currentTimeMillis()-startTime)/1000)+" seconds")
      startTime = System.currentTimeMillis()
      
      // Solution for 2)
      fileRddSplittedNFiltered.map(arr => ((arr(0),arr(2)) , arr(3).toInt)).reduceByKey(_+_).map(tup => new String(tup._1._1+","+tup._1._2+","+tup._2)).saveAsTextFile(opPath+"/Causes_Of_Death_Year_vs_Type_vs_Total_Death")
      println("Job 2 Done in "+((System.currentTimeMillis()-startTime)/1000)+" seconds")
      startTime = System.currentTimeMillis()
      
      // Solution for 3)
      fileRddSplittedNFiltered.filter(arr => arr.length > 4).map(arr => (arr(2),getLatNLog(arr(4)),getLatNLog(arr(5)))).map(tup => new String(tup._1+","+tup._2+","+tup._3)).saveAsTextFile(opPath+"/Causes_Of_Death_Year_vs_Lat_n_Long")
      println("Job 3 Done in "+((System.currentTimeMillis()-startTime)/1000)+" seconds")
      
      sc.stop()
      
      println("Processing Completed")
    }
    
  }
  
  def isNumeric(str : String): Boolean = {
    val pattern = "[0-9]+".r
    str match { 
      case pattern() => true
      case _ => false
    }
  }
  
  def getLatNLog(str : String) : String = {
    var newString:String = str
    if(newString.startsWith("\"(")) {newString = str.substring(2)}
    if(newString.endsWith(")\"")) {newString = newString.substring(0,newString.length-2)}
    newString.trim
}
}