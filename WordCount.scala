package mywork

import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.shell.Count

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.out.println("Usage: <input> <output>")
      System.exit(1)
    }
    val infile = args(0) // Should be some file on your system
    println("  =======================")
    println(" || WordCount in Spark !||")
    println("  =======================")
    println("Input: " + args(0) + ",size:" + getFileSize(infile) + "bytes")
    println("Onput: " + args(1))
      
    
    val conf = new SparkConf().setAppName("word count")
    val sc = new SparkContext(conf)
    val indata = sc.textFile(infile, 2).cache()
    //flatMap把每行按空格分割单词
    //map把每个单词映射成(word,1)的格式
    //reduceByKey则按key，在此即单词，做整合，即把相同单词的次数1相加
    val words = indata.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a,b) => a+b)
    //获取包含ERROR或WARN的所有行
    val errlineRDD = indata.filter(line => line.contains("ERROR"))
    val warnlineRDD = indata.filter(line => line.contains("WARN"))
    words.saveAsTextFile(args(1))
    
    println()
    println("All words are counted!")
    val res = words.count().toInt
 
    //打印出来部分统计结果
    if (res > 20) {
      println("The first 20 words are ... ")
      words.take(20).foreach(println)
      println(" ... ")
    }else {
      words.take(res).foreach(println)
    }
      
    println() 
    
    val errline = errlineRDD.count
    val warnline = warnlineRDD.count
    if (errline == 0) {
      println("Good Luck! " + errline + " lines with ERROR!" + "\n")
    }else {
      println(errline + " lines with ERROR and the first line with ERROR is:")
      println(errlineRDD.first + "\n")
    }
    if (warnline == 0) {
      println("Very Good Luck! " + warnline + " lines with WARN!" + "\n")
    }else {
      println(warnline + " lines with WARN and the first line is:  ")
      println(warnlineRDD.first + "\n")
    }
  }
  
  //获取文件大小
  	def getFileSize(fname: String): Long = {
  	  new File(fname) match {
  	    case null => 0
  	    case cat if cat.isFile() => cat.length()
  	  }
  	}

}
