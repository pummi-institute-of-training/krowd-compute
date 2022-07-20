// Import Required Classes.
import org.apache.spark.mllib.clustering
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.rdd.RDD

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel, OnlineLDAOptimizer, LocalLDAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.{col, monotonicallyIncreasingId, udf}
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.NGram
import scala.collection.mutable.WrappedArray
import cc.mallet.util.Maths.jensenShannonDivergence
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.JavaConversions._
import com.github.nscala_time.time.Imports._
import org.joda.time.Days
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.databricks.spark.csv
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.regression.{GBTRegressor}
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import breeze.linalg._

val path_entities = "/mnt/ZOMATO_SYDNEY_JAN2020/ZOMATO_SYDNEY_JAN2020_TOPICMODEL/zomato_sydney_reviews-topic-composition_spark.txt"
val filename = "/mnt/ZOMATO_SYDNEY_JAN2020/ZOMATO_SYDNEY_JAN2020_MODEL"
val mysqlDb = "database_here"
val mysqlHost = "host_url_here"
// val mysqlHost = "54.87.219.29"

val sqlContext = spark

def stoD(s: String, d: String = "0"): Double = {
    return "[0-9.]+".r.findFirstIn(s).getOrElse(d).toDouble
}

def mean[T](item:Traversable[T])(implicit n:Numeric[T]) = {
  n.toDouble(item.sum) / item.size.toDouble
}

def meanStoD(s: String, d: String = "0"): Double = {
    val list = "[0-9.]+".r.findAllIn(s).toList
    var avg = 0.0
    if (s.length != 0) {
        avg = mean(list.map(_.toDouble))
    }
    return avg
}

def stoI(s: String): Int = {
    return "[0-9]+".r.findFirstIn(s).getOrElse("0").toInt
}

def dtoD(d: java.math.BigDecimal): Double = {
    return d.doubleValue()
}

def divideString(n: String, d: String): Double = {
    var res: Double = 1.0
    if (stoD(d) != 0) {
        res = stoD(n) / stoD(d, "1")
    }
    math.min(res, 1)
}

def dateCriteriaMet(date: String): Boolean = {
    if (date == null) {
        return true
    }
    return DateTime.parse(date) > DateTime.parse("2016-03-01")
}

def stoDate(s: String): DateTime = {
    if (s == null) {
        return DateTime.now
    }
    var dateParsed = DateTime.now
    val date = s.toLowerCase().replace("reviewed ","").replace("reviewed on ","")
    try {
        dateParsed = DateTime.parse(date.capitalize, DateTimeFormat.forPattern("MMMM dd, yyyy"))
    } catch {
        case e: Exception => dateParsed = DateTime.parse(date, DateTimeFormat.forPattern("dd MMMM yyyy"))
    }
    return dateParsed
}

def convertStringToPrice(s: String): Double = {
    var price = 0.0;
    var t = s.replace(",","")
    if (t.trim == "") {
        price = 0;
    } else if(t.contains("£")) {
        price = (meanStoD(t) * 1.32) / 2
    } else if (t.contains("€")) {
        price = (meanStoD(t) * 1.11) / 2
    } else if(t.contains("₹")) {
        price = (meanStoD(t) * 0.015) / 2
    } else if(t.contains("AED")) {
        price = (meanStoD(t) * 0.27) / 2
    } else if(t.contains("A$")) {
        price = (meanStoD(t) * 0.69) / 2
    }

    return price;
}

def convertStringToPriceRange(s: String): Double = {
    val price: Double = convertStringToPrice(s);

    return price match {
        case i if (i > 0 && i <=10 ) => 1.0
        case i if (i > 10 && i <=30) => 2.0
        case i if (i > 30 && i <=55) => 3.0
        case i if (i > 55) => 4.0
        case _ => 0.0
    }
}

val decimalToDouble = udf[Double, java.math.BigDecimal]( dtoD(_))
val toDouble = udf[Double, String]( stoD(_))
val toInt = udf[Int, String]( stoI(_))
val division = udf[Double, String, String](divideString)
val dateMet = udf[Boolean, String] ((s) => dateCriteriaMet(s))
val toColDate = udf[String, String]( (s: String) => stoDate(s).toString())
val EUPSscore = udf[Double, Seq[Double], Seq[Double]]((p1,p2) => jensenShannonDivergence(p1.toArray,p2.toArray))
val priceInUSD = udf[Double, String]((s) => convertStringToPrice(s))

val entityTopicDF = sqlContext.read.json(path_entities).withColumnRenamed("topicDistribution", "entityTopicDistribution").cache

val mysqlUsername = ""
val mysqlPassword = ""
val mysqlDriver = "com.mysql.jdbc.Driver"
//val mysqlUrl = "jdbc:mysql://${mysqlHost}:3306/${mysqlDb}"
val mysqlUrl ="jdbc:mysql://url_here:port_heree/database_here"
import sqlContext.implicits._
val prop = new java.util.Properties
prop.setProperty("user", mysqlUsername)
prop.setProperty("password", mysqlPassword)
prop.setProperty("driver", mysqlDriver)
prop.setProperty("zeroDateTimeBehavior","convertToNull")

val entity: DataFrame = sqlContext.read.jdbc(mysqlUrl, "entity", prop).select("id", "entity_id")
val serviceEntity: DataFrame = sqlContext.read.jdbc(mysqlUrl, "service_entity", prop).select("id", "website", "cuisines","price","latlong")
val splitToList = udf[Array[String], String]((s) => s.replace("Asian","").replace("Mediterranean","").replace("International","").replace("European","").replace("Middle Eastern","").replace("Gluten Free", "").split(",").map(_.replace(160.asInstanceOf[Char].toString,"").replace(" ","")).filter(!_.isEmpty))
val mm = entity.join(serviceEntity, entity("entity_id") === serviceEntity("id")).select(entity("id"),serviceEntity("cuisines"),serviceEntity("price"),serviceEntity("latlong")).withColumn("CuisineList", splitToList($"cuisines")).withColumn("priceRange",priceInUSD($"price"))


val entitySummary: DataFrame = sqlContext.read.jdbc(mysqlUrl, "entity_summary", prop).select("entity_id" , "rating_count", "positive_reviews_count", "negative_reviews_count", "folink_rating", "global_rank").cache
val reviews: DataFrame = sqlContext.read.jdbc(mysqlUrl, "reviews", prop).select("user_id", "entity_id", "rating", "reviewed_on").withColumn("reviewed_on_formatted", toColDate($"reviewed_on")).drop("reviewed_on").withColumnRenamed("reviewed_on_formatted","reviewed_on")
val entity_other: DataFrame = sqlContext.read.jdbc(mysqlUrl, "entity", prop).select($"id".as("entity_id"),$"name").cache


val temp = entity_other.join(entitySummary, "entity_id").withColumn("ThaKrowdScore", toDouble($"global_rank")).select("entity_id","name","ThaKrowdScore").join(entityTopicDF.select($"id".as("entity_id"), $"entityTopicDistribution".as("topicDistributions")), "entity_id").select($"entity_id".as("id"),$"name",$"topicDistributions",$"ThaKrowdScore")

// Generate cuisines_v2.json
temp.join(mm.select($"id",$"CuisineList",$"priceRange",$"latlong"), "id").repartition(1).write.format("json").mode("overwrite").save(filename)
