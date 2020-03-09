package cn.edu.nyist.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ClassName StatisticsRecommender
  * @Description TODO
  * @Date 2020/3/9 13:34
  * @Created by typ
  */
/**
  * 评分样例类
  * @param userId
  * @param productId
  * @param score
  * @param timestamp
  */
case class Rating( userId: Int, productId: Int, score: Double, timestamp: Int )
/**
  * mongodb
  * @param uri
  * @param db
  */
case class MongoConfig( uri: String, db: String )

/**
  * 离线统计数据
  */
object StatisticsRecommender {

  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"
  //历史热门商品
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  //近期热门商品
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  //每个商品的平均评分
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.179.10:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //sparkconfig
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    //spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    //mongodb配置
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据
    //评分数据
    val ratingDF = loadDFFromMongoDB(spark, MONGODB_RATING_COLLECTION).as[Rating].toDF()
    ratingDF.show()
    //创建一个临时表
    ratingDF.createOrReplaceTempView("ratings")

    //1 历史热门，评分个数最多
    val rateMoreProductsDF = spark.sql("select productId,count(productId) as count from ratings group by productId order by count desc")
    //存进mongodb
    storeDFToMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    //2 近期热门
    //日期格式化 自定义方法为changeDate
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    //先将时间戳转换 yyyyMM 到月份 1329321600 --》201202
    val ratingOfYearMonthDF = spark.sql("select productId,score,changeDate(timestamp) as yearMonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark.sql("select productId,count(productId) as count,yearMonth from ratingOfMonth group by yearMonth,productId order by yearMonth desc, count desc")
    //存进mongodb
    storeDFToMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    //3 平均分
    val averageProductDF = spark.sql("select productId,avg(score) as avg from ratings group by productId order by avg desc")
    //存进mongodb
    storeDFToMongoDB(averageProductDF, AVERAGE_PRODUCTS)
    spark.stop()
  }


  //存进mongodb
  def storeDFToMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  //加载数据-从mongodb
  def loadDFFromMongoDB(spark: SparkSession, collection_name: String)(implicit mongoConfig: MongoConfig): DataFrame = {
    spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .format("com.mongodb.spark.sql")
      .load()
  }
}
