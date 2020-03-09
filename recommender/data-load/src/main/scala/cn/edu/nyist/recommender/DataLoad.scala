package cn.edu.nyist.recommender

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ClassName DataLoad
  * @Description 数据加载
  * @Date 2020/3/7 14:44
  * @Created by typ
  */
/**
  * 商品元数据
  * productId     商品ID
  * name          商品名称
  * categoryIds   分类IDs
  * amazonId      亚马逊ID
  * imageUrl      url
  * categories    分类
  * tags          UGC标签
  */
case class Product( productId: Int, name: String, imageUrl: String, categories: String, tags: String )
/**
  * 评分数据集
  *  userId       用户ID
  * prudcutId     商品ID
  * rating        评分
  * timestamp     时间戳
  */
case class Rating( userId: Int, productId: Int, score: Double, timestamp: Int )

//case class TestDF(str1: String,str2: String, str3: String)

/**
  * MongoDB配置
  * @param uri
  * @param db   操作的DB
  */
case class MongoConfig(uri: String,db: String)

/**
  * 数据加载
  */
object DataLoad {
  //数据路径
  val PRODUCT_DATA_PATH = "E:\\Software\\IdeaProjects\\recommend\\recommender\\data-load\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "E:\\Software\\IdeaProjects\\recommend\\recommender\\data-load\\src\\main\\resources\\ratings.csv"

  //表名称
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.179.10:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //sparkconfig
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoad")
    //spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    //加载数据
    //商品
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map( product =>{
      val attr = product.split("\\^")
      //转换成样例类 product
      Product(attr(0).toInt,attr(1).trim,attr(4).trim,attr(5).trim,attr(6).trim)
    }).toDF()

    //评分
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(rating =>{
      val attr = rating.split(",")
      //转换成Rating
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    /*ratingDF.show()
    productDF.show()*/
    //存进数据库
    storeDFToMongoDB(productDF,MONGODB_PRODUCT_COLLECTION)
    storeDFToMongoDB(ratingDF,MONGODB_RATING_COLLECTION)

    spark.stop()
  }

  //存进mongodb
  def storeDFToMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
