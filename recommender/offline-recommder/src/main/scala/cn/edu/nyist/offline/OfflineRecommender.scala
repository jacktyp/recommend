package cn.edu.nyist.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
  * @ClassName OfflineRecommender
  * @Description TODO
  * @Date 2020/3/9 14:16
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
case class ProductRating( userId: Int, productId: Int, score: Double, timestamp: Int )
/**
  * MongoDB配置
  * @param uri
  * @param db   操作的DB
  */
case class MongoConfig(uri: String,db: String)

// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )
// 定义用户的推荐列表
case class UserRecs( userId: Int, recs: Seq[Recommendation] )
// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )
/**
  * 离线推荐之基于隐语义模型的协同过滤
  */
object OfflineRecommender {

  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.179.10:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    //加载数据--转换成RDD
    val ratingRDD = loadDFFromMongoDB(spark,MONGODB_RATING_COLLECTION)
      .as[ProductRating]
      .rdd
      .map(
        rating => (rating.userId, rating.productId,rating.score)
      ).cache()

    //从评分数据集中 提取商品和用户
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    // 1. 训练隐语义模型
    val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3))
    // 定义模型训练的参数，rank隐特征个数，iterations迭代词数，lambda正则化系数
    val ( rank, iterations, lambda) = (5, 10, 0.01)
    val model = ALS.train(trainData, rank , iterations , lambda )

    // 2. 获得预测评分矩阵，得到用户的推荐列表
    //用户和商品做笛卡尔积，得到一个空的 user-product RDD
    val userProducts = userRDD.cartesian(productRDD)
    //预测评分矩阵
    val preRating = model.predict(userProducts)
    //从预测评分矩阵中获取用户的推荐列表
    val userRecs = preRating.filter(_.rating > 0)
        .map(rating => (rating.user, (rating.product, rating.rating) )
        )
        //通过user 分组
        .groupByKey()
        .map{
          //_._2 > _._2 降序
          case (userId, recs) =>
            UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1,x._2)) )
        }
          .toDF()

    //用户推荐列表存入mongodb
    storeDFToMongoDB(userRecs,USER_RECS)

    // 3. 利用商品的特征向量，计算商品的相似度列表
    val productFeatures = model.productFeatures.map{
      //DoubleMatrix 传入一个数组转换成对应的矩阵 当前为商品ID--特征向量
      case (productId,features) => (productId, new DoubleMatrix(features) )
    }
    //每两个配对，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
        .filter{
          //过滤掉 不能自己和自己比对
          case (a, b) => a._1 != b._1
        }
        //计算余弦相似度
        .map{
        case(a, b) =>
          val simScore = cosinSim( a._2, b._2)
          (a._1, (b._1, simScore) )
          }
          //simScore（相似度） 大于 0.4
        .filter(_._2._2 > 0.4)
        //通过user 分组
        .groupByKey()
        .map{
          //_._2 > _._2 降序
          case (productId, recs) =>
            ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1,x._2)) )
        }
      .toDF()

    //商品相似度列表存入mongodb
    storeDFToMongoDB(productRecs,PRODUCT_RECS)
    spark.stop()
  }

  //计算余弦相似度
  def cosinSim(productOne: DoubleMatrix, productTwo: DoubleMatrix): Double={
    productOne.dot(productTwo) / (productOne.norm2() * productTwo.norm2())
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
