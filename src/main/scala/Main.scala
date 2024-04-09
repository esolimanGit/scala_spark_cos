import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, current_date, from_json, lit, max, substring, to_date, trim}
import org.apache.spark.sql.types.DateType
import org.apache.spark.storage.StorageLevel

object Main {

  val topicName = "isc-clientinterest-in"
  val schema = Schema.CLIENT_INTEREST_KAFKA
  val currentDate = "2024-03-07"
  var spark: SparkSession = null
  val cosConfig: CosConfig = Credentials.cosConfigStaging

  def main(args: Array[String]): Unit = {

    spark = SparkSession
      .builder()
      .appName("ScalaSpark")
      .config("spark.sql.session.timeZone", "UTC")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryoserializer.buffer", "500m")
      //.config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.sql.broadcastTimeout", 900)
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.hadoop.fs.stocator.scheme.list", "cos")
      .config(
        "spark.hadoop.fs.cos.impl",
        "com.ibm.stocator.fs.ObjectStoreFileSystem"
      )
      .config(
        "spark.hadoop.fs.stocator.cos.impl",
        "com.ibm.stocator.fs.cos.COSAPIClient"
      )
      .config("spark.hadoop.fs.stocator.cos.scheme", "cos")
      .config("spark.hadoop.fs.cos.service.iam.api.key", cosConfig.apiKey)
      .config("spark.hadoop.fs.cos.service.iam.service.id", cosConfig.serviceId)
      .config("spark.hadoop.fs.cos.service.endpoint", cosConfig.endpoint)
      .master("local[*]")
      .getOrCreate()

//    val ciDF = SparkCosUtil.readParquet(
//      cosConfig,
//      s"cos://${cosConfig.bucketName}.service/landingzone/isc/isc-contact-in/extract.parquet/EXTRACT_DATE=${currentDate}",
////      Some(schema)
//      None
//    )
//    ciDF.count()

    val dimDF = SparkCosUtil.readParquet(
      cosConfig,
      s"cos://${cosConfig.bucketName}.service/dimension/CLIENT_INTEREST_DIMENSION/transform.parquet",
      //      Some(schema)
      None
    )
    dimDF.count()
//      val dimDF = SparkCosUtil.readParquet(
//        cosConfig,
//        s"cos://epm-enterprise-dimensions-staging-2.service/dimension/PERIOD_V4_DIMENSION_GREGORIAN/transform.parquet",
//        //      Some(schema)
//        None
//      )
//    val utDF = ciDF.withColumn("UTCODE",
//      coalesce(substring(col("IBM_UTL30_CODE"), 1, 5),
//        coalesce(substring(col("IBM_UTL20_CODE"), 1, 5),
//          coalesce(substring(col("IBM_UTL17_CODE"), 1, 5),
//            coalesce(substring(col("IBM_UTL15_CODE"), 1, 5),
//              coalesce(substring(col("IBM_UTL10_CODE"), 1, 5)), lit(-1))))))
//    utDF.count()
//    extractFromKafka
//joinISCObjects

  }

  def joinISCObjects(): Unit = {
    val ciDF = SparkCosUtil.readParquet(
      cosConfig,
      s"cos://${cosConfig.bucketName}.service/landingzone/isc/isc-clientinterest-in/extract.parquet/extract_date=${currentDate}",
      Some(schema)
    )
    val contactDF = SparkCosUtil
      .readParquet(
        cosConfig,
        s"cos://${cosConfig.bucketName}.service/landingzone/isc/isc-contact-in/extract.parquet/extract_date=${currentDate}",
        Some(Schema.CONTACT_KAFKA)
      )
      .withColumnRenamed("Id__c", "Contact__c")
      .select("Contact__c", "Country1__c")

    val accountDF = SparkCosUtil
      .readParquet(
        cosConfig,
        s"cos://${cosConfig.bucketName}.service/landingzone/isc/isc-account-in/extract.parquet/extract_date=2024-02-14",
        Some(Schema.ACCOUNT_KAFKA)
      )
      .withColumnRenamed("Id__c", "Account_name__c")
      .select("Account_name__c", "mpp_number__c")

    val resultDF = ciDF
      .withColumn(
        "IBM_UTL30_Code__c",
        substring(col("IBM_UTL30_Code__c"), 0, 5)
      )
      .withColumn(
        "IBM_UTL20_Code__c",
        substring(col("IBM_UTL20_Code__c"), 0, 5)
      )
      .withColumn(
        "IBM_UTL15_Code__c",
        substring(col("IBM_UTL15_Code__c"), 0, 5)
      )
      .withColumn(
        "IBM_UTL17_Code__c",
        substring(col("IBM_UTL17_Code__c"), 0, 5)
      )
      .withColumn(
        "IBM_UTL10_Code__c",
        substring(col("IBM_UTL10_Code__c"), 0, 5)
      )
      .join(accountDF, Seq("Account_name__c"), "left_outer")
      .join(contactDF, Seq("Contact__c"), "left_outer")
      .withColumn("CreatedDate", col("CreatedDate__c").cast(DateType))
      .select(
        "Id__c",
        "Name__c",
        "Country1__c",
        "Status__c",
        "Reason__c",
        "Leadsource__c",
        "Campaign_Code__c",
        "Opportunity__c",
        "CreatedDate",
        "IsClosed__c",
        "mpp_number__c",
        "LastModifiedDate__c",
        "IBM_UTL30_Code__c",
        "IBM_UTL20_Code__c",
        "IBM_UTL17_Code__c",
        "IBM_UTL15_Code__c",
        "IBM_UTL10_Code__c"
      )
    //        .where("IBM_UTL30_Code__c IS NULL AND IBM_UTL20_Code__c IS NULL AND IBM_UTL17_Code__c IS  NULL AND IBM_UTL10_Code__c IS NOT NULL")
    //        .groupBy("IBM_UTL30_Code__c","IBM_UTL20_Code__c", "IBM_UTL17_Code__c","IBM_UTL10_Code__c").count().where("count > 0").orderBy(col("count").desc)

    resultDF.count()

    ciDF.count();
  }

  /** *
    * Read from Kafka Topic and Write data to COS
    */
  def extractFromKafka: Unit = {
    val topicDF = spark.read
      .format("kafka")
      .options(
        KafkaUtil.buildKafkaConnectionProperties(
          KafkaConfig.brokers,
          KafkaConfig.username,
          KafkaConfig.password,
          topic = topicName
        )
      )
      .load()

    val extractDF = topicDF
      .selectExpr(
        "CAST(offset AS INT)",
        "CAST(key AS STRING)",
        "CAST(value AS STRING)"
      )

      .withColumn("value", from_json(col("value"), schema))
      .select("offset", "key", "value.*")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val maxSystemTS = extractDF
      .groupBy("Id__c")
      .agg(max("offset").as("offset"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val latestDF = extractDF
      .join(maxSystemTS, Seq("Id__c", "offset"), "inner")
      .withColumn("EXTRACT_DATE", current_date())
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"extractDF.count() ${extractDF.count()}")
    println(s"latestDF.count() ${latestDF.count()}")

    SparkCosUtil.writeParquet(
      cosConfig = cosConfig,
      dataFrame = latestDF.coalesce(5),
      saveMode = SaveMode.Overwrite,
      partitionBy = Seq("EXTRACT_DATE"),
      options = Map("partitionOverwriteMode" -> "dynamic"),
      path =
        s"cos://${cosConfig.bucketName}.service/landingzone/isc-jospeh/${topicName}/extract.parquet"
    )

  }
}
