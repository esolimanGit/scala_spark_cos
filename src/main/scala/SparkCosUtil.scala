import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object SparkCosUtil {

  /**
   * Reads a DataFrame from Cloud Object Storage
   *
   * @param cosConfig [[CosConfig]] COS configuration
   * @param path      [[String]] the path to the parquet file
   * @param schema    [[Option]] the parquet schema (optional)
   * @return [[DataFrame]] the data in the parquet file
   */
  def readParquet(cosConfig: CosConfig,
                  path: String,
                  schema: Option[StructType] = None): DataFrame = {
    println(s"Reading parquet at $path")
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    this.getHadoopConfiguration(cosConfig)
    schema match {
      case Some(structType: StructType) =>
        spark.read.schema(structType).parquet(path)
      case None => spark.read.parquet(path)
    }
  }

  /**
   * Reads multiple parquets into a DataFrame from Cloud Object Storage
   *
   * @param cosConfig [[CosConfig]] COS configuration
   * @param paths      [[String]] the paths to the multiple parquet files
   * @return [[DataFrame]] the data in the parquet files
   */
  def readMultipleParquets(cosConfig: CosConfig, paths: String*): DataFrame = {
    val sparkobj: SparkSession = SparkSession.builder().getOrCreate()
    this.getHadoopConfiguration(cosConfig)
    sparkobj.read.parquet(paths: _*)
  }

  /**
   * Reads a DataFrame from a csv in Cloud Object Storage
   *
   * @param cosConfig [[CosConfig]] cosConfig
   * @param path [[String]] path to read data from
   * @return [[DataFrame]] the data in the CSV file
   */
  def readCSV(cosConfig: CosConfig, path: String): DataFrame = {
    println(s"Reading csv at $path")
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    this.getHadoopConfiguration(cosConfig)
    spark.read.option("header", "true").option("inferSchema", "true").csv(paths = path)
  }

  /**
   * Writes a spark stream to the sink
   *
   * @param cosConfig      [[CosConfig]] the COS configuration
   * @param dataFrame      [[DataFrame]] the data set
   * @param path           [[String]] the path to write to
   * @param checkpointPath [[String]] the path to write checkpoints to
   * @param outputMode     [[OutputMode]] the output mode
   */
  def writeParquetSink(cosConfig: CosConfig,
                       dataFrame: DataFrame,
                       path: String,
                       checkpointPath: String,
                       outputMode: OutputMode): Unit = {
    this.getHadoopConfiguration(cosConfig)
    val outStream = dataFrame.writeStream
      .format("parquet")
      .option("checkpointLocation", checkpointPath)
      .option("path", path)
      .outputMode(outputMode)
      .start()
    outStream.awaitTermination()
  }

  /**
   * Writes a DataFrame to Cloud Object Storage
   *
   * @param cosConfig [[CosConfig]] COS configuration
   * @param dataFrame [[DataFrame]] the data to write
   * @param saveMode  [[SaveMode]] the save mode for the write
   * @param path      [[String]] the path to write the DataFrame to
   */
  def writeParquet(cosConfig: CosConfig,
                   dataFrame: DataFrame,
                   saveMode: SaveMode,
                   path: String): Unit = {
    println(s"Writing parquet to $path")
    this.getHadoopConfiguration(cosConfig)
    dataFrame.write.mode(saveMode).parquet(path)
  }

  /**
   * Generic write method
   *
   * @param cosConfig [[CosConfig]] COS configuration
   * @param dataFrame [[DataFrame]] the data set to overwrite
   * @param saveMode [[SaveMode]] the same mode
   * @param path [[String]] the path at which to save the data
   * @param format [[String]] the output format (i.e. csv, json, or parquet)
   */
  def write(cosConfig: CosConfig,
            dataFrame: DataFrame,
            options: Map[String, String],
            format: String,
            saveMode: SaveMode,
            path: String): Unit = {
    println(s"Writing $format to $path")
    this.getHadoopConfiguration(cosConfig)
    dataFrame.write.format(format).options(options).mode(saveMode).save(path)
  }

  /**
   * Writes partitioned output to Cloud Object Storage,
   * Have to lowercase the partitioned columns so HIVE can load/map them with its tables. (EPMDATA-9661)
   * @param cosConfig   [[CosConfig]] COS configuration
   * @param dataFrame   [[DataFrame]] the data set
   * @param saveMode    [[SaveMode]] the save mode
   * @param partitionBy [[Seq]] the column(s) to partition by
   * @param options     [[Map]] write options (Optional)
   * @param format      [[String]] the output format (i.e. csv, json, or parquet)
   * @param path        [[String]] the wrote path in COS
   */
  def write(cosConfig: CosConfig,
            dataFrame: DataFrame,
            saveMode: SaveMode,
            partitionBy: Seq[String],
            options: Map[String, String] = Map(),
            format: String,
            path: String): Unit = {
    println(s"Writing $format to $path")
    this.getHadoopConfiguration(cosConfig)
    val columnsList = dataFrame.columns.map(f =>
      if (partitionBy.contains(f)) { col(f).as(f.toLowerCase()) } else { col(f) })
    dataFrame
      .select(columnsList: _*)
      .write
      .format(format)
      .mode(saveMode)
      .partitionBy(partitionBy.map(name => name.toLowerCase): _*)
      .options(options)
      .save(path)
  }

  /**
   * Writes partitioned output to Cloud Object Storage,
   * Have to lowercase the partitioned columns so HIVE can load/map them with its tables. (EPMDATA-9661)
   *
   * @param cosConfig   [[CosConfig]] COS configuration
   * @param dataFrame   [[DataFrame]] the data set
   * @param saveMode    [[SaveMode]] the save mode
   * @param partitionBy [[Seq]] the column(s) to partition by
   * @param options     [[Map]] write options (Optional)
   * @param path        [[String]] the wrote path in COS
   */
  def writeParquet(cosConfig: CosConfig,
                   dataFrame: DataFrame,
                   saveMode: SaveMode,
                   partitionBy: Seq[String],
                   options: Map[String, String] = Map(),
                   path: String): Unit = {
    println(s"Writing parquet to $path")
    this.getHadoopConfiguration(cosConfig)
    val columnsList = dataFrame.columns.map(f =>
      if (partitionBy.contains(f)) { col(f).as(f.toLowerCase()) } else { col(f) })
    dataFrame
      .select(columnsList: _*)
      .write
      .mode(saveMode)
      .partitionBy(partitionBy.map(name => name.toLowerCase): _*)
      .options(options)
      .parquet(path)
  }

  /**
   * Provides access to the file system at the specified path
   *
   * @param cosConfig [[CosConfig]] the COS configuration
   * @param path      [[Path]] the file path in COS
   * @return [[FileSystem]] the file system
   */
  def getFileSystem(cosConfig: CosConfig, path: Path): FileSystem = {
    path.getFileSystem(this.getHadoopConfiguration(cosConfig))
  }

  /**
   * Retrieves a list of files (recursive) from COS
   *
   * @param cosConfig [[CosConfig]] the COS configuration
   * @param path      [[Path]] the COS path
   * @return [[Array]] the files
   */
  def getPathFiles(cosConfig: CosConfig, path: Path): Array[FileStatus] = {
    this.getFileSystem(cosConfig, path).listStatus(path)
  }

  /**
   * Function to check if a path exist in the COS bucket
   *
   * @param path [[String]] the file path in COS
   * @param cosConfig [[CosConfig]] the COS configuration
   * @return: [[Boolean]] true if the path exists
   */
  def checkPath(path: Path, cosConfig: CosConfig): Boolean = {
    this.getFileSystem(cosConfig, path).exists(path)
  }

  /**
   * Builds and returns the hadoop configuration for COS
   *
   * @param cosConfig [[CosConfig]] the COS configuration
   * @return [[Configuration]] the hadoop configuration
   */
  private def getHadoopConfiguration(cosConfig: CosConfig): Configuration = {
    val hadoopConfiguration: Configuration =
      SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration

    // COS API configuration
    hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
    hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
    hadoopConfiguration.set("fs.cos.service.iam.api.key", cosConfig.apiKey)
    hadoopConfiguration.set("fs.cos.service.iam.service.id", cosConfig.serviceId)
    hadoopConfiguration.set("fs.cos.service.endpoint", cosConfig.endpoint)
    hadoopConfiguration.set("fs.cos.client.execution.timeout", "18000000")

    // S3 API Configuration
    if (!Option(cosConfig.accessKey).forall(_.isEmpty) && !Option(cosConfig.secretKey).forall(
      _.isEmpty)) {
      hadoopConfiguration.set("fs.s3a.endpoint", cosConfig.endpoint)
      hadoopConfiguration.set("fs.s3a.access.key", cosConfig.accessKey)
      hadoopConfiguration.set("fs.s3a.secret.key", cosConfig.secretKey)
    }

    hadoopConfiguration
  }

  /**
   * Reads a csv file from Cloud Object Storage
   *
   * @param cosConfig [[CosConfig]] COS configuration
   * @param path      [[String]] the path to the parquet file
   * @param schema    [[Option]] the parquet schema (optional)
   * @return [[DataFrame]] the data in the parquet file
   */
  def readCSV(cosConfig: CosConfig, path: String, schema: Option[StructType] = None): DataFrame = {
    println(s"Reading parquet at $path")
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    this.getHadoopConfiguration(cosConfig)
    schema match {
      case Some(structType: StructType) =>
        spark.read
          .format("csv")
          .option("header", "true")
          .schema(structType)
          .load(path)
      case None =>
        spark.read
          .format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(path)
    }
  }
}
