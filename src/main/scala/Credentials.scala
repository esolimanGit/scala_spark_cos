object Credentials {

  // Marketing Credentials for Red/Black (R/w)
    val cosConfigProd: CosConfig = CosConfig(
      endpoint = "s3-api.us-geo.objectstorage.softlayer.net",
      serviceId = "",
      apiKey = "",
      bucketName = "epm-marketing-red-2",
      instanceId = "",
      accessKey = "",
      secretKey = ""
    )
  // Marketing Credentials for Staging (R/w)
  val cosConfigStaging: CosConfig = CosConfig(
    endpoint = "s3-api.us-geo.objectstorage.softlayer.net",
    serviceId = "",
    apiKey = "",
    bucketName = "epm-marketing-staging-2",
    instanceId = "",
    accessKey = "",
    secretKey = ""
  )

}
