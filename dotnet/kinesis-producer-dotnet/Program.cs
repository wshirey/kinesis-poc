using System;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using DotNetEnv;

namespace KinesisProducerDotNet
{
  class Program
  {
    public static AmazonKinesisClient kinesisClient { get; } = getKinesisClient();
    static void Main(string[] args)
    {
      DotNetEnv.Env.Load(".env");

      var sqlConnection = getSqlConnection();
      var sql = "SELECT TOP 10 companyid, memid, fname, lname from dbo.Members";

      sqlConnection.Open();
      using (var command = new SqlCommand(sql, sqlConnection))
      using (var reader = command.ExecuteReader())
      using (var schema = reader.GetSchemaTable())
      {
        while (reader.Read())
        {
          var key = getKey(reader);
          var blob = getPayload(reader);
          sendToKinesis(key, blob);
        }
      }
      sqlConnection.Close();
    }

    public static void sendToKinesis(string key, byte[] blob)
    {
      PutRecordRequest requestRecord = new PutRecordRequest();
      requestRecord.StreamName = System.Environment.GetEnvironmentVariable("KINESIS_STREAM");
      requestRecord.Data = new MemoryStream(blob);
      requestRecord.PartitionKey = key;
      var putResult = kinesisClient.PutRecordAsync(requestRecord);
      putResult.Wait();
      Console.Error.WriteLine(
          String.Format("Successfully putrecord in partition key = {0,15}, shard ID = {1}",
              putResult.Id, requestRecord.PartitionKey, putResult.Result.ShardId));
    }
    public static string getKey(SqlDataReader reader)
    {
      return reader.GetGuid(0).ToString();
    }
    public static byte[] getPayload(SqlDataReader reader)
    {
      var record = reader.GetGuid(0) + "|"
        + reader.GetInt32(1) + "|"
        + reader.GetString(2) + "|"
        + reader.GetString(3);
      return UTF8Encoding.UTF8.GetBytes(record);
    }

    public static AmazonKinesisClient getKinesisClient()
    {
      var client = new AmazonKinesisClient(RegionEndpoint.USEast1);
      return client;
    }

    public static SqlConnection getSqlConnection()
    {
      var builder = new SqlConnectionStringBuilder();
      builder.DataSource = System.Environment.GetEnvironmentVariable("SQL_HOST");
      builder.UserID = System.Environment.GetEnvironmentVariable("SQL_USER");
      builder.Password = System.Environment.GetEnvironmentVariable("SQL_PASS");
      builder.InitialCatalog = System.Environment.GetEnvironmentVariable("SQL_DB");
      return new SqlConnection(builder.ConnectionString);
    }
  }
}
