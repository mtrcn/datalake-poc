using Amazon.Lambda.CloudWatchLogsEvents;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.Json;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(JsonSerializer))]

namespace DataPuller
{
    public class Function
    {
        IAmazonS3 S3Client { get; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        public Function(IAmazonS3 s3Client)
        {
            S3Client = s3Client;
        }
        
        /// <summary>
        /// This method takes the response from a HTTP end-point and save to S3.
        /// </summary>
        public async Task<string> FunctionHandler(CloudWatchLogsEvent evnt, ILambdaContext context)
        {
            context.Logger.LogLine("Starting...");
            try
            {
                var httpClient = new HttpClient();
                context.Logger.LogLine("Downloading remote data...");
                var response = await httpClient.GetByteArrayAsync(Environment.GetEnvironmentVariable("HTTP_ENDPOINT"));
                context.Logger.LogLine("Uploading to S3...");
                await S3Client.PutObjectAsync(new PutObjectRequest()
                {
                    InputStream = new MemoryStream(response),
                    AutoCloseStream = true,
                    AutoResetStreamPosition = true,
                    BucketName = Environment.GetEnvironmentVariable("BUCKET_NAME"),
                    Key = Environment.GetEnvironmentVariable("KEY_PREFIX") + DateTime.Now.Ticks
                });
                context.Logger.LogLine("Done");
                return "Done";
            }
            catch(Exception e)
            {
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }
    }
}
