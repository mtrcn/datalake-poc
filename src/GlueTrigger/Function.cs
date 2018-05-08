using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Lambda.Serialization.Json;
using System;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(JsonSerializer))]

namespace GlueTrigger
{
    public class Function
    {
        private IAmazonGlue GlueClient { get; }


        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            GlueClient = new AmazonGlueClient();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        public Function(IAmazonGlue glueClient)
        {
        }
        
        /// <summary>
        /// This method is called when associated S3 bucket has new file and trigger AWS Glue to transform it.
        /// </summary>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            context.Logger.LogLine("Started!");
            var s3Event = evnt.Records?[0].S3;
            if(s3Event == null)
            {
                context.Logger.LogLine("Event not found!");
                return "Event not found!";
            }

            try
            {
                context.Logger.LogLine("Starting job run...");
                await GlueClient.StartJobRunAsync(new StartJobRunRequest()
                {
                    AllocatedCapacity = 2,
                    JobName = Environment.GetEnvironmentVariable("GLUE_JOB_NAME")
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
