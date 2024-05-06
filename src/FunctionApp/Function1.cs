using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace FunctionApp
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function("Function1")]
        public IActionResult Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");
            return new OkObjectResult("Welcome to Azure Functions!");
        }

        [Function("KafkaTrigger")]
        public static void Run(
            [KafkaTrigger("BrokerList",
                  "topic",
                  Username = "$ConnectionString",
                  Password = "EventHubConnectionString",
                  Protocol = BrokerProtocol.SaslSsl,
                  AuthenticationMode = BrokerAuthenticationMode.Plain,
                  ConsumerGroup = "$Default")] string eventData, FunctionContext context)
        {
            var logger = context.GetLogger("KafkaFunction");
            logger.LogInformation($"C# Kafka trigger function processed a message: {JObject.Parse(eventData)["Value"]}");
        }
    }
}
