using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Converters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Model;

namespace FunctionApp;
internal class Functions(IConfiguration configuration)
{
    [Function(nameof(ProcessTransaction))]
    public async Task ProcessTransaction(
        [KafkaTrigger(
            brokerList: "BOOTSTRAP_SERVER",
            topic: "transactions",
            ConsumerGroup = "function-consumers")] byte[] bytez,
        [InputConverter(typeof(AvroInputConverter))] Transaction transaction,
        string key,
        FunctionContext context)
    {
        var logger = context.GetLogger<Functions>();

        logger.LogInformation($"Key: {key}; Product: {transaction.Product}; Quantity: {transaction.Quantity}; Value: {transaction.Value}");
    }
}
