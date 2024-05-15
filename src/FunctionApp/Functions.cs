using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Azure.Functions.Worker;
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
        ConsumerGroup = "function-consumers")] byte[] value,
        string key,
        FunctionContext context)
    {
        Transaction transaction;

        using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = configuration["SCHEMA_REGISTRY"] }))
        {
            var deserializer = new AvroDeserializer<Transaction>(schemaRegistry);
            transaction = await deserializer.DeserializeAsync(value, false, SerializationContext.Empty);
        }

        var logger = context.GetLogger<Functions>();

        logger.LogInformation($"Key: {key}; Product: {transaction.Product}; Quantity: {transaction.Quantity}; Value: {transaction.Value}");
    }
}
