using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Model;

namespace FunctionApp;
internal class Functions(AvroDeserializer deserializer)
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
        var transaction = await deserializer.DeserializeAsync<Transaction>(value);

        var logger = context.GetLogger<Functions>();
        logger.LogInformation($"key: {key}; product: {transaction.Product}; quantity: {transaction.Quantity}; value: {transaction.Value}");
    }
}
