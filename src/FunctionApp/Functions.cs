using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Model;

namespace FunctionApp;
internal class Functions(AvroSerde serde)
{
    [Function(nameof(ProcessTransaction))]
    public async Task ProcessTransaction(
        [KafkaTrigger(
            brokerList: "Kafka:BootstrapServers",
            topic: "transactions",
            ////Protocol = BrokerProtocol.SaslSsl,
            ////AuthenticationMode = BrokerAuthenticationMode.Plain,
            ////Username = "%Kafka:Username%",
            ////Password = "%Kafka:Password%",
            ConsumerGroup = "function-consumers")] byte[] value,
        string key,
        IEnumerable<KeyValuePair<string, string>> headers,
        FunctionContext context)
    {
        var transaction = serde.Deserialize<Transaction>(value, headers);

        var logger = context.GetLogger(nameof(ProcessTransaction));
        logger.LogInformation($"key: {key}; product: {transaction.Product}; quantity: {transaction.Quantity}; value: {transaction.Value}");
    }
}
