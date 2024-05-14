using System.Net;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Model;
using Spectre.Console;

const string Topic = "transactions";

var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = Dns.GetHostName(),
    Acks = Acks.All,
    //EnableIdempotence = true,
    Partitioner = Partitioner.Murmur2Random
};

var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8085" };

AnsiConsole.WriteLine("Welcome to Kafka Transactions.");
AnsiConsole.WriteLine();

using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
using (var producer = new ProducerBuilder<string, Transaction>(producerConfig)
    .SetValueSerializer(new AvroSerializer<Transaction>(schemaRegistry))
    .Build())
{
    do
    {
        var product = AnsiConsole.Ask<string>("Product: ");
        var quantity = AnsiConsole.Ask<int>("Quantity: ");
        var value = AnsiConsole.Ask<double>("Value: ");

        var transaction = new Transaction
        {
            Product = product,
            Quantity = quantity,
            Value = value
        };

        var deliveryReport = await producer.ProduceAsync(
            Topic,
            new Message<string, Transaction>
            {
                Key = product,
                Value = transaction
            });

        AnsiConsole.MarkupLine($"[green]Delivered message to {deliveryReport.TopicPartitionOffset}[/]");
    } while (true);
}
