using System.Net;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Model;
using Spectre.Console;

var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

const string Topic = "transactions";

ProducerConfig producerConfig = new()
{
    BootstrapServers = config.GetConnectionString("Kafka"),
    ClientId = Dns.GetHostName(),
    Acks = Acks.All,
    //EnableIdempotence = true,
    Partitioner = Partitioner.Murmur2Random
};

var schemaRegistryConfig = new SchemaRegistryConfig { Url = config.GetConnectionString("SchemaRegistry") };

AnsiConsole.Write(new Rule($"Welcome to Kafka Transactions - [yellow]Producer console app[/]").RuleStyle("grey").LeftJustified());
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

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine($"[green]Delivered message to {deliveryReport.TopicPartitionOffset}[/]");
        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Rule().RuleStyle("grey"));
        AnsiConsole.WriteLine();
    } while (true);
}
