using System.Net;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Model;
using Shared;
using Spectre.Console;

const string Topic = "transactions";

var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var options = new KafkaOptions();
config.GetSection("Kafka").Bind(options);

var producerConfig = new ProducerConfig
{
    BootstrapServers = options.BootstrapServers,
    ClientId = Dns.GetHostName(),
    Acks = Acks.All,
    Partitioner = Partitioner.Murmur2Random
};

////var schemaRegistryConfig = new SchemaRegistryConfig
////{
////    Url = options.SchemaRegistry
////};

AnsiConsole.Write(new Rule($"Welcome to Kafka Transactions - [yellow]Producer console app[/]").RuleStyle("grey").LeftJustified());
AnsiConsole.WriteLine();

////using (var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig))
using (var producer = new ProducerBuilder<string, string>(producerConfig)
    ////.SetValueSerializer(new AvroSerializer<Transaction>(schemaRegistryClient))
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
            new Message<string, string>
            {
                Key = product,
                Value = JsonSerializer.Serialize(transaction)
            });

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine($"[green]Delivered message to {deliveryReport.TopicPartitionOffset}[/]");
        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Rule().RuleStyle("grey"));
        AnsiConsole.WriteLine();
    } while (true);
}
