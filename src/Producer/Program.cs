using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
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
var kafkaOptions = Options.Create(options);

var kafkaFactory = new KafkaFactory(kafkaOptions);
var producerConfig = kafkaFactory.GetProducerConfig();
var serializer = kafkaFactory.GetAsyncSerializer<Transaction>();

AnsiConsole.Write(new Rule($"Welcome to Kafka Transactions - [yellow]Producer console app[/]").RuleStyle("grey").LeftJustified());
AnsiConsole.WriteLine();

using (var producer = new ProducerBuilder<string, Transaction>(producerConfig)
    .SetValueSerializer(serializer)
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
