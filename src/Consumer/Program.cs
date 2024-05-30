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
var consumerConfig = kafkaFactory.GetConsumerConfig("console-consumers");
var deserializer = kafkaFactory.GetDeserializer<Transaction>();

CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

AnsiConsole.Write(new Rule($"Welcome to Kafka Transactions - [yellow]Consumer console app[/]").RuleStyle("grey").LeftJustified());
AnsiConsole.WriteLine();

using (var consumer = new ConsumerBuilder<string, Transaction>(consumerConfig)
    .SetValueDeserializer(deserializer)
    .Build())
{
    consumer.Subscribe(Topic);

    try
    {
        while (!cts.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(cts.Token);

                if (result.IsPartitionEOF)
                {
                    await Task.Delay(1000, cts.Token);
                    continue;
                }

                var transaction = result.Message.Value;

                var table = new Table();
                table.AddColumn("Key");
                table.AddColumn("Product");
                table.AddColumn("Quantity");
                table.AddColumn("Value");
                table.AddRow(result.Message.Key, transaction.Product, transaction.Quantity.ToString(), transaction.Value.ToString());
                AnsiConsole.Write(table);

                consumer.StoreOffset(result);
                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                AnsiConsole.WriteException(ex);
                AnsiConsole.WriteLine();
            }
        }
    }
    catch (OperationCanceledException)
    {
        // Ctrl-C was pressed.
    }
}
