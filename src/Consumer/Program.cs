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

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = options.BootstrapServers,
    ClientId = Dns.GetHostName(),
    GroupId = "console-consumers",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false
};

////var schemaRegistryConfig = new SchemaRegistryConfig
////{
////    Url = options.SchemaRegistry
////};

CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

AnsiConsole.Write(new Rule($"Welcome to Kafka Transactions - [yellow]Consumer console app[/]").RuleStyle("grey").LeftJustified());
AnsiConsole.WriteLine();

////using (var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig))
using (var consumer = new ConsumerBuilder<string, string>(consumerConfig)
    ////.SetValueDeserializer(new AvroDeserializer<Transaction>(schemaRegistryClient).AsSyncOverAsync())
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

                var transaction = JsonSerializer.Deserialize<Transaction>(result.Message.Value)!;

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
