using System.Net;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
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

ConsumerConfig consumerConfig = new()
{
    BootstrapServers = config.GetConnectionString("Kafka"),
    ClientId = Dns.GetHostName(),
    GroupId = "console-consumers",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false
};

var schemaRegistryConfig = new SchemaRegistryConfig { Url = config.GetConnectionString("SchemaRegistry") };

CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

AnsiConsole.Write(new Rule($"Welcome to Kafka Transactions - [yellow]Consumer console app[/]").RuleStyle("grey").LeftJustified());
AnsiConsole.WriteLine();

using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
using (var consumer = new ConsumerBuilder<string, Transaction>(consumerConfig)
    .SetValueDeserializer(new AvroDeserializer<Transaction>(schemaRegistry).AsSyncOverAsync())
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
