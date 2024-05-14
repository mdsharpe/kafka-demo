// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Model;
using Spectre.Console;

const string Topic = "transactions";

ConsumerConfig consumerConfig = new()
{
    BootstrapServers = "localhost:9092",
    GroupId = "console-consumers",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false
};

var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8085" };

CancellationTokenSource cts = new();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

AnsiConsole.WriteLine("Welcome to Kafka Adventure Console Consumer.");
AnsiConsole.WriteLine();

using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
using (var consumer = new ConsumerBuilder<string, Transaction>(consumerConfig)
    .SetValueDeserializer(new AvroDeserializer<Transaction>(schemaRegistry).AsSyncOverAsync())
    .Build())
{
    consumer.Subscribe(Topic);

    while (!cts.IsCancellationRequested)
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
}
