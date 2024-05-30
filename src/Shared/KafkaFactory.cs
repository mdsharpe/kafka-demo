using System.Net;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Options;

namespace Shared;

public class KafkaFactory
{
    private readonly IOptions<KafkaOptions> _options;

    public KafkaFactory(IOptions<KafkaOptions> options)
    {
        _options = options;
    }

    public ProducerConfig GetProducerConfig()
    {
        var options = _options.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = Dns.GetHostName(),
            Acks = Acks.All,
            Partitioner = Partitioner.Murmur2Random
        };

        if (!options.IsLocalDevEnvironment)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = options.Username;
            config.SaslPassword = options.Password;
        }

        return config;
    }

    public ConsumerConfig GetConsumerConfig(string groupId)
    {
        var options = _options.Value;

        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = Dns.GetHostName(),
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        if (!options.IsLocalDevEnvironment)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = options.Username;
            config.SaslPassword = options.Password;
        }

        return config;
    }

    public IAsyncSerializer<T> GetAsyncSerializer<T>()
    {
        var options = _options.Value;

        if (options.IsLocalDevEnvironment)
        {
            return new Confluent.SchemaRegistry.Serdes.AvroSerializer<T>(
                // This really should be a singleton.
                new Confluent.SchemaRegistry.CachedSchemaRegistryClient(
                    new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = options.SchemaRegistry }));
        }
        else
        {
            return new Microsoft.Azure.Kafka.SchemaRegistry.Avro.KafkaAvroAsyncSerializer<T>(
                options.SchemaRegistry,
                new Azure.Identity.DefaultAzureCredential(),
                options.AzSchemaGroup);
        }
    }

    public IDeserializer<T> GetDeserializer<T>()
    {
        var options = _options.Value;

        if (options.IsLocalDevEnvironment)
        {
            return new Confluent.SchemaRegistry.Serdes.AvroDeserializer<T>(
                // This really should be a singleton.
                new Confluent.SchemaRegistry.CachedSchemaRegistryClient(
                    new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = options.SchemaRegistry }))
                .AsSyncOverAsync();
        }
        else
        {
            return new Microsoft.Azure.Kafka.SchemaRegistry.Avro.KafkaAvroDeserializer<T>(
                options.SchemaRegistry,
                new Azure.Identity.DefaultAzureCredential());
        }
    }
}
