using System.Net;
using Azure.Core;
using Azure.Identity;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Options;

namespace Shared;

public class KafkaFactory(IOptions<KafkaOptions> options)
{
    public IProducer<TKey, TValue> CreateProducer<TKey, TValue>()
    {
        var builder = new ProducerBuilder<TKey, TValue>(GetProducerConfig())
            .SetValueSerializer(GetAsyncSerializer<TValue>());

        if (options.Value.IsLocalDevEnvironment)
        {
            builder = builder.SetOAuthBearerTokenRefreshHandler(OAuthTokenRefreshCallback);
        }

        return builder.Build();
    }

    public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>()
    {
        var builder = new ConsumerBuilder<TKey, TValue>(GetProducerConfig())
            .SetValueDeserializer(GetDeserializer<TValue>());

        if (options.Value.IsLocalDevEnvironment)
        {
            builder = builder.SetOAuthBearerTokenRefreshHandler(OAuthTokenRefreshCallback);
        }

        return builder.Build();
    }

    public ProducerConfig GetProducerConfig()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            ClientId = Dns.GetHostName(),
            Acks = Acks.All,
            Partitioner = Partitioner.Murmur2Random
        };

        if (!options.Value.IsLocalDevEnvironment)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
        }

        return config;
    }

    public ConsumerConfig GetConsumerConfig(string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            ClientId = Dns.GetHostName(),
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        if (!options.Value.IsLocalDevEnvironment)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
        }

        return config;
    }

    public IAsyncSerializer<T> GetAsyncSerializer<T>()
    {
        if (options.Value.IsLocalDevEnvironment)
        {
            return new Confluent.SchemaRegistry.Serdes.AvroSerializer<T>(
                // This really should be a singleton.
                new Confluent.SchemaRegistry.CachedSchemaRegistryClient(
                    new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = options.Value.SchemaRegistry }));
        }
        else
        {
            return new Microsoft.Azure.Kafka.SchemaRegistry.Avro.KafkaAvroAsyncSerializer<T>(
                options.Value.SchemaRegistry,
                new DefaultAzureCredential(),
                options.Value.AzSchemaGroup);
        }
    }

    public IDeserializer<T> GetDeserializer<T>()
    {
        if (options.Value.IsLocalDevEnvironment)
        {
            return new Confluent.SchemaRegistry.Serdes.AvroDeserializer<T>(
                // This really should be a singleton.
                new Confluent.SchemaRegistry.CachedSchemaRegistryClient(
                    new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = options.Value.SchemaRegistry }))
                .AsSyncOverAsync();
        }
        else
        {
            return new Microsoft.Azure.Kafka.SchemaRegistry.Avro.KafkaAvroDeserializer<T>(
                options.Value.SchemaRegistry,
                new DefaultAzureCredential());
        }
    }

    /// <summary>
    /// Kafka Client callback to get or refresh a Token
    /// </summary>
    /// <param name="client"></param>
    /// <param name="config"></param>
    private void OAuthTokenRefreshCallback(IClient client, string config)
    {
        try
        {
            var token = new DefaultAzureCredential()
                .GetToken(
                    new TokenRequestContext([$"https://enable-ms-kafka-demo.servicebus.windows.net/.default"]));

            client.OAuthBearerSetToken(
                tokenValue: token.Token,
                lifetimeMs: token.ExpiresOn.ToUnixTimeMilliseconds(),
                principalName: default);
        }
        catch (Exception ex)
        {
            client.OAuthBearerSetTokenFailure(ex.ToString());
        }
    }
}
