using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace FunctionApp;
internal class AvroSerde(ISchemaRegistryClient schemaRegistry)
{
    public Task<byte[]> SerializeAsync<T>(T value)
    {
        var serializer = new AvroSerializer<T>(schemaRegistry);
        return serializer.SerializeAsync(value, SerializationContext.Empty);
    }

    public Task<T> DeserializeAsync<T>(byte[] value)
    {
        var deserializer = new AvroDeserializer<T>(schemaRegistry);
        return deserializer.DeserializeAsync(value, false, SerializationContext.Empty);
    }
}
