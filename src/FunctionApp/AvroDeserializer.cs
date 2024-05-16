using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace FunctionApp;
internal class AvroDeserializer(ISchemaRegistryClient schemaRegistry)
{
    public Task<T> DeserializeAsync<T>(byte[] value)
    {
        var deserializer = new Confluent.SchemaRegistry.Serdes.AvroDeserializer<T>(schemaRegistry);
        return deserializer.DeserializeAsync(value, false, SerializationContext.Empty);
    }
}
