using System.Text;
using Confluent.Kafka;
using Shared;

namespace FunctionApp;
internal class AvroSerde(KafkaFactory kafkaFactory)
{
    public T Deserialize<T>(byte[] value, IEnumerable<KeyValuePair<string, string>> base64EncodedHeaders)
    {
        var deserializer = kafkaFactory.GetDeserializer<T>();

        var headersEnumerable = base64EncodedHeaders
            .Select(header => new Header(header.Key, Convert.FromBase64String(header.Value)));

        var context = new SerializationContext(
            component: default,
            topic: default,
            headers: [.. headersEnumerable]);

        return deserializer.Deserialize(value, false, context);
    }
}
