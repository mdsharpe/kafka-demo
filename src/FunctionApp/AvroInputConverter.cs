using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Converters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Model;

namespace FunctionApp
{
    internal class AvroInputConverter(IConfiguration configuration) : IInputConverter
    {
        public async ValueTask<ConversionResult> ConvertAsync(ConverterContext context)
        {
            if (context.TargetType.Name != "Transaction")
            {
                return ConversionResult.Unhandled();
            }

            // TODO If we can add the binding in the middleware before it runs, if the middleware runs before this, then we might not need to specify the byte[] parameter on the function?
            var avroBytesBindingData = await context.FunctionContext.BindInputAsync<byte[]>(new MockBindingMetadata());

            var avroBytes = avroBytesBindingData.Value;

            ////if (context.FunctionContext.BindingContext.BindingData.TryGetValue("Value", out var value)
            ////    && value is string avroBytesButInAString)
            ////{
                ////var avroBytes = System.Text.Encoding.UTF8.GetBytes(avroBytesButInAString);

                try
                {
                    // TODO [MS] DI this in.
                    using var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = configuration["SCHEMA_REGISTRY"] });
                    var deserializer = new AvroDeserializer<Transaction>(schemaRegistry);

                    var transaction = await deserializer.DeserializeAsync(avroBytes, false, SerializationContext.Empty);

                    return ConversionResult.Success(transaction);
                }
                catch (Exception ex)
                {
                    return ConversionResult.Failed(ex);
                }
            }

            ////return ConversionResult.Unhandled();
        ////}
    }
}
