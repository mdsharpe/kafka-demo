using Confluent.SchemaRegistry;
using FunctionApp;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var configuration = new ConfigurationBuilder().Build();

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureAppConfiguration(configure =>
    {
        configure.AddConfiguration(configuration);
    })
    .ConfigureServices(services =>
    {
        services.AddSingleton<ISchemaRegistryClient, CachedSchemaRegistryClient>(
            s => new CachedSchemaRegistryClient(
                new SchemaRegistryConfig
                {
                    Url = s.GetRequiredService<IConfiguration>()["SCHEMA_REGISTRY"]
                }));

        services.AddSingleton<AvroDeserializer>();
    })
    .Build();

host.Run();
