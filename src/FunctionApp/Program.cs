using FunctionApp;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared;

var configuration = new ConfigurationBuilder().Build();

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureAppConfiguration(configure =>
    {
        configure.AddConfiguration(configuration);
    })
    .ConfigureServices((builder, services) =>
    {
        services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
        services.AddSingleton<KafkaFactory>();
        services.AddSingleton<AvroSerde>();
    })
    .Build();

host.Run();
