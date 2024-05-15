using FunctionApp;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

var config = new ConfigurationBuilder()
    .Build();

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults(fwab =>
    {
        fwab.InputConverters.Register<AvroInputConverter>();
    })
    .ConfigureAppConfiguration(configuration =>
    {
        configuration.AddConfiguration(config);
    })
    .ConfigureServices(services =>
    {
    })
    .Build();

host.Run();
