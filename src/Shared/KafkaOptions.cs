﻿namespace Shared;
public class KafkaOptions
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string SchemaRegistry { get; set; } = string.Empty;
    public string AzEventHubsNamespace { get; set; } = string.Empty;
    public string AzSchemaGroup { get; set; } = string.Empty;
    public bool IsLocalDevEnvironment { get; set; }
}
