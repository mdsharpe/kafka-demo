$settings = @{
    "IsEncrypted" = $false;
    "Values"      = @{
        "AzureWebJobsStorage"         = "UseDevelopmentStorage=true";
        "FUNCTIONS_WORKER_RUNTIME"    = "dotnet-isolated";
        "Kafka:BootstrapServers"      = "localhost:9092";
        "Kafka:Username"              = "";
        "Kafka:Password"              = "";
        "Kafka:SchemaRegistry"        = "localhost:8085";
        "Kafka:AzSchemaGroup"         = "";
        "Kafka:IsLocalDevEnvironment" = $true;
    }
}

$settings | ConvertTo-Json -Depth 10 | % { [System.Text.RegularExpressions.Regex]::Unescape($_) } | Set-content 'local.settings.json' -Force;
