$settings = @{
    "IsEncrypted" = $false;
    "Values"      = @{
        "AzureWebJobsStorage"                                             = "UseDevelopmentStorage=true";
        "FUNCTIONS_WORKER_RUNTIME"                                        = "dotnet-isolated";
        "BOOTSTRAP_SERVER"                                                = "localhost:9092";
        "SCHEMA_REGISTRY"                                                 = "http://localhost:8085";
    }
}

$settings | ConvertTo-Json -Depth 10 | % { [System.Text.RegularExpressions.Regex]::Unescape($_) } | Set-content 'local.settings.json' -Force;
