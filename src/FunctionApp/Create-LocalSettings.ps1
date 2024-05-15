$settings = @{
    "Values"      = @{
        "AzureWebJobsStorage"                                             = "UseDevelopmentStorage=true";
        "FUNCTIONS_WORKER_RUNTIME"                                        = "dotnet-isolated";
    }
}

$settings | ConvertTo-Json -Depth 10 | % { [System.Text.RegularExpressions.Regex]::Unescape($_) } | Set-content 'local.settings.json' -Force;
