# Configures OpenMetadata MariaDB Service via API
$ErrorActionPreference = "Stop"

$uri = "http://127.0.0.1:8585/api/v1/services/databaseServices"
$headers = @{
    "Content-Type" = "application/json"
}

$body = @{
    name        = "dq_retail_db_service"
    serviceType = "MariaDB"
    connection  = @{
        config = @{
            type           = "MariaDB"
            scheme         = "mysql+pymysql"
            username       = "dq_user"
            authType       = @{ password = "dq_password" }
            hostPort       = "dq_mariadb:3306"
            databaseSchema = "data_quality"
        }
    }
}

Write-Host "Configuring MariaDB Service in OpenMetadata..."
try {
    # Check if exists first
    try {
        $existing = Invoke-RestMethod -Uri "$uri/name/dq_retail_db_service" -Method Get -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Host "Service already exists: $($existing.name)"
            exit 0
        }
    }
    catch {}

    # Create
    $json = $body | ConvertTo-Json -Depth 10
    $response = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers -Body $json
    Write-Host "SUCCESS: Created Service '$($response.name)' (ID: $($response.id))"
}
catch {
    Write-Host "ERROR: $($_.Exception.Message)"
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader $_.Exception.Response.GetResponseStream()
        Write-Host "Details: $($reader.ReadToEnd())"
    }
    exit 1
}
