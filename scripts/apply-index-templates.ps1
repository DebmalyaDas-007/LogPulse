$ErrorActionPreference = 'Stop'
$es = if ($env:ELASTICSEARCH_URL) { $env:ELASTICSEARCH_URL } else { 'http://localhost:9200' }
foreach ($name in @('server-logs', 'anomalies')) {
    $body = Get-Content "elastic_config/index_templates/$name-template.json" -Raw
    Invoke-RestMethod -Method Put -Uri "$es/_index_template/$name-template" -ContentType 'application/json' -Body $body
}
