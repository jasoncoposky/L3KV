Write-Host "Killing existing l3svc processes..."
Stop-Process -Name l3svc -ErrorAction SilentlyContinue

Start-Sleep -Seconds 1

Write-Host "Starting Node 1 (8080/9090)..."
Start-Process -FilePath "Release\l3svc.exe" -ArgumentList "config_node1.json" -RedirectStandardOutput "node1_out.log" -RedirectStandardError "node1_err.log" -WindowStyle Hidden

Write-Host "Starting Node 2 (8081/9091)..."
Start-Process -FilePath "Release\l3svc.exe" -ArgumentList "config_node2.json" -RedirectStandardOutput "node2_out.log" -RedirectStandardError "node2_err.log" -WindowStyle Hidden

Write-Host "Starting Node 3 (8082/9092)..."
Start-Process -FilePath "Release\l3svc.exe" -ArgumentList "config_node3.json" -RedirectStandardOutput "node3_out.log" -RedirectStandardError "node3_err.log" -WindowStyle Hidden

Write-Host "Cluster started."
