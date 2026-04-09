$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = (Get-Command python -ErrorAction Stop).Source
$TaskName = "NSE NIFTY500 Daily Update"
$RunTimes = @("18:00", "20:00", "22:00")

$Action = New-ScheduledTaskAction `
    -Execute $Python `
    -Argument "daily_update.py" `
    -WorkingDirectory $ProjectDir

$Triggers = @()
foreach ($RunTime in $RunTimes) {
    $Triggers += New-ScheduledTaskTrigger -Daily -At $RunTime
}

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Triggers `
    -Settings $Settings `
    -Description "Downloads daily NSE NIFTY 500 OHLCV, updates database, recalculates indicators, refreshes portfolio, exports dashboard data. Retries at 6 PM, 8 PM, and 10 PM." `
    -Force

Write-Host "Scheduled task '$TaskName' registered to run daily at 18:00, 20:00, and 22:00."
