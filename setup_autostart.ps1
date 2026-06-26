$WshShell = New-Object -comObject WScript.Shell
$StartupFolder = [Environment]::GetFolderPath('Startup')
$ShortcutPath = Join-Path -Path $StartupFolder -ChildPath 'Gidi_Gateway_Supervisor.lnk'

$Shortcut = $WshShell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = 'c:\xampp74\htdocs\gatewaypython\START.bat'
$Shortcut.WorkingDirectory = 'c:\xampp74\htdocs\gatewaypython'
$Shortcut.Description = 'Auto-start GIDI Gateway Python Consumers'
$Shortcut.Save()

Write-Host "Shortcut created successfully at $ShortcutPath"
