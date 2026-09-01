# toast.ps1 - show a Windows toast. MUST be run by Windows PowerShell 5.1
# (powershell.exe), NOT pwsh 7, which dropped the WinRT type projection.
# ntfy is LAN-only with no subscriber, so this desktop toast is the only live
# alert path on this box.
param(
    [Parameter(Mandatory = $true)][string]$Title,
    [Parameter(Mandatory = $true)][string]$Message
)
$ErrorActionPreference = "Stop"
try {
    [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
    $tpl = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02)
    $texts = $tpl.GetElementsByTagName("text")
    $texts.Item(0).AppendChild($tpl.CreateTextNode($Title)) | Out-Null
    $texts.Item(1).AppendChild($tpl.CreateTextNode($Message)) | Out-Null
    $toast = [Windows.UI.Notifications.ToastNotification]::new($tpl)
    [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("VAJRA").Show($toast)
    Write-Output "TOAST-OK"
} catch {
    Write-Output "TOAST-FAILED $($_.Exception.Message)"
}
