Output docs/demos/04-congestion-management.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# E2E: Distribution Congestion Management Flow"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/integration/test_congestion_management.py -v --tb=short 2>&1"
Enter
Sleep 5s

Sleep 2s
