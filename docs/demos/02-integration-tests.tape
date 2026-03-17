Output docs/demos/02-integration-tests.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Data Space Grid - Integration Tests (96 tests)"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/integration/ -v --tb=short 2>&1 | tail -40"
Enter
Sleep 5s

Sleep 2s
