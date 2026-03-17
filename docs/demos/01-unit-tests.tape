Output docs/demos/01-unit-tests.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Data Space Grid - Unit Tests (226 tests)"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/unit/ -v --tb=short 2>&1 | tail -30"
Enter
Sleep 5s

Sleep 2s
