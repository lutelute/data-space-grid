Output docs/demos/08-anonymizer.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Privacy: Data anonymization & purpose-based disclosure"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/unit/test_anonymizer.py -v --tb=short 2>&1"
Enter
Sleep 5s

Sleep 2s
