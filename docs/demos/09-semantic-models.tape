Output docs/demos/09-semantic-models.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Semantic Models: CIM / IEC 61850 / OpenADR / Consumer"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/unit/test_semantic_models.py -v --tb=short 2>&1 | tail -40"
Enter
Sleep 5s

Sleep 2s
