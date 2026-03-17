Output docs/demos/10-policy-engine.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Policy Engine: Purpose constraints, sensitivity tiers, emergency override"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/unit/test_policy.py -v --tb=short 2>&1"
Enter
Sleep 5s

Sleep 2s
