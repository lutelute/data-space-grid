Output docs/demos/06-audit-trail.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Audit Trail: Hash verification & completeness"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/integration/test_audit_trail.py -v --tb=short 2>&1"
Enter
Sleep 5s

Sleep 2s
