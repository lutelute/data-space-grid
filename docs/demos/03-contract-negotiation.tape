Output docs/demos/03-contract-negotiation.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Contract Negotiation: OFFERED -> NEGOTIATING -> ACTIVE"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/integration/test_contract_negotiation.py -v --tb=short 2>&1"
Enter
Sleep 5s

Sleep 2s
