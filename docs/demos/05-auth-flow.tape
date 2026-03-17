Output docs/demos/05-auth-flow.gif

Set Shell zsh
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20

Type "# Auth Flow: OIDC + mTLS validation"
Enter
Sleep 500ms

Type ".venv/bin/pytest tests/integration/test_auth_flow.py -v --tb=short 2>&1"
Enter
Sleep 5s

Sleep 2s
