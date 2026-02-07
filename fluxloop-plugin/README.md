# FluxLoop Claude Code Plugin

A Claude Code plugin for automated AI agent testing and evaluation.

## 🚀 Installation

### Step 1: Add Marketplace

```
/plugin marketplace add Fluxloop-AI/fluxloop-claude-plugin
```

### Step 2: Install Plugin

```
/plugin install fluxloop@fluxloop
```

That's it! Now just talk to Claude naturally.

---

## ⭐ The Core: Agent Test Skill

**The skill is everything.** Just speak naturally and FluxLoop handles the rest.

### How to Use

Simply ask Claude:

```
"test my agent"
"generate test data for my chatbot"
"run a simulation"
"evaluate my agent's responses"
"set up fluxloop for this project"
```

The **`fluxloop-agent-test`** skill automatically:
- ✅ Checks your current setup state
- ✅ Guides you through installation if needed
- ✅ Generates test data and personas
- ✅ Runs simulations against your agent
- ✅ Uploads results to FluxLoop Web
- ✅ Triggers evaluations and surfaces insights/recommendations (even for partial runs)
- ✅ Shows you what to fix

### Skill Workflow

```
You: "test my agent"
      ↓
[Skill activates automatically]
      ↓
1. Check context (fluxloop context show)
2. Summarize current state to you
3. Present options (no auto-execution)
4. Execute after your confirmation
5. Show results and next steps
```

**No commands to memorize. No manual steps. Just ask.**

### Auto-Activation Keywords

The skill activates when you mention:
- `test`, `testing`, `test my agent`
- `simulation`, `run simulation`
- `generate test data`, `synthesize inputs`
- `evaluate`, `agent evaluation`
- `scenario`, `create scenario`

---

## 📋 Requirements

- **FluxLoop Account**: <a href="https://alpha.app.fluxloop.ai" target="_blank">alpha.app.fluxloop.ai</a>
- **Python 3.8+**

> 💡 **The skill installs FluxLoop CLI for you!** Just say "set up fluxloop" and everything gets configured automatically.

---

## 🔧 Example Conversation

```
You: "I want to test my customer support agent"

Claude: I'll help you test your agent with FluxLoop.

Let me check your current setup...

📊 Current State:
- FluxLoop CLI: ✓ Installed
- Authentication: ✓ Logged in  
- Project: customer-support (proj_abc123)
- Scenario: support-bot
- Bundle: v3 (50 inputs, 2 days ago)

Would you like to:
1. Run tests with existing bundle (v3)
2. Generate new test data
3. Create a new scenario

You: "1"

Claude: Running tests with bundle v3...
[Executes: fluxloop sync pull + fluxloop test]

✓ Test completed! 47/50 passed (94%)
⚠️ 3 warnings detected

View detailed results: https://alpha.app.fluxloop.ai/...

Would you like me to analyze the failures?
```

---

## 🧪 Server-Side Evaluation (Optional)

Trigger evaluations for an experiment and optionally wait for completion:

```bash
fluxloop evaluate --experiment-id <experiment_id>
fluxloop evaluate --experiment-id <experiment_id> --wait
fluxloop evaluate --experiment-id <experiment_id> --wait --timeout 900 --poll-interval 5
```

- `--wait` polls until the job is `completed` or `partial` and prints the latest insight/recommendation headlines.
- Insights/recommendations are generated when at least one run completes, even if the job ends as `partial`.
- If a job stays `queued` for >30s without a worker lock, you'll see a warning about worker/backlog.

---

## 📁 Slash Commands (Manual Alternative)

For when you need direct control:

| Command | Description |
|---------|-------------|
| `/fluxloop:setup` | First-time setup guide |
| `/fluxloop:test` | Run tests |
| `/fluxloop:smoke` | Quick smoke test |
| `/fluxloop:pull` | Pull test data from Web |
| `/fluxloop:evaluate` | Trigger server-side evaluation (supports `--wait`) |
| `/fluxloop:improve` | Analyze results → fix agent → re-test → re-evaluate |
| `/fluxloop:status` | Check current status |

> 💡 **Recommendation**: Use natural language with the skill instead. It's smarter and handles edge cases automatically.

---

## 🪝 Hooks (Optional)

Auto-run smoke tests after file edits:

```json
{
  "hooks": [
    {
      "type": "PostToolUse",
      "matcher": "Write|Edit",
      "command": "fluxloop test --smoke --quiet"
    }
  ]
}
```

## 📁 Project Structure

```
your-project/
├── .fluxloop/
│   ├── project.json          # Project connection info
│   ├── context.json          # Current scenario pointer
│   ├── .env                  # API key
│   └── scenarios/
│       └── my-test/
│           ├── agents/       # Agent wrappers
│           ├── configs/      # Configuration files
│           ├── contracts/    # Scenario contracts (YAML)
│           ├── inputs/       # Test inputs
│           └── experiments/  # Test results
└── fluxloop.yaml             # Project settings
```

## 🔗 Links

- **FluxLoop Web**: <a href="https://alpha.app.fluxloop.ai" target="_blank">alpha.app.fluxloop.ai</a>
- **Documentation**: <a href="https://docs.fluxloop.ai" target="_blank">docs.fluxloop.ai</a>

## 📄 License

MIT License
