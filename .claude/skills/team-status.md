---
name: team-status
description: Check current status and progress of the multi-agent improvement team
---

# Team Status

Displays current status of all team members and recent progress.

## Usage
```
/team-status
/team-status [team-name]
```

## Implementation

Check the multi-agent team status and progress:

```python
import os

# Get team name from args or use default
team_name = "${team_name}" if "${team_name}" else "pycypher-improvement"

print(f"📊 Team Status for '{team_name}'")
print("=" * 50)
```

Show team configuration:

```python
try:
    # Read team configuration
    config_path = f"~/.claude/teams/{team_name}/config.json"
    expanded_path = os.path.expanduser(config_path)

    if os.path.exists(expanded_path):
        Read(file_path=expanded_path)
        print("\n👥 Team Members:")
        print("   🧠 Hari (Coordinator) - Psychohistory management expert")
        print("   🏗️  Frank (Architect) - Dune systems design master")
        print("   🧪 Data (Testing) - TNG systematic testing specialist")
        print("   🔧 Andy (Refactor) - The Martian precision engineer")
        print("   📚 Ursula (Documentation) - Le Guin communication expert")
        print("   ⚡ Takeshi (Performance) - Altered Carbon efficiency optimizer")
        print("   🛡️  Starbuck (Security) - BSG paranoid security auditor")
        print("   ✨ Luke (UX) - Star Wars elegance and simplicity advocate")
        print("   👁️  Philip (Observability) - PKD surveillance monitoring specialist")
        print("   🔗 Benjamin (Dependencies) - DS9 complex relationship manager")
        print("   ⚠️  Malcolm (Error Handling) - Firefly disaster preparedness expert")
        print("   🔄 Doctor (Compatibility) - Doctor Who consistency maintainer")
    else:
        print(f"❌ Team '{team_name}' not found. Use /start-team to create it.")
        return

except Exception as e:
    print(f"⚠️  Could not read team config: {e}")
```

Show current task status:

```python
print(f"\n📋 Current Task Status:")
print("-" * 30)

try:
    TaskList()
except Exception as e:
    print(f"⚠️  Could not read task list: {e}")
    print("💡 Tasks may not be initialized yet, or team may be idle")
```

Request status update from Hari:

```python
print(f"\n📡 Requesting live status update from team...")

try:
    SendMessage(
        to="Hari",
        message="""Please provide a brief status update for the team including:

1. Current cycle number and progress
2. Active agents and what they're working on
3. Recent completions and discoveries
4. Any interesting technical debates happening between agents
5. Overall team efficiency metrics
6. Any bottlenecks or issues that need attention

Keep it concise but entertaining - include the fun personality interactions!""",
        summary="Requesting team status update"
    )

    print("✅ Status update request sent to Hari")
    print("⏳ Hari will respond with current team status and progress")

except Exception as e:
    print(f"⚠️  Could not request status update: {e}")
    print("💡 Team may not be active or Hari may be busy")

print(f"\n🔄 Continuous Operation Status:")
print("   • Each agent runs their own improvement cycle")
print("   • Auto-survey → Report → Execute → Validate → Repeat")
print("   • Hari coordinates and performs meta-system improvements")
print("   • Team will continue until diminishing returns or manual shutdown")

print(f"\n📊 Monitoring Tips:")
print("   • Watch for agent idle notifications (normal between cycles)")
print("   • Look for peer collaboration messages and technical debates")
print("   • Check for conflict resolution and integration updates")
print("   • Monitor for meta-system improvements to MULTI_AGENT_LOOP_SYSTEM.md")

print(f"\n🛠️  Team Management:")
print("   • Use /stop-team for graceful shutdown")
print("   • Agents will auto-terminate when no more improvements found")
print("   • Hari will provide final summary and meta-improvements")
```

Team status check complete!