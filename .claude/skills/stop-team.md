---
name: stop-team
description: Gracefully shutdown the multi-agent improvement team after current cycle completion
---

# Stop Multi-Agent Team

Initiates graceful shutdown of the multi-agent team after current cycle completion.

## Usage
```
/stop-team
/stop-team [team-name]
```

## Implementation

Gracefully shutdown the multi-agent improvement team:

```python
# Get team name from args or use default
team_name = "${team_name}" if "${team_name}" else "pycypher-improvement"

print(f"🛑 Initiating graceful shutdown of team '{team_name}'")
print("⏳ Agents will complete their current work before shutting down...")
```

Send shutdown request to Hari (coordinator):

```python
SendMessage(
    to="Hari",
    message="""Please initiate graceful team shutdown after current cycle completion.

Before shutting down:
1. Complete final meta-system improvements to MULTI_AGENT_LOOP_SYSTEM.md based on session learnings
2. Generate comprehensive summary report of all improvements completed
3. Update framework documentation with discovered optimizations
4. Coordinate final integration and validation
5. Ensure all agents have opportunity to complete their current tasks

Thank you for the excellent coordination work!""",
    summary="User requested graceful team shutdown"
)

print("📈 Hari will perform final meta-improvements to MULTI_AGENT_LOOP_SYSTEM.md")
print("📋 Comprehensive summary report will be generated")
print("🔄 All agents will complete current tasks before shutdown")
print("✅ Team will auto-dissolve after final integration and validation")
print()
print("Expected shutdown sequence:")
print("1. 🛑 Shutdown signal broadcast to all agents")
print("2. ⏳ Agents finish current tasks and respond with approval")
print("3. 🔄 Final integration of all pending work")
print("4. 📈 Meta-system improvements applied to framework")
print("5. ✅ Comprehensive test suite validation")
print("6. 📊 Final summary report generated")
print("7. 💾 Framework documentation updated")
print("8. 👋 Team dissolution")
```

The team will gracefully shutdown after completing all current work and improvements.