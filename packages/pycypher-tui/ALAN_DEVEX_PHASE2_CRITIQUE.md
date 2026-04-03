# Alan - DevEx Specialist Phase 2 Coordination Methodology Critique

## Core Thesis: Structural Enforcement, Not Social Coordination

The Task #15 failure happened because coordination relied on messages (social) rather than tooling (structural). Don went silent, Vannevar stepped in, both edited base.py independently. No mechanism prevented this.

## 1. Developer Experience Impact Analysis

The competing implementations created a hidden DevEx hazard: two different fix approaches to the same test failures means future developers debugging test regressions won't know which fix is load-bearing. If Don fixed escape handling one way and Vannevar fixed it another, a developer reading base.py sees merged logic without understanding which parts came from which solution. This makes the codebase harder to reason about.

## 2. Tooling Coordination Assessment

The core tooling gap is that all agents share one working tree with no structural isolation. In a real dev team, competing PRs are visible in git - you see two branches touching the same files and a reviewer catches the conflict. Here, edits happen in-place with no branch isolation, so "competing work" is invisible until someone runs tests and gets confused by unexpected state. For build systems and CI, this means you can't even detect the problem - there's no diff to compare.

## 3. DevEx Prevention Mechanisms (Lightweight)

Three structural fixes that would prevent 90% of coordination failures:

1. **File intent registration** - before editing a shared file, agents register intent in the task system (e.g. "editing base.py for Task #15"). Other agents see the warning before touching the same file. This is cheap and prevents most conflicts.
2. **Branch-per-agent isolation** - git worktrees so competing edits are visible as separate branches, not invisible in-place overwrites.
3. **Claim timeout protocol** - no status response within N minutes auto-expires the task claim, making reassignment explicit rather than ambiguous.

Additional mechanisms:
- **Test ownership tags**: when fixing specific test failures, claim the specific test names in the task description. Two agents claiming the same test name triggers an alert.
- **Post-edit verification**: after completing work, run the full test suite AND diff against the last known-good state to detect unexpected changes from other agents.

## 4. Mandatory vs Independent Coordination

**Mandatory coordination required for:**
- Shared infrastructure files (base.py, app.py, conftest.py)
- Test fixture changes
- Build config changes (pyproject.toml)
- Any file over 200 LOC that multiple features depend on

**Independent work acceptable for:**
- New test files
- New screen implementations
- Documentation
- Isolated widget additions that don't modify existing code

## 5. Development Risk Framework

Simple rule: if your change modifies an existing file that another agent has touched in this cycle, coordination is mandatory. If your change only adds new files or modifies files no one else has touched, proceed independently.

Risk scales with file centrality:
- **High-risk**: base.py, app.py, conftest.py (everything depends on them)
- **Medium-risk**: existing screen files, mode files
- **Zero-risk**: new test files, new documentation, new isolated modules

## Bottom Line

The methodology needs structural enforcement (branching, file locking, claim timeouts) not just social coordination. Messages get lost, agents go silent, and good intentions don't prevent merge conflicts. The foundation-first architecture (VimNavigableScreen) saved us this time because it constrained both solutions to be compatible, but that was lucky, not methodologically sound.
