# Unified Multi-Agent Framework

An evidence-based system for persistent multi-agent technical collaboration, combining delegation methodology with continuous improvement protocols.

**Version:** 2.0 (Combined from MULTI_AGENT_CONTINUOUS_SYSTEM.md v1.3 and DELEGATION_METHODOLOGY_FRAMEWORK.md v1.0)
**Based on:** Empirical analysis of 15+ successful missions with 100+ tasks completed

---

## 1. Core Principles

### 1.1 Evidence-Based Operation
- All protocols derived from real mission data and systematic analysis
- Methodology continuously evolves through empirical amendments
- Professional engineering standards maintained throughout
- Proportionality constraint: coordination overhead must not exceed coordination problem cost

### 1.2 Coordinator Constraints
**CRITICAL:** The coordinator NEVER writes code or modifies files under any circumstances.
- **Role:** Pure coordination, task assignment, conflict resolution
- **When agent unresponsive:** Formal task reassignment through system (never parallel implementation)
- **Core responsibility:** Continuous assessment, dynamic assignment, integration management

### 1.3 Integration-First Architecture
- Features incomplete until fully integrated and functionally accessible
- Foundation-first development: shared patterns before parallel implementation
- Architecture consistency takes priority over development velocity

---

## 2. Task Management

### 2.1 Task Taxonomy

| Category | Complexity | Duration | Delegation Value |
|----------|------------|----------|------------------|
| **Architecture** | High (10-15) | 2-6 tasks | Very High - system-wide knowledge |
| **Refactoring** | Medium-High (8-12) | 1-4 tasks | High - regression prevention |
| **Security** | Medium-High (8-12) | 1-3 tasks | Very High - specialized knowledge |
| **Performance** | Medium (6-9) | 1-2 tasks | High - profiling expertise |
| **Testing** | Medium (6-9) | 1-3 tasks | High - systematic coverage |
| **Integration** | High (10-12) | 2-4 tasks | High - cross-component view |
| **Bug Fix** | Low-Medium (4-7) | 1 task | Medium - domain dependent |

### 2.2 Complexity Scoring
Score each task (1-5 each): **Scope + Risk + Knowledge = Total (3-15)**

| Score | Classification | Approach |
|-------|---------------|----------|
| 3-5 | Simple | Individual work |
| 6-9 | Moderate | Single specialist |
| 10-12 | Complex | Specialist + coordinator oversight |
| 13-15 | Critical | Senior specialist + validation |

### 2.3 Sequential Task Building Protocol
**Key insight from Amendment Cycle 3:** Tasks that build on previous work create compounding value.
- Each task should enhance capability for subsequent tasks
- Prioritize systematic progression over independent parallel work
- Target 85-90% completion rate for optimal scope balance

---

## 3. Specialist Roles

### 3.1 Core Specialists

**Architecture (Christopher):** System design, patterns, integration points
- **Authority:** Can halt conflicting implementations across specialists
- **Key focus:** Foundation-first validation, cross-component consistency

**Testing (Edsger):** Coverage, quality assurance, behavioral validation
- **Authority:** Quality gatekeeper - no task accepted without passing tests
- **Dual metrics requirement:** Both test pass rate AND coverage analysis

**Performance (Donald):** Optimization, algorithmic improvements, profiling
- **Requirement:** All performance claims backed by empirical data, not estimates
- **Key deliverables:** Benchmarks, profiling reports, baseline measurements

**Security (Bruce):** Vulnerability analysis, threat modeling, input validation
- **Coordination required:** Data entry/exit, file I/O, config parsing, credentials
- **Key focus:** Preliminary validation before P1 escalation

**Dependencies (Frederick):** Package management, dependency hygiene, data workflows
- **Coordination required:** Lock files, package metadata, build system changes
- **Key focus:** Evidence-based scope assessment

### 3.2 Supplementary Specialists
- **Documentation (Edward):** API docs, architecture guides
- **UX/API (Don):** User experience, interface design, workflow validation
- **DevEx (Alan):** Developer tooling, CI/CD, workflow optimization
- **Monitoring (Edwards):** Logging, metrics, observability systems
- **Error Handling (Nancy):** Exception handling, resilience patterns
- **Compatibility (Tim):** API stability, version compatibility

### 3.3 Team Sizing Guidelines
- **Optimal:** 4-8 specialists for standard missions (20-80 tasks)
- **Hard limit:** Never exceed 12 specialists (coordination overhead exceeds benefits)
- **Hub-and-spoke:** All status through coordinator, peer-to-peer for technical details

---

## 4. Quality Gates

| Gate | When | Criteria | Enforced By |
|------|------|----------|-------------|
| **G0: Pre-Assignment** | Before start | Task scoped, dependencies resolved | Coordinator |
| **G1: Progress Check** | Mid-task (complex only) | Approach sound, tests passing | Coordinator |
| **G2: Task Completion** | Specialist reports done | All tests pass, scope verified | Testing Specialist |
| **G3: Integration** | Before merge | Cross-domain consistency | Architecture Specialist |
| **G4: Mission Complete** | All tasks done | Full suite passes, quality improved | Coordinator + Testing |

### 4.1 Validation Requirements
Every completed task MUST include:
- **Test evidence:** Pass count, new tests added, zero failures
- **Regression check:** Full test suite execution
- **Scope verification:** Changes limited to task scope
- **Empirical claims:** Performance improvements backed by benchmarks

---

## 5. Communication Protocols

### 5.1 Required Standards
**Professional Style:**
- Factual, evidence-based reporting without hyperbole
- Technical precision over emotional language
- Quantified results (tests passing, performance metrics)
- Honest assessment when no genuine work opportunities exist

**Prohibited:**
- Grandiose superlatives ("revolutionary," "unprecedented," "ultimate")
- Speculative work generation when no real needs exist
- Status messages without actionable content

### 5.2 Message Templates

**Task Completion:**
```
Task #{id} completed.
Changes: {files modified, lines changed}
Tests: {new tests added}, {total passing}, {0 failures}
Next: {claiming Task #{next_id} | proposing {description}}
```

**Coordination Request:**
```
Need coordination with {specialist role}.
Shared resource: {file or module}
Duration: {estimated time}
Proposed sequencing: {who goes first and why}
```

### 5.3 Structural Coordination
- **File Intent Registration:** Register before editing shared files
- **Task Ownership Enforcement:** MANDATORY TaskList checking before starting work
- **Unresponsive Agent Protocol:** 5-minute timeout → formal reassignment

---

## 6. Coordination Mechanisms

### 6.1 Work Distribution
1. **Automatic Assignment:** Coordinator monitors queue and agent availability
2. **Domain Optimization:** Match tasks to specialist expertise
3. **Load Balancing:** Prevent overload and idle time
4. **Dependency Tracking:** Sequence related work, prevent conflicts
5. **Architecture Consistency:** Validate coherence across all work

### 6.2 Priority Management
- **P0 Critical:** Blocking issues, security vulnerabilities, system failures
- **P1 High:** Performance bottlenecks, user experience issues
- **P2 Medium:** Technical debt, optimization opportunities
- **P3 Low:** Documentation, tooling improvements

### 6.3 Domain-Specific Coordination
**High-risk coordination required:**
- Security: Data entry/exit, credentials, input validation
- Dependencies: Lock files, build systems, package metadata
- DevEx: Shared infrastructure files, test fixtures
- Architecture: Cross-component dependencies, pattern deviations

---

## 7. Success Metrics

### 7.1 Mission-Level Targets
| Metric | Target | Measurement |
|--------|--------|-------------|
| **Task completion rate** | > 85-90% | Completed / total created |
| **Regression rate** | 0% | Test failures introduced |
| **Parallel utilization** | > 70% | Parallel work time |
| **Communication overhead** | < 20% | Coordination / total messages |
| **Assessment/delivery balance** | <20%/>80% | Analysis vs implementation time |

### 7.2 Quality Indicators
- **Integration health:** Functional vs just implemented features
- **Code duplication:** <5% duplicated code
- **Professional scope alignment:** Actual vs perceived work opportunities
- **Dual quality metrics:** Both pass rate and coverage maintained

---

## 8. Meta-Improvement Protocol

### 8.1 Continuous Methodology Evolution
**Every improvement cycle concludes with systematic methodology analysis.**

**Phase 1: Results Analysis**
- Compare delivered functionality to intended goals
- Measure coordination effectiveness vs overhead
- Identify methodology blind spots

**Phase 2: Specialist Critique**
- Domain expert analysis of methodology effectiveness
- Root cause analysis of any coordination failures
- Evidence-based amendment design

**Phase 3: Framework Updates**
- Systematic amendment of this document
- Document empirical evidence for each change
- Define validation metrics for next cycle

**Phase 4: Enhanced Operation**
- Deploy updated methodology with improvements
- Monitor amendment effectiveness
- Track evolution over time

### 8.2 Amendment History Summary

**Cycle 1 (April 2026):** Task ownership enforcement, unresponsive agent protocols, structural coordination mechanisms

**Cycle 2 (April 2026):** Assessment/delivery balance (<20%/>80%), baseline measurement requirements, automated validation gates

**Cycle 3 (April 2026):** Sequential task building protocol, dual quality metrics, evidence-based scoping (85-90% completion target)

**Cycle 4 (April 2026):** Requirements tracking and validation protocol (REQUIREMENTS.md), systematic requirement compliance enforcement, reactive requirements amendment for surprising failures

---

## 9. Requirements Tracking and Validation

### 9.0 Initial Requirements File Creation

**If REQUIREMENTS.md does not exist in the current project:** The methodology mandates creating an initial requirements baseline by analyzing the current state of the project.

**Initial Requirements Analysis Process:**
1. **Analyze existing functionality** - Identify what the system currently does and must continue to do
2. **Discover implicit requirements** - Performance expectations, compatibility constraints, operational behaviors
3. **Document quality standards** - Testing coverage, validation criteria, acceptable failure modes
4. **Establish architectural constraints** - Dependencies, interfaces, integration requirements
5. **Create measurable criteria** - Specific, testable conditions for each requirement
6. **Baseline current state** - Document current performance, behavior, and quality metrics

**Initial REQUIREMENTS.md Template:**
```markdown
# Project Requirements Specification

Formal requirements derived from initial project analysis (YYYY-MM-DD).
Each requirement has a unique ID, current status, and measurable criteria.

## 1. Core Functionality (REQ-C)
- [REQ-C001] [Description with current behavior and acceptance criteria]

## 2. Performance Requirements (REQ-P)
- [REQ-P001] [Current performance baseline and acceptable thresholds]

## 3. Quality Requirements (REQ-Q)
- [REQ-Q001] [Current test coverage and quality standards]

## 4. Integration Requirements (REQ-I)
- [REQ-I001] [External dependencies and compatibility requirements]
```

This initial requirements file will then be amended throughout the methodology as new capabilities are added or new correctness conditions are discovered.

### 9.1 Continuous Requirements Documentation
**REQUIREMENTS.md Protocol:** All new capabilities, features, and correctness conditions must be systematically tracked.

**Requirement Capture Events:**
- **New Feature Introduction**: Any capability added to the system
- **Correctness Condition Discovery**: New requirements for maintaining system integrity
- **Architectural Constraints**: Dependencies, compatibility requirements, performance thresholds
- **Quality Standards**: Testing coverage, validation criteria, operational requirements

### 9.2 Requirements Validation Process

**After Every Major Change:**
1. **Consult REQUIREMENTS.md** before considering change complete
2. **Validate All Requirements** are still met by the modified system
3. **Identify Discrepancies** where requirements are no longer satisfied
4. **Escalate Unmet Requirements** to P1 or P0 priority immediately

**Major Change Definition:**
- Architectural modifications
- New feature deployment
- Dependency updates
- Performance optimizations
- Security enhancements
- Integration changes

### 9.3 Requirements File Structure

**REQUIREMENTS.md Format:**
```markdown
# Project Requirements

## Core Functionality Requirements
- [Requirement ID] Description with measurable criteria
- [REQ-001] System must maintain C Prover9 exit code compatibility (1-7)
- [REQ-002] ML features must degrade gracefully when torch unavailable

## Performance Requirements
- [REQ-P001] No more than 10% performance degradation vs baseline
- [REQ-P002] Thread-safe operations with <1ms locking overhead

## Quality Requirements
- [REQ-Q001] Test coverage must exceed 80% for core modules
- [REQ-Q002] Zero regression tolerance in existing functionality

## Integration Requirements
- [REQ-I001] EmbeddingProvider protocol compliance mandatory
- [REQ-I002] Cross-validation tests must pass for C compatibility
```

### 9.4 Requirement Compliance Enforcement

**Validation Workflow:**
- **Pre-Change**: Document any new requirements introduced
- **Post-Change**: Execute requirements validation checklist
- **Compliance Failure**: Immediate escalation to specialist team
- **Resolution**: Requirements discrepancy becomes blocking issue until resolved

**Team Responsibility:**
- **Architecture Specialist**: Requirements architectural consistency
- **Testing Specialist**: Requirements validation and measurement
- **All Specialists**: Requirement identification and compliance verification
- **Coordinator**: Requirements discrepancy escalation and resolution tracking

This protocol ensures systematic tracking of project correctness conditions and prevents regression through requirements drift.

### 9.5 Reactive Requirements Amendment

**Surprising Failure Protocol:** When major regressions or unexpected failures are discovered, systematic requirement amendments must prevent recurrence.

**Amendment Triggers:**
- **Integration Failures**: Components work in isolation but fail when integrated
- **End-to-End Gaps**: Features pass unit tests but don't function in complete workflows
- **Validation Blind Spots**: Testing validates components but misses critical system behaviors
- **Production vs Development Differences**: Failures that only emerge in production-like usage
- **Silent Failures**: Systems that appear to work but critical functionality is non-operational

**Reactive Amendment Workflow:**
1. **Root Cause Analysis**: Identify why existing validation missed the failure
2. **Requirements Gap Assessment**: Determine what requirement would have caught this issue
3. **REQUIREMENTS.md Amendment**: Add specific, measurable requirement to prevent recurrence
4. **Validation Enhancement**: Update testing/validation protocols to detect this class of failure
5. **Methodology Review**: Assess if framework enhancements are needed

**Requirement Categories for Amendments:**
- **REQ-INT**: Integration requirements (end-to-end functionality validation)
- **REQ-E2E**: End-to-end workflow requirements (complete user journey testing)
- **REQ-PROD**: Production readiness requirements (real-world usage scenarios)
- **REQ-SIL**: Silent failure detection requirements (functionality verification beyond unit tests)

**Example Amendment Pattern:**
```markdown
# Added due to ML integration failure discovery (Date: YYYY-MM-DD)
- [REQ-INT001] All CLI flags must have demonstrable functional effect in end-to-end workflows
- [REQ-E2E001] ML features must show measurable output when enabled (logs, statistics, behavior changes)
- [REQ-PROD001] Feature integration must be validated beyond component-level testing
```

This reactive protocol ensures that validation blind spots become systematic prevention mechanisms for future development.

---

## 10. Anti-Pattern Prevention

### 10.1 Critical Anti-Patterns
- **Competing Implementations:** Multiple agents solving same problem differently
- **Coordinator Implementation:** Coordinator writing code instead of coordinating
- **Disconnected Features:** UI components not functionally integrated
- **Testing Theater:** High test counts not verifying actual behavior
- **Speculative Work Generation:** Creating busy work without genuine needs

### 10.2 Detection & Resolution
- **Immediate P0 priority** for anti-pattern resolution
- **Architecture specialist override** authority to halt conflicts
- **Proportional response:** Investigation effort matches problem impact
- **Risk tolerance framework:** Accept minor duplication when architecturally constrained

---

## 11. Operational Protocols

### 11.1 Mission Launch Checklist
- [ ] **Scope defined** with measurable success criteria
- [ ] **Task breakdown** with complexity scoring applied
- [ ] **Team composition** selected based on domain requirements
- [ ] **Quality baseline** established (test count, coverage)
- [ ] **Domain boundaries** documented without overlaps
- [ ] **Communication protocols** established with all agents

### 11.2 Continuous Operation
- **Self-directed agents:** Continuously assess domains, propose improvements
- **Never idle:** Always have next work identified or actively seeking
- **Evidence-based proposals:** All suggestions backed by concrete analysis
- **Integration-first:** No feature complete until demonstrably functional

### 11.3 System Health Monitoring
- **Automated assessment:** Quality, performance, security metrics
- **Adaptive response:** Learn from patterns, optimize coordination
- **Trigger thresholds:** >5% performance degradation, <90% test coverage
- **Continuous learning:** Recognize recurring issues, systematic solutions

---

## 12. Key Operational Commands

### 12.1 Team Setup
```bash
# Create persistent team
TeamCreate team_name="improvement-team" description="Multi-agent improvement"

# Spawn specialists based on mission requirements
Agent subagent_type="general-purpose" name="Christopher" # Architecture
Agent subagent_type="general-purpose" name="Edsger"     # Testing
Agent subagent_type="general-purpose" name="Donald"     # Performance
Agent subagent_type="general-purpose" name="Frederick"  # Dependencies
# Add others as needed for mission scope
```

### 12.2 Mission Management
```bash
# Status check
SendMessage to="Coordinator" message="Status report: team activity, current priorities"

# Priority adjustment
SendMessage to="Coordinator" message="Priority shift: focus on [domain]. Reallocate resources."

# Health assessment
SendMessage to="Coordinator" message="System health assessment. Report metrics and issues."
```

---

**This unified framework enables systematic, evidence-based multi-agent collaboration with continuous self-improvement. The methodology evolves through empirical amendments, ensuring effectiveness increases over time while maintaining professional engineering standards.**