Architecture Decision Records
=============================

This directory records significant architectural decisions made in PyCypher.
Each ADR describes the context, decision, alternatives considered, and
consequences.

**Format:** ADRs follow the `Michael Nygard template
<https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions>`_
— Context, Decision, Alternatives, Consequences.

**Status values:** ``Accepted`` (active), ``Superseded`` (replaced by another
ADR), ``Deprecated`` (no longer relevant).

.. toctree::
   :maxdepth: 1
   :caption: Decisions:

   001-binding-frame-ir
   002-lark-earley-parser
   003-pydantic-ast-models
   004-shadow-write-atomicity
   005-composable-evaluators
   006-pluggable-backends
   007-graph-native-indexes
   008-monorepo-workspace
   009-star-coordinator
   010-vectorized-evaluation
   011-grammar-mixin-package
   012-auto-test-markers
