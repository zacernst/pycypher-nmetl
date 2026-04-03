"""Adapter layer between pycypher data structures and TUI view models."""

from pycypher_tui.adapters.data_model import DataModelAdapter
from pycypher_tui.adapters.view_models import (
    ColumnMappingViewModel,
    EntityDetailViewModel,
    EntitySourceViewModel,
    EntityViewModel,
    ModelStatsViewModel,
    PropertyViewModel,
    RelationshipDetailViewModel,
    RelationshipSourceViewModel,
    RelationshipViewModel,
    SourceMappingViewModel,
    ValidationIssue,
)

__all__ = [
    "DataModelAdapter",
    "ColumnMappingViewModel",
    "EntityDetailViewModel",
    "EntitySourceViewModel",
    "EntityViewModel",
    "ModelStatsViewModel",
    "PropertyViewModel",
    "RelationshipDetailViewModel",
    "RelationshipSourceViewModel",
    "RelationshipViewModel",
    "SourceMappingViewModel",
    "ValidationIssue",
]
