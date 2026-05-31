"""TUI screens for pipeline configuration."""

from pycypher_tui.screens.base import (
    BaseDetailPanel,
    BaseListItem,
    VimNavigableScreen,
)
from pycypher_tui.screens.data_model import DataModelScreen
from pycypher_tui.screens.data_sources import (
    DataSourcesScreen,
    SourceDetailPanel,
    SourceItem,
    SourceListItem,
)
from pycypher_tui.screens.entity_browser import EntityBrowserScreen
from pycypher_tui.screens.entity_editor import EntityEditorScreen
from pycypher_tui.screens.entity_tables import (
    ColumnMapping,
    EntityTableInfo,
    EntityTablesScreen,
)
from pycypher_tui.screens.pipeline_overview import (
    PipelineOverviewScreen,
    SectionDetailPanel,
    SectionInfo,
    SectionWidget,
)
from pycypher_tui.screens.pipeline_testing import (
    DiagnosticEntry,
    ExecutionPlan,
    ExecutionStep,
    PipelineTestingScreen,
    StepDetailPanel,
    StepListItem,
    StepStatus,
    build_execution_plan,
    run_dry_execution,
)
from pycypher_tui.screens.query_editor import (
    QueryEditorScreen,
    QueryResult,
)
from pycypher_tui.screens.query_lineage import QueryLineageScreen
from pycypher_tui.screens.relationship_browser import (
    RelationshipBrowserScreen,
)
from pycypher_tui.screens.relationship_editor import (
    RelationshipEditorScreen,
)
from pycypher_tui.screens.relationships import (
    RelationshipItem,
    RelationshipScreen,
)
from pycypher_tui.screens.source_mapper import DataSourceMapperScreen
from pycypher_tui.screens.template_browser import (
    TemplateBrowserScreen,
    TemplateSummary,
    summarise_template,
)

# FodCatalogScreen lives in the fastopendata package; import defensively
# so pycypher-tui works without the optional fastopendata dependency.
try:
    from fastopendata.tui.fod_catalog import FodCatalogScreen
except ImportError:  # pragma: no cover - optional dep
    FodCatalogScreen = None  # type: ignore[assignment, misc]

__all__ = [
    "BaseDetailPanel",
    "BaseListItem",
    "ColumnMapping",
    "DataModelScreen",
    "DataSourceMapperScreen",
    "DataSourcesScreen",
    "DiagnosticEntry",
    "EntityBrowserScreen",
    "EntityEditorScreen",
    "EntityTableInfo",
    "EntityTablesScreen",
    "ExecutionPlan",
    "ExecutionStep",
    "FodCatalogScreen",
    "PipelineOverviewScreen",
    "PipelineTestingScreen",
    "QueryEditorScreen",
    "QueryLineageScreen",
    "QueryResult",
    "RelationshipBrowserScreen",
    "RelationshipEditorScreen",
    "RelationshipItem",
    "RelationshipScreen",
    "SectionDetailPanel",
    "SectionInfo",
    "SectionWidget",
    "SourceDetailPanel",
    "SourceItem",
    "SourceListItem",
    "StepDetailPanel",
    "StepListItem",
    "StepStatus",
    "TemplateBrowserScreen",
    "TemplateSummary",
    "VimNavigableScreen",
    "build_execution_plan",
    "run_dry_execution",
    "summarise_template",
]
