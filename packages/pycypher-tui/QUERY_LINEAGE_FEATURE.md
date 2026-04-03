# Query Lineage & Data Flow Visualization

## Overview

The Query Lineage screen provides an interactive visualization of your ETL pipeline's data flow, showing how data moves from sources through transformations to outputs.

## Features

### Pipeline Component Visualization
- **Sources**: Entity and relationship data sources
- **Queries**: Cypher transformations
- **Outputs**: Result sinks and destinations

### Interactive Navigation
- **VIM-style navigation**: j/k to move between components, Enter to view details
- **Component filtering**: Tab key to cycle through component types (all/source/query/output)
- **Search**: `/pattern` to search component names and descriptions
- **Dependency drill-down**: View upstream and downstream dependencies

### ASCII Flow Diagram
Displays the complete pipeline flow with:
- Data sources (📊)
- Query transformations (🔄)
- Output destinations (💾)
- Connection arrows showing data flow

### Comprehensive Analysis
- **Critical Path**: Identifies the longest dependency chain
- **Orphaned Components**: Finds components with no dependencies or dependents
- **Pipeline Statistics**: Component counts and complexity metrics

## Navigation

From the main Pipeline Overview screen:
1. Navigate to "Query Lineage & Data Flow" section
2. Press Enter to open the lineage visualization
3. Use j/k to navigate between components
4. Press Tab to filter by component type
5. Press Enter on any component to view detailed information

## Detail Tabs

Each component provides four information tabs:

### Overview
- Component type and basic information
- Dependency counts
- Metadata (URIs, descriptions, etc.)

### Dependencies
- Interactive table of input dependencies
- Interactive table of output dependents
- Click to navigate to related components

### Flow Diagram
- ASCII visualization of complete pipeline
- Shows component relationships and data flow
- Highlights current component's position

### Analysis
- Critical path analysis
- Orphaned component detection
- Pipeline complexity statistics
- Performance insights

## Key Bindings

| Key | Action |
|-----|--------|
| j/k | Navigate between components |
| Enter/l | View component details |
| Tab | Cycle component type filter |
| h/Escape | Back to pipeline overview |
| / | Search components |
| n/N | Next/previous search match |
| gg/G | Jump to first/last component |

## Integration

The Query Lineage screen is fully integrated with the TUI architecture:
- Consistent VIM navigation patterns
- Async loading for large pipelines
- Search and filtering capabilities
- Breadcrumb navigation
- Status indicators and validation

## Technical Implementation

- **Dependency Analysis**: Automatically infers dependencies between components
- **ASCII Rendering**: Clean text-based flow diagrams
- **Async Processing**: Non-blocking pipeline analysis
- **Memory Efficient**: Streams large datasets without loading everything into memory
- **Extensible**: Framework supports future enhancements like query optimization suggestions