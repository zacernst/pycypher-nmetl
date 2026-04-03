# Entity/Relationship Attribute Inspector Guide

The enhanced Data Model screen provides comprehensive attribute inspection for entity types and relationship types through a tabbed interface in the detail panel.

## Overview

When you select an entity or relationship type in the Data Model screen, the detail panel displays a tabbed interface with comprehensive attribute information:

- **Overview**: Basic node information and connections
- **Attributes**: Schema details with column types and row counts
- **Validation**: Mapping validation results and issues
- **Statistics**: Column statistics including null counts and unique values
- **Lineage**: Data flow and lineage information

## Navigation

### VIM Keybindings

- `j/k` - Navigate between entity/relationship types
- `Tab` - Switch between attribute tabs in detail panel
- `h/Escape` - Return to pipeline overview
- `Enter/l` - Drill down to source details
- `/` - Search entity/relationship types

### Tab Navigation

The attribute inspector uses a tabbed interface that can be navigated with:
- `Tab` - Next tab
- `Shift+Tab` - Previous tab
- Mouse clicks on tab headers

## Tab Details

### Overview Tab

Displays basic information about the selected entity or relationship type:

- **Type**: Entity or relationship designation
- **Source Count**: Number of data sources feeding this type
- **Source IDs**: List of backing data source identifiers
- **Connections**: Graph relationships (for relationship types)

### Attributes Tab

Shows detailed schema information for all sources:

- **Column Names and Types**: Arrow/Parquet type information
- **Row Counts**: Total rows per source
- **Tabular Display**: Scrollable table of all attributes

Example display:
```
Source: customers_csv
Rows: 1,000
Columns: 4

Column          Type
id              int64
name            string
email           string
created_at      timestamp
```

### Validation Tab

Displays mapping validation results with status indicators:

- **Pass** (Green): No issues found
- **Warning** (Yellow): Potential issues that don't break functionality
- **Error** (Red): Critical issues that will cause failures

Common validation checks:
- **Entity Types**:
  - ID column presence (warning if missing)
  - Attribute completeness
- **Relationship Types**:
  - Source column existence (error if missing)
  - Target column existence (error if missing)
  - Column type compatibility

Example validation output:
```
Source: customers_db
Status: WARNING

Issues:
  WARNING: Column name mismatch: 'customer_id' vs 'id'
```

### Statistics Tab

Provides detailed column statistics for data quality assessment:

- **Data Type**: Arrow type information
- **Null Count**: Number of missing values
- **Unique Count**: Number of distinct values
- **Min/Max Values**: Range information (for ordered types)

Example statistics:
```
id:
  Type: int64
  Null Count: 0
  Unique Count: 1,000
  Min: 1
  Max: 1,000

name:
  Type: string
  Null Count: 5
  Unique Count: 995
  Min: Alice
  Max: Zoe
```

### Lineage Tab

Shows data flow and lineage information:

- **Data Sources**: Source URIs and types
- **Graph Connections**: How this type connects to others
- **Flow Direction**: Input/output relationships

*Note: Advanced lineage tracking is planned for future releases*

## Async Loading

The attribute inspector loads data asynchronously to maintain UI responsiveness:

1. **Immediate Display**: Overview tab updates immediately
2. **Loading Indicators**: Attribute tabs show loading spinners
3. **Progressive Updates**: Tabs populate as data becomes available
4. **Error Handling**: Failed loads show user-friendly error messages

## Data Sources Integration

The inspector integrates with the `DataSourceIntrospector` to provide:

- **Schema Detection**: Automatic column type inference
- **Efficient Sampling**: Statistics computed on samples for large datasets
- **Multiple Formats**: Support for CSV, Parquet, JSON, and SQL sources
- **Caching**: Results cached for improved performance

## Performance Considerations

- **Lazy Loading**: Attribute data only loaded when nodes are selected
- **Sampling**: Statistics computed on data samples to avoid full scans
- **Caching**: Schema and statistics cached per session
- **Cancellation**: Previous loads cancelled when switching nodes

## Error Handling

The inspector gracefully handles various error conditions:

- **Source Unavailable**: Shows clear error messages
- **Schema Issues**: Validation tab highlights problems
- **Network Errors**: Timeouts and connection failures handled
- **Format Errors**: Unsupported file formats reported clearly

## Use Cases

### Data Quality Assessment

1. Navigate to entity/relationship type
2. Check **Statistics** tab for null counts and unique values
3. Review **Validation** tab for mapping issues
4. Use **Attributes** tab to verify expected columns

### Schema Validation

1. Select relationship type
2. Check **Validation** tab for source/target column issues
3. Review **Attributes** tab for column type mismatches
4. Verify mapping completeness

### Pipeline Debugging

1. Navigate to failing entity/relationship
2. Check **Validation** tab for configuration errors
3. Review **Statistics** tab for data quality issues
4. Use **Overview** tab to understand connections

## Integration with Other Screens

The attribute inspector integrates seamlessly with other TUI screens:

- **Data Sources Screen**: Drill down from model types to source details
- **Pipeline Overview**: Return via breadcrumb navigation
- **Entity/Relationship Screens**: Direct navigation to source management

## Future Enhancements

Planned improvements include:

- **Real-time Validation**: Live validation as sources change
- **Advanced Lineage**: Complete data flow visualization
- **Query Preview**: Sample queries generated from attributes
- **Export Capabilities**: Schema export to various formats
- **Batch Validation**: Validate all types simultaneously