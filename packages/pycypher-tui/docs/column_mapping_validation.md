# Visual Column Mapping Validation for Relationships

This document describes the visual column mapping validation feature implemented for relationship sources in the PyCypher TUI.

## Overview

The visual column mapping validation provides an interactive ASCII-based interface for viewing and editing relationship column mappings, with real-time validation and configuration persistence.

## Features

### 1. Visual Mapping Display
- ASCII-based visualization showing source and target column mappings
- Color-coded validation status:
  - **Green (valid)**: Mapping is correct and type-compatible
  - **Yellow (warning)**: Minor issues like type mismatches or same column mapped
  - **Red (error)**: Critical issues like missing columns

### 2. Real-Time Validation
- Column existence validation
- Type compatibility checking
- Duplicate mapping detection
- Comprehensive error reporting

### 3. Interactive Editing
- VIM-style navigation (j/k for up/down)
- Enter to edit mappings
- Tab to switch between source/target column selectors
- Escape to cancel editing
- Automatic configuration persistence

### 4. Integration with Existing UI
- Seamlessly integrated into the data model screen's Validation tab
- Works with Alan's existing tabbed interface architecture
- Uses established async loading patterns from Don's implementation

## Usage

### Navigation
- **j/k or ↑/↓**: Navigate between column mappings
- **Enter**: Edit the selected mapping
- **Tab**: Switch between source and target column selectors (in edit mode)
- **Enter**: Save changes (in edit mode)
- **Escape**: Cancel editing (in edit mode)

### Validation Status Indicators
```
person_id (integer) ━━━━━> friend_id (integer)    [VALID]
employee_id (varchar) ━━━━━✗ company_id (integer) [TYPE MISMATCH]
missing_col (unknown) ━━━━━✗ target_col (varchar) [COLUMN NOT FOUND]
```

### Interactive Editing
When editing mode is activated:
```
Edit Column Mapping (Tab to switch, Enter to save, Esc to cancel):
Source Column: [Dropdown with available columns]
Target Column: [Dropdown with available columns]
```

## Implementation Architecture

### Components

#### 1. ColumnMappingWidget
- **Location**: `pycypher_tui/widgets/column_mapping.py`
- **Purpose**: Main widget providing visualization and interaction
- **Key Methods**:
  - `update_relationship_sources()`: Load and validate relationship mappings
  - `_validate_column_mappings()`: Perform async validation
  - `_enter_edit_mode()`: Enable interactive editing
  - `on_key()`: Handle VIM-style navigation

#### 2. Data Classes
- **ColumnMapping**: Represents a single source→target column mapping
- **MappingValidationResult**: Contains validation results for all mappings

#### 3. ModelDetailPanel Integration
- **Location**: `pycypher_tui/screens/data_model.py`
- **Enhancement**: Extended validation tab for relationship nodes
- **Key Methods**:
  - `_update_relationship_validation_tab()`: Mount column mapping widget
  - `on_column_mapping_widget_mapping_changed()`: Handle configuration updates

### Validation Logic

#### Column Validation
1. **Existence Check**: Verify source and target columns exist in the data
2. **Type Compatibility**: Check for compatible data types
3. **Duplication Check**: Ensure source ≠ target columns
4. **Schema Consistency**: Validate against actual data source schema

#### Type Compatibility Rules
- **Exact Match**: Same types are always compatible
- **Numeric Types**: int, integer, bigint, float, double, decimal, number
- **String Types**: string, varchar, text, char
- **Cross-type**: Different type families are incompatible

### Configuration Persistence

Changes made through the interactive editor are automatically persisted to the pipeline configuration:

```python
# Configuration update flow
user_edits_mapping()
→ MappingChanged message
→ on_column_mapping_widget_mapping_changed()
→ config_manager.update_relationship_source()
→ configuration saved
```

## Testing

### Unit Tests
- **Location**: `packages/pycypher-tui/tests/test_column_mapping_widget.py`
- **Coverage**: Data classes, validation logic, type compatibility, navigation
- **Mocking**: Uses mock DataSourceIntrospector for isolated testing

### Integration Tests
- **Location**: `packages/pycypher-tui/tests/test_data_model_column_mapping_integration.py`
- **Coverage**: UI integration, configuration updates, message handling
- **Scope**: End-to-end functionality within the data model screen

## Future Enhancements

### Potential Improvements
1. **Multi-source Editing**: Support for editing multiple relationship sources simultaneously
2. **Bulk Operations**: Copy mappings between similar relationship types
3. **Schema Suggestions**: Intelligent column mapping suggestions based on naming patterns
4. **Visual Graph View**: ASCII graph showing entity-relationship connections
5. **Mapping Templates**: Save and reuse common mapping patterns
6. **Advanced Validation**: Cross-reference with entity sources for referential integrity

### Performance Optimizations
1. **Lazy Loading**: Load schema information only when needed
2. **Caching**: Cache validation results for repeated access
3. **Incremental Updates**: Only re-validate changed mappings
4. **Async Batching**: Batch multiple validation operations

## Technical Notes

### Dependencies
- **Textual**: UI framework for terminal-based interface
- **DataSourceIntrospector**: Schema analysis and column statistics
- **ConfigManager**: Configuration persistence and management
- **Async/Await**: Non-blocking validation operations

### Design Patterns
- **Observer Pattern**: MappingChanged messages for loose coupling
- **Strategy Pattern**: Pluggable validation rules
- **Template Method**: Consistent async loading patterns
- **Composite Pattern**: Widget composition within tabbed interface

### Error Handling
- **Graceful Degradation**: Show error states when data sources unavailable
- **User Feedback**: Clear error messages with actionable suggestions
- **Logging**: Comprehensive logging for debugging and monitoring
- **Validation Recovery**: Automatic re-validation after configuration changes

## Example Usage Scenarios

### Scenario 1: New Relationship Mapping
1. User navigates to relationship type in data model screen
2. Clicks on Validation tab
3. Sees column mapping visualization with validation status
4. Identifies type mismatch between source and target columns
5. Presses Enter to edit mapping
6. Selects compatible columns from dropdowns
7. Presses Enter to save changes
8. Configuration automatically updated and validation re-runs

### Scenario 2: Schema Migration
1. Data source schema changes (column renamed)
2. User opens data model screen and sees validation errors
3. Column mapping shows "column not found" errors in red
4. User edits mappings to use new column names
5. Validation status updates to green (valid)
6. Pipeline configuration reflects new schema

### Scenario 3: Bulk Validation Review
1. User reviews all relationship types for data quality
2. Navigates through relationship types using VIM keys
3. Quickly identifies problematic mappings by color coding
4. Fixes issues using interactive editor
5. Validates entire data model for consistency

This implementation provides a robust foundation for visual column mapping validation while maintaining consistency with the existing TUI architecture and user experience patterns.