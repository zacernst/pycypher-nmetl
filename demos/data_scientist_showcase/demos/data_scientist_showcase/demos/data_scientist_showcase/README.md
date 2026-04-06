# Data Scientist Demonstration Scripts

A comprehensive 6-script progressive demonstration series showcasing the full capabilities of the **pycypher-nmetl** system for highly intelligent data scientists and engineers who work with large, messy, inconsistent datasets but are not familiar with Cypher or graph database paradigms.

## Overview

This demonstration series progressively shows why pycypher-nmetl is valuable for complex analytical work by addressing data scientists' core concerns:

1. **Immediate Value** - Working queries in 5 minutes
2. **Intelligent Scaling** - Automatic backend optimization
3. **Real-World Data** - Handling messy, inconsistent datasets
4. **Cross-Source Integration** - Data fusion without ETL complexity
5. **Production Ready** - Enterprise-grade capabilities
6. **Advanced Analytics** - Sophisticated analysis impossible with traditional tools

## Target Audience

- **Data Scientists & Engineers** working with complex analytical problems
- **Technical Decision Makers** evaluating graph database solutions
- **Analysts** struggling with messy, multi-source datasets
- **Engineers** needing production-ready analytical capabilities

**Prior Knowledge:** Familiarity with pandas DataFrames and SQL concepts helpful but not required.

## Prerequisites

- Python 3.8+ with uv package manager
- pycypher-nmetl system installed
- 5-30 minutes per script depending on complexity

## Execution Guide

Run each script from this directory (`demos/data_scientist_showcase/`):

```bash
# Quick introduction (5 minutes)
uv run python 01_quick_start.py

# Performance and scaling (10 minutes)
uv run python 02_backend_performance.py

# Real-world data handling (15 minutes)
uv run python 03_real_world_messiness.py

# Multi-source data integration (20 minutes)
uv run python 04_multi_dataset_integration.py

# Enterprise production patterns (25 minutes)
uv run python 05_production_patterns.py

# Advanced graph analytics (30 minutes)
uv run python 06_advanced_analytics.py
```

## Script Descriptions

### 1. Quick Start (`01_quick_start.py`)
**Purpose:** 5-minute introduction with immediate gratification
**Key Messages:**
- Three-line setup: DataFrame → ContextBuilder → Star
- Cypher feels natural for analytical questions
- Graph relationships make complex joins simple

**Progressive Levels:**
- Basic entity queries
- Property filtering
- Multiple entity types
- Relationship traversal
- Aggregations

### 2. Backend Performance (`02_backend_performance.py`)
**Purpose:** Demonstrate intelligent backend selection and performance scaling
**Key Messages:**
- System automatically chooses optimal backend (pandas/duckdb/polars)
- Same query syntax across all scales (1K → 200K+ rows)
- Performance improvements without code changes

**Demonstrations:**
- Scale comparisons with embedded benchmarking
- Backend selection logic explanation
- Sub-linear scaling with vectorized operations

### 3. Real-World Messiness (`03_real_world_messiness.py`)
**Purpose:** Show system strength with messy, inconsistent government data
**Key Messages:**
- Works with sparse, inconsistent data out of the box
- Handles geographic complexity (FIPS codes, multi-jurisdictions)
- Graph queries reveal patterns invisible to traditional approaches

**Real-World Analytics:**
- Contract award geographic distribution
- Vendor relationship networks
- Agency spending patterns
- Data quality handling and normalization

### 4. Multi-Dataset Integration (`04_multi_dataset_integration.py`)
**Purpose:** Demonstrate seamless integration of diverse data sources
**Key Messages:**
- Cross-dataset fusion without ETL complexity
- Graph queries across multiple data sources naturally
- Real insights from heterogeneous data integration

**Integration Scenarios:**
- HR + PMO + Finance system integration
- Cross-department analytics
- Employee-project relationship discovery
- Workload and resource analysis

### 5. Production Patterns (`05_production_patterns.py`)
**Purpose:** Show production-ready features and enterprise capabilities
**Key Messages:**
- Production-scale data handling (1M+ rows)
- Comprehensive error handling and monitoring
- Performance optimization and caching built-in

**Enterprise Features:**
- Configuration presets and validation
- Query profiling and caching (380x speedup demonstrations)
- Structured audit logging and rate limiting
- Pipeline API for extensible processing

### 6. Advanced Analytics (`06_advanced_analytics.py`)
**Purpose:** Showcase sophisticated graph analytics impossible with traditional SQL
**Key Messages:**
- Complex graph traversals and network analysis
- Advanced temporal and pattern recognition
- Fraud detection and anomaly identification

**Advanced Patterns:**
- Multi-hop vendor relationship networks
- Network hub detection and cluster analysis
- Anomaly detection and bottleneck identification
- Fraud pattern recognition and network health analysis

## Expected Outputs

Each script provides:
- **Clear explanatory text** explaining concepts and techniques
- **Embedded timing metrics** showing performance characteristics
- **Real analytical insights** demonstrating practical value
- **Progressive complexity** building understanding naturally

## Data Sources

**Synthetic Data:**
- Customer/product relationships (Scripts 1-2)
- Scalable social graphs for performance testing
- Realistic employee/project scenarios

**Real Government Data:**
- Georgia federal contract records (~34K records from 6.6M+ available)
- Government agency and vendor relationships
- Geographic and temporal contract patterns

## Troubleshooting

**Import Errors:**
```bash
# Ensure pycypher-nmetl is installed
uv add pycypher-nmetl
```

**Performance Issues:**
- Scripts automatically select appropriate backends
- Large-scale demonstrations may take 1-2 minutes on slower hardware
- Memory usage is optimized for demonstration purposes

**Data Loading Issues:**
- All sample data is generated automatically
- No external data dependencies required
- Deterministic seeded generators ensure reproducible results

## Technical Implementation

**Architecture:**
- Self-contained scripts requiring minimal setup
- Shared utilities in `_common.py` for consistent presentation
- Data generation utilities in `data/` subdirectory
- Progressive complexity with clear conceptual building

**Quality Standards:**
- Zero errors, zero warnings across all scripts
- Professional presentation suitable for technical audiences
- Embedded performance metrics and timing
- Real analytical insights with practical value

## Next Steps

After completing this demonstration series, users typically:

1. **Explore Advanced Examples** - Dive into domain-specific use cases
2. **Integrate Real Data** - Connect to production data sources
3. **Scale to Production** - Implement enterprise monitoring and optimization
4. **Extend Analytics** - Build custom analytical pipelines and reports

## Support

For questions, issues, or feedback about these demonstrations:
- Technical issues: Check the main project documentation
- Use case questions: Explore the examples/ directory for domain-specific patterns
- Performance optimization: Review the production patterns and backend selection guides

---

**This demonstration series showcases pycypher-nmetl's capabilities for sophisticated data science work with complex, multi-source datasets.**