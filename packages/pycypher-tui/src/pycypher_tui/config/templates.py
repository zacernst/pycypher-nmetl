"""Built-in pipeline templates for quick-start configuration.

Templates provide parameterised pipeline configurations for common
use cases.  Users pick a template and supply parameters (project name,
data directory); the template produces a complete :class:`PipelineConfig`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    ProjectConfig,
    QueryConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)

__all__ = [
    "PipelineTemplate",
    "get_template",
    "list_templates",
]


@dataclass(frozen=True)
class PipelineTemplate:
    """A parameterised pipeline configuration template.

    Attributes:
        name: Short identifier (e.g. ``"csv_analytics"``).
        description: Human-readable description.
        category: Grouping category (e.g. ``"analytics"``, ``"etl"``).
        parameters: List of parameter names the template accepts.

    """

    name: str
    description: str
    category: str
    parameters: list[str] = field(default_factory=lambda: ["project_name", "data_dir"])
    _builder: Any = field(default=None, repr=False, compare=False)

    def instantiate(self, **kwargs: str) -> PipelineConfig:
        """Create a PipelineConfig from this template with given parameters.

        Args:
            **kwargs: Parameter values.  At minimum ``project_name`` and
                ``data_dir`` should be provided.

        Returns:
            A fully-formed :class:`PipelineConfig`.
        """
        if self._builder is None:
            msg = f"Template {self.name!r} has no builder function."
            raise RuntimeError(msg)
        return self._builder(**kwargs)


# ---------------------------------------------------------------------------
# Template builder functions
# ---------------------------------------------------------------------------


def _build_csv_analytics(
    project_name: str = "csv_analytics",
    data_dir: str = "data",
    **_: str,
) -> PipelineConfig:
    """CSV Analytics template: CSV files → customer analytics queries."""
    return PipelineConfig(
        version="1.0",
        project=ProjectConfig(
            name=project_name,
            description="CSV analytics pipeline",
        ),
        sources=SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="customers",
                    uri=f"{data_dir}/customers.csv",
                    entity_type="Customer",
                    id_col="customer_id",
                ),
                EntitySourceConfig(
                    id="transactions",
                    uri=f"{data_dir}/transactions.csv",
                    entity_type="Transaction",
                    id_col="transaction_id",
                ),
            ],
        ),
        queries=[
            QueryConfig(
                id="customer_summary",
                description="Aggregate customer metrics",
                inline=(
                    "MATCH (c:Customer)\n"
                    "WITH c.customer_id AS id, c.name AS name\n"
                    "RETURN id, name"
                ),
            ),
        ],
        output=[
            OutputConfig(query_id="customer_summary", uri=f"{data_dir}/output/summary.csv"),
        ],
    )


def _build_ecommerce_pipeline(
    project_name: str = "ecommerce",
    data_dir: str = "data",
    **_: str,
) -> PipelineConfig:
    """E-commerce pipeline: Customer/Order/Product relationship analysis."""
    return PipelineConfig(
        version="1.0",
        project=ProjectConfig(
            name=project_name,
            description="E-commerce pipeline with customer/order/product analysis",
        ),
        sources=SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="customers",
                    uri=f"{data_dir}/customers.csv",
                    entity_type="Customer",
                    id_col="customer_id",
                ),
                EntitySourceConfig(
                    id="products",
                    uri=f"{data_dir}/products.csv",
                    entity_type="Product",
                    id_col="product_id",
                ),
                EntitySourceConfig(
                    id="orders",
                    uri=f"{data_dir}/orders.csv",
                    entity_type="CustomerOrder",
                    id_col="order_id",
                ),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="purchases",
                    uri=f"{data_dir}/order_items.csv",
                    relationship_type="PURCHASED",
                    source_col="customer_id",
                    target_col="product_id",
                ),
            ],
        ),
        queries=[
            QueryConfig(
                id="top_products",
                description="Find most popular products",
                inline=(
                    "MATCH (c:Customer)-[p:PURCHASED]->(prod:Product)\n"
                    "WITH prod.name AS product, count(c) AS buyers\n"
                    "RETURN product, buyers"
                ),
            ),
            QueryConfig(
                id="customer_spend",
                description="Calculate customer total spend",
                inline=(
                    "MATCH (c:Customer)-[p:PURCHASED]->(prod:Product)\n"
                    "WITH c.name AS customer, sum(prod.price) AS total\n"
                    "RETURN customer, total"
                ),
            ),
        ],
        output=[
            OutputConfig(query_id="top_products", uri=f"{data_dir}/output/top_products.csv"),
            OutputConfig(query_id="customer_spend", uri=f"{data_dir}/output/spend.csv"),
        ],
    )


def _build_social_network(
    project_name: str = "social_network",
    data_dir: str = "data",
    **_: str,
) -> PipelineConfig:
    """Social network template: User relationships and influence analysis."""
    return PipelineConfig(
        version="1.0",
        project=ProjectConfig(
            name=project_name,
            description="Social network relationship and influence analysis",
        ),
        sources=SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="users",
                    uri=f"{data_dir}/users.csv",
                    entity_type="User",
                    id_col="user_id",
                ),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="follows",
                    uri=f"{data_dir}/follows.csv",
                    relationship_type="FOLLOWS",
                    source_col="follower_id",
                    target_col="followed_id",
                ),
                RelationshipSourceConfig(
                    id="likes",
                    uri=f"{data_dir}/likes.csv",
                    relationship_type="LIKES",
                    source_col="user_id",
                    target_col="post_id",
                ),
            ],
        ),
        queries=[
            QueryConfig(
                id="influencers",
                description="Find users with most followers",
                inline=(
                    "MATCH (u:User)<-[f:FOLLOWS]-(follower:User)\n"
                    "WITH u.name AS user, count(follower) AS followers\n"
                    "RETURN user, followers"
                ),
            ),
        ],
        output=[
            OutputConfig(query_id="influencers", uri=f"{data_dir}/output/influencers.csv"),
        ],
    )


def _build_time_series(
    project_name: str = "time_series",
    data_dir: str = "data",
    **_: str,
) -> PipelineConfig:
    """Time series template: Temporal data analysis patterns."""
    return PipelineConfig(
        version="1.0",
        project=ProjectConfig(
            name=project_name,
            description="Time series analysis pipeline",
        ),
        sources=SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="events",
                    uri=f"{data_dir}/events.csv",
                    entity_type="Event",
                    id_col="event_id",
                    schema_hints={"timestamp": "TIMESTAMP", "value": "DOUBLE"},
                ),
                EntitySourceConfig(
                    id="sensors",
                    uri=f"{data_dir}/sensors.csv",
                    entity_type="Sensor",
                    id_col="sensor_id",
                ),
            ],
        ),
        queries=[
            QueryConfig(
                id="sensor_summary",
                description="Summarise readings per sensor",
                inline=(
                    "MATCH (e:Event)\n"
                    "WITH e.sensor_id AS sensor, avg(e.value) AS avg_value\n"
                    "RETURN sensor, avg_value"
                ),
            ),
        ],
        output=[
            OutputConfig(query_id="sensor_summary", uri=f"{data_dir}/output/sensor_summary.csv"),
        ],
    )


# ---------------------------------------------------------------------------
# Template registry
# ---------------------------------------------------------------------------

_TEMPLATES: list[PipelineTemplate] = [
    PipelineTemplate(
        name="csv_analytics",
        description="CSV files to customer analytics queries",
        category="analytics",
        _builder=_build_csv_analytics,
    ),
    PipelineTemplate(
        name="ecommerce_pipeline",
        description="Customer/Order/Product relationship analysis",
        category="etl",
        _builder=_build_ecommerce_pipeline,
    ),
    PipelineTemplate(
        name="social_network",
        description="User relationships and influence analysis",
        category="analytics",
        _builder=_build_social_network,
    ),
    PipelineTemplate(
        name="time_series",
        description="Temporal data analysis patterns",
        category="analytics",
        _builder=_build_time_series,
    ),
]


def list_templates() -> list[PipelineTemplate]:
    """Return all available pipeline templates."""
    return list(_TEMPLATES)


def get_template(name: str) -> PipelineTemplate | None:
    """Look up a template by name.

    Args:
        name: Template name (e.g. ``"csv_analytics"``).

    Returns:
        The template, or ``None`` if not found.
    """
    for t in _TEMPLATES:
        if t.name == name:
            return t
    return None
