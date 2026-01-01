from __future__ import annotations
from abc import ABC, abstractmethod
from pydantic import field_validator, BaseModel
from typing import Any, List, Optional
from enum import Enum
import random
import hashlib
import rich
import pandas as pd


def random_hash() -> str:
    return hashlib.md5(
        bytes(str(random.random()), encoding="utf-8")
    ).hexdigest()


class JoinType(str, Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class GraphObjectType:
    pass


class ConvertableToPandas(ABC):
    @abstractmethod
    def to_pandas(self, context: Context) -> pd.DataFrame:
        raise NotImplementedError(
            "Classes declared as ConvertableToPandas must implement to_pandas method"
        )


class Algebraic(BaseModel):
    pass


class Table(Algebraic):
    identifier: str = ""
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}

    @field_validator("identifier", mode="after")
    @classmethod
    def set_identifier(cls, v: str) -> str:
        if v == "":
            return random_hash()
        return v


class EntityTable(Table, ConvertableToPandas):
    entity_type: str
    attributes: List[str]
    entity_identifier_attribute: str
    variables_to_columns: dict[str, str] = {}

    def __init__(self, **data: Any):
        super().__init__(**data)
        for attribute in self.attributes:
            column_hash: str = random_hash()
            self.column_name_to_hash[attribute] = column_hash
            self.hash_to_column_name[column_hash] = attribute

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = context.obj_map[self.entity_type].rename(
            mapper=self.column_name_to_hash,
            axis=1,
        )
        return df


class RelationshipTable(Table, ConvertableToPandas):
    relationship_type: str
    source_entity_type: str
    target_entity_type: str
    attributes: List[str]
    relationship_identifier_attribute: Optional[str] = (
        None  # Maybe use later for rel attributes?
    )
    variables_to_columns: dict[str, str] = {}

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        for attribute in self.attributes:
            column_hash: str = random_hash()
            self.column_name_to_hash[attribute] = column_hash
            self.hash_to_column_name[column_hash] = attribute

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = context.obj_map[self.relationship_type]
        return df


class Context(BaseModel):
    entity_tables: List[EntityTable]
    relationship_tables: List[RelationshipTable]
    obj_map: dict[str, Any] = {}

    def get_entity_table(self, entity_type: str) -> EntityTable:
        for entity_table in self.entity_tables:
            if entity_table.entity_type == entity_type:
                return entity_table
        else:
            raise ValueError(f"Entity table for type {entity_type} not found")

    def get_relationship_table(
        self, relationship_type: str
    ) -> RelationshipTable:
        for relationship_table in self.relationship_tables:
            if relationship_table.relationship_type == relationship_type:
                return relationship_table
        else:
            raise ValueError(
                f"Relationship table for type {relationship_type} not found"
            )


class Boolean(BaseModel):
    pass


class HasAttributeValue(Boolean):
    attribute: str
    value: str | int | float | bool | None


class Node(BaseModel, GraphObjectType):
    variable: str
    label: str
    attributes: dict = {}

    def to_algebra(self, context: Context) -> Filter | EntityTable:
        entity_table: EntityTable = context.get_entity_table(self.label)
        entity_table.variables_to_columns[self.variable] = (
            entity_table.column_name_to_hash[
                entity_table.entity_identifier_attribute
            ]
        )
        out: None | Filter = None
        for attr_name, attr_value in self.attributes.items():
            out = Filter(
                table=entity_table if not out else out,
                column_name_to_hash=entity_table.column_name_to_hash,
                hash_to_column_name=entity_table.hash_to_column_name,
                variables_to_columns=entity_table.variables_to_columns,
                condition=HasAttributeValue(
                    attribute=attr_name, value=attr_value
                ),
            )
        return out or entity_table

    def __str__(self) -> str:
        return f"({self.label}:{self.variable})"


class DropColumn(Algebraic, ConvertableToPandas):
    table: EntityTable | Join | DropColumn | SelectColumns
    column_name: str
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}
    variables_to_columns: dict[str, str] = {}

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = self.table.to_pandas(context)
        if self.column_name in df.columns:
            df_dropped: pd.DataFrame = df.drop(columns=[self.column_name])
            return df_dropped
        else:
            return df


class SelectColumns(Algebraic, ConvertableToPandas):
    table: EntityTable | Join | DropColumn | SelectColumns
    column_names: list[str]  # list of hashed column names
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}
    variables_to_columns: dict[str, str] = {}

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = self.table.to_pandas(context)
        selected_df: pd.DataFrame = df[self.column_names]  # pyrefly:ignore[bad-assignment]
        return selected_df


class RelationshipConjunction(GraphObjectType):
    relationships: List[Relationship]


class Join(Algebraic, ConvertableToPandas):
    left: EntityTable | RelationshipTable | Filter | Join
    right: EntityTable | RelationshipTable | Filter | Join
    join_type: JoinType = JoinType.INNER
    left_on: str
    right_on: str
    variable_list: List[str] = []
    variables_to_columns: dict[str, str] = {}
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}

    def to_pandas(self, context: Context) -> pd.DataFrame:
        left_df: pd.DataFrame = self.left.to_pandas(context)
        right_df: pd.DataFrame = self.right.to_pandas(context)
        if self.join_type == JoinType.INNER:
            merged_df: pd.DataFrame = pd.merge(
                left_df,
                right_df,
                how="inner",
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=("_left", "_right"),
            )
        else:
            raise ValueError(f"Unsupported join type: {self.join_type}")
        return merged_df


class Filter(Algebraic, ConvertableToPandas):
    table: Join | Filter | DropColumn | EntityTable | RelationshipTable
    condition: HasAttributeValue
    variables_to_columns: dict[str, str] = {}
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = self.table.to_pandas(context)
        match self.condition:
            case HasAttributeValue():
                column_name: str = self.table.column_name_to_hash[
                    self.condition.attribute
                ]
                filtered_df: pd.DataFrame = df[  # pyrefly:ignore[bad-assignment]
                    df[column_name] == self.condition.value
                ]
            case _:
                raise ValueError(
                    f"Unsupported condition type: {type(self.condition)}"
                )
        return filtered_df


class Relationship(BaseModel, GraphObjectType):
    variable: str
    label: str
    attributes: Optional[dict] = None
    source_node: Node
    target_node: Node

    def to_algebra(self, context: Context) -> RenameColumn:
        relationship_table: RelationshipTable = context.get_relationship_table(
            self.label
        )
        relationship_table.variables_to_columns[self.variable] = (
            "relationship_id"
        )
        source_table: EntityTable | Filter = self.source_node.to_algebra(
            context
        )
        target_table: EntityTable | Filter = self.target_node.to_algebra(
            context
        )
        left_join = Join(
            left=source_table,
            right=relationship_table,
            left_on=source_table.variables_to_columns[
                self.source_node.variable
            ],
            right_on="source_name",
            join_type=JoinType.INNER,
            variables_to_columns=source_table.variables_to_columns,
            column_name_to_hash=source_table.column_name_to_hash,
            hash_to_column_name=source_table.hash_to_column_name,
        )
        import pdb

        pdb.set_trace()
        left_join.variables_to_columns[self.variable] = (
            relationship_table.variables_to_columns[self.variable]
        )  # TODO

        right_join = Join(
            left=left_join,
            right=target_table,
            left_on="target_name",
            right_on=target_table.variables_to_columns[
                self.target_node.variable
            ],
            join_type=JoinType.INNER,
            variables_to_columns={
                **left_join.variables_to_columns,
                **target_table.variables_to_columns,
            },
            column_name_to_hash={
                **left_join.column_name_to_hash,
                **target_table.column_name_to_hash,
            },
            hash_to_column_name={
                **left_join.hash_to_column_name,
                **target_table.hash_to_column_name,
            },
        )

        dropped: DropColumn = DropColumn(
            table=DropColumn(
                table=right_join,
                column_name_to_hash=right_join.column_name_to_hash,
                hash_to_column_name=right_join.hash_to_column_name,
                column_name="source_name",
                variables_to_columns=right_join.variables_to_columns,
            ),
            column_name="target_name",
            column_name_to_hash=right_join.column_name_to_hash,
            hash_to_column_name=right_join.hash_to_column_name,
            variables_to_columns=right_join.variables_to_columns,
        )

        selected: SelectColumns = SelectColumns(
            table=dropped,
            column_names=list(right_join.variables_to_columns.values())
            + ["relationship_id"],
            column_name_to_hash=dropped.column_name_to_hash,
            hash_to_column_name=dropped.hash_to_column_name,
            variables_to_columns=right_join.variables_to_columns,
        )

        new_column_name: str = random_hash()
        renamed: RenameColumn = RenameColumn(
            table=selected,
            old_column_name="relationship_id",
            new_column_name=new_column_name,
            column_name_to_hash=selected.column_name_to_hash,
            hash_to_column_name=selected.hash_to_column_name,
            variables_to_columns=selected.variables_to_columns,
        )

        renamed.column_name_to_hash[self.variable] = new_column_name
        renamed.hash_to_column_name[new_column_name] = self.variable
        renamed.variables_to_columns[self.variable] = new_column_name
        return renamed

    def __str__(self) -> str:
        return f"({self.source_node.variable})-[:{self.label}]->({self.target_node.variable})"


class RenameColumn(Algebraic, ConvertableToPandas):
    table: EntityTable | Join | DropColumn | SelectColumns
    old_column_name: str
    new_column_name: str
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}
    variables_to_columns: dict[str, str] = {}

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = self.table.to_pandas(context)
        renamed_df: pd.DataFrame = df.rename(
            columns={self.old_column_name: self.new_column_name}
        )
        return renamed_df


if __name__ == "__main__":
    # Define tables
    person_table: EntityTable = EntityTable(
        name="Person",
        entity_type="Person",
        attributes=["name", "age"],
        entity_identifier_attribute="name",
    )

    city_table = EntityTable(
        name="City",
        entity_type="City",
        attributes=["name", "population"],
        entity_identifier_attribute="name",
    )

    state_table = EntityTable(
        name="State",
        entity_type="State",
        attributes=["name", "mittenlike", "humid"],
        entity_identifier_attribute="name",
    )

    lives_in_table = RelationshipTable(
        name="LIVES_IN",
        relationship_type="LIVES_IN",
        source_entity_type="Person",
        target_entity_type="City",
        attributes=[],
    )
    
    city_in_state_table = RelationshipTable(
        name="CITY_IN_STATE",
        relationship_type="CITY_IN_STATE",
        source_entity_type="City",
        target_entity_type="State",
        attributes=[],
    )

    # Define nodes and relationship
    person_node: Node = Node(
        variable="p", label="Person", attributes={"name": "Alice"}
    )
    person_df = pd.DataFrame(
        data=[
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "carol", "age": 22},
        ]
    )

    city_node: Node = Node(variable="c", label="City", attributes={})
    city_df = pd.DataFrame(
        data=[
            {"name": "Cairo", "population": 100},
            {"name": "Alexandria", "population": 50},
        ],
    )
    
    state_node: Node = Node(variable="s", label="State", attributes={})
    state_df = pd.DataFrame(
        data=[
            {"name": "Georgia", "humid": True, "mittenlike": False},
            {"name": "Virginia", "humid": False, "mittenlike": False},
            {"name": "Michigan", "humid": False, "mittenlike": True},
        ],
    )

    lives_in: Relationship = Relationship(
        variable="r",
        label="LIVES_IN",
        source_node=person_node,
        target_node=city_node,
    )
    
    

    lives_in_df: pd.DataFrame = pd.DataFrame(
        data=[
            {
                "source_name": "Alice",
                "target_name": "Cairo",
                "relationship_id": "r1",
            },
            {
                "source_name": "Bob",
                "target_name": "Alexandria",
                "relationship_id": "r2",
            },
            {
                "source_name": "carol",
                "target_name": "Cairo",
                "relationship_id": "r3",
            },
        ],
    )
    
    city_in_state: Relationship = Relationship(
        variable="s",
        label="CITY_IN_STATE",
        source_node=city_node,
        target_node=state_node,
    )
    
    city_in_state_df: pd.DataFrame = pd.DataFrame(
        data=[
            {
                "source_name": "Cairo",
                "target_name": "Georgia",
                "relationship_id": "in1",
            },
            {
                "source_name": "Alexandria",
                "target_name": "Virginia",
                "relationship_id": "in2",
            },
            {
                "source_name": "Kalamazoo",
                "target_name": "Michigan",
                "relationship_id": "in3",
            },
        ],
    )

    context: Context = Context(
        entity_tables=[city_table, state_table, person_table],
        relationship_tables=[lives_in_table, city_in_state_table],
        obj_map={
            "Person": person_df,
            "City": city_df,
            "LIVES_IN": lives_in_df,
            "State": state_df,
            "CITY_IN_STATE": city_in_state_df,
        },
    )

    lives_in_alg: Algebraic = lives_in.to_algebra(context)
    rich.print(lives_in_alg)
