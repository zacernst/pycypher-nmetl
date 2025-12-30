from __future__ import annotations
from abc import ABC, abstractmethod
from pydantic import field_validator, BaseModel
from typing import Any, List, Optional
from enum import Enum
import pandas as pd

# ID_KEY: str = "__id__"
VARIABLE_PREFIX: str = "__var__"


class JoinType(str, Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class Algebraic(BaseModel):

    @abstractmethod
    def to_pandas(self, context: Context) -> pd.DataFrame:
        raise NotImplementedError(
            "Subclasses must implement to_pandas method"
        )

class Table(Algebraic):
    name: str


class EntityTable(Table):
    entity_type: str
    attributes: List[str]
    identifier_column_name: str

    def to_pandas(self, context: Context) -> pd.DataFrame:
        if self.entity_type not in context.obj_map:
            raise ValueError(f"Entity type {self.entity_type} not found in context")
        # Create __id__ column if not exists
        df: pd.DataFrame = context.obj_map[self.entity_type]
        if ID_KEY not in df.columns:
            df = df.copy()
            df[ID_KEY] = df[self.identifier_column_name]
            context.obj_map[self.entity_type] = df
        return context.obj_map[self.entity_type]


class RelationshipTable(Table):
    relationship_type: str
    source_entity_type: str
    target_entity_type: str
    attributes: List[str]
    variable_list: List[str] = []
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        if self.relationship_type not in context.obj_map:
            raise ValueError(f"Relationship type {self.relationship_type} not found in context")
        return context.obj_map[self.relationship_type]


class ConvertableToAlgebra(ABC, BaseModel):
    @abstractmethod
    def to_algebra(self, context: Context) -> Algebraic:
        raise NotImplementedError(
            "Subclasses must implement to_algebra method"
        )


class Join(Algebraic):
    left: Algebraic
    right: Algebraic
    join_type: JoinType = JoinType.INNER
    left_on: str
    right_on: str
    variable_list: List[str] = []


    def to_pandas(self, context: Context) -> pd.DataFrame:
        left_df: pd.DataFrame = self.left.to_pandas(context)
        right_df: pd.DataFrame = self.right.to_pandas(context)
        if self.join_type == JoinType.INNER:
            import pdb; pdb.set_trace()
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


class Filter(Algebraic):
    table: Table
    condition: str


class Context(BaseModel):
    entity_tables: List[EntityTable]
    relationship_tables: List[RelationshipTable]
    obj_map: dict[str, Any] = {}

    def get_entity_table(self, entity_type: str) -> EntityTable:
        for table in self.entity_tables:
            if table.entity_type == entity_type:
                return table
        else:
            raise Exception(
                f"Entity type {entity_type} not found in entity tables"
            )

    def get_relationship_table(
        self, relationship_type: str
    ) -> RelationshipTable:
        for table in self.relationship_tables:
            if table.relationship_type == relationship_type:
                return table
        else:
            raise Exception(
                f"Relationship type {relationship_type} not found in relationship tables"
            )


class Node(ConvertableToAlgebra):
    variable: str
    label: str
    properties: Optional[dict] = None
    variable_list: list[str] = []


    def to_algebra(self, context: Context) -> RenameColumn:
        entity_table: Table = context.get_entity_table(self.label)
        # variable_table = VariableTable(parent_node = self, table=entity_table, variable_list=[self.variable])
        rename_column = RenameColumn(
            name=entity_table.name,
            table=entity_table,
            source_column=context.get_entity_table(self.label).identifier_column_name,
            target_column=f"{VARIABLE_PREFIX}{self.variable}",
            variable_list=[self.variable]
        )
        return rename_column
    
    def __str__(self) -> str:
        return f"({self.variable})"


class Relationship(ConvertableToAlgebra):
    variable: str
    label: str
    properties: Optional[dict] = None
    source_node: Node
    target_node: Node

    def to_algebra(self, context: Context) -> Algebraic:
        relationship_table: Table = context.get_relationship_table(self.label)
        source_table: Table = self.source_node.to_algebra(context)
        target_table: Algebraic = self.target_node.to_algebra(context)
        left_join = Join(
            left=source_table,
            right=relationship_table,
            left_on=f"{VARIABLE_PREFIX}{self.source_node.variable}",
            right_on='source_node',
            join_type=JoinType.INNER,
        )
        renamed_left_join: RenameColumn = RenameColumn(
            table=left_join,
            source_column=source_table.identifier_column_name,
            target_column=f"{VARIABLE_PREFIX}{self.variable}",
            variable_list=[self.variable] + left_join.variable_list,
        )

        right_join = Join(
            left=renamed_left_join,
            right=target_table,
            left_on='target_node',
            right_on=f"{VARIABLE_PREFIX}{self.target_node.variable}",
            join_type=JoinType.INNER,
            variable_list=[self.variable] + getattr(source_table, 'variable_list', []) + getattr(target_table, 'variable_list', [])
        )
        return right_join
    
    def __str__(self) -> str:
        return f"({self.source_node.variable})-[:{self.label}]->({self.target_node.variable})"


class RenameColumn(Table):
    table: Table | Join
    source_column: str
    target_column: str
    variable_list: list[str] = []
    identifier_column_name: Optional[str] = None

    @field_validator("identifier_column_name", mode="before")

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = self.table.to_pandas(context)
        df = df.rename(columns={self.source_column: self.target_column})
        return df


if __name__ == "__main__":
    person_df: pd.DataFrame = pd.DataFrame(
        data=[{"name": "Alice", "age": 30, "city": "Cairo"}], columns=["name", "age", "city"]
    )

    city_df: pd.DataFrame = pd.DataFrame(
        data=[{"name": "Cairo", "population": 100}], columns=["name", "population"]
    )

    lives_in_df: pd.DataFrame = pd.DataFrame(
        data=[{"source_name": "Alice", "target_name": "Cairo"}],
    )

    person_table = EntityTable(
        name="Person", entity_type="Person", attributes=["name", "age"],
        identifier_column_name="name"
    )

    city_table = EntityTable(
        name="City", entity_type="City", attributes=["name", "population"],
        identifier_column_name="name"
    )

    lives_in_table = RelationshipTable(
        name="LIVES_IN",
        relationship_type="LIVES_IN",
        source_entity_type="Person",
        target_entity_type="City",
        attributes=[],
        obj=lives_in_df,
        variable_list=["r"],
    )

    person_node = Node(
        variable="p", label="Person", properties={}
    )

    city_node = Node(
        variable="c", label="City", properties={}
    )

    relationship = Relationship(
        variable="r",
        label="LIVES_IN",
        source_node=person_node,
        target_node=city_node,
    )
    context = Context(
        entity_tables=[person_table, city_table], relationship_tables=[lives_in_table],
        obj_map={"Person": person_df, "City": city_df, "LIVES_IN": lives_in_df},
    )
    # obj: Algebraic = relationship.to_algebra(context)
    # print(obj)
    
    import rich
    obj: Algebraic = relationship.to_algebra(context)
    rich.print(obj)
    print(obj.to_pandas(context))