"""
Fixtures for the unit tests.
"""

# pylint: disable=missing-function-docstring,protected-access,redefined-outer-name,too-many-lines

from pycypher.etl.data_source import DataSourceMapping, FixtureDataSource
from pycypher.etl.goldberg import Goldberg
from pycypher.etl.trigger import VariableAttribute

data_source = FixtureDataSource(
    name="people_fixture",
    data=[
        {
            "person_id": "001",
            "name": "Alice",
            "age": 25,
            "zip_code": "02056",
            "widgets": 5,
        },
        {
            "person_id": "002",
            "name": "Bob",
            "age": 30,
            "zip_code": "02055",
            "widgets": 3,
        },
        {
            "person_id": "003",
            "name": "Charlie",
            "age": 35,
            "zip_code": "02054",
            "widgets": 2,
        },
        {
            "person_id": "004",
            "name": "David",
            "age": 40,
            "zip_code": "02053",
            "widgets": 1,
        },
        {
            "person_id": "005",
            "name": "Eve",
            "age": 45,
            "zip_code": "02052",
            "widgets": 4,
        },
        {
            "person_id": "006",
            "name": "Frank",
            "age": 50,
            "zip_code": "02051",
            "widgets": 6,
        },
        {
            "person_id": "007",
            "name": "Grace",
            "age": 55,
            "zip_code": "02050",
            "widgets": 7,
        },
    ],
    loop=True,
)


data_source_mapping_0 = DataSourceMapping(
    attribute_key="person_id",
    identifier_key="person_id",
    attribute="Identifier",
    label="Person",
)
data_source_mapping_1 = DataSourceMapping(
    attribute_key="name",
    identifier_key="person_id",
    attribute="Name",
    label="Person",
)
data_source_mapping_2 = DataSourceMapping(
    attribute_key="age",
    identifier_key="person_id",
    attribute="age",
    label="Person",
)
data_source_mapping_3 = DataSourceMapping(
    attribute_key="zip_code",
    identifier_key="person_id",
    attribute="ZipCode",
    label="Person",
)
data_source_mapping_4 = DataSourceMapping(
    attribute_key="widgets",
    identifier_key="person_id",
    attribute="WidgetsPurchased",
    label="Person",
)
mapping_list = [
    data_source_mapping_0,
    data_source_mapping_1,
    data_source_mapping_2,
    data_source_mapping_3,
    data_source_mapping_4,
]


def populated_goldberg(
    fixture_0_data_source_mapping_list, empty_goldberg, fixture_data_source_0
):
    # Get data source mappings
    # Attach data source mappings to data source
    # Attach data source to goldberg

    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_goldberg.attach_data_source(fixture_data_source_0)
    return empty_goldberg


if __name__ == "__main__":
    goldberg = Goldberg()

    @goldberg.cypher_trigger("MATCH (n:Person {age: 25}) RETURN n.Identifier")
    def f(n) -> VariableAttribute["n", "age"]:
        return n

    data_source.attach_mapping(mapping_list)
    goldberg.attach_data_source(data_source)
    goldberg.start_threads()
    goldberg.block_until_finished()
