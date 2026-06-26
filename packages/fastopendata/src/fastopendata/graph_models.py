"""
Graph node models auto-generated from fod_input_configs.yaml.template.
DO NOT EDIT — regenerate with: make generate-models
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel


class NodeLabel(str, Enum):
    County = "County"
    FederalAward = "FederalAward"
    HousingSurvey = "HousingSurvey"
    HousingSurvey5yr = "HousingSurvey5yr"
    IndustryGroup = "IndustryGroup"
    LanguageGroup = "LanguageGroup"
    OSMNode = "OSMNode"
    OccupationGroup = "OccupationGroup"
    OccupationSubgroup = "OccupationSubgroup"
    PUMA = "PUMA"
    PersonSurvey = "PersonSurvey"
    PersonSurvey5yr = "PersonSurvey5yr"
    State = "State"
    Tract = "Tract"


class RelationshipType(str, Enum):
    EMPLOYED_IN = "EMPLOYED_IN"
    IN = "IN"
    LIVES_IN = "LIVES_IN"
    LOCATED_IN = "LOCATED_IN"
    RESIDES_IN = "RESIDES_IN"
    SPEAKS = "SPEAKS"
    WORKS_IN = "WORKS_IN"


class County(BaseModel):
    pass


class FederalAward(BaseModel):
    pass


class HousingSurvey(BaseModel):
    pass


class HousingSurvey5yr(BaseModel):
    pass


class IndustryGroup(BaseModel):
    pass


class LanguageGroup(BaseModel):
    pass


class OSMNode(BaseModel):
    decoded_tags: Optional[str] = None
    foo: Optional[float] = None


class OccupationGroup(BaseModel):
    pass


class OccupationSubgroup(BaseModel):
    pass


class PUMA(BaseModel):
    agriculture_mining_ind_count_1yr: Optional[int] = None
    agriculture_mining_ind_count_5yr: Optional[int] = None
    architecture_engineering_count_1yr: Optional[int] = None
    architecture_engineering_count_5yr: Optional[int] = None
    arts_design_entertainment_count_1yr: Optional[int] = None
    arts_design_entertainment_count_5yr: Optional[int] = None
    arts_entertainment_food_count_1yr: Optional[int] = None
    arts_entertainment_food_count_5yr: Optional[int] = None
    building_grounds_count_1yr: Optional[int] = None
    building_grounds_count_5yr: Optional[int] = None
    business_financial_count_1yr: Optional[int] = None
    business_financial_count_5yr: Optional[int] = None
    community_social_service_count_1yr: Optional[int] = None
    community_social_service_count_5yr: Optional[int] = None
    computer_math_count_1yr: Optional[int] = None
    computer_math_count_5yr: Optional[int] = None
    construction_extraction_count_1yr: Optional[int] = None
    construction_extraction_count_5yr: Optional[int] = None
    construction_ind_count_1yr: Optional[int] = None
    construction_ind_count_5yr: Optional[int] = None
    east_southeast_asian_pacific_lang_count_1yr: Optional[int] = None
    east_southeast_asian_pacific_lang_count_5yr: Optional[int] = None
    education_healthcare_count_1yr: Optional[int] = None
    education_healthcare_count_5yr: Optional[int] = None
    education_library_count_1yr: Optional[int] = None
    education_library_count_5yr: Optional[int] = None
    english_only_count_1yr: Optional[int] = None
    english_only_count_5yr: Optional[int] = None
    european_lang_count_1yr: Optional[int] = None
    european_lang_count_5yr: Optional[int] = None
    farming_fishing_forestry_count_1yr: Optional[int] = None
    farming_fishing_forestry_count_5yr: Optional[int] = None
    finance_real_estate_count_1yr: Optional[int] = None
    finance_real_estate_count_5yr: Optional[int] = None
    food_preparation_serving_count_1yr: Optional[int] = None
    food_preparation_serving_count_5yr: Optional[int] = None
    healthcare_practitioners_count_1yr: Optional[int] = None
    healthcare_practitioners_count_5yr: Optional[int] = None
    healthcare_support_count_1yr: Optional[int] = None
    healthcare_support_count_5yr: Optional[int] = None
    housing_unit_count_1yr: Optional[int] = None
    housing_unit_count_5yr: Optional[int] = None
    information_count_1yr: Optional[int] = None
    information_count_5yr: Optional[int] = None
    installation_maintenance_repair_count_1yr: Optional[int] = None
    installation_maintenance_repair_count_5yr: Optional[int] = None
    legal_count_1yr: Optional[int] = None
    legal_count_5yr: Optional[int] = None
    life_physical_social_science_count_1yr: Optional[int] = None
    life_physical_social_science_count_5yr: Optional[int] = None
    management_count_1yr: Optional[int] = None
    management_count_5yr: Optional[int] = None
    manufacturing_count_1yr: Optional[int] = None
    manufacturing_count_5yr: Optional[int] = None
    material_moving_count_1yr: Optional[int] = None
    material_moving_count_5yr: Optional[int] = None
    mgmt_business_science_arts_count_1yr: Optional[int] = None
    mgmt_business_science_arts_count_5yr: Optional[int] = None
    middle_eastern_african_lang_count_1yr: Optional[int] = None
    middle_eastern_african_lang_count_5yr: Optional[int] = None
    military_count_1yr: Optional[int] = None
    military_count_5yr: Optional[int] = None
    military_industry_count_1yr: Optional[int] = None
    military_industry_count_5yr: Optional[int] = None
    natural_resources_construction_count_1yr: Optional[int] = None
    natural_resources_construction_count_5yr: Optional[int] = None
    office_admin_support_count_1yr: Optional[int] = None
    office_admin_support_count_5yr: Optional[int] = None
    other_services_ind_count_1yr: Optional[int] = None
    other_services_ind_count_5yr: Optional[int] = None
    other_unspecified_lang_count_1yr: Optional[int] = None
    other_unspecified_lang_count_5yr: Optional[int] = None
    pct_metro_tracts: Optional[float] = None
    pct_rural_tracts: Optional[float] = None
    personal_care_service_count_1yr: Optional[int] = None
    personal_care_service_count_5yr: Optional[int] = None
    pop_estimate_1yr: Optional[int] = None
    pop_estimate_5yr: Optional[int] = None
    production_count_1yr: Optional[int] = None
    production_count_5yr: Optional[int] = None
    production_transportation_count_1yr: Optional[int] = None
    production_transportation_count_5yr: Optional[int] = None
    professional_management_ind_count_1yr: Optional[int] = None
    professional_management_ind_count_5yr: Optional[int] = None
    protective_service_count_1yr: Optional[int] = None
    protective_service_count_5yr: Optional[int] = None
    public_administration_count_1yr: Optional[int] = None
    public_administration_count_5yr: Optional[int] = None
    retail_trade_count_1yr: Optional[int] = None
    retail_trade_count_5yr: Optional[int] = None
    rucc_avg: Optional[float] = None
    sales_count_1yr: Optional[int] = None
    sales_count_5yr: Optional[int] = None
    sales_office_count_1yr: Optional[int] = None
    sales_office_count_5yr: Optional[int] = None
    service_count_1yr: Optional[int] = None
    service_count_5yr: Optional[int] = None
    south_central_asian_lang_count_1yr: Optional[int] = None
    south_central_asian_lang_count_5yr: Optional[int] = None
    spanish_count_1yr: Optional[int] = None
    spanish_count_5yr: Optional[int] = None
    transportation_count_1yr: Optional[int] = None
    transportation_count_5yr: Optional[int] = None
    transportation_utilities_count_1yr: Optional[int] = None
    transportation_utilities_count_5yr: Optional[int] = None
    veteran_count_1yr: Optional[int] = None
    veteran_count_5yr: Optional[int] = None
    wholesale_trade_count_1yr: Optional[int] = None
    wholesale_trade_count_5yr: Optional[int] = None


class PersonSurvey(BaseModel):
    pass


class PersonSurvey5yr(BaseModel):
    pass


class State(BaseModel):
    agriculture_mining_ind_count_1yr: Optional[int] = None
    agriculture_mining_ind_count_5yr: Optional[int] = None
    architecture_engineering_count_1yr: Optional[int] = None
    architecture_engineering_count_5yr: Optional[int] = None
    arts_design_entertainment_count_1yr: Optional[int] = None
    arts_design_entertainment_count_5yr: Optional[int] = None
    arts_entertainment_food_count_1yr: Optional[int] = None
    arts_entertainment_food_count_5yr: Optional[int] = None
    award_count: Optional[int] = None
    building_grounds_count_1yr: Optional[int] = None
    building_grounds_count_5yr: Optional[int] = None
    business_financial_count_1yr: Optional[int] = None
    business_financial_count_5yr: Optional[int] = None
    community_social_service_count_1yr: Optional[int] = None
    community_social_service_count_5yr: Optional[int] = None
    computer_math_count_1yr: Optional[int] = None
    computer_math_count_5yr: Optional[int] = None
    construction_extraction_count_1yr: Optional[int] = None
    construction_extraction_count_5yr: Optional[int] = None
    construction_ind_count_1yr: Optional[int] = None
    construction_ind_count_5yr: Optional[int] = None
    east_southeast_asian_pacific_lang_count_1yr: Optional[int] = None
    east_southeast_asian_pacific_lang_count_5yr: Optional[int] = None
    education_healthcare_count_1yr: Optional[int] = None
    education_healthcare_count_5yr: Optional[int] = None
    education_library_count_1yr: Optional[int] = None
    education_library_count_5yr: Optional[int] = None
    english_only_count_1yr: Optional[int] = None
    english_only_count_5yr: Optional[int] = None
    european_lang_count_1yr: Optional[int] = None
    european_lang_count_5yr: Optional[int] = None
    farming_fishing_forestry_count_1yr: Optional[int] = None
    farming_fishing_forestry_count_5yr: Optional[int] = None
    finance_real_estate_count_1yr: Optional[int] = None
    finance_real_estate_count_5yr: Optional[int] = None
    food_preparation_serving_count_1yr: Optional[int] = None
    food_preparation_serving_count_5yr: Optional[int] = None
    healthcare_practitioners_count_1yr: Optional[int] = None
    healthcare_practitioners_count_5yr: Optional[int] = None
    healthcare_support_count_1yr: Optional[int] = None
    healthcare_support_count_5yr: Optional[int] = None
    information_count_1yr: Optional[int] = None
    information_count_5yr: Optional[int] = None
    installation_maintenance_repair_count_1yr: Optional[int] = None
    installation_maintenance_repair_count_5yr: Optional[int] = None
    legal_count_1yr: Optional[int] = None
    legal_count_5yr: Optional[int] = None
    life_physical_social_science_count_1yr: Optional[int] = None
    life_physical_social_science_count_5yr: Optional[int] = None
    management_count_1yr: Optional[int] = None
    management_count_5yr: Optional[int] = None
    manufacturing_count_1yr: Optional[int] = None
    manufacturing_count_5yr: Optional[int] = None
    material_moving_count_1yr: Optional[int] = None
    material_moving_count_5yr: Optional[int] = None
    metro_county_count: Optional[int] = None
    mgmt_business_science_arts_count_1yr: Optional[int] = None
    mgmt_business_science_arts_count_5yr: Optional[int] = None
    middle_eastern_african_lang_count_1yr: Optional[int] = None
    middle_eastern_african_lang_count_5yr: Optional[int] = None
    military_count_1yr: Optional[int] = None
    military_count_5yr: Optional[int] = None
    military_industry_count_1yr: Optional[int] = None
    military_industry_count_5yr: Optional[int] = None
    natural_resources_construction_count_1yr: Optional[int] = None
    natural_resources_construction_count_5yr: Optional[int] = None
    num_counties: Optional[int] = None
    office_admin_support_count_1yr: Optional[int] = None
    office_admin_support_count_5yr: Optional[int] = None
    other_services_ind_count_1yr: Optional[int] = None
    other_services_ind_count_5yr: Optional[int] = None
    other_unspecified_lang_count_1yr: Optional[int] = None
    other_unspecified_lang_count_5yr: Optional[int] = None
    over_hundred_counties: Optional[bool] = None
    pct_metro_tracts: Optional[float] = None
    pct_rural_tracts: Optional[float] = None
    personal_care_service_count_1yr: Optional[int] = None
    personal_care_service_count_5yr: Optional[int] = None
    production_count_1yr: Optional[int] = None
    production_count_5yr: Optional[int] = None
    production_transportation_count_1yr: Optional[int] = None
    production_transportation_count_5yr: Optional[int] = None
    professional_management_ind_count_1yr: Optional[int] = None
    professional_management_ind_count_5yr: Optional[int] = None
    protective_service_count_1yr: Optional[int] = None
    protective_service_count_5yr: Optional[int] = None
    public_administration_count_1yr: Optional[int] = None
    public_administration_count_5yr: Optional[int] = None
    retail_trade_count_1yr: Optional[int] = None
    retail_trade_count_5yr: Optional[int] = None
    rucc_avg: Optional[float] = None
    rural_county_count: Optional[int] = None
    sales_count_1yr: Optional[int] = None
    sales_count_5yr: Optional[int] = None
    sales_office_count_1yr: Optional[int] = None
    sales_office_count_5yr: Optional[int] = None
    service_count_1yr: Optional[int] = None
    service_count_5yr: Optional[int] = None
    south_central_asian_lang_count_1yr: Optional[int] = None
    south_central_asian_lang_count_5yr: Optional[int] = None
    spanish_count_1yr: Optional[int] = None
    spanish_count_5yr: Optional[int] = None
    transportation_count_1yr: Optional[int] = None
    transportation_count_5yr: Optional[int] = None
    transportation_utilities_count_1yr: Optional[int] = None
    transportation_utilities_count_5yr: Optional[int] = None
    wholesale_trade_count_1yr: Optional[int] = None
    wholesale_trade_count_5yr: Optional[int] = None


class Tract(BaseModel):
    rucc_2023: Optional[int] = None
    rucc_description: Optional[str] = None


# Relationship endpoint pairs extracted from the pipeline config.
# Format: {RelationshipType: [(source_label, target_label), ...]}
RELATIONSHIP_ENDPOINTS: dict[str, list[tuple[str, str]]] = {
    "EMPLOYED_IN": [
        ("PersonSurvey", "IndustryGroup"),
        ("PersonSurvey5yr", "IndustryGroup"),
    ],
    "IN": [
        ("County", "State"),
        ("OSMNode", "Tract"),
        ("PUMA", "State"),
        ("Tract", "County"),
        ("Tract", "PUMA"),
    ],
    "LIVES_IN": [
        ("PersonSurvey", "PUMA"),
        ("PersonSurvey5yr", "PUMA"),
    ],
    "LOCATED_IN": [
        ("HousingSurvey", "PUMA"),
        ("HousingSurvey5yr", "PUMA"),
    ],
    "RESIDES_IN": [
        ("PersonSurvey", "HousingSurvey"),
        ("PersonSurvey5yr", "HousingSurvey5yr"),
    ],
    "SPEAKS": [
        ("PersonSurvey", "LanguageGroup"),
        ("PersonSurvey5yr", "LanguageGroup"),
    ],
    "WORKS_IN": [
        ("PersonSurvey", "OccupationGroup"),
        ("PersonSurvey", "OccupationSubgroup"),
        ("PersonSurvey5yr", "OccupationGroup"),
        ("PersonSurvey5yr", "OccupationSubgroup"),
    ],
}
