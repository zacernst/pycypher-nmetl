"""
Graph node models auto-generated from fod_input_configs.yaml.template.
DO NOT EDIT — regenerate with: make generate-models
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class NodeLabel(str, Enum):
    CJARSRecord = "CJARSRecord"
    County = "County"
    CountyElectionResult = "CountyElectionResult"
    FederalAward = "FederalAward"
    HousingSurvey1yr = "HousingSurvey1yr"
    HousingSurvey5yr = "HousingSurvey5yr"
    IndustryGroup = "IndustryGroup"
    LanguageGroup = "LanguageGroup"
    OSMNode = "OSMNode"
    OccupationGroup = "OccupationGroup"
    OccupationSubgroup = "OccupationSubgroup"
    PUMA = "PUMA"
    PersonSurvey1yr = "PersonSurvey1yr"
    PersonSurvey5yr = "PersonSurvey5yr"
    School = "School"
    State = "State"
    Tract = "Tract"


class RelationshipType(str, Enum):
    EMPLOYED_IN = "EMPLOYED_IN"
    LOCATED_IN = "LOCATED_IN"
    RESIDES_IN = "RESIDES_IN"
    SPEAKS = "SPEAKS"
    WORKS_IN = "WORKS_IN"


class CJARSRecord(BaseModel):
    pass


class County(BaseModel):
    cjars_felony_death_rate: Optional[float] = Field(default=None, description="Share who have died following a felony charge in each County")
    cjars_felony_earnings: Optional[float] = Field(default=None, description="Average annual employment earnings for individuals charged with a felony, across all offense types, ages, sexes, and races in each County")
    cjars_felony_employment_rate: Optional[float] = Field(default=None, description="Share employed in the year of a felony charge, across all offense types, ages, sexes, and races in each County")
    cjars_felony_hud_rate: Optional[float] = Field(default=None, description="Share receiving HUD rental housing assistance among individuals charged with a felony in each County")
    cjars_felony_medicaid_rate: Optional[float] = Field(default=None, description="Share enrolled in Medicaid among individuals charged with a felony in each County")
    cjars_felony_ssi_rate: Optional[float] = Field(default=None, description="Share receiving Supplemental Security Income among individuals charged with a felony in each County")
    cjars_misdemeanor_death_rate: Optional[float] = Field(default=None, description="Share who have died following a misdemeanor charge in each County")
    cjars_misdemeanor_earnings: Optional[float] = Field(default=None, description="Average annual employment earnings for individuals charged with a misdemeanor in each County")
    cjars_misdemeanor_employment_rate: Optional[float] = Field(default=None, description="Share employed in the year of a misdemeanor charge, across all offense types, ages, sexes, and races in each County")
    cjars_misdemeanor_hud_rate: Optional[float] = Field(default=None, description="Share receiving HUD rental housing assistance among individuals charged with a misdemeanor in each County")
    cjars_misdemeanor_medicaid_rate: Optional[float] = Field(default=None, description="Share enrolled in Medicaid among individuals charged with a misdemeanor in each County")
    cjars_misdemeanor_ssi_rate: Optional[float] = Field(default=None, description="Share receiving Supplemental Security Income among individuals charged with a misdemeanor in each County")
    is_metro: Optional[bool] = Field(default=None, description="Flag County nodes in metro areas (RUCC codes 1-3)")
    is_rural: Optional[bool] = Field(default=None, description="Flag County nodes that are completely rural (RUCC codes 8-9)")
    pres_dem_share_2024: Optional[float] = Field(default=None, description="Democrat share of the two-party presidential vote (2024) in each County (0.0–1.0)")
    pres_dem_votes_2024: Optional[float] = Field(default=None, description="Total Democrat two-party votes cast for US President (2024) in each County")
    pres_rep_margin_2024: Optional[float] = Field(default=None, description="Republican two-party margin in the 2024 presidential election for each County (positive = R advantage, range -1.0 to 1.0)")
    pres_rep_share_2024: Optional[float] = Field(default=None, description="Republican share of the two-party presidential vote (2024) in each County (0.0–1.0)")
    pres_rep_votes_2024: Optional[float] = Field(default=None, description="Total Republican two-party votes cast for US President (2024) in each County")
    pres_two_party_votes_2024: Optional[float] = Field(default=None, description="Total two-party (Republican + Democrat) votes for US President (2024) in each County")
    pres_winner_2024: Optional[float] = Field(default=None, description="Winning party in the 2024 US Presidential election for each County ('REPUBLICAN', 'DEMOCRAT', or 'TIE')")
    rucc_class: Optional[str] = Field(default=None, description="Set a three-way metro/nonmetro_urban/rural classification string on each County node")
    school_avg_federal_revenue_share: Optional[float] = Field(default=None, description="Average federal revenue share across schools in each County; high values indicate elevated exposure to federal funding cuts")
    school_avg_instruction_spend_share: Optional[float] = Field(default=None, description="Average share of expenditures directed to instruction across schools in each County; a proxy for how much spending reaches students vs. administration")
    school_avg_local_revenue_share: Optional[float] = Field(default=None, description="Average local revenue share across schools in each County; high values correlate with property-tax-rich counties")
    school_avg_per_pupil_spend: Optional[float] = Field(default=None, description="Aggregate average per-pupil total expenditure across all schools in each County")
    school_charter_share: Optional[float] = Field(default=None, description="Compute the share of schools in each County that are charter schools")
    school_count: Optional[int] = Field(default=None, description="Count the number of schools located in each County")
    school_elementary_count: Optional[int] = Field(default=None, description="Count of elementary schools in each County")
    school_high_count: Optional[int] = Field(default=None, description="Count of high schools in each County")
    school_high_federal_dependency_share: Optional[float] = Field(default=None, description="Share of schools in each County where federal revenue exceeds 25% of total revenue; a county-level policy risk signal")
    school_middle_count: Optional[int] = Field(default=None, description="Count of middle schools in each County")
    school_per_pupil_spend_vs_state_avg: Optional[float] = Field(default=None, description="Difference between a County's average per-pupil spend and its State's average; positive values indicate above-average local investment")
    school_special_ed_share: Optional[float] = Field(default=None, description="Share of schools in each County classified as special education (SCH_TYPE = 2)")
    school_total_enrollment: Optional[float] = Field(default=None, description="Total student enrollment across all schools in each County")


class CountyElectionResult(BaseModel):
    pass


class FederalAward(BaseModel):
    pass


class HousingSurvey1yr(BaseModel):
    pass


class HousingSurvey5yr(BaseModel):
    pass


class IndustryGroup(BaseModel):
    pass


class LanguageGroup(BaseModel):
    pass


class OSMNode(BaseModel):
    decoded_tags: Optional[str] = Field(default=None, description="Testing whether we can call a function to decode serialized data")
    foo: Optional[float] = Field(default=None, description="Test retrieving raw longitude value")


class OccupationGroup(BaseModel):
    pass


class OccupationSubgroup(BaseModel):
    pass


class PUMA(BaseModel):
    agriculture_mining_ind_count_1yr: Optional[int] = Field(default=None, description="Count Agriculture, Forestry, Fishing, Hunting, and Mining industry workers per PUMA")
    agriculture_mining_ind_count_5yr: Optional[int] = Field(default=None, description="Count Agriculture, Forestry, Fishing, Hunting, and Mining industry workers per PUMA (5-year)")
    architecture_engineering_count_1yr: Optional[int] = Field(default=None, description="Count Architecture and Engineering Occupations workers per PUMA")
    architecture_engineering_count_5yr: Optional[int] = Field(default=None, description="Count Architecture and Engineering Occupations workers per PUMA (5-year)")
    arts_design_entertainment_count_1yr: Optional[int] = Field(default=None, description="Count Arts, Design, Entertainment, Sports, and Media Occupations workers per PUMA")
    arts_design_entertainment_count_5yr: Optional[int] = Field(default=None, description="Count Arts, Design, Entertainment, Sports, and Media Occupations workers per PUMA (5-year)")
    arts_entertainment_food_count_1yr: Optional[int] = Field(default=None, description="Count Arts, Entertainment, Recreation, and Food Services industry workers per PUMA")
    arts_entertainment_food_count_5yr: Optional[int] = Field(default=None, description="Count Arts, Entertainment, Recreation, and Food Services industry workers per PUMA (5-year)")
    avg_rucc_score: Optional[float] = Field(default=None, description="Compute average RUCC score and rural/metro tract fractions for each PUMA")
    building_grounds_count_1yr: Optional[int] = Field(default=None, description="Count Building and Grounds Cleaning and Maintenance Occupations workers per PUMA")
    building_grounds_count_5yr: Optional[int] = Field(default=None, description="Count Building and Grounds Cleaning and Maintenance Occupations workers per PUMA (5-year)")
    business_financial_count_1yr: Optional[int] = Field(default=None, description="Count Business and Financial Operations Occupations workers per PUMA")
    business_financial_count_5yr: Optional[int] = Field(default=None, description="Count Business and Financial Operations Occupations workers per PUMA (5-year)")
    community_social_service_count_1yr: Optional[int] = Field(default=None, description="Count Community and Social Service Occupations workers per PUMA")
    community_social_service_count_5yr: Optional[int] = Field(default=None, description="Count Community and Social Service Occupations workers per PUMA (5-year)")
    computer_math_count_1yr: Optional[int] = Field(default=None, description="Count Computer and Mathematical Occupations workers per PUMA")
    computer_math_count_5yr: Optional[int] = Field(default=None, description="Count Computer and Mathematical Occupations workers per PUMA (5-year)")
    construction_extraction_count_1yr: Optional[int] = Field(default=None, description="Count Construction and Extraction Occupations workers per PUMA")
    construction_extraction_count_5yr: Optional[int] = Field(default=None, description="Count Construction and Extraction Occupations workers per PUMA (5-year)")
    construction_ind_count_1yr: Optional[int] = Field(default=None, description="Count Construction industry workers per PUMA")
    construction_ind_count_5yr: Optional[int] = Field(default=None, description="Count Construction industry workers per PUMA (5-year)")
    east_southeast_asian_pacific_lang_count_1yr: Optional[int] = Field(default=None, description="Count East/Southeast Asian and Pacific Island language speakers per PUMA")
    east_southeast_asian_pacific_lang_count_5yr: Optional[int] = Field(default=None, description="Count East/Southeast Asian and Pacific Island language speakers per PUMA (5-year)")
    education_healthcare_count_1yr: Optional[int] = Field(default=None, description="Count Educational Services and Health Care industry workers per PUMA")
    education_healthcare_count_5yr: Optional[int] = Field(default=None, description="Count Educational Services and Health Care industry workers per PUMA (5-year)")
    education_library_count_1yr: Optional[int] = Field(default=None, description="Count Educational Instruction and Library Occupations workers per PUMA")
    education_library_count_5yr: Optional[int] = Field(default=None, description="Count Educational Instruction and Library Occupations workers per PUMA (5-year)")
    english_only_count_1yr: Optional[int] = Field(default=None, description="Count English-only speakers per PUMA")
    english_only_count_5yr: Optional[int] = Field(default=None, description="Count English-only speakers per PUMA (5-year)")
    european_lang_count_1yr: Optional[int] = Field(default=None, description="Count European language speakers per PUMA")
    european_lang_count_5yr: Optional[int] = Field(default=None, description="Count European language speakers per PUMA (5-year)")
    farming_fishing_forestry_count_1yr: Optional[int] = Field(default=None, description="Count Farming, Fishing, and Forestry Occupations workers per PUMA")
    farming_fishing_forestry_count_5yr: Optional[int] = Field(default=None, description="Count Farming, Fishing, and Forestry Occupations workers per PUMA (5-year)")
    finance_real_estate_count_1yr: Optional[int] = Field(default=None, description="Count Finance, Insurance, and Real Estate industry workers per PUMA")
    finance_real_estate_count_5yr: Optional[int] = Field(default=None, description="Count Finance, Insurance, and Real Estate industry workers per PUMA (5-year)")
    food_preparation_serving_count_1yr: Optional[int] = Field(default=None, description="Count Food Preparation and Serving Related Occupations workers per PUMA")
    food_preparation_serving_count_5yr: Optional[int] = Field(default=None, description="Count Food Preparation and Serving Related Occupations workers per PUMA (5-year)")
    healthcare_practitioners_count_1yr: Optional[int] = Field(default=None, description="Count Healthcare Practitioners and Technical Occupations workers per PUMA")
    healthcare_practitioners_count_5yr: Optional[int] = Field(default=None, description="Count Healthcare Practitioners and Technical Occupations workers per PUMA (5-year)")
    healthcare_support_count_1yr: Optional[int] = Field(default=None, description="Count Healthcare Support Occupations workers per PUMA")
    healthcare_support_count_5yr: Optional[int] = Field(default=None, description="Count Healthcare Support Occupations workers per PUMA (5-year)")
    housing_unit_count_1yr: Optional[int] = Field(default=None, description="Count ACS PUMS housing units per PUMA")
    housing_unit_count_5yr: Optional[int] = Field(default=None, description="Count 5-year ACS PUMS housing units per PUMA")
    information_count_1yr: Optional[int] = Field(default=None, description="Count Information industry workers per PUMA")
    information_count_5yr: Optional[int] = Field(default=None, description="Count Information industry workers per PUMA (5-year)")
    installation_maintenance_repair_count_1yr: Optional[int] = Field(default=None, description="Count Installation, Maintenance, and Repair Occupations workers per PUMA")
    installation_maintenance_repair_count_5yr: Optional[int] = Field(default=None, description="Count Installation, Maintenance, and Repair Occupations workers per PUMA (5-year)")
    legal_count_1yr: Optional[int] = Field(default=None, description="Count Legal Occupations workers per PUMA")
    legal_count_5yr: Optional[int] = Field(default=None, description="Count Legal Occupations workers per PUMA (5-year)")
    life_physical_social_science_count_1yr: Optional[int] = Field(default=None, description="Count Life, Physical, and Social Science Occupations workers per PUMA")
    life_physical_social_science_count_5yr: Optional[int] = Field(default=None, description="Count Life, Physical, and Social Science Occupations workers per PUMA (5-year)")
    management_count_1yr: Optional[int] = Field(default=None, description="Count Management Occupations workers per PUMA")
    management_count_5yr: Optional[int] = Field(default=None, description="Count Management Occupations workers per PUMA (5-year)")
    manufacturing_count_1yr: Optional[int] = Field(default=None, description="Count Manufacturing industry workers per PUMA")
    manufacturing_count_5yr: Optional[int] = Field(default=None, description="Count Manufacturing industry workers per PUMA (5-year)")
    material_moving_count_1yr: Optional[int] = Field(default=None, description="Count Material Moving Occupations workers per PUMA")
    material_moving_count_5yr: Optional[int] = Field(default=None, description="Count Material Moving Occupations workers per PUMA (5-year)")
    mgmt_business_science_arts_count_1yr: Optional[int] = Field(default=None, description="Count Management/Business/Science/Arts workers per PUMA")
    mgmt_business_science_arts_count_5yr: Optional[int] = Field(default=None, description="Count Management, Business, Science, and Arts occupation workers per PUMA (5-year)")
    middle_eastern_african_lang_count_1yr: Optional[int] = Field(default=None, description="Count Middle Eastern, African, and other language speakers per PUMA")
    middle_eastern_african_lang_count_5yr: Optional[int] = Field(default=None, description="Count Middle Eastern, African, and other language speakers per PUMA (5-year)")
    military_count_1yr: Optional[int] = Field(default=None, description="Count Military Specific Occupations workers per PUMA")
    military_count_5yr: Optional[int] = Field(default=None, description="Count Military Specific Occupations workers per PUMA (5-year)")
    military_industry_count_1yr: Optional[int] = Field(default=None, description="Count Military industry workers per PUMA")
    military_industry_count_5yr: Optional[int] = Field(default=None, description="Count Military industry workers per PUMA (5-year)")
    natural_resources_construction_count_1yr: Optional[int] = Field(default=None, description="Count Natural Resources/Construction/Maintenance workers per PUMA")
    natural_resources_construction_count_5yr: Optional[int] = Field(default=None, description="Count Natural Resources, Construction, and Maintenance occupation workers per PUMA (5-year)")
    office_admin_support_count_1yr: Optional[int] = Field(default=None, description="Count Office and Administrative Support Occupations workers per PUMA")
    office_admin_support_count_5yr: Optional[int] = Field(default=None, description="Count Office and Administrative Support Occupations workers per PUMA (5-year)")
    other_services_ind_count_1yr: Optional[int] = Field(default=None, description="Count Other Services industry workers per PUMA")
    other_services_ind_count_5yr: Optional[int] = Field(default=None, description="Count Other Services industry workers per PUMA (5-year)")
    other_unspecified_lang_count_1yr: Optional[int] = Field(default=None, description="Count other and unspecified language speakers per PUMA")
    other_unspecified_lang_count_5yr: Optional[int] = Field(default=None, description="Count other and unspecified language speakers per PUMA (5-year)")
    pct_metro_tracts: Optional[float] = Field(default=None, description="Compute average RUCC score and rural/metro tract fractions for each PUMA")
    pct_rural_tracts: Optional[float] = Field(default=None, description="Compute average RUCC score and rural/metro tract fractions for each PUMA")
    personal_care_service_count_1yr: Optional[int] = Field(default=None, description="Count Personal Care and Service Occupations workers per PUMA")
    personal_care_service_count_5yr: Optional[int] = Field(default=None, description="Count Personal Care and Service Occupations workers per PUMA (5-year)")
    pop_estimate_1yr: Optional[int] = Field(default=None, description="Count ACS PUMS person records per PUMA as population estimate")
    pop_estimate_5yr: Optional[int] = Field(default=None, description="Count 5-year ACS PUMS person records per PUMA as population estimate")
    production_count_1yr: Optional[int] = Field(default=None, description="Count Production Occupations workers per PUMA")
    production_count_5yr: Optional[int] = Field(default=None, description="Count Production Occupations workers per PUMA (5-year)")
    production_transportation_count_1yr: Optional[int] = Field(default=None, description="Count Production/Transportation/Material Moving workers per PUMA")
    production_transportation_count_5yr: Optional[int] = Field(default=None, description="Count Production, Transportation, and Material Moving occupation workers per PUMA (5-year)")
    professional_management_ind_count_1yr: Optional[int] = Field(default=None, description="Count Professional, Scientific, Management, and Administrative industry workers per PUMA")
    professional_management_ind_count_5yr: Optional[int] = Field(default=None, description="Count Professional, Scientific, Management, and Administrative industry workers per PUMA (5-year)")
    protective_service_count_1yr: Optional[int] = Field(default=None, description="Count Protective Service Occupations workers per PUMA")
    protective_service_count_5yr: Optional[int] = Field(default=None, description="Count Protective Service Occupations workers per PUMA (5-year)")
    public_administration_count_1yr: Optional[int] = Field(default=None, description="Count Public Administration industry workers per PUMA")
    public_administration_count_5yr: Optional[int] = Field(default=None, description="Count Public Administration industry workers per PUMA (5-year)")
    retail_trade_count_1yr: Optional[int] = Field(default=None, description="Count Retail Trade industry workers per PUMA")
    retail_trade_count_5yr: Optional[int] = Field(default=None, description="Count Retail Trade industry workers per PUMA (5-year)")
    sales_count_1yr: Optional[int] = Field(default=None, description="Count Sales and Related Occupations workers per PUMA")
    sales_count_5yr: Optional[int] = Field(default=None, description="Count Sales and Related Occupations workers per PUMA (5-year)")
    sales_office_count_1yr: Optional[int] = Field(default=None, description="Count Sales and Office workers per PUMA")
    sales_office_count_5yr: Optional[int] = Field(default=None, description="Count Sales and Office occupation workers per PUMA (5-year)")
    service_count_1yr: Optional[int] = Field(default=None, description="Count Service workers per PUMA")
    service_count_5yr: Optional[int] = Field(default=None, description="Count Service occupation workers per PUMA (5-year)")
    south_central_asian_lang_count_1yr: Optional[int] = Field(default=None, description="Count South and Central Asian language speakers per PUMA")
    south_central_asian_lang_count_5yr: Optional[int] = Field(default=None, description="Count South and Central Asian language speakers per PUMA (5-year)")
    spanish_count_1yr: Optional[int] = Field(default=None, description="Count Spanish speakers per PUMA")
    spanish_count_5yr: Optional[int] = Field(default=None, description="Count Spanish speakers per PUMA (5-year)")
    transportation_count_1yr: Optional[int] = Field(default=None, description="Count Transportation Occupations workers per PUMA")
    transportation_count_5yr: Optional[int] = Field(default=None, description="Count Transportation Occupations workers per PUMA (5-year)")
    transportation_utilities_count_1yr: Optional[int] = Field(default=None, description="Count Transportation, Warehousing, and Utilities industry workers per PUMA")
    transportation_utilities_count_5yr: Optional[int] = Field(default=None, description="Count Transportation, Warehousing, and Utilities industry workers per PUMA (5-year)")
    veteran_count_1yr: Optional[int] = Field(default=None, description="Count veteran person records per PUMA")
    veteran_count_5yr: Optional[int] = Field(default=None, description="Count veteran person records per PUMA from 5-year ACS")
    wholesale_trade_count_1yr: Optional[int] = Field(default=None, description="Count Wholesale Trade industry workers per PUMA")
    wholesale_trade_count_5yr: Optional[int] = Field(default=None, description="Count Wholesale Trade industry workers per PUMA (5-year)")


class PersonSurvey1yr(BaseModel):
    pass


class PersonSurvey5yr(BaseModel):
    pass


class School(BaseModel):
    federal_revenue_share: Optional[float] = Field(default=None, description="Compute the share of total revenue derived from federal sources for each School node")
    is_charter: Optional[bool] = Field(default=None, description="Flag School nodes that are charter schools")
    is_high_federal_dependency: Optional[bool] = Field(default=None, description="Flag schools where federal revenue exceeds 25% of total revenue, indicating elevated dependency on federal funding")


class State(BaseModel):
    agriculture_mining_ind_count_1yr: Optional[int] = Field(default=None, description="Roll up Agriculture, Forestry, Fishing, Hunting, and Mining industry worker counts from PUMA to State")
    agriculture_mining_ind_count_5yr: Optional[int] = Field(default=None, description="Roll up Agriculture, Forestry, Fishing, Hunting, and Mining industry worker counts from PUMA to State (5-year)")
    architecture_engineering_count_1yr: Optional[int] = Field(default=None, description="Roll up Architecture and Engineering Occupations worker counts from PUMA to State")
    architecture_engineering_count_5yr: Optional[int] = Field(default=None, description="Roll up Architecture and Engineering Occupations worker counts from PUMA to State (5-year)")
    arts_design_entertainment_count_1yr: Optional[int] = Field(default=None, description="Roll up Arts, Design, Entertainment, Sports, and Media Occupations worker counts from PUMA to State")
    arts_design_entertainment_count_5yr: Optional[int] = Field(default=None, description="Roll up Arts, Design, Entertainment, Sports, and Media Occupations worker counts from PUMA to State (5-year)")
    arts_entertainment_food_count_1yr: Optional[int] = Field(default=None, description="Roll up Arts, Entertainment, Recreation, and Food Services industry worker counts from PUMA to State")
    arts_entertainment_food_count_5yr: Optional[int] = Field(default=None, description="Roll up Arts, Entertainment, Recreation, and Food Services industry worker counts from PUMA to State (5-year)")
    avg_rucc_score: Optional[float] = Field(default=None, description="Roll up RUCC stats from PUMA to State and count rural and metro counties directly")
    building_grounds_count_1yr: Optional[int] = Field(default=None, description="Roll up Building and Grounds Cleaning and Maintenance Occupations worker counts from PUMA to State")
    building_grounds_count_5yr: Optional[int] = Field(default=None, description="Roll up Building and Grounds Cleaning and Maintenance Occupations worker counts from PUMA to State (5-year)")
    business_financial_count_1yr: Optional[int] = Field(default=None, description="Roll up Business and Financial Operations Occupations worker counts from PUMA to State")
    business_financial_count_5yr: Optional[int] = Field(default=None, description="Roll up Business and Financial Operations Occupations worker counts from PUMA to State (5-year)")
    community_social_service_count_1yr: Optional[int] = Field(default=None, description="Roll up Community and Social Service Occupations worker counts from PUMA to State")
    community_social_service_count_5yr: Optional[int] = Field(default=None, description="Roll up Community and Social Service Occupations worker counts from PUMA to State (5-year)")
    computer_math_count_1yr: Optional[int] = Field(default=None, description="Roll up Computer and Mathematical Occupations worker counts from PUMA to State")
    computer_math_count_5yr: Optional[int] = Field(default=None, description="Roll up Computer and Mathematical Occupations worker counts from PUMA to State (5-year)")
    construction_extraction_count_1yr: Optional[int] = Field(default=None, description="Roll up Construction and Extraction Occupations worker counts from PUMA to State")
    construction_extraction_count_5yr: Optional[int] = Field(default=None, description="Roll up Construction and Extraction Occupations worker counts from PUMA to State (5-year)")
    construction_ind_count_1yr: Optional[int] = Field(default=None, description="Roll up Construction industry worker counts from PUMA to State")
    construction_ind_count_5yr: Optional[int] = Field(default=None, description="Roll up Construction industry worker counts from PUMA to State (5-year)")
    east_southeast_asian_pacific_lang_count_1yr: Optional[int] = Field(default=None, description="Roll up East/Southeast Asian and Pacific Island language speaker counts from PUMA to State")
    east_southeast_asian_pacific_lang_count_5yr: Optional[int] = Field(default=None, description="Roll up East/Southeast Asian and Pacific Island language speaker counts from PUMA to State (5-year)")
    education_healthcare_count_1yr: Optional[int] = Field(default=None, description="Roll up Educational Services and Health Care industry worker counts from PUMA to State")
    education_healthcare_count_5yr: Optional[int] = Field(default=None, description="Roll up Educational Services and Health Care industry worker counts from PUMA to State (5-year)")
    education_library_count_1yr: Optional[int] = Field(default=None, description="Roll up Educational Instruction and Library Occupations worker counts from PUMA to State")
    education_library_count_5yr: Optional[int] = Field(default=None, description="Roll up Educational Instruction and Library Occupations worker counts from PUMA to State (5-year)")
    english_only_count_1yr: Optional[int] = Field(default=None, description="Roll up English-only speaker counts from PUMA to State")
    english_only_count_5yr: Optional[int] = Field(default=None, description="Roll up English-only speaker counts from PUMA to State (5-year)")
    european_lang_count_1yr: Optional[int] = Field(default=None, description="Roll up European language speaker counts from PUMA to State")
    european_lang_count_5yr: Optional[int] = Field(default=None, description="Roll up European language speaker counts from PUMA to State (5-year)")
    farming_fishing_forestry_count_1yr: Optional[int] = Field(default=None, description="Roll up Farming, Fishing, and Forestry Occupations worker counts from PUMA to State")
    farming_fishing_forestry_count_5yr: Optional[int] = Field(default=None, description="Roll up Farming, Fishing, and Forestry Occupations worker counts from PUMA to State (5-year)")
    federal_award_count: Optional[int] = Field(default=None, description="Count federal contract award transactions per state by place of performance")
    finance_real_estate_count_1yr: Optional[int] = Field(default=None, description="Roll up Finance, Insurance, and Real Estate industry worker counts from PUMA to State")
    finance_real_estate_count_5yr: Optional[int] = Field(default=None, description="Roll up Finance, Insurance, and Real Estate industry worker counts from PUMA to State (5-year)")
    food_preparation_serving_count_1yr: Optional[int] = Field(default=None, description="Roll up Food Preparation and Serving Related Occupations worker counts from PUMA to State")
    food_preparation_serving_count_5yr: Optional[int] = Field(default=None, description="Roll up Food Preparation and Serving Related Occupations worker counts from PUMA to State (5-year)")
    healthcare_practitioners_count_1yr: Optional[int] = Field(default=None, description="Roll up Healthcare Practitioners and Technical Occupations worker counts from PUMA to State")
    healthcare_practitioners_count_5yr: Optional[int] = Field(default=None, description="Roll up Healthcare Practitioners and Technical Occupations worker counts from PUMA to State (5-year)")
    healthcare_support_count_1yr: Optional[int] = Field(default=None, description="Roll up Healthcare Support Occupations worker counts from PUMA to State")
    healthcare_support_count_5yr: Optional[int] = Field(default=None, description="Roll up Healthcare Support Occupations worker counts from PUMA to State (5-year)")
    information_count_1yr: Optional[int] = Field(default=None, description="Roll up Information industry worker counts from PUMA to State")
    information_count_5yr: Optional[int] = Field(default=None, description="Roll up Information industry worker counts from PUMA to State (5-year)")
    installation_maintenance_repair_count_1yr: Optional[int] = Field(default=None, description="Roll up Installation, Maintenance, and Repair Occupations worker counts from PUMA to State")
    installation_maintenance_repair_count_5yr: Optional[int] = Field(default=None, description="Roll up Installation, Maintenance, and Repair Occupations worker counts from PUMA to State (5-year)")
    is_large_state: Optional[float] = Field(default=None, description="Do a simple test aggregation")
    legal_count_1yr: Optional[int] = Field(default=None, description="Roll up Legal Occupations worker counts from PUMA to State")
    legal_count_5yr: Optional[int] = Field(default=None, description="Roll up Legal Occupations worker counts from PUMA to State (5-year)")
    life_physical_social_science_count_1yr: Optional[int] = Field(default=None, description="Roll up Life, Physical, and Social Science Occupations worker counts from PUMA to State")
    life_physical_social_science_count_5yr: Optional[int] = Field(default=None, description="Roll up Life, Physical, and Social Science Occupations worker counts from PUMA to State (5-year)")
    management_count_1yr: Optional[int] = Field(default=None, description="Roll up Management Occupations worker counts from PUMA to State")
    management_count_5yr: Optional[int] = Field(default=None, description="Roll up Management Occupations worker counts from PUMA to State (5-year)")
    manufacturing_count_1yr: Optional[int] = Field(default=None, description="Roll up Manufacturing industry worker counts from PUMA to State")
    manufacturing_count_5yr: Optional[int] = Field(default=None, description="Roll up Manufacturing industry worker counts from PUMA to State (5-year)")
    material_moving_count_1yr: Optional[int] = Field(default=None, description="Roll up Material Moving Occupations worker counts from PUMA to State")
    material_moving_count_5yr: Optional[int] = Field(default=None, description="Roll up Material Moving Occupations worker counts from PUMA to State (5-year)")
    metro_county_count: Optional[int] = Field(default=None, description="Roll up RUCC stats from PUMA to State and count rural and metro counties directly")
    mgmt_business_science_arts_count_1yr: Optional[int] = Field(default=None, description="Roll up Management/Business/Science/Arts worker counts from PUMA to State")
    mgmt_business_science_arts_count_5yr: Optional[int] = Field(default=None, description="Roll up Management, Business, Science, and Arts worker counts from PUMA to State (5-year)")
    middle_eastern_african_lang_count_1yr: Optional[int] = Field(default=None, description="Roll up Middle Eastern and African language speaker counts from PUMA to State")
    middle_eastern_african_lang_count_5yr: Optional[int] = Field(default=None, description="Roll up Middle Eastern and African language speaker counts from PUMA to State (5-year)")
    military_count_1yr: Optional[int] = Field(default=None, description="Roll up Military Specific Occupations worker counts from PUMA to State")
    military_count_5yr: Optional[int] = Field(default=None, description="Roll up Military Specific Occupations worker counts from PUMA to State (5-year)")
    military_industry_count_1yr: Optional[int] = Field(default=None, description="Roll up Military industry worker counts from PUMA to State")
    military_industry_count_5yr: Optional[int] = Field(default=None, description="Roll up Military industry worker counts from PUMA to State (5-year)")
    natural_resources_construction_count_1yr: Optional[int] = Field(default=None, description="Roll up Natural Resources/Construction/Maintenance worker counts from PUMA to State")
    natural_resources_construction_count_5yr: Optional[int] = Field(default=None, description="Roll up Natural Resources, Construction, and Maintenance worker counts from PUMA to State (5-year)")
    num_counties: Optional[int] = Field(default=None, description="Do a simple test aggregation")
    office_admin_support_count_1yr: Optional[int] = Field(default=None, description="Roll up Office and Administrative Support Occupations worker counts from PUMA to State")
    office_admin_support_count_5yr: Optional[int] = Field(default=None, description="Roll up Office and Administrative Support Occupations worker counts from PUMA to State (5-year)")
    other_services_ind_count_1yr: Optional[int] = Field(default=None, description="Roll up Other Services industry worker counts from PUMA to State")
    other_services_ind_count_5yr: Optional[int] = Field(default=None, description="Roll up Other Services industry worker counts from PUMA to State (5-year)")
    other_unspecified_lang_count_1yr: Optional[int] = Field(default=None, description="Roll up other and unspecified language speaker counts from PUMA to State")
    other_unspecified_lang_count_5yr: Optional[int] = Field(default=None, description="Roll up other and unspecified language speaker counts from PUMA to State (5-year)")
    pct_metro_counties: Optional[float] = Field(default=None, description="Roll up RUCC stats from PUMA to State and count rural and metro counties directly")
    pct_rural_counties: Optional[float] = Field(default=None, description="Roll up RUCC stats from PUMA to State and count rural and metro counties directly")
    personal_care_service_count_1yr: Optional[int] = Field(default=None, description="Roll up Personal Care and Service Occupations worker counts from PUMA to State")
    personal_care_service_count_5yr: Optional[int] = Field(default=None, description="Roll up Personal Care and Service Occupations worker counts from PUMA to State (5-year)")
    production_count_1yr: Optional[int] = Field(default=None, description="Roll up Production Occupations worker counts from PUMA to State")
    production_count_5yr: Optional[int] = Field(default=None, description="Roll up Production Occupations worker counts from PUMA to State (5-year)")
    production_transportation_count_1yr: Optional[int] = Field(default=None, description="Roll up Production/Transportation/Material Moving worker counts from PUMA to State")
    production_transportation_count_5yr: Optional[int] = Field(default=None, description="Roll up Production, Transportation, and Material Moving worker counts from PUMA to State (5-year)")
    professional_management_ind_count_1yr: Optional[int] = Field(default=None, description="Roll up Professional, Scientific, Management, and Administrative industry worker counts from PUMA to State")
    professional_management_ind_count_5yr: Optional[int] = Field(default=None, description="Roll up Professional, Scientific, Management, and Administrative industry worker counts from PUMA to State (5-year)")
    protective_service_count_1yr: Optional[int] = Field(default=None, description="Roll up Protective Service Occupations worker counts from PUMA to State")
    protective_service_count_5yr: Optional[int] = Field(default=None, description="Roll up Protective Service Occupations worker counts from PUMA to State (5-year)")
    public_administration_count_1yr: Optional[int] = Field(default=None, description="Roll up Public Administration industry worker counts from PUMA to State")
    public_administration_count_5yr: Optional[int] = Field(default=None, description="Roll up Public Administration industry worker counts from PUMA to State (5-year)")
    retail_trade_count_1yr: Optional[int] = Field(default=None, description="Roll up Retail Trade industry worker counts from PUMA to State")
    retail_trade_count_5yr: Optional[int] = Field(default=None, description="Roll up Retail Trade industry worker counts from PUMA to State (5-year)")
    rural_county_count: Optional[int] = Field(default=None, description="Roll up RUCC stats from PUMA to State and count rural and metro counties directly")
    sales_count_1yr: Optional[int] = Field(default=None, description="Roll up Sales and Related Occupations worker counts from PUMA to State")
    sales_count_5yr: Optional[int] = Field(default=None, description="Roll up Sales and Related Occupations worker counts from PUMA to State (5-year)")
    sales_office_count_1yr: Optional[int] = Field(default=None, description="Roll up Sales and Office worker counts from PUMA to State")
    sales_office_count_5yr: Optional[int] = Field(default=None, description="Roll up Sales and Office worker counts from PUMA to State (5-year)")
    school_avg_per_pupil_spend: Optional[float] = Field(default=None, description="Average per-pupil expenditure across all schools in each State; prerequisite for county deviation calculation")
    service_count_1yr: Optional[int] = Field(default=None, description="Roll up Service worker counts from PUMA to State")
    service_count_5yr: Optional[int] = Field(default=None, description="Roll up Service occupation worker counts from PUMA to State (5-year)")
    south_central_asian_lang_count_1yr: Optional[int] = Field(default=None, description="Roll up South and Central Asian language speaker counts from PUMA to State")
    south_central_asian_lang_count_5yr: Optional[int] = Field(default=None, description="Roll up South and Central Asian language speaker counts from PUMA to State (5-year)")
    spanish_count_1yr: Optional[int] = Field(default=None, description="Roll up Spanish speaker counts from PUMA to State")
    spanish_count_5yr: Optional[int] = Field(default=None, description="Roll up Spanish speaker counts from PUMA to State (5-year)")
    transportation_count_1yr: Optional[int] = Field(default=None, description="Roll up Transportation Occupations worker counts from PUMA to State")
    transportation_count_5yr: Optional[int] = Field(default=None, description="Roll up Transportation Occupations worker counts from PUMA to State (5-year)")
    transportation_utilities_count_1yr: Optional[int] = Field(default=None, description="Roll up Transportation, Warehousing, and Utilities industry worker counts from PUMA to State")
    transportation_utilities_count_5yr: Optional[int] = Field(default=None, description="Roll up Transportation, Warehousing, and Utilities industry worker counts from PUMA to State (5-year)")
    wholesale_trade_count_1yr: Optional[int] = Field(default=None, description="Roll up Wholesale Trade industry worker counts from PUMA to State")
    wholesale_trade_count_5yr: Optional[int] = Field(default=None, description="Roll up Wholesale Trade industry worker counts from PUMA to State (5-year)")


class Tract(BaseModel):
    earthquake_building_eal: Optional[float] = Field(default=None, description="Set earthquake risk score, rating, and annualized building loss on each Tract node")
    earthquake_risk_rating: Optional[float] = Field(default=None, description="Set earthquake risk score, rating, and annualized building loss on each Tract node")
    flood_building_eal: Optional[float] = Field(default=None, description="Set riverine flood risk score, rating, and annualized building loss on each Tract node")
    flood_risk_rating: Optional[float] = Field(default=None, description="Set riverine flood risk score, rating, and annualized building loss on each Tract node")
    hail_building_eal: Optional[float] = Field(default=None, description="Set tornado, strong wind, and hail risk scores and annualized building losses on each Tract node")
    hail_risk_score: Optional[float] = Field(default=None, description="Set tornado, strong wind, and hail risk scores and annualized building losses on each Tract node")
    hurricane_building_eal: Optional[float] = Field(default=None, description="Set hurricane risk score, rating, and annualized building loss on each Tract node")
    hurricane_risk_rating: Optional[float] = Field(default=None, description="Set hurricane risk score, rating, and annualized building loss on each Tract node")
    is_elevated_property_risk: Optional[float] = Field(default=None, description="Flag tracts whose combined annualized building loss across all six hazards exceeds $100,000 per year")
    is_high_earthquake_risk: Optional[float] = Field(default=None, description="Flag tracts where earthquake risk score exceeds the 75th-percentile threshold (score > 75)")
    is_high_flood_risk: Optional[float] = Field(default=None, description="Flag tracts where riverine flood risk score exceeds the 75th-percentile threshold (score > 75)")
    is_high_hurricane_risk: Optional[float] = Field(default=None, description="Flag tracts where hurricane risk score exceeds the 75th-percentile threshold (score > 75)")
    is_high_wildfire_risk: Optional[float] = Field(default=None, description="Flag tracts where wildfire risk score exceeds the 75th-percentile threshold (score > 75)")
    nri_combined_building_eal: Optional[float] = Field(default=None, description="Sum annualized building losses across all six key hazards to produce a single per-tract property-risk estimate")
    nri_eal_score: Optional[float] = Field(default=None, description="Copy NRI composite risk, EAL, social vulnerability, and resilience scores onto each Tract node")
    nri_eal_total: Optional[float] = Field(default=None, description="Copy NRI composite risk, EAL, social vulnerability, and resilience scores onto each Tract node")
    nri_earthquake_risk_score: Optional[float] = Field(default=None, description="Set earthquake risk score, rating, and annualized building loss on each Tract node")
    nri_flood_risk_score: Optional[float] = Field(default=None, description="Set riverine flood risk score, rating, and annualized building loss on each Tract node")
    nri_hurricane_risk_score: Optional[float] = Field(default=None, description="Set hurricane risk score, rating, and annualized building loss on each Tract node")
    nri_resilience: Optional[float] = Field(default=None, description="Copy NRI composite risk, EAL, social vulnerability, and resilience scores onto each Tract node")
    nri_risk_rating: Optional[float] = Field(default=None, description="Copy NRI composite risk, EAL, social vulnerability, and resilience scores onto each Tract node")
    nri_risk_score: Optional[float] = Field(default=None, description="Copy NRI composite risk, EAL, social vulnerability, and resilience scores onto each Tract node")
    nri_social_vulnerability: Optional[float] = Field(default=None, description="Copy NRI composite risk, EAL, social vulnerability, and resilience scores onto each Tract node")
    nri_tornado_risk_score: Optional[float] = Field(default=None, description="Set tornado, strong wind, and hail risk scores and annualized building losses on each Tract node")
    nri_wildfire_risk_score: Optional[float] = Field(default=None, description="Set wildfire risk score, rating, and annualized building loss on each Tract node")
    rucc_2023: Optional[int] = Field(default=None, description="Propagate county RUCC code and description down to each tract")
    rucc_description: Optional[str] = Field(default=None, description="Propagate county RUCC code and description down to each tract")
    tornado_building_eal: Optional[float] = Field(default=None, description="Set tornado, strong wind, and hail risk scores and annualized building losses on each Tract node")
    wildfire_building_eal: Optional[float] = Field(default=None, description="Set wildfire risk score, rating, and annualized building loss on each Tract node")
    wildfire_risk_rating: Optional[float] = Field(default=None, description="Set wildfire risk score, rating, and annualized building loss on each Tract node")
    wind_building_eal: Optional[float] = Field(default=None, description="Set tornado, strong wind, and hail risk scores and annualized building losses on each Tract node")
    wind_risk_score: Optional[float] = Field(default=None, description="Set tornado, strong wind, and hail risk scores and annualized building losses on each Tract node")


# Relationship endpoint pairs extracted from the pipeline config.
# Format: {RelationshipType: [(source_label, target_label), ...]}
RELATIONSHIP_ENDPOINTS: dict[str, list[tuple[str, str]]] = {
    "EMPLOYED_IN": [
        ("PersonSurvey1yr", "IndustryGroup"),
        ("PersonSurvey5yr", "IndustryGroup"),
    ],
    "LOCATED_IN": [
        ("County", "State"),
        ("CountyElectionResult", "County"),
        ("HousingSurvey1yr", "PUMA"),
        ("HousingSurvey5yr", "PUMA"),
        ("OSMNode", "Tract"),
        ("PUMA", "State"),
        ("PersonSurvey1yr", "PUMA"),
        ("PersonSurvey5yr", "PUMA"),
        ("Tract", "County"),
        ("Tract", "PUMA"),
    ],
    "RESIDES_IN": [
        ("PersonSurvey1yr", "HousingSurvey1yr"),
        ("PersonSurvey5yr", "HousingSurvey5yr"),
    ],
    "SPEAKS": [
        ("PersonSurvey1yr", "LanguageGroup"),
        ("PersonSurvey5yr", "LanguageGroup"),
    ],
    "WORKS_IN": [
        ("PersonSurvey1yr", "OccupationGroup"),
        ("PersonSurvey1yr", "OccupationSubgroup"),
        ("PersonSurvey5yr", "OccupationGroup"),
        ("PersonSurvey5yr", "OccupationSubgroup"),
    ],
}
