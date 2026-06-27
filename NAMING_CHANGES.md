# Naming Convention Standardization

Applied to: `fod_input_configs.yaml.template`, `fod_input_configs.yaml`, `graph_models.py`

## Principles

1. **Geographic containment relationships** → all use `LOCATED_IN`
2. **Node type symmetry** → 1-year ACS variants carry `1yr` suffix matching 5-year variants
3. **Boolean flags** → all use `is_` prefix
4. **Domain prefixes** → NRI properties prefix with `nri_`, CJARS with `cjars_`, school-derived county/state properties with `school_`
5. **`pct_rural_tracts` / `pct_metro_tracts` on State** → renamed to reflect actual unit (counties, not tracts)
6. **Miscellaneous clarity** → `rucc_avg` → `avg_rucc_score`, `award_count` → `federal_award_count`, `over_hundred_counties` → `is_large_state`

## Relationship types

| Old | New | Where |
|-----|-----|-------|
| `IN` | `LOCATED_IN` | county_in_state, tract_in_county, tract_in_puma, puma_in_state, osm_node_in_tract |
| `LIVES_IN` | `LOCATED_IN` | person_lives_in_puma |

## Node (entity) types

| Old | New |
|-----|-----|
| `HousingSurvey` | `HousingSurvey1yr` |
| `PersonSurvey` | `PersonSurvey1yr` |

## Boolean flag properties (→ `is_` prefix)

| Old | New | Node |
|-----|-----|------|
| `high_flood_risk` | `is_high_flood_risk` | Tract |
| `high_wildfire_risk` | `is_high_wildfire_risk` | Tract |
| `high_hurricane_risk` | `is_high_hurricane_risk` | Tract |
| `high_earthquake_risk` | `is_high_earthquake_risk` | Tract |
| `elevated_property_risk` | `is_elevated_property_risk` | Tract |
| `high_federal_dependency` | `is_high_federal_dependency` | School |
| `over_hundred_counties` | `is_large_state` | State |

## NRI properties (add `nri_` prefix)

| Old | New | Node |
|-----|-----|------|
| `flood_risk_score` | `nri_flood_risk_score` | Tract |
| `wildfire_risk_score` | `nri_wildfire_risk_score` | Tract |
| `hurricane_risk_score` | `nri_hurricane_risk_score` | Tract |
| `earthquake_risk_score` | `nri_earthquake_risk_score` | Tract |
| `tornado_risk_score` | `nri_tornado_risk_score` | Tract |
| `combined_building_eal` | `nri_combined_building_eal` | Tract |

## CJARS properties (add `cjars_` prefix)

| Old | New | Node |
|-----|-----|------|
| `felony_employment_rate` | `cjars_felony_employment_rate` | County |
| `felony_earnings` | `cjars_felony_earnings` | County |
| `felony_medicaid_rate` | `cjars_felony_medicaid_rate` | County |
| `felony_ssi_rate` | `cjars_felony_ssi_rate` | County |
| `felony_hud_rate` | `cjars_felony_hud_rate` | County |
| `felony_death_rate` | `cjars_felony_death_rate` | County |
| `misdemeanor_employment_rate` | `cjars_misdemeanor_employment_rate` | County |
| `misdemeanor_earnings` | `cjars_misdemeanor_earnings` | County |
| `misdemeanor_medicaid_rate` | `cjars_misdemeanor_medicaid_rate` | County |
| `misdemeanor_ssi_rate` | `cjars_misdemeanor_ssi_rate` | County |
| `misdemeanor_hud_rate` | `cjars_misdemeanor_hud_rate` | County |
| `misdemeanor_death_rate` | `cjars_misdemeanor_death_rate` | County |

## School-derived county/state properties (add `school_` prefix)

| Old | New | Node |
|-----|-----|------|
| `avg_per_pupil_spend` | `school_avg_per_pupil_spend` | County, State |
| `total_enrollment` | `school_total_enrollment` | County |
| `avg_federal_revenue_share` | `school_avg_federal_revenue_share` | County |
| `avg_local_revenue_share` | `school_avg_local_revenue_share` | County |
| `avg_instruction_spend_share` | `school_avg_instruction_spend_share` | County |
| `charter_school_share` | `school_charter_share` | County |
| `special_ed_school_share` | `school_special_ed_share` | County |
| `high_federal_dependency_school_share` | `school_high_federal_dependency_share` | County |
| `per_pupil_spend_vs_state_avg` | `school_per_pupil_spend_vs_state_avg` | County |
| `elementary_school_count` | `school_elementary_count` | County |
| `middle_school_count` | `school_middle_count` | County |
| `high_school_count` | `school_high_count` | County |

## Miscellaneous

| Old | New | Node | Reason |
|-----|-----|------|--------|
| `pct_rural_tracts` | `pct_rural_counties` | State | Was counting counties, not tracts |
| `pct_metro_tracts` | `pct_metro_counties` | State | Was counting counties, not tracts |
| `rucc_avg` | `avg_rucc_score` | PUMA, State | Consistent noun-last convention |
| `award_count` | `federal_award_count` | State | Clarifies domain |

## Out of scope

- `LIVES_IN` in `test_pipeline.py` and `test_load_datasets.py` — synthetic test relationship names, not config-derived
- `LIVES_IN_PUMA` in `pipeline.py` and `test_generic_loader_relationships.py` — separate hard-coded derivation, independent of relationship_type config field
- Year suffixes on `pres_*_2024` properties — election data is meaningfully vintage-specific; other sources use single-vintage replacement, so year is documented in field descriptions
