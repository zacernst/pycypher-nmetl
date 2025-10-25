from pydantic import BaseModel, Field, confloat

class ThresholdRecommendationRequest(BaseModel):
    account_id: str = Field(..., description="New Relic account ID")
    nrql_query: str = Field(..., description="NRQL query string")
    context: ThresholdContext = Field(..., description="Request context")

class ThresholdContext(BaseModel):
    threshold_operator: str = Field(..., description="Threshold comparison operator")
    sensitivity: confloat(gt=0.0, le=1.0) = Field(..., description="Sensitivity factor")

class ThresholdRecommendationFacet(BaseModel):
    threshold_value: float = Field(..., description="Recommended threshold value")
    facet_values: list[str] = Field(..., description="Associated facet values")
    confidence_score: float = Field(..., description="Recommendation confidence")
    
class ThresholdRecommendationResponse(BaseModel):
    threshold_list: list[ThresholdRecommendationFacet] = Field(
        ..., description="List of threshold recommendations"
    )
