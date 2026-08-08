from typing import Optional

from pydantic import BaseModel


class GenericRequest(BaseModel):
    """For metadata common to each request"""

    account_key: str


class Address(BaseModel):
    """A single address"""

    street_number: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]


class SingleAddressRequest(GenericRequest):
    """A request for data on a single request"""

    address: Address


class BatchAddressRequest(GenericRequest):
    """A single request for a list of addresses"""

    address_list: list[Address]
