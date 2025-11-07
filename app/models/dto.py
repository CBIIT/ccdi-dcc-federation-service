"""
Data Transfer Objects (DTOs) for the CCDI Federation Service.

This module contains Pydantic models for request/response serialization
based on the OpenAPI specification.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from enum import Enum

from pydantic import BaseModel, Field, ConfigDict


# ============================================================================
# Base Models
# ============================================================================

class BaseIdentifier(BaseModel):
    """Base identifier model."""
    organization: str = Field(..., description="Organization identifier")
    name: str = Field(..., description="Name")


class NamespaceIdentifier(BaseIdentifier):
    """Namespace identifier model."""
    pass


class OrganizationIdentifier(BaseModel):
    """Organization identifier model."""
    model_config = ConfigDict(extra="forbid")
    
    identifier: str = Field(..., description="Organization identifier")


# ============================================================================
# Metadata Field Models
# ============================================================================

class FieldDetails(BaseModel):
    """Details regarding the harmonization process."""
    method: Optional[str] = None
    harmonizer: Optional[str] = None
    url: Optional[str] = None


class UnharmonizedField(BaseModel):
    """Unharmonized metadata field."""
    value: Any = Field(..., description="Field value")
    ancestors: Optional[List[str]] = Field(default=None, exclude=True)
    details: Optional[FieldDetails] = None
    comment: Optional[str] = None


class HarmonizedStandard(BaseModel):
    """Standard to which a field is harmonized."""
    name: Optional[str] = Field(None, description="Standard name")
    url: Optional[str] = Field(None, description="Standard URL")


class HarmonizedFieldDescription(BaseModel):
    """Harmonized metadata field description."""
    harmonized: bool = Field(True, description="Always true for harmonized fields")
    path: str = Field(..., description="Dot-delimited path to field location")
    wiki_url: str = Field(..., description="Wiki URL for field documentation")
    standard: Optional[HarmonizedStandard] = None


class MetadataFieldInfo(BaseModel):
    """Metadata field information for /metadata/fields endpoint."""
    path: str = Field(..., description="Field path")
    harmonized: bool = Field(..., description="Whether the field is harmonized")
    wiki_url: str = Field(..., description="Wiki URL for field documentation")
    standard: HarmonizedStandard = Field(..., description="Standard information")


class MetadataFieldsInfoResponse(BaseModel):
    """Response for /metadata/fields/{field_type} endpoint."""
    fields: List[MetadataFieldInfo] = Field(..., description="List of metadata fields")


class UnharmonizedFieldDescription(BaseModel):
    """Unharmonized metadata field description."""
    harmonized: bool = Field(False, description="Always false for unharmonized fields")
    name: Optional[str] = None
    description: Optional[str] = None
    path: str = Field(..., description="Dot-delimited path to field location")
    standard: Optional[str] = None
    url: Optional[str] = None


FieldDescription = Union[HarmonizedFieldDescription, UnharmonizedFieldDescription]


# ============================================================================
# Gateway Models
# ============================================================================

class GatewayKind(str, Enum):
    """Gateway access kinds."""
    OPEN = "Open"
    REGISTERED = "Registered"
    CONTROLLED = "Controlled"
    CLOSED = "Closed"


class LinkKind(str, Enum):
    """Link types."""
    DIRECT = "Direct"
    APPROXIMATE = "Approximate"
    INFORMATIONAL = "Informational"
    MAILTO = "MailTo"


class DirectLink(BaseModel):
    """Direct link to resource."""
    url: str = Field(..., description="Resource URL")
    kind: LinkKind = Field(LinkKind.DIRECT)


class ApproximateLink(BaseModel):
    """Approximate link with instructions."""
    url: str = Field(..., description="Approximate resource URL")
    instructions: str = Field(..., description="Manual instructions")
    kind: LinkKind = Field(LinkKind.APPROXIMATE)


class InformationalLink(BaseModel):
    """Informational link about access."""
    url: str = Field(..., description="Information URL")
    kind: LinkKind = Field(LinkKind.INFORMATIONAL)


class MailToLink(BaseModel):
    """Email link for access requests."""
    url: str = Field(..., description="Email URL")
    instructions: str = Field(..., description="Email instructions")
    kind: LinkKind = Field(LinkKind.MAILTO)


GatewayLink = Union[DirectLink, ApproximateLink, InformationalLink, MailToLink]


class ClosedStatus(str, Enum):
    """Closed gateway status."""
    INDEFINITELY_CLOSED = "IndefinitelyClosed"
    AWAITING_PUBLICATION = "AwaitingPublication"
    EMBARGOED = "Embargoed"


class IndefinitelyClosedGateway(BaseModel):
    """Indefinitely closed gateway."""
    status: ClosedStatus = Field(ClosedStatus.INDEFINITELY_CLOSED)
    description: str = Field(..., description="Gateway description")
    kind: GatewayKind = Field(GatewayKind.CLOSED)


class AwaitingPublicationGateway(BaseModel):
    """Gateway awaiting publication."""
    status: ClosedStatus = Field(ClosedStatus.AWAITING_PUBLICATION)
    available_at: Optional[datetime] = None
    description: str = Field(..., description="Gateway description")
    kind: GatewayKind = Field(GatewayKind.CLOSED)


class EmbargoedGateway(BaseModel):
    """Embargoed gateway."""
    status: ClosedStatus = Field(ClosedStatus.EMBARGOED)
    available_at: datetime = Field(..., description="Embargo end date")
    description: str = Field(..., description="Gateway description")
    kind: GatewayKind = Field(GatewayKind.CLOSED)


class OpenGateway(BaseModel):
    """Open access gateway."""
    link: GatewayLink = Field(..., description="Gateway link")
    kind: GatewayKind = Field(GatewayKind.OPEN)


class RegisteredGateway(BaseModel):
    """Registered access gateway."""
    link: GatewayLink = Field(..., description="Gateway link")
    kind: GatewayKind = Field(GatewayKind.REGISTERED)


class ControlledGateway(BaseModel):
    """Controlled access gateway."""
    link: GatewayLink = Field(..., description="Gateway link")
    kind: GatewayKind = Field(GatewayKind.CONTROLLED)


Gateway = Union[
    OpenGateway,
    RegisteredGateway, 
    ControlledGateway,
    IndefinitelyClosedGateway,
    AwaitingPublicationGateway,
    EmbargoedGateway
]


class AnonymousGateway(BaseModel):
    """Anonymous gateway embedded in response."""
    gateway: Gateway = Field(..., description="Gateway details")
    kind: str = Field("Anonymous")


class GatewayReference(BaseModel):
    """Reference to a named gateway."""
    gateway: str = Field(..., description="Gateway name reference")
    kind: str = Field("Reference")


GatewayOrReference = Union[AnonymousGateway, GatewayReference]


class NamedGateway(BaseModel):
    """Named gateway for response gateways collection."""
    name: str = Field(..., description="Gateway name")
    # Include all gateway fields inline
    link: Optional[GatewayLink] = None
    status: Optional[ClosedStatus] = None
    available_at: Optional[datetime] = None
    description: Optional[str] = None
    kind: GatewayKind = Field(..., description="Gateway kind")


# ============================================================================
# Common Metadata Models
# ============================================================================

class DepositionAccession(BaseModel):
    """Deposition accession model."""
    kind: str = Field(..., description="Repository type")
    value: str = Field(..., description="Accession value")


class CommonMetadata(BaseModel):
    """Common metadata shared across entities."""
    depositions: Optional[List[DepositionAccession]] = Field(
        None, 
        description="Public repository depositions"
    )


# ============================================================================
# Entity-Specific Models
# ============================================================================

class SubjectKind(str, Enum):
    """Subject kinds."""
    HOMO_SAPIENS = "Homo sapiens"


class SubjectIdentifier(BaseModel):
    """Subject identifier model."""
    namespace: NamespaceIdentifier = Field(..., description="Namespace identifier")
    name: str = Field(..., description="Subject name", examples=["SubjectName001"])


class SubjectMetadata(CommonMetadata):
    """Subject metadata model."""
    sex: Optional[UnharmonizedField] = None
    race: Optional[UnharmonizedField] = None
    ethnicity: Optional[UnharmonizedField] = None
    identifiers: Optional[List[UnharmonizedField]] = None
    vital_status: Optional[UnharmonizedField] = None
    age_at_vital_status: Optional[UnharmonizedField] = None
    associated_diagnoses: Optional[UnharmonizedField] = None
    unharmonized: Optional[Dict[str, UnharmonizedField]] = Field(
        None,
        description="Unharmonized metadata fields"
    )


class SampleIdentifier(BaseModel):
    """Sample identifier model."""
    namespace: NamespaceIdentifier = Field(..., description="Namespace identifier")
    name: str = Field(..., description="Sample name", examples=["SampleName001"])


class SampleMetadata(CommonMetadata):
    """Sample metadata model."""
    disease_phase: Optional[UnharmonizedField] = None
    anatomical_sites: Optional[UnharmonizedField] = None
    library_selection_method: Optional[UnharmonizedField] = None
    library_strategy: Optional[UnharmonizedField] = None
    library_source_material: Optional[UnharmonizedField] = None
    preservation_method: Optional[UnharmonizedField] = None
    tumor_grade: Optional[UnharmonizedField] = None
    specimen_molecular_analyte_type: Optional[UnharmonizedField] = None
    tissue_type: Optional[UnharmonizedField] = None
    tumor_classification: Optional[UnharmonizedField] = None
    age_at_diagnosis: Optional[UnharmonizedField] = None
    age_at_collection: Optional[UnharmonizedField] = None
    tumor_tissue_morphology: Optional[UnharmonizedField] = None
    diagnosis: Optional[UnharmonizedField] = None
    identifiers: Optional[List[UnharmonizedField]] = None
    unharmonized: Optional[Dict[str, UnharmonizedField]] = Field(
        None,
        description="Unharmonized metadata fields"
    )


class FileIdentifier(BaseModel):
    """File identifier model."""
    namespace: NamespaceIdentifier = Field(..., description="Namespace identifier")
    name: str = Field(..., description="File name", examples=["File001.txt"])


class FileChecksums(BaseModel):
    """File checksums model."""
    md5: Optional[str] = None


class FileMetadata(CommonMetadata):
    """File metadata model."""
    type: Optional[UnharmonizedField] = None
    size: Optional[UnharmonizedField] = None
    checksums: Optional[UnharmonizedField] = None
    description: Optional[UnharmonizedField] = None
    unharmonized: Optional[Dict[str, UnharmonizedField]] = Field(
        None,
        description="Unharmonized metadata fields"
    )


# ============================================================================
# Entity Models
# ============================================================================

# ============================================================================
# Nested Structure Models for CCDI-DCC Format
# ============================================================================

class NamespaceIdentifier(BaseModel):
    """Namespace identifier for nested structure."""
    organization: str = Field(default="CCDI-DCC", description="Organization identifier")
    name: str = Field(..., description="Namespace name (study ID)")

class SubjectId(BaseModel):
    """Subject identifier with nested namespace."""
    namespace: NamespaceIdentifier = Field(..., description="Namespace identifier")
    name: str = Field(..., description="Subject name (participant ID)")

class MetadataField(BaseModel):
    """Metadata field with value and ancestors."""
    value: Any = Field(..., description="Field value")
    ancestors: Optional[Any] = Field(default=None, description="Ancestor entities", exclude=True)

class AssociatedDiagnosisField(BaseModel):
    """Associated diagnosis field with detailed structure."""
    value: str = Field(..., description="Diagnosis ID")
    ancestors: Optional[Any] = Field(default=None, description="Ancestor entities", exclude=True)
    owned: bool = Field(default=True, description="Ownership status")
    comment: Optional[str] = Field(default=None, description="Comment")

class IdentifierValue(BaseModel):
    """Identifier value with nested structure."""
    namespace: NamespaceIdentifier = Field(..., description="Namespace identifier")
    name: str = Field(..., description="Identifier name")
    server: Optional[str] = Field(default=None, description="Server URL", exclude=True)
    type: Optional[str] = Field(default=None, description="Identifier type", exclude=True)

class IdentifierField(BaseModel):
    """Identifier field with nested value."""
    value: IdentifierValue = Field(..., description="Identifier value")
    ancestors: Optional[Any] = Field(default=None, description="Ancestor entities", exclude=True)

class SubjectMetadata(BaseModel):
    """Subject metadata with nested field structure."""
    sex: Optional[MetadataField] = None
    race: Optional[List[MetadataField]] = None
    ethnicity: Optional[MetadataField] = None
    identifiers: Optional[List[IdentifierField]] = None
    associated_diagnoses: Optional[List[AssociatedDiagnosisField]] = None
    unharmonized: Optional[Any] = None
    vital_status: Optional[MetadataField] = None
    age_at_vital_status: Optional[MetadataField] = None
    depositions: Optional[List[DepositionAccession]] = None

class Subject(BaseModel):
    """Subject model with nested CCDI-DCC structure."""
    model_config = ConfigDict(extra="allow")
    
    # Required fields for CCDI-DCC format
    id: SubjectId = Field(..., description="Subject identifier with nested namespace")
    kind: str = Field(default="Participant", description="Entity kind")
    metadata: SubjectMetadata = Field(..., description="Subject metadata")
    gateways: List[Any] = Field(default_factory=list, description="Gateway access information")
    
    # Allow any additional fields to be added dynamically
    def __init__(self, **data):
        # Set defaults if not provided
        if 'kind' not in data:
            data['kind'] = "Participant"
        if 'gateways' not in data:
            data['gateways'] = []
        super().__init__(**data)


class Sample(BaseModel):
    """Flexible sample model that can contain any fields."""
    model_config = ConfigDict(extra="allow")
    
    # Allow any additional fields to be added dynamically
    def __init__(self, **data):
        super().__init__(**data)


class File(BaseModel):
    """Flexible file model that can contain any fields."""
    model_config = ConfigDict(extra="allow")
    
    # Allow any additional fields to be added dynamically
    def __init__(self, **data):
        super().__init__(**data)


class Organization(BaseModel):
    """Organization model."""
    identifier: str = Field(..., description="Organization identifier")
    name: str = Field(..., description="Organization name")
    metadata: Optional[Dict[str, Any]] = None


class NamespaceMetadata(CommonMetadata):
    """Namespace metadata model."""
    study_short_title: Optional[UnharmonizedField] = None
    study_name: Optional[UnharmonizedField] = None
    study_funding_id: Optional[List[UnharmonizedField]] = None
    study_id: Optional[UnharmonizedField] = None
    depositions: Optional[List[Dict[str, str]]] = None
    unharmonized: Optional[Dict[str, UnharmonizedField]] = Field(
        None,
        description="Unharmonized metadata fields"
    )


class Namespace(BaseModel):
    """Namespace model."""
    id: NamespaceIdentifier = Field(..., description="Namespace identifier")
    description: str = Field(..., description="Namespace description")
    contact_email: str = Field(..., description="Support email address")
    metadata: Optional[NamespaceMetadata] = None


# ============================================================================
# Response Models
# ============================================================================

class Summary(BaseModel):
    """Summary response model."""
    # TODO: Define summary fields based on requirements
    total_count: int = Field(..., description="Total entity count")
    # Add more summary fields as needed


class CountResult(BaseModel):
    """Count result for by-field counting."""
    value: str = Field(..., description="Field value")
    count: int = Field(..., description="Count for this value")


class CountResponse(BaseModel):
    """Generic count response for field counting."""
    total: int = Field(default=0, description="Total counts of all unique participants")
    missing: int = Field(default=0, description="Count of unique participants with null value in the field")
    values: List[CountResult] = Field(default_factory=list, description="Count results for field values")


class SummaryCounts(BaseModel):
    """Summary counts structure."""
    total: int = Field(..., description="Total entity count")

class SummaryResponse(BaseModel):
    """Generic summary response model."""
    counts: SummaryCounts = Field(..., description="Summary counts")


class SubjectsResponse(BaseModel):
    """Flexible subjects list response that can accommodate any subject structure."""
    model_config = ConfigDict(extra="allow")
    
    subjects: List[Subject] = Field(..., description="List of subjects with flexible structure")
    gateways: Optional[Dict[str, NamedGateway]] = Field(
        None,
        description="Named gateways referenced by subjects"
    )


class SamplesResponse(BaseModel):
    """Flexible samples list response that can accommodate any sample structure."""
    model_config = ConfigDict(extra="allow")
    
    samples: List[Sample] = Field(..., description="List of samples with flexible structure")
    gateways: Optional[Dict[str, NamedGateway]] = Field(
        None,
        description="Named gateways referenced by samples"
    )


class FilesResponse(BaseModel):
    """Flexible files list response that can accommodate any file structure."""
    model_config = ConfigDict(extra="allow")
    
    files: List[File] = Field(..., description="List of files with flexible structure")
    gateways: Optional[Dict[str, NamedGateway]] = Field(
        None,
        description="Named gateways referenced by files"
    )


class SubjectResponse(BaseModel):
    """Subject response with nested structure matching CCDI-DCC format."""
    model_config = ConfigDict(extra="allow")
    
    # Top-level response structure
    summary: Optional[Dict[str, Any]] = Field(None, description="Summary statistics")
    data: List[Subject] = Field(..., description="List of subjects with nested structure")
    
    # For paginated responses
    pagination: Optional[Any] = Field(None, description="Pagination information")


class SampleResponse(BaseModel):
    """Flexible sample response that can handle both single samples and lists with pagination."""
    model_config = ConfigDict(extra="allow")
    
    # For single sample responses
    # sample: Optional[Sample] = Field(None, description="Single sample details")
    
    # For multiple sample responses  
    samples: Optional[List[Sample]] = Field(None, description="List of samples")
    
    # Common fields
    gateways: Optional[Dict[str, NamedGateway]] = Field(
        None,
        description="Named gateways referenced by samples"
    )
    
    # For paginated responses
    pagination: Optional[Any] = Field(None, description="Pagination information")


class FileResponse(BaseModel):
    """Flexible file response that can handle both single files and lists with pagination."""
    model_config = ConfigDict(extra="allow")
    
    # For single file responses
    file: Optional[File] = Field(None, description="Single file details")
    
    # For multiple file responses  
    files: Optional[List[File]] = Field(None, description="List of files")
    
    # Common fields
    gateways: Optional[Dict[str, NamedGateway]] = Field(
        None,
        description="Named gateways referenced by files"
    )
    
    # For paginated responses
    pagination: Optional[Any] = Field(None, description="Pagination information")


class SubjectCountResponse(BaseModel):
    """Subject count by field response."""
    results: List[CountResult] = Field(..., description="Count results")


class SampleCountResponse(BaseModel):
    """Sample count by field response."""
    results: List[CountResult] = Field(..., description="Count results")


class FileCountResponse(BaseModel):
    """File count by field response."""
    results: List[CountResult] = Field(..., description="Count results")


class FieldDescriptionsResponse(BaseModel):
    """Field descriptions response."""
    fields: Dict[str, FieldDescription] = Field(
        ...,
        description="Available metadata fields"
    )


class MetadataFieldsResponse(BaseModel):
    """Metadata fields response."""
    harmonized: List[str] = Field(..., description="List of harmonized field names")
    unharmonized: List[str] = Field(..., description="List of unharmonized field names")


class NamespacesResponse(BaseModel):
    """Namespaces list response."""
    namespaces: List[Namespace] = Field(..., description="List of namespaces")


class NamespaceResponse(BaseModel):
    """Single namespace response."""
    namespace: Namespace = Field(..., description="Namespace details")


class OrganizationsResponse(BaseModel):
    """Organizations list response."""
    organizations: List[Organization] = Field(..., description="List of organizations")


class OrganizationResponse(BaseModel):
    """Single organization response."""
    organization: Organization = Field(..., description="Organization details")


class Information(BaseModel):
    """Server information model."""
    name: str = Field(..., description="Server name")
    version: str = Field(..., description="Server version")
    description: Optional[str] = None
    contact_email: str = Field(..., description="Contact email")


class InformationResponse(BaseModel):
    """Information response."""
    information: Information = Field(..., description="Server information")
