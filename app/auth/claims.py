"""JWT claims extraction — mirrors UserConsts.cs claim names exactly."""

from dataclasses import dataclass, field
from typing import List

# Exact claim names from the .NET Auth Server
# Source: services/identity/src/Onboarding.IdentityService.Domain.Shared/Users/UserConsts.cs
CLAIM_USER_TYPE = "userType"
CLAIM_AGENCY_ID = "AgencyIdClaim"
CLAIM_INVITED_AGENCY_ID = "InvitedAgencyIdClaim"
CLAIM_AGENT_ID = "AgentIdClaim"
CLAIM_FMO_ID = "FMOIdClaim"
CLAIM_AGENCY_STAFF_DEPARTMENTS = "AgencyStaffDepartmentsClaim"
CLAIM_INVITATION_USER_TYPE = "InvitationUserTypeClaim"
CLAIM_TENANT_NAME = "tenant_name"
CLAIM_FIRST_NAME = "FirstNameClaim"
CLAIM_LAST_NAME = "LastNameClaim"
CLAIM_AGENT_NAME = "AgentNameClaim"
CLAIM_AGENCY_NAME = "AgencyNameClaim"
CLAIM_NPN = "NPNClaim"
CLAIM_FMO_USER_TYPE = "FMOUserTypeClaim"

# Standard OIDC claims
CLAIM_ROLES = "role"
CLAIM_SUB = "sub"
CLAIM_EMAIL = "email"
CLAIM_TENANT_ID = "tenantid"  # ABP multi-tenancy

# Role names from UsersRoles.cs
# Source: services/identity/src/Onboarding.IdentityService.Domain/Models/UsersRoles.cs
ROLE_ADMIN = "admin"
ROLE_ADMIN_USER = "AdminUser"
ROLE_FMO = "FMO"
ROLE_AGENCY = "Agency"
ROLE_AGENT = "Agent"
ROLE_AGENCY_STAFF = "AgencyStaff"
ROLE_INVITED_AGENCY = "InvitedAgency"
ROLE_INVITED_AGENT = "InvitedAgent"


@dataclass
class UserClaims:
    """Parsed user claims from JWT token."""
    user_id: str = ""
    email: str = ""
    tenant_id: str = ""
    tenant_name: str = ""
    user_type: str = ""
    roles: List[str] = field(default_factory=list)
    agency_id: int = 0
    invited_agency_id: int = 0
    agent_id: int = 0
    fmo_id: int = 0
    staff_departments: str = ""
    invitation_user_type: str = ""
    first_name: str = ""
    last_name: str = ""
    agent_name: str = ""
    agency_name: str = ""
    npn: str = ""
    fmo_user_type: str = ""

    @property
    def display_name(self) -> str:
        """Get display name with fallback chain: FirstName LastName > AgentName > AgencyName > Email > UserID."""
        if self.first_name or self.last_name:
            return f"{self.first_name} {self.last_name}".strip()
        if self.agent_name:
            return self.agent_name
        if self.agency_name:
            return self.agency_name
        return self.email or self.user_id

    @property
    def primary_role(self) -> str:
        """Determine the primary role following the .NET hierarchy.

        Mirrors GetCurrentUserType() in ReportCommonAppService.cs:
        Admin > AdminUser > FMO > Agency > AgencyStaff > Agent > InvitedAgency > InvitedAgent

        Returns:
            The highest-priority role found, defaulting to 'Agent' if none match.
        """
        role_priority = [
            ROLE_ADMIN,
            ROLE_ADMIN_USER,
            ROLE_FMO,
            ROLE_AGENCY,
            ROLE_AGENCY_STAFF,
            ROLE_AGENT,
            ROLE_INVITED_AGENCY,
            ROLE_INVITED_AGENT,
        ]
        for role in role_priority:
            if role in self.roles:
                return role
        return ROLE_AGENT  # default fallback, same as .NET

    @property
    def effective_agency_id(self) -> int:
        """Get the agency ID to use for hierarchy filtering.

        Mirrors GetAgencyIdByCurrentAgency() in ReportCommonAppService.cs:
        - AgencyStaff/Agent/InvitedAgent → use InvitedAgencyIdClaim
        - Agency/FMO → use AgencyIdClaim

        Returns:
            The appropriate agency ID based on role, or 0 if none.
        """
        if self.primary_role in (ROLE_AGENCY_STAFF, ROLE_AGENT, ROLE_INVITED_AGENT):
            return self.invited_agency_id
        return self.agency_id

    @property
    def is_admin_or_fmo(self) -> bool:
        """Check if user has Admin, AdminUser, or FMO role.

        Mirrors IsFMOOrAdminUser() in ReportCommonAppService.cs.

        Returns:
            True if user is Admin, AdminUser, or FMO; False otherwise.
        """
        return self.primary_role in (ROLE_ADMIN, ROLE_ADMIN_USER, ROLE_FMO)


def parse_claims(token_payload: dict) -> UserClaims:
    """Parse a decoded JWT payload into UserClaims.

    Args:
        token_payload: Decoded JWT token payload (dict from python-jose)

    Returns:
        UserClaims with all fields populated from token
    """
    # Handle 'role' claim — can be string or list
    roles_raw = token_payload.get(CLAIM_ROLES, [])
    if isinstance(roles_raw, str):
        roles = [roles_raw]
    else:
        roles = list(roles_raw)

    def safe_int(value, default=0) -> int:
        """Convert to int with fallback."""
        try:
            return int(value) if value else default
        except (ValueError, TypeError):
            return default

    def safe_str(value, default="") -> str:
        """Convert to string with fallback."""
        return str(value) if value else default

    return UserClaims(
        user_id=safe_str(token_payload.get(CLAIM_SUB)),
        email=safe_str(token_payload.get(CLAIM_EMAIL)),
        tenant_id=safe_str(token_payload.get(CLAIM_TENANT_ID)),
        tenant_name=safe_str(token_payload.get(CLAIM_TENANT_NAME)),
        user_type=safe_str(token_payload.get(CLAIM_USER_TYPE)),
        roles=roles,
        agency_id=safe_int(token_payload.get(CLAIM_AGENCY_ID)),
        invited_agency_id=safe_int(token_payload.get(CLAIM_INVITED_AGENCY_ID)),
        agent_id=safe_int(token_payload.get(CLAIM_AGENT_ID)),
        fmo_id=safe_int(token_payload.get(CLAIM_FMO_ID)),
        staff_departments=safe_str(token_payload.get(CLAIM_AGENCY_STAFF_DEPARTMENTS)),
        invitation_user_type=safe_str(token_payload.get(CLAIM_INVITATION_USER_TYPE)),
        first_name=safe_str(token_payload.get(CLAIM_FIRST_NAME)),
        last_name=safe_str(token_payload.get(CLAIM_LAST_NAME)),
        agent_name=safe_str(token_payload.get(CLAIM_AGENT_NAME)),
        agency_name=safe_str(token_payload.get(CLAIM_AGENCY_NAME)),
        npn=safe_str(token_payload.get(CLAIM_NPN)),
        fmo_user_type=safe_str(token_payload.get(CLAIM_FMO_USER_TYPE)),
    )
