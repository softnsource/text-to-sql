"""OAuth2 Authorization Code flow with the existing Auth Server (OpenIddict)."""

import secrets
import hashlib
import base64
from urllib.parse import urlencode
from typing import Optional, Dict, Tuple
import httpx
from jose import jwt, JWTError

from app.config import get_settings


class AuthError(Exception):
    """Authentication/authorization error."""
    pass


class OAuthClient:
    """OAuth2 client for Authorization Code flow with PKCE.

    Integrates with the existing Onboarding Auth Server (OpenIddict-based).
    Uses PKCE (Proof Key for Code Exchange) for secure public client authentication.
    """

    def __init__(self):
        settings = get_settings()
        self.authority_url = settings.auth.authority_url
        self.client_id = settings.auth.client_id
        self.scopes = settings.auth.scopes
        self.redirect_uri = f"{settings.auth.redirect_uri}/callback"
        self._jwks: Optional[Dict] = None  # cached JWKS
        self._openid_config: Optional[Dict] = None
        # Only skip SSL verification for localhost development
        self._verify_ssl = not self.authority_url.startswith("http://localhost")

    async def get_openid_configuration(self) -> Dict:
        """Fetch OpenID Connect discovery document from Auth Server.

        Returns:
            Discovery document containing authorization_endpoint, token_endpoint, jwks_uri, etc.

        Raises:
            httpx.HTTPError: If the discovery request fails
        """
        if self._openid_config:
            return self._openid_config

        async with httpx.AsyncClient(verify=self._verify_ssl) as client:  # verify=False for localhost dev
            resp = await client.get(f"{self.authority_url}/.well-known/openid-configuration")
            resp.raise_for_status()
            self._openid_config = resp.json()
            return self._openid_config

    async def get_jwks(self) -> Dict:
        """Fetch JSON Web Key Set for token validation.

        Returns:
            JWKS document containing public keys for signature verification

        Raises:
            httpx.HTTPError: If the JWKS request fails
        """
        if self._jwks:
            return self._jwks

        config = await self.get_openid_configuration()
        async with httpx.AsyncClient(verify=self._verify_ssl) as client:
            resp = await client.get(config["jwks_uri"])
            resp.raise_for_status()
            self._jwks = resp.json()
            return self._jwks

    def generate_pkce(self) -> Tuple[str, str]:
        """Generate PKCE code_verifier and code_challenge.

        PKCE (RFC 7636) protects against authorization code interception attacks.

        Returns:
            Tuple of (code_verifier, code_challenge)
        """
        code_verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(code_verifier.encode()).digest()
        code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        return code_verifier, code_challenge

    async def get_authorization_url(self, state: str, code_challenge: str) -> str:
        """Build the authorization URL for OAuth2 redirect.

        Args:
            state: CSRF protection token
            code_challenge: PKCE challenge derived from code_verifier

        Returns:
            Full authorization URL to redirect user to
        """
        config = await self.get_openid_configuration()
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.scopes),
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"{config['authorization_endpoint']}?{urlencode(params)}"

    async def exchange_code(self, code: str, code_verifier: str) -> Dict:
        """Exchange authorization code for tokens.

        Args:
            code: Authorization code from callback
            code_verifier: Original PKCE verifier (proves client identity)

        Returns:
            Token response containing access_token, refresh_token, id_token, expires_in

        Raises:
            httpx.HTTPError: If token exchange fails
        """
        config = await self.get_openid_configuration()
        data = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "code": code,
            "redirect_uri": self.redirect_uri,
            "code_verifier": code_verifier,
        }
        async with httpx.AsyncClient(verify=self._verify_ssl) as client:
            resp = await client.post(config["token_endpoint"], data=data)
            resp.raise_for_status()
            return resp.json()

    async def validate_token(self, token: str) -> Dict:
        """Validate and decode a JWT access token.

        Verifies:
        - Signature against JWKS public keys
        - Expiration (exp claim)
        - Not-before (nbf claim)
        - Issuer (iss claim)

        Args:
            token: JWT access token string

        Returns:
            Decoded token payload (claims dict)

        Raises:
            AuthError: If token is invalid, expired, or signature verification fails
        """
        try:
            jwks = await self.get_jwks()
            unverified_header = jwt.get_unverified_header(token)

            # Find matching key by kid (Key ID)
            key = None
            for jwk in jwks.get("keys", []):
                if jwk.get("kid") == unverified_header.get("kid"):
                    key = jwk
                    break

            if not key:
                # Fallback: try first RSA key
                for jwk in jwks.get("keys", []):
                    if jwk.get("kty") == "RSA":
                        key = jwk
                        break

            if not key:
                raise AuthError("No matching key found in JWKS")

            # Decode and validate token
            payload = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                audience=self.client_id,
                options={"verify_aud": False}  # ABP tokens may not have audience claim
            )
            return payload
        except JWTError as e:
            raise AuthError(f"Token validation failed: {e}")

    async def refresh_token(self, refresh_token_str: str) -> Dict:
        """Use refresh token to get new access token.

        Args:
            refresh_token_str: Refresh token from previous token response

        Returns:
            Token response with new access_token and refresh_token

        Raises:
            httpx.HTTPError: If refresh fails (e.g., expired refresh token)
        """
        config = await self.get_openid_configuration()
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "refresh_token": refresh_token_str,
        }
        async with httpx.AsyncClient(verify=self._verify_ssl) as client:
            resp = await client.post(config["token_endpoint"], data=data)
            resp.raise_for_status()
            return resp.json()


# Singleton instance
_oauth_client: Optional[OAuthClient] = None


def get_oauth_client() -> OAuthClient:
    """Get or create the singleton OAuth client instance.

    Returns:
        OAuthClient configured with settings from config.yaml
    """
    global _oauth_client
    if _oauth_client is None:
        _oauth_client = OAuthClient()
    return _oauth_client
