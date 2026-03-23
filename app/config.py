# """Configuration loader — resolves environment variables and validates settings."""

# import os
# import re
# import sys
# from dataclasses import dataclass, field
# from typing import List, Optional
# from dataclasses import dataclass, field
# from typing import Dict, List, Optional
# import yaml
# from dotenv import load_dotenv


# @dataclass
# class GeminiConfig:
#     # Primary key — used only as a fallback reference.
#     # The GeminiKeyManager loads all keys (GEMINI_API_KEY_1…13) from env directly.
#     api_key: str
#     model: str
#     embedding_model: str
#     max_tokens: int
#     temperature: float


# @dataclass
# class QdrantConfig:
#     url: str
#     api_key: Optional[str]
#     collection_prefix: str
#     timeout: int          # HTTP request timeout in seconds
#     verify_ssl: bool      # Set False for self-signed certs on internal servers


# @dataclass
# class QueryConfig:
#     max_rows_per_query: int
#     query_timeout_seconds: int
#     max_retries: int
#     confidence_threshold: float
#     top_k_tables: int


# @dataclass
# class UploadsConfig:
#     dir: str
#     max_size_mb: int
#     allowed_extensions: List[str]


# @dataclass
# class SessionConfig:
#     ttl_hours: int
#     max_concurrent: int
#     cleanup_interval_minutes: int

# @dataclass
# class ServiceConfig:
#     connection_key: str
#     enabled: bool = True

# @dataclass
# class Settings:
#     gemini: GeminiConfig
#     qdrant: QdrantConfig
#     query: QueryConfig
#     uploads: UploadsConfig
#     session: SessionConfig
#     auth: AuthConfig
#     services: Dict[str, ServiceConfig] = field(default_factory=dict)  # ← add this

#     def get_enabled_services(self) -> Dict[str, ServiceConfig]:        # ← add this
#         return {
#             name: cfg
#             for name, cfg in self.services.items()
#             if cfg.enabled
#         }


# _settings: Optional[Settings] = None


# def _resolve_env_vars(value):
#     """Recursively resolve ${ENV_VAR} placeholders."""
#     if isinstance(value, str):
#         pattern = re.compile(r'\$\{([^}]+)\}')
#         for env_var in pattern.findall(value):
#             env_value = os.getenv(env_var)
#             if env_value is None:
#                 raise ValueError(f"Environment variable '{env_var}' is not set")
#             value = value.replace(f"${{{env_var}}}", env_value)
#         return value
#     elif isinstance(value, dict):
#         return {k: _resolve_env_vars(v) for k, v in value.items()}
#     elif isinstance(value, list):
#         return [_resolve_env_vars(item) for item in value]
#     return value


# def _collect_missing(raw, missing: set, optional: set):
#     """Collect env var names referenced in config that are not set.
#     Vars in `optional` are skipped.
#     """
#     if isinstance(raw, str):
#         for var in re.findall(r'\$\{([^}]+)\}', raw):
#             if var not in optional and not os.getenv(var):
#                 missing.add(var)
#     elif isinstance(raw, dict):
#         for v in raw.values():
#             _collect_missing(v, missing, optional)
#     elif isinstance(raw, list):
#         for item in raw:
#             _collect_missing(item, missing, optional)


# def _resolve_primary_gemini_key() -> str:
#     """Return the first available Gemini API key from numbered or legacy env var."""
#     for i in range(1, 14):
#         val = os.getenv(f"GEMINI_API_KEY_{i}", "").strip()
#         if val:
#             return val
#     legacy = os.getenv("GEMINI_API_KEY", "").strip()
#     if legacy:
#         return legacy
#     print(
#         "ERROR: No Gemini API key found. "
#         "Set GEMINI_API_KEY_1 (or GEMINI_API_KEY) in your .env file."
#     )
#     sys.exit(1)


# def _load_config() -> Settings:
#     env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
#     if os.path.exists(env_path):
#         load_dotenv(env_path)

#     config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
#     if not os.path.exists(config_path):
#         print(f"ERROR: config.yaml not found at {config_path}")
#         sys.exit(1)

#     with open(config_path, 'r') as f:
#         raw = yaml.safe_load(f)

#     # GEMINI_API_KEY in yaml is optional — key manager loads keys itself
#     optional_vars = {"GEMINI_API_KEY", "GEMINI_API_KEY_1"}

#     missing: set = set()
#     _collect_missing(raw, missing, optional_vars)
#     if missing:
#         print("ERROR: Missing required environment variables:")
#         for v in sorted(missing):
#             print(f"  - {v}")
#         sys.exit(1)

#     # Inject the primary Gemini key before resolving (so ${GEMINI_API_KEY} works)
#     primary_key = _resolve_primary_gemini_key()
#     os.environ.setdefault("GEMINI_API_KEY", primary_key)
#     os.environ.setdefault("GEMINI_API_KEY_1", primary_key)

#     cfg = _resolve_env_vars(raw)

#     g = cfg['gemini']
#     q = cfg['qdrant']
#     qry = cfg['query']
#     u = cfg['uploads']
#     s = cfg['session']

#     return Settings(
#         gemini=GeminiConfig(
#             api_key=primary_key,
#             model=g['model'],
#             embedding_model=g['embedding_model'],
#             max_tokens=g['max_tokens'],
#             temperature=g['temperature'],
#         ),
#         qdrant=QdrantConfig(
#             url=q['url'],
#             api_key=q.get('api_key') or None,
#             collection_prefix=q['collection_prefix'],
#             timeout=q.get('timeout', 30),
#             verify_ssl=q.get('verify_ssl', True),
#         ),
#         query=QueryConfig(
#             max_rows_per_query=qry['max_rows_per_query'],
#             query_timeout_seconds=qry['query_timeout_seconds'],
#             max_retries=qry['max_retries'],
#             confidence_threshold=qry['confidence_threshold'],
#             top_k_tables=qry['top_k_tables'],
#         ),
#         uploads=UploadsConfig(
#             dir=u['dir'],
#             max_size_mb=u['max_size_mb'],
#             allowed_extensions=u['allowed_extensions'],
#         ),
#         session=SessionConfig(
#             ttl_hours=s['ttl_hours'],
#             max_concurrent=s['max_concurrent'],
#             cleanup_interval_minutes=s['cleanup_interval_minutes'],
#         ),
#     )


# def get_settings() -> Settings:
#     global _settings
#     if _settings is None:
#         _settings = _load_config()
#     return _settings

"""Configuration loader — resolves environment variables and validates settings."""

import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv


@dataclass
class GeminiConfig:
    # Primary key — used only as a fallback reference.
    # The GeminiKeyManager loads all keys (GEMINI_API_KEY_1…13) from env directly.
    api_key: str
    model: str
    embedding_model: str
    max_tokens: int
    temperature: float


@dataclass
class QdrantConfig:
    url: str
    api_key: Optional[str]
    collection_prefix: str
    timeout: int          # HTTP request timeout in seconds
    verify_ssl: bool      # Set False for self-signed certs on internal servers


@dataclass
class QueryConfig:
    max_rows_per_query: int
    query_timeout_seconds: int
    max_retries: int
    confidence_threshold: float
    top_k_tables: int
    max_merged_rows: int = 1000


@dataclass
class UploadsConfig:
    dir: str
    max_size_mb: int
    allowed_extensions: List[str]


@dataclass
class SessionConfig:
    ttl_hours: int
    max_concurrent: int
    cleanup_interval_minutes: int


@dataclass
class AuthConfig:
    authority_url: str
    client_id: str
    scopes: List[str]
    redirect_uri: str

@dataclass
class CrawlerConfig:
    excluded_schemas: List[str] = field(default_factory=list)
    excluded_table_patterns: List[str] = field(default_factory=list)


@dataclass
class ServiceConfig:
    connection_key: str
    enabled: bool = True
    display_name: str = ""


@dataclass
class Settings:
    gemini: GeminiConfig
    qdrant: QdrantConfig
    query: QueryConfig
    uploads: UploadsConfig
    session: SessionConfig
    auth: AuthConfig
    crawler: CrawlerConfig = field(default_factory=CrawlerConfig)   # ← add this
    services: Dict[str, ServiceConfig] = field(default_factory=dict)

    def get_enabled_services(self) -> Dict[str, ServiceConfig]:
        return {
            name: cfg
            for name, cfg in self.services.items()
            if cfg.enabled
        }


_settings: Optional[Settings] = None


def _resolve_env_vars(value: Any) -> Any:
    """Recursively resolve ${ENV_VAR} placeholders."""
    if isinstance(value, str):
        pattern = re.compile(r'\$\{([^}]+)\}')
        for env_var in pattern.findall(value):
            env_value = os.getenv(env_var)
            if env_value is None:
                raise ValueError(f"Environment variable '{env_var}' is not set")
            value = value.replace(f"${{{env_var}}}", env_value)
        return value
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def _collect_missing(raw: Any, missing: set, optional: set) -> None:
    """Collect env var names referenced in config that are not set.
    Vars in `optional` are skipped.
    """
    if isinstance(raw, str):
        for var in re.findall(r'\$\{([^}]+)\}', raw):
            if var not in optional and not os.getenv(var):
                missing.add(var)
    elif isinstance(raw, dict):
        for v in raw.values():
            _collect_missing(v, missing, optional)
    elif isinstance(raw, list):
        for item in raw:
            _collect_missing(item, missing, optional)


def _resolve_primary_gemini_key() -> str:
    """Return the first available Gemini API key from numbered or legacy env var."""
    for i in range(1, 14):
        val = os.getenv(f"GEMINI_API_KEY_{i}", "").strip()
        if val:
            return val
    legacy = os.getenv("GEMINI_API_KEY", "").strip()
    if legacy:
        return legacy
    print(
        "ERROR: No Gemini API key found. "
        "Set GEMINI_API_KEY_1 (or GEMINI_API_KEY) in your .env file."
    )
    sys.exit(1)


def _load_config() -> Settings:
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)

    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
    if not os.path.exists(config_path):
        print(f"ERROR: config.yaml not found at {config_path}")
        sys.exit(1)

    with open(config_path, 'r') as f:
        raw: Any = yaml.safe_load(f)

    # GEMINI_API_KEY in yaml is optional — key manager loads keys itself
    optional_vars = {"GEMINI_API_KEY", "GEMINI_API_KEY_1"}

    missing: set = set()
    _collect_missing(raw, missing, optional_vars)
    if missing:
        print("ERROR: Missing required environment variables:")
        for v in sorted(missing):
            print(f"  - {v}")
        sys.exit(1)

    # Inject the primary Gemini key before resolving (so ${GEMINI_API_KEY} works)
    primary_key = _resolve_primary_gemini_key()
    os.environ.setdefault("GEMINI_API_KEY", primary_key)
    os.environ.setdefault("GEMINI_API_KEY_1", primary_key)

    # cfg is a plain dict after yaml.safe_load + resolve — tell Pylance explicitly
    cfg: Dict[str, Any] = _resolve_env_vars(raw)

    g: Dict[str, Any] = cfg['gemini']
    q: Dict[str, Any] = cfg['qdrant']
    qry: Dict[str, Any] = cfg['query']
    u: Dict[str, Any] = cfg['uploads']
    s: Dict[str, Any] = cfg['session']
    a: Dict[str, Any] = cfg.get('auth', {})
    raw_services: Dict[str, Any] = cfg.get('services', {})

    services: Dict[str, ServiceConfig] = {
        name: ServiceConfig(
            connection_key=svc.get('connection_key', ''),
            enabled=svc.get('enabled', True),
        )
        for name, svc in raw_services.items()
    }

    return Settings(
        gemini=GeminiConfig(
            api_key=primary_key,
            model=g['model'],
            embedding_model=g['embedding_model'],
            max_tokens=g['max_tokens'],
            temperature=g['temperature'],
        ),
        qdrant=QdrantConfig(
            url=q['url'],
            api_key=q.get('api_key') or None,
            collection_prefix=q['collection_prefix'],
            timeout=q.get('timeout', 30),
            verify_ssl=q.get('verify_ssl', True),
        ),
        query=QueryConfig(
            max_rows_per_query=qry['max_rows_per_query'],
            query_timeout_seconds=qry['query_timeout_seconds'],
            max_retries=qry['max_retries'],
            confidence_threshold=qry['confidence_threshold'],
            top_k_tables=qry['top_k_tables'],
        ),
        uploads=UploadsConfig(
            dir=u['dir'],
            max_size_mb=u['max_size_mb'],
            allowed_extensions=u['allowed_extensions'],
        ),
        session=SessionConfig(
            ttl_hours=s['ttl_hours'],
            max_concurrent=s['max_concurrent'],
            cleanup_interval_minutes=s['cleanup_interval_minutes'],
        ),
        auth=AuthConfig(
            authority_url=a.get('authority_url', ''),
            client_id=a.get('client_id', ''),
            scopes=a.get('scopes', []),
            redirect_uri=a.get('redirect_uri', ''),
        ),
        services=services,
    )


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = _load_config()
    return _settings