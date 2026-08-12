"""Pydantic configuration models for the Elasticsearch component."""

import re
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator

# Elasticsearch time value, e.g. "5m", "30s", "1h", "2d". Used to validate pit_keep_alive.
_ES_TIME_VALUE_RE = re.compile(r"^\d+(nanos|micros|ms|s|m|h|d)$")


class AuthType(str, Enum):
    basic = "basic"
    api_key = "api_key"
    bearer = "bearer"
    no_auth = "no_auth"


class SearchMethod(str, Enum):
    scroll = "scroll"
    search_after = "search_after"


class DbConfig(BaseModel):
    hostname: str
    port: int


class AuthenticationConfig(BaseModel):
    auth_type: AuthType = AuthType.no_auth
    username: Optional[str] = None
    password: Optional[str] = Field(None, alias="#password")
    api_key_id: Optional[str] = None
    api_key: Optional[str] = Field(None, alias="#api_key")
    bearer: Optional[str] = Field(None, alias="#bearer")

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_auth_fields(self) -> "AuthenticationConfig":
        if self.auth_type == AuthType.basic:
            if not self.username or not self.password:
                raise ValueError("You must specify both username and password for basic type authorization")
        elif self.auth_type == AuthType.api_key:
            if not self.api_key_id or not self.api_key:
                raise ValueError("You must specify both api_key_id and api_key for api_key type authorization")
        elif self.auth_type == AuthType.bearer:
            if not self.bearer:
                raise ValueError("You must specify bearer token for bearer type authorization")
        return self


class SshKeysConfig(BaseModel):
    private_key: Optional[str] = Field(None, alias="#private")

    model_config = {"populate_by_name": True}


class SshOptionsConfig(BaseModel):
    enabled: bool = False
    keys: Optional[SshKeysConfig] = None
    user: Optional[str] = None
    sshHost: Optional[str] = None
    sshPort: int = 22


class DateConfig(BaseModel):
    append_date: bool = False
    format: str = "%Y-%m-%d"
    shift: str = "yesterday"
    time_zone: str = "UTC"


class Configuration(BaseModel):
    db: DbConfig
    authentication: Optional[AuthenticationConfig] = None
    ssh_options: Optional[SshOptionsConfig] = Field(None, alias="ssh_options")
    date: DateConfig = Field(default_factory=DateConfig)
    request_body: str = "{}"
    index_name: str = ""
    storage_table: str = "ex-elasticsearch-result"
    primary_keys: list[str] = Field(default_factory=list)
    incremental: bool = False
    include_meta_fields: bool = False
    search_method: SearchMethod = SearchMethod.scroll
    pit_keep_alive: str = "5m"
    scheme: str = "http"
    # Legacy SSH dict — present means legacy mode
    ssh: Optional[dict] = None

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_pit_keep_alive(self) -> "Configuration":
        # Only enforced for the PIT + search_after path; the scroll path never reads this value,
        # so existing scroll configs (which default to "5m") are unaffected.
        if self.search_method != SearchMethod.search_after:
            return self
        normalized = self.pit_keep_alive.strip()
        if not _ES_TIME_VALUE_RE.match(normalized):
            raise ValueError(
                "pit_keep_alive must be an Elasticsearch time value such as '5m', '30s', '1h' or '2d' "
                f"(got '{self.pit_keep_alive}')."
            )
        # Store the trimmed value so it is sent to Elasticsearch exactly as validated.
        self.pit_keep_alive = normalized
        return self
