"""Application settings loaded from environment variables.

All configuration flows through this module. Secrets are held in
:class:`pydantic.SecretStr` fields and must be explicitly unwrapped with
``.get_secret_value()`` before use, which keeps them out of ``repr`` and
logs by default.

Environment variables take precedence over values in a ``.env`` file. The
pydantic-settings loader handles ``.env`` discovery internally (it uses
python-dotenv under the hood), so no manual ``load_dotenv`` call is needed.

There are **no hardcoded defaults for secrets**: every secret field is
required and will raise :class:`pydantic.ValidationError` if missing.
"""

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Audit database connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="ARCHETYPE_DB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    audit_url: SecretStr = Field(
        ...,
        description="SQLAlchemy URL for the audit database (credentials embedded).",
    )


class AWSSettings(BaseSettings):
    """AWS client configuration and S3 bucket names."""

    model_config = SettingsConfigDict(
        env_prefix="ARCHETYPE_AWS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    region: str = Field(..., description="AWS region for all regional resources.")
    endpoint_url: str | None = Field(
        default=None,
        description="Optional endpoint override (e.g. LocalStack at http://localhost:4566).",
    )
    kinesis_stream_name: str | None = Field(
        default=None,
        description="Kinesis stream name for real-time document ingestion.",
    )
    raw_bucket: str = Field(..., description="S3 bucket for raw source artifacts.")
    processed_bucket: str = Field(..., description="S3 bucket for processed outputs.")
    audit_bucket: str = Field(..., description="S3 bucket for audit artifacts.")
    access_key_id: SecretStr | None = Field(
        default=None,
        description="Optional static access key; prefer IAM role credentials in production.",
    )
    secret_access_key: SecretStr | None = Field(
        default=None,
        description="Optional static secret key; prefer IAM role credentials in production.",
    )


class AirflowSettings(BaseSettings):
    """Airflow / MWAA runtime configuration."""

    model_config = SettingsConfigDict(
        env_prefix="ARCHETYPE_AIRFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    dag_bucket: str = Field(..., description="S3 bucket holding Airflow DAGs.")
    environment_name: str = Field(..., description="MWAA environment name.")
    execution_role_arn: SecretStr | None = Field(
        default=None,
        description="ARN of the MWAA execution role (sensitive — identifies the account).",
    )


class DatabricksSettings(BaseSettings):
    """Databricks workspace and SQL warehouse configuration."""

    model_config = SettingsConfigDict(
        env_prefix="ARCHETYPE_DATABRICKS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(..., description="Databricks workspace URL.")
    warehouse_id: str = Field(..., description="SQL warehouse ID for statement execution.")
    catalog: str = Field(..., description="Unity Catalog catalog name.")
    schema_name: str = Field(default="default", description="Unity Catalog schema name.")


class BedrockSettings(BaseSettings):
    """Amazon Bedrock model and region configuration."""

    model_config = SettingsConfigDict(
        env_prefix="ARCHETYPE_BEDROCK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    model_id: str = Field(..., description="Bedrock model identifier.")
    region: str = Field(..., description="AWS region for the Bedrock runtime endpoint.")


class Settings(BaseSettings):
    """Top-level settings container aggregating every config section."""

    model_config = SettingsConfigDict(
        env_prefix="ARCHETYPE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = Field(
        ...,
        description="Deployment environment identifier (dev, staging, prod).",
    )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    aws: AWSSettings = Field(default_factory=AWSSettings)
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)
    bedrock: BedrockSettings = Field(default_factory=lambda: BedrockSettings())
    databricks: DatabricksSettings = Field(default_factory=lambda: DatabricksSettings())
