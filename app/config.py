"""Configuration loader from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration."""

    # BigQuery
    BQ_PROJECT_ID: str = os.getenv("BQ_PROJECT_ID", "")
    BQ_DATASET: str = os.getenv("BQ_DATASET", "")
    BQ_TABLE: str = os.getenv("BQ_TABLE", "")

    # OpenAI
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    DEFAULT_LLM_MODEL: str = os.getenv("DEFAULT_LLM_MODEL", "gpt-4o-mini")

    # Zapier
    ZAPIER_HOOK_URL: str = os.getenv("ZAPIER_HOOK_URL", "")

    # Email defaults
    INSIGHTS_SEND_TO: str = os.getenv("INSIGHTS_SEND_TO", "")

    # Timezone
    TIMEZONE: str = "Europe/London"

    @classmethod
    def validate(cls) -> None:
        """Validate required environment variables are set."""
        required = [
            "BQ_PROJECT_ID",
            "BQ_DATASET",
            "BQ_TABLE",
            "OPENAI_API_KEY",
            "ZAPIER_HOOK_URL"
        ]
        missing = [key for key in required if not getattr(cls, key)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    @classmethod
    def get_full_table_id(cls) -> str:
        """Return fully-qualified BigQuery table ID."""
        return f"{cls.BQ_PROJECT_ID}.{cls.BQ_DATASET}.{cls.BQ_TABLE}"


config = Config()
