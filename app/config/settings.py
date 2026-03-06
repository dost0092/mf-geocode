from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field
from urllib.parse import quote_plus


class Settings(BaseSettings):
    """
    Application configuration loaded from .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

    # ==============================
    # Database Configuration
    # ==============================
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "postgres"
    db_user: str = "postgres"
    db_password: str = "postgres"

    @computed_field
    @property
    def database_url(self) -> str:
        password = quote_plus(self.db_password)
        return (
            f"postgresql+psycopg2://{self.db_user}:"
            f"{password}@{self.db_host}:"
            f"{self.db_port}/{self.db_name}"
        )

    # ==============================
    # Nominatim Configuration
    # ==============================
    nominatim_base_url: str = "https://nominatim.openstreetmap.org"
    nominatim_user_agent: str = (
        "hotel-geocoder/1.0 (contact: waqasdost@gmail.com)"
    )
    nominatim_rps: float = 1.0  # Requests per second (OSM limit = 1 rps)

    # ==============================
    # Masterfile Table Configuration
    # ==============================
    masterfile_schema: str = "test"
    masterfile_table: str = "hotel_masterfile"
    masterfile_pk: str = "id"

    # Column mappings
    col_country_code: str = "country_code"
    col_lat: str = "latitude"
    col_lng: str = "longitude"
    col_state_code: str = "state_code"

    col_address1: str = "address_line_1"
    col_city: str = "city"
    col_state_text: str = "state"
    col_postal: str = "postal_code"

    # ==============================
    # US States Reference Table
    # ==============================
    us_states_schema: str = "test"
    us_states_table: str = "us_states"
    us_states_name_col: str = "state_name"
    us_states_code_col: str = "state_code"

    # ==============================
    # Processing Controls
    # ==============================
    default_limit: int = 200
    default_max_seconds: int = 25
    max_move_km: float = 50.0

    # ==============================
    # Runtime Flags
    # ==============================
    dry_run: bool = False


settings = Settings()