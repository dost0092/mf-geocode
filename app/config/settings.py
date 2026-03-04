from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"

    nominatim_base_url: str = "https://nominatim.openstreetmap.org"
    nominatim_user_agent: str = "hotel-geocoder/1.0 (contact: waqasdost@gmail.com)"
    nominatim_rps: float = 1.0

    masterfile_schema: str = "ingestion"
    masterfile_table: str = "hotel_masterfile"
    masterfile_pk: str = "hotel_id"

    col_country_code: str = "country_code"
    col_lat: str = "latitude"
    col_lng: str = "longitude"
    col_state_code: str = "state_code"

    col_address1: str = "address_line_1"
    col_city: str = "city"
    col_state_text: str = "state"
    col_postal: str = "postal_code"

    us_states_schema: str = "test"
    us_states_table: str = "us_states"
    us_states_name_col: str = "state_name"
    us_states_code_col: str = "state_code"

    default_limit: int = 200
    default_max_seconds: int = 25
    max_move_km: float = 50.0

    dry_run: bool = False

settings = Settings()
