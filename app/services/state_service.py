from typing import Optional
from sqlalchemy import text
from app.config.settings import settings

def normalize_state_code(db, state_name_or_code: Optional[str]) -> Optional[str]:
    if not state_name_or_code:
        return None
    s = str(state_name_or_code).strip()
    if len(s) == 2 and s.isalpha():
        return s.upper()

    q = text(f'''
        SELECT {settings.us_states_code_col}
        FROM {settings.us_states_schema}.{settings.us_states_table}
        WHERE lower({settings.us_states_name_col}) = lower(:name)
           OR lower({settings.us_states_name_col}) LIKE lower(:name_like)
        LIMIT 1
    ''')
    row = db.execute(q, {"name": s, "name_like": f"%{s}%"}).fetchone()
    return row[0].upper() if row and row[0] else None
