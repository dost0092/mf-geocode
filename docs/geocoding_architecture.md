## Geocoding architecture – tiered strategy, batching, and fallbacks

### Scope and goals

- **Goal**: Backfill and validate geocodes for **711,245+ hotels** whose coordinates are bad or NULL, starting with **9,077 US hotels**.
- **Constraints**: Avoid API rate-limit violations, keep costs predictable, and make the pipeline easy to extend (Tier 2, Tier 3) without changing Tier 1 logic.

The current codebase already implements the first part of **Tier 1 (US, full-address focus)** using **Nominatim** and the `hotel_masterfile` table. This document describes the full architecture around that code and how to extend it.

---

### Tiered geocoding strategy

- **Tier 1 – US hotels**
  - **Tier 1A – US with existing coordinates but missing state**
    - Input: `country_code = 'US'`, `latitude/longitude` present and non‑zero, `state_code` NULL/empty.
    - Action: **Reverse** geocode with Nominatim to derive `state_name`, then normalize to `state_code`.
    - Implementation: `run_us_missing_state_with_coords` (current code).
  - **Tier 1B – US with bad/NULL coordinates**
    - Input: `country_code = 'US'` and `latitude/longitude` NULL or 0.
    - Action: **Forward** geocode using full US address (address line 1, city, state, postal, country).
    - Validation: Use `validate_candidate` + `haversine_km` to ensure candidates are reasonable.
    - Implementation: `run_us_missing_latlng` (current code).

- **Tier 2 – International hotels (non‑US)**
  - Input: `country_code <> 'US'`, coordinates NULL or 0, or clearly out of bounds.
  - Strategy:
    - Start with **city + country** first (cheaper, fewer requests).
    - Where possible, add `address_line_1` and `postal_code`.
  - Geocoders:
    - Primary: existing Nominatim client, with **country bias** via `country_code`.
    - Optional: plug‑in commercial geocoder (e.g., Google, Here, Mapbox) per configuration.
  - Status in code:
    - No Tier 2 logic is wired yet; this document defines how to implement it without impacting Tier 1.

- **Tier 3 – Complex international cases**
  - Examples: ambiguous cities, hotels in regions with poor addressing, or repeated geocode failures.
  - Strategy:
    - Use additional signals (hotel brand metadata, region metadata, known bounding boxes).
    - Potentially defer to a human‑in‑the‑loop or dedicated data provider.

---

### Batch processing and rate‑limit control

- **Driver pattern (already implemented for Tier 1 US)**
  - The `run_us_missing_state_with_coords` and `run_us_missing_latlng` functions:
    - Pull **a limited batch** of candidate hotels using `repo.fetch_*`.
    - Iterate row‑by‑row and stop when:
      - **`processed` reaches `limit`**, or
      - **elapsed time exceeds `max_seconds`**.
  - Both are exposed via the `/geocode/us/run` endpoint with:
    - `limit`: max rows to process in one run.
    - `max_seconds`: hard cap on processing time.
    - `commit`: whether to persist or roll back.

- **Rate‑limit compliance**
  - Nominatim requests are already throttled by the `nominatim_rps` setting.
  - Each geocoder must:
    - Implement its own **rate limiter** against its provider’s policy.
    - Respect a **shared concurrency limit** if multiple worker processes run in parallel.

- **Scaling to 711K+ hotels**
  - Run the pipeline in **small batches**, e.g. 200–1000 hotels per run:
    - Use job scheduler (cron) or orchestration (Airflow, Prefect, etc.) to call the API regularly.
  - Track:
    - **Selected**: rows fetched from DB.
    - **Processed / Updated / Failed**: per run, aggregated in external monitoring.
  - For large backfills:
    - Run with **`commit=false`** in lower environments to validate behavior before production.

---

### Fallback chain

The fallback chain is defined **per tier** but follows the same pattern:

1. **Primary geocoder**
   - Tier 1: Nominatim (current implementation).
   - Tier 2+: configurable (can remain Nominatim or change).

2. **Secondary geocoder**
   - Optional, used when:
     - Primary returns no result.
     - Validation flags output as suspicious.
   - Example: a commercial provider configured via settings.

3. **Flag for manual review**
   - If both geocoders fail or the result fails validation:
     - Do not update coordinates.
     - Write row into a **manual review table** or queue, e.g. `geocode_review_queue`.

**Implementation note:** current code only uses a primary geocoder (Nominatim). The abstraction should allow plugging in a secondary geocoder for Tier 2+ without changing Tier 1 function signatures.

---

### Validation rules (detecting bad geocodes)

Validation is centralized in `validators.py`:

- **Coordinate sanity check – `coord_ok`**
  - Rejects:
    - `NULL` latitude/longitude.
    - Zero coordinates (`0,0`).
    - Values outside \[-90, 90\] latitude or \[-180, 180\] longitude.

- **Movement check – `validate_candidate`**
  - Computes distance using `haversine_km`.
  - If original coordinates are valid:
    - Rejects candidate when distance exceeds `max_move_km` (settings‑driven, currently 50 km).
  - Returns `(ok: bool, reason: str)` for easier debugging.

Additional validation for Tier 2 and Tier 3:

- **City/region bounds**
  - Maintain a table of **city/region bounding boxes** by ID.
  - After geocoding, ensure the candidate falls within:
    - The hotel’s city bounding box (if available), **or**
    - A broader region/country bounding box.

- **Cross‑field consistency**
  - Compare:
    - Nominatim’s `country_code`, `state`, `city` with masterfile’s values.
    - If they disagree (e.g. geocode city/state is different from masterfile), downgrade confidence or send to manual review.

---

### Storage, updates, and rollback plan

- **Primary store**: `ingestion.hotel_masterfile`
  - Fields:
    - `latitude`, `longitude`, `state_code`, plus address fields.
  - Updates:
    - Per‑row `UPDATE` statements via `repo.update_state_code` and `repo.update_latlng_and_state`.

- **Change tracking**
  - Each run should be identified by:
    - A run ID or timestamp.
    - Pipeline mode (`missing_state_with_coords`, `missing_latlng`, or future Tier 2 modes).
  - Options:
    - Add audit columns to `hotel_masterfile` (e.g. `geocode_source`, `geocode_run_id`, `geocode_updated_at`), or
    - Maintain a separate **audit table** (recommended for larger backfills).

- **Rollback plan**
  - For safety in production:
    - Keep a snapshot of affected rows (by run_id) in an audit table **before** updating.
    - To rollback:
      - Re‑apply original `latitude`, `longitude`, `state_code` from audit.
  - During testing:
    - Use API parameter `commit=false` or `settings.dry_run=True` to avoid permanent changes.

---

### Cost estimation (per tier)

Assuming:

- **Free/open source geocoder (Nominatim)**:
  - Monetary cost is effectively **$0**, but you are constrained by:
    - Strict usage policy.
    - Low allowed requests per second.
  - Recommended for **Tier 1 (US subset)** and validation/spot checks.

- **Commercial geocoder (Tier 2+)**
  - Prices vary, but many providers charge in bands, e.g.:
    - $X per 1,000 requests, or
    - Monthly allowance with overage charges.

Approximate cost calculation approach:

- Let:
  - \(H\) = 711,245 hotels.
  - \(p\_tier1\) = proportion of hotels processed entirely by free/open source (Nominatim).
  - \(p\_tier2\) = proportion that fall back to a commercial provider.
  - \(R\) = average number of requests per hotel (1–2).
  - \(C\_{1k}\) = cost per 1,000 commercial requests.

- Then:
\[
\text{Total commercial requests} \approx H \cdot p\_{tier2} \cdot R
\]
\[
\text{Total cost} \approx \frac{\text{Total commercial requests}}{1000} \cdot C\_{1k}
\]

- Example (for planning only):
  - Assume 80% of hotels are resolved in Tier 1 with Nominatim (`p_tier1=0.8`), 20% (`p_tier2=0.2`) need Tier 2.
  - Assume 1.5 requests/hotel on average.
  - Assume $5 per 1,000 commercial requests.
  - Then:
    - Total commercial requests ≈ 711,245 × 0.2 × 1.5 ≈ 213,373.
    - Total cost ≈ 213.4 × $5 ≈ **$1,067**.

Actual numbers should be recalculated using:

- Real failure rate of Tier 1 (from logs).
- Actual contracted pricing for the chosen Tier 2 provider.

---

### Mapping to current code and future‑proofing

- **Current Tier 1 US implementation (in code)**
  - `pipeline.py`:
    - `run_us_missing_state_with_coords` – Tier 1A.
    - `run_us_missing_latlng` – Tier 1B.
  - `repo.py`:
    - `fetch_us_missing_state_with_coords`, `fetch_us_missing_latlng`, and update helpers.
  - `validators.py`:
    - `coord_ok`, `haversine_km`, `validate_candidate`.
  - `routes/geocode.py`:
    - `/geocode/us/run` – entrypoint for Tier 1 US modes.

- **Future Tier 2 (international) work – without breaking Tier 1**
  - Add new repo functions:
    - `fetch_intl_missing_latlng`, `fetch_intl_bad_latlng`, etc.
  - Add new pipeline functions:
    - `run_intl_missing_latlng`, optionally wired under `/geocode/intl/run`.
  - Introduce a simple geocoder interface:
    - `Geocoder.forward(...)`, `Geocoder.reverse(...)`.
    - Implementations for Nominatim and any commercial provider.
  - Reuse `validate_candidate` and extend with city/country bounding‑box checks where data is available.

This design keeps **Tier 1 US** stable while providing a clear, incremental path for **Tier 2 international** and **Tier 3 complex** hotels.

