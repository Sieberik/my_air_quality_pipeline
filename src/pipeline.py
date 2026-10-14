import logging
import json
import sqlalchemy
from typing import List, Dict, Any
from src.models import AirLocationRecord, AirQualityRecord


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class AirQualityPipeline:
    def __init__(self, db_url: str, file_path: str):
        self.engine = sqlalchemy.create_engine(db_url)
        self.file_path = file_path
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        query = """
        CREATE TABLE IF NOT EXISTS staging_airlocation
            id SERIAL PRIMARY KEY,
            location VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text(query))
        query = """
        CREATE TAVLE IF NOT EXISTS staging_airquality
            id  SERIAL  PRIMARY KEY,
            location_id INT,
            parameter VARCHAR(50),
            value NUMERIC,
            unit VARCHAR(50),
            last_updated TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); 
        """
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text(query))
        logger.info("Database schema validated and ready.")

    def extract_latest_data(self) -> List[Dict[str, Any]]:
        try:
            logger.info(f"Extracting live data from local file source: {self.file_path}")
            with open(self.file_path, "r") as f:
                data = json.load(f)
            results_list = data.get("results", [])
            return results_list
        except Exception as e:
            logger.error(f"Local file extraction completely failed: {e}")
            return []

    def transform_and_validate(self, raw_results: List) -> tuple:
        valid_locations = []
        valid_measurements = []

        logger.info(f"Processing and validating {len(raw_records)} records...")
        
        for record in raw_results:
            try:
                location_record = AirLocationRecord(**row)

                # Skip completely empty regions early (like Berlin in your feed)
                if not location_record.measurements:
                  continue

                validated_item = AirLocationRecord(**record)
                valid_locations.append(validated_item)

                # 2. Extract and separate the nested measurements list
                for m in location_record.measurements:
                    # Filter out null sensor entries immediately (like Paris PM2.5)
                    if m.value is None:
                        continue
                
                # Temp container to pass to the load phase later
                valid_measurements.append({
                    "location_name": location_record.location_name, # Used to find the ID later
                    "parameter": m.parameter,
                    "value": m.value,
                    "unit": m.unit,
                    "last_updated": m.last_updated
                })

            except Exception as val_error:
                logger.info(f"Skipped a broken structural entry block: {e}")
                continue                
        return valid_locations, valid_measurements

    def load_to_postgres(self, location_records: List[AirLocationRecord], measurement_records: List[Dict[str, Any]]):
        if not location_records:
            logger.info("No fresh location records available to write.")
            return

def load_to_postgres(self, locations: list, measurements: list):
    """Loads a normalized data structure safely into separate relational tables."""
    if not locations or not measurements:
        print("No valid records available to write to the database.")
        return

    # 1. SQL queries using named parameters (:placeholder)
    # RETURNING id tells Postgres to send back the auto-generated SERIAL number instantly
    location_query = """
    INSERT INTO staging_locations (location_name, city, country)
    VALUES (:location_name, :city, :country)
    ON CONFLICT (location_name) DO UPDATE SET city = EXCLUDED.city
    RETURNING id;
    """

    measurement_query = """
    INSERT INTO staging_measurements (location_id, parameter, value, unit, last_updated)
    VALUES (:location_id, :parameter, :value, :unit, :last_updated);
    """

    try:
        # Open an atomic database transaction block
        with self.engine.begin() as conn:
            # We will use this dictionary to keep track of: {"Amsterdam-Stadhouderskade": 1, ...}
            location_id_map = {}

            # STEP A: Insert locations one by one to capture their individual generated IDs
            for loc in locations:
                # Convert the Pydantic model directly into a clean python dictionary
                loc_data = loc.model_dump()
                
                # Execute query and capture the resulting cursor row payload
                result = conn.execute(sqlalchemy.text(location_query), loc_data)
                generated_id = result.scalar()  # .scalar() pulls the single numeric ID integer
                
                # Map the location name to the newly generated database primary key integer
                location_id_map[loc.location_name] = generated_id

            # STEP B: Build the flattened measurements batch payload using the ID mapping
            batch_measurements_payload = []
            for meas in measurements:
                # Use our map dictionary to pull the exact ID number for this location name
                parent_id = location_id_map.get(meas["location_name"])
                
                if parent_id is not None:
                    batch_measurements_payload.append({
                        "location_id": parent_id, # Injecting the verified foreign key integer!
                        "parameter": meas["parameter"],
                        "value": meas["value"],
                        "unit": meas["unit"],
                        "last_updated": meas["last_updated"]
                    })

            # STEP C: Efficiently batch insert all measurements in a single database network trip
            if batch_measurements_payload:
                conn.execute(sqlalchemy.text(measurement_query), batch_measurements_payload)
                print(f"🚀 Success! Normalized {len(locations)} locations and {len(batch_measurements_payload)} measurements into Postgres.")

    except Exception as db_error:
        print(f"❌ Database transactional load sequence aborted: {db_error}")
