{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "67c8657b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Collecting duckdb\n",
      "  Downloading duckdb-1.5.3-cp313-cp313-macosx_11_0_arm64.whl.metadata (4.2 kB)\n",
      "Downloading duckdb-1.5.3-cp313-cp313-macosx_11_0_arm64.whl (15.4 MB)\n",
      "\u001b[2K   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m15.4/15.4 MB\u001b[0m \u001b[31m4.3 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m00:01\u001b[0m00:01\u001b[0m\n",
      "\u001b[?25hInstalling collected packages: duckdb\n",
      "Successfully installed duckdb-1.5.3\n"
     ]
    }
   ],
   "source": [
    "import sys\n",
    "!{sys.executable} -m pip install duckdb"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "909b51f3",
   "metadata": {},
   "source": [
    "# Task 4. Schema from Assignment 1"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "071f3482",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/opt/anaconda3/lib/python3.13/site-packages/pandas/core/computation/expressions.py:22: UserWarning: Pandas requires version '2.10.2' or newer of 'numexpr' (version '2.10.1' currently installed).\n",
      "  from pandas.core.computation.check import NUMEXPR_INSTALLED\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "649da97e784447ffb5bfc0f6d6050a42",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, layout=Layout(width='auto'), style=ProgressStyle(bar_color='black'))"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Database created: assignment2_energy_stream.duckdb\n",
      "facilities: 353 rows\n",
      "facility_units: 353 rows\n",
      "power_emissions_measurements: 710385 rows\n"
     ]
    }
   ],
   "source": [
    "import duckdb\n",
    "import pandas as pd\n",
    "from pathlib import Path\n",
    "\n",
    "CSV_PATH = \"integrated_power_emissions_data.csv\"\n",
    "DB_PATH = \"assignment2_energy_stream.duckdb\"\n",
    "\n",
    "\n",
    "def build_database(csv_path=CSV_PATH, db_path=DB_PATH):\n",
    "    if not Path(csv_path).exists():\n",
    "        raise FileNotFoundError(f\"Cannot find {csv_path}\")\n",
    "\n",
    "    con = duckdb.connect(db_path)\n",
    "\n",
    "    con.execute(\"INSTALL spatial;\")\n",
    "    con.execute(\"LOAD spatial;\")\n",
    "\n",
    "    con.execute(f\"\"\"\n",
    "    CREATE OR REPLACE TABLE raw_power_emissions AS\n",
    "    SELECT *\n",
    "    FROM read_csv_auto('{csv_path}', header=True);\n",
    "    \"\"\")\n",
    "\n",
    "    con.execute(\"\"\"\n",
    "    CREATE OR REPLACE TABLE facilities AS\n",
    "    SELECT DISTINCT\n",
    "        TRIM(facility_code) AS facility_code,\n",
    "        TRIM(facility_name) AS facility_name,\n",
    "        CAST(lat AS DOUBLE) AS latitude,\n",
    "        CAST(lon AS DOUBLE) AS longitude,\n",
    "        ST_GeomFromText('POINT(' || CAST(lon AS VARCHAR) || ' ' || CAST(lat AS VARCHAR) || ')') AS geometry\n",
    "    FROM raw_power_emissions\n",
    "    WHERE facility_code IS NOT NULL\n",
    "      AND facility_name IS NOT NULL\n",
    "      AND lat IS NOT NULL\n",
    "      AND lon IS NOT NULL;\n",
    "    \"\"\")\n",
    "\n",
    "    con.execute(\"\"\"\n",
    "    CREATE OR REPLACE TABLE facility_units AS\n",
    "    SELECT DISTINCT\n",
    "        TRIM(unit_code) AS unit_code,\n",
    "        TRIM(facility_code) AS facility_code\n",
    "    FROM raw_power_emissions\n",
    "    WHERE unit_code IS NOT NULL\n",
    "      AND facility_code IS NOT NULL;\n",
    "    \"\"\")\n",
    "\n",
    "    con.execute(\"\"\"\n",
    "    CREATE OR REPLACE TABLE power_emissions_measurements AS\n",
    "    SELECT\n",
    "        CAST(timestamp AS TIMESTAMP) AS event_time,\n",
    "        TRIM(facility_code) AS facility_code,\n",
    "        TRIM(unit_code) AS unit_code,\n",
    "        CAST(power_mw AS DOUBLE) AS power_mw,\n",
    "        CAST(emissions_t AS DOUBLE) AS emissions_t\n",
    "    FROM raw_power_emissions\n",
    "    WHERE timestamp IS NOT NULL\n",
    "      AND facility_code IS NOT NULL\n",
    "      AND unit_code IS NOT NULL;\n",
    "    \"\"\")\n",
    "\n",
    "    con.execute(\"\"\"\n",
    "    CREATE OR REPLACE TABLE mqtt_received_measurements (\n",
    "        received_at TIMESTAMP DEFAULT current_timestamp,\n",
    "        event_time TIMESTAMP,\n",
    "        facility_name VARCHAR,\n",
    "        facility_code VARCHAR,\n",
    "        unit_code VARCHAR,\n",
    "        power_mw DOUBLE,\n",
    "        emissions_t DOUBLE,\n",
    "        latitude DOUBLE,\n",
    "        longitude DOUBLE,\n",
    "        geometry GEOMETRY\n",
    "    );\n",
    "    \"\"\")\n",
    "\n",
    "    con.execute(\"\"\"\n",
    "    CREATE OR REPLACE VIEW latest_facility_status AS\n",
    "    WITH latest AS (\n",
    "        SELECT\n",
    "            facility_code,\n",
    "            MAX(event_time) AS latest_time\n",
    "        FROM mqtt_received_measurements\n",
    "        GROUP BY facility_code\n",
    "    )\n",
    "    SELECT\n",
    "        m.facility_name,\n",
    "        m.facility_code,\n",
    "        m.unit_code,\n",
    "        m.event_time,\n",
    "        m.power_mw,\n",
    "        m.emissions_t,\n",
    "        m.latitude,\n",
    "        m.longitude,\n",
    "        m.geometry\n",
    "    FROM mqtt_received_measurements m\n",
    "    JOIN latest l\n",
    "      ON m.facility_code = l.facility_code\n",
    "     AND m.event_time = l.latest_time;\n",
    "    \"\"\")\n",
    "\n",
    "    print(\"Database created:\", db_path)\n",
    "\n",
    "    for table in [\"facilities\", \"facility_units\", \"power_emissions_measurements\"]:\n",
    "        n = con.execute(f\"SELECT COUNT(*) FROM {table}\").fetchone()[0]\n",
    "        print(f\"{table}: {n} rows\")\n",
    "\n",
    "    con.close()\n",
    "\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    build_database()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "base",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.13.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
