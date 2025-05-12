import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self, db_path: str = "climate_data.db"):
        """Initialize database connection."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established successfully")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def execute_query(self, query: str, params: tuple = ()) -> List[tuple]:
        """Execute a SQL query and return results."""
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error executing query: {e}")
            raise

    def execute_many(self, query: str, params: List[tuple]):
        """Execute multiple SQL statements."""
        try:
            self.cursor.executemany(query, params)
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error executing multiple statements: {e}")
            raise

    def get_temperature_data(self, region: Optional[str] = None, start_year: Optional[int] = None) -> pd.DataFrame:
        """Get temperature data for a specific region and time period."""
        query = """
        SELECT t.year, t.temperature, t.anomaly, r.name as region
        FROM temperature_data t
        JOIN regions r ON t.region_id = r.id
        WHERE 1=1
        """
        params = []
        
        if region:
            query += " AND r.name = ?"
            params.append(region)
        if start_year:
            query += " AND t.year >= ?"
            params.append(start_year)
            
        query += " ORDER BY t.year"
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except sqlite3.Error as e:
            logger.error(f"Error fetching temperature data: {e}")
            raise

    def get_emissions_data(self, country: Optional[str] = None, start_year: Optional[int] = None) -> pd.DataFrame:
        """Get greenhouse gas emissions data for a specific country and time period."""
        query = """
        SELECT g.year, g.carbon_dioxide, g.methane, g.nitrous_oxide,
               g.other_gases, g.total_emissions, g.percent_change,
               c.name as country
        FROM greenhouse_gas_emissions g
        JOIN countries c ON g.country_id = c.id
        WHERE 1=1
        """
        params = []
        
        if country:
            query += " AND c.name = ?"
            params.append(country)
        if start_year:
            query += " AND g.year >= ?"
            params.append(start_year)
            
        query += " ORDER BY g.year"
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except sqlite3.Error as e:
            logger.error(f"Error fetching emissions data: {e}")
            raise

    def get_sea_level_data(self, region: Optional[str] = None, start_year: Optional[int] = None) -> pd.DataFrame:
        """Get sea level data for a specific region and time period."""
        query = """
        SELECT s.year, s.sea_level, r.name as region
        FROM sea_level_data s
        JOIN regions r ON s.region_id = r.id
        WHERE 1=1
        """
        params = []
        
        if region:
            query += " AND r.name = ?"
            params.append(region)
        if start_year:
            query += " AND s.year >= ?"
            params.append(start_year)
            
        query += " ORDER BY s.year"
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except sqlite3.Error as e:
            logger.error(f"Error fetching sea level data: {e}")
            raise

    def get_extreme_weather_data(self, region: Optional[str] = None, start_year: Optional[int] = None) -> pd.DataFrame:
        """Get extreme weather events data for a specific region and time period."""
        query = """
        SELECT w.year, w.event_type, w.frequency, w.severity, r.name as region
        FROM extreme_weather_events w
        JOIN regions r ON w.region_id = r.id
        WHERE 1=1
        """
        params = []
        
        if region:
            query += " AND r.name = ?"
            params.append(region)
        if start_year:
            query += " AND w.year >= ?"
            params.append(start_year)
            
        query += " ORDER BY w.year"
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except sqlite3.Error as e:
            logger.error(f"Error fetching extreme weather data: {e}")
            raise

    def get_renewable_energy_data(self, country: Optional[str] = None, start_year: Optional[int] = None) -> pd.DataFrame:
        """Get renewable energy data for a specific country and time period."""
        query = """
        SELECT r.year, r.energy_type, r.capacity, c.name as country
        FROM renewable_energy r
        JOIN countries c ON r.country_id = c.id
        WHERE 1=1
        """
        params = []
        
        if country:
            query += " AND c.name = ?"
            params.append(country)
        if start_year:
            query += " AND r.year >= ?"
            params.append(start_year)
            
        query += " ORDER BY r.year"
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except sqlite3.Error as e:
            logger.error(f"Error fetching renewable energy data: {e}")
            raise

    def insert_temperature_data(self, data: List[Dict[str, Any]]):
        """Insert temperature data into the database."""
        query = """
        INSERT INTO temperature_data (year, temperature, anomaly, region_id)
        VALUES (?, ?, ?, (SELECT id FROM regions WHERE name = ?))
        """
        params = [(d['year'], d['temperature'], d['anomaly'], d['region']) for d in data]
        self.execute_many(query, params)

    def insert_emissions_data(self, data: List[Dict[str, Any]]):
        """Insert greenhouse gas emissions data into the database."""
        query = """
        INSERT INTO greenhouse_gas_emissions 
        (year, carbon_dioxide, methane, nitrous_oxide, other_gases, 
         total_emissions, percent_change, country_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, (SELECT id FROM countries WHERE name = ?))
        """
        params = [(
            d['year'], d['carbon_dioxide'], d['methane'], d['nitrous_oxide'],
            d['other_gases'], d['total_emissions'], d['percent_change'], d['country']
        ) for d in data]
        self.execute_many(query, params)

    def insert_sea_level_data(self, data: List[Dict[str, Any]]):
        """Insert sea level data into the database."""
        query = """
        INSERT INTO sea_level_data (year, sea_level, region_id)
        VALUES (?, ?, (SELECT id FROM regions WHERE name = ?))
        """
        params = [(d['year'], d['sea_level'], d['region']) for d in data]
        self.execute_many(query, params)

    def insert_extreme_weather_data(self, data: List[Dict[str, Any]]):
        """Insert extreme weather events data into the database."""
        query = """
        INSERT INTO extreme_weather_events 
        (year, event_type, frequency, severity, region_id)
        VALUES (?, ?, ?, ?, (SELECT id FROM regions WHERE name = ?))
        """
        params = [(
            d['year'], d['event_type'], d['frequency'], 
            d['severity'], d['region']
        ) for d in data]
        self.execute_many(query, params)

    def insert_renewable_energy_data(self, data: List[Dict[str, Any]]):
        """Insert renewable energy data into the database."""
        query = """
        INSERT INTO renewable_energy (year, energy_type, capacity, country_id)
        VALUES (?, ?, ?, (SELECT id FROM countries WHERE name = ?))
        """
        params = [(
            d['year'], d['energy_type'], d['capacity'], d['country']
        ) for d in data]
        self.execute_many(query, params) 