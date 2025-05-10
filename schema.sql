-- Create tables for climate change data visualization

-- Regions table
CREATE TABLE regions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    latitude REAL,
    longitude REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Countries table
CREATE TABLE countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    region_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (region_id) REFERENCES regions(id)
);

-- Temperature data table
CREATE TABLE temperature_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    temperature REAL NOT NULL,
    anomaly REAL NOT NULL,
    region_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (region_id) REFERENCES regions(id)
);

-- Greenhouse gas emissions table
CREATE TABLE greenhouse_gas_emissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    carbon_dioxide REAL NOT NULL,
    methane REAL NOT NULL,
    nitrous_oxide REAL NOT NULL,
    other_gases REAL NOT NULL,
    total_emissions REAL NOT NULL,
    percent_change REAL NOT NULL,
    country_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (country_id) REFERENCES countries(id)
);

-- Sea level data table
CREATE TABLE sea_level_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    sea_level REAL NOT NULL,
    region_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (region_id) REFERENCES regions(id)
);

-- Extreme weather events table
CREATE TABLE extreme_weather_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    frequency INTEGER NOT NULL,
    severity VARCHAR(20),
    region_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (region_id) REFERENCES regions(id)
);

-- Renewable energy table
CREATE TABLE renewable_energy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    energy_type VARCHAR(50) NOT NULL,
    capacity REAL NOT NULL,
    country_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (country_id) REFERENCES countries(id)
);

-- User tracking tables
CREATE TABLE user_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id VARCHAR(100) NOT NULL UNIQUE,
    user_agent TEXT,
    ip_address VARCHAR(45),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    referrer TEXT,
    device_type VARCHAR(50),
    browser VARCHAR(100),
    os VARCHAR(100)
);

CREATE TABLE page_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id VARCHAR(100) NOT NULL,
    page_url TEXT NOT NULL,
    view_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    time_spent INTEGER, -- in seconds
    scroll_depth INTEGER, -- percentage of page scrolled
    FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
);

CREATE TABLE user_interactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id VARCHAR(100) NOT NULL,
    interaction_type VARCHAR(50) NOT NULL,
    element_id TEXT,
    element_type TEXT,
    interaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
);

CREATE TABLE user_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id VARCHAR(100) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value REAL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
);

-- Create indexes for better query performance
CREATE INDEX idx_temperature_year ON temperature_data(year);
CREATE INDEX idx_ghg_year ON greenhouse_gas_emissions(year);
CREATE INDEX idx_sea_level_year ON sea_level_data(year);
CREATE INDEX idx_weather_year ON extreme_weather_events(year);
CREATE INDEX idx_renewable_year ON renewable_energy(year);
CREATE INDEX idx_region_name ON regions(name);
CREATE INDEX idx_country_name ON countries(name);

-- Create indexes for user tracking tables
CREATE INDEX idx_session_id ON user_sessions(session_id);
CREATE INDEX idx_page_views_session ON page_views(session_id);
CREATE INDEX idx_interactions_session ON user_interactions(session_id);
CREATE INDEX idx_metrics_session ON user_metrics(session_id);

-- Insert default regions
INSERT INTO regions (name) VALUES 
('Global'),
('Arctic'),
('Antarctic'),
('North America'),
('South America'),
('Europe'),
('Asia'),
('Africa'),
('Oceania');

-- Insert default countries
INSERT INTO countries (name, region_id) VALUES 
('United States', (SELECT id FROM regions WHERE name = 'North America')),
('China', (SELECT id FROM regions WHERE name = 'Asia')),
('India', (SELECT id FROM regions WHERE name = 'Asia')),
('European Union', (SELECT id FROM regions WHERE name = 'Europe')),
('Brazil', (SELECT id FROM regions WHERE name = 'South America')); 