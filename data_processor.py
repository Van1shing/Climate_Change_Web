import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from db_connection import DatabaseConnection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClimateDataProcessor:
    def __init__(self, db_path: str = "climate_data.db"):
        """Initialize the data processor with database connection."""
        self.db = DatabaseConnection(db_path)
        self.db.connect()

    def __del__(self):
        """Clean up database connection."""
        if hasattr(self, 'db'):
            self.db.disconnect()

    def get_temperature_trends(self, region: Optional[str] = None, start_year: Optional[int] = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Get temperature trends and analysis for a specific region and time period.
        Returns both the data and analysis summary.
        """
        try:
            df = self.db.get_temperature_data(region, start_year)
            
            # Calculate trends
            analysis = {
                'mean_temperature': df['temperature'].mean(),
                'max_temperature': df['temperature'].max(),
                'min_temperature': df['temperature'].min(),
                'temperature_increase': df['temperature'].iloc[-1] - df['temperature'].iloc[0],
                'anomaly_trend': df['anomaly'].mean(),
                'yearly_change': df['temperature'].diff().mean()
            }
            
            return df, analysis
        except Exception as e:
            logger.error(f"Error processing temperature data: {e}")
            raise

    def get_emissions_analysis(self, country: Optional[str] = None, start_year: Optional[int] = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Get greenhouse gas emissions data and analysis for a specific country and time period.
        Returns both the data and analysis summary.
        """
        try:
            df = self.db.get_emissions_data(country, start_year)
            
            # Calculate emissions analysis
            analysis = {
                'total_emissions': df['total_emissions'].sum(),
                'emissions_per_year': df['total_emissions'].mean(),
                'max_emissions_year': df.loc[df['total_emissions'].idxmax(), 'year'],
                'emissions_change': df['percent_change'].mean(),
                'co2_contribution': (df['carbon_dioxide'] / df['total_emissions']).mean() * 100,
                'methane_contribution': (df['methane'] / df['total_emissions']).mean() * 100
            }
            
            return df, analysis
        except Exception as e:
            logger.error(f"Error processing emissions data: {e}")
            raise

    def get_sea_level_analysis(self, region: Optional[str] = None, start_year: Optional[int] = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Get sea level data and analysis for a specific region and time period.
        Returns both the data and analysis summary.
        """
        try:
            df = self.db.get_sea_level_data(region, start_year)
            
            # Calculate sea level analysis
            analysis = {
                'mean_sea_level': df['sea_level'].mean(),
                'sea_level_rise': df['sea_level'].iloc[-1] - df['sea_level'].iloc[0],
                'yearly_rise_rate': df['sea_level'].diff().mean(),
                'max_sea_level': df['sea_level'].max(),
                'min_sea_level': df['sea_level'].min()
            }
            
            return df, analysis
        except Exception as e:
            logger.error(f"Error processing sea level data: {e}")
            raise

    def get_extreme_weather_analysis(self, region: Optional[str] = None, start_year: Optional[int] = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Get extreme weather events data and analysis for a specific region and time period.
        Returns both the data and analysis summary.
        """
        try:
            df = self.db.get_extreme_weather_data(region, start_year)
            
            # Calculate extreme weather analysis
            analysis = {
                'total_events': df['frequency'].sum(),
                'events_per_year': df.groupby('year')['frequency'].sum().mean(),
                'most_common_event': df.groupby('event_type')['frequency'].sum().idxmax(),
                'severity_distribution': df['severity'].value_counts().to_dict(),
                'yearly_trend': df.groupby('year')['frequency'].sum().diff().mean()
            }
            
            return df, analysis
        except Exception as e:
            logger.error(f"Error processing extreme weather data: {e}")
            raise

    def get_renewable_energy_analysis(self, country: Optional[str] = None, start_year: Optional[int] = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Get renewable energy data and analysis for a specific country and time period.
        Returns both the data and analysis summary.
        """
        try:
            df = self.db.get_renewable_energy_data(country, start_year)
            
            # Calculate renewable energy analysis
            analysis = {
                'total_capacity': df['capacity'].sum(),
                'capacity_per_year': df.groupby('year')['capacity'].sum().mean(),
                'most_common_type': df['energy_type'].value_counts().idxmax(),
                'capacity_growth': df.groupby('year')['capacity'].sum().diff().mean(),
                'type_distribution': df.groupby('energy_type')['capacity'].sum().to_dict()
            }
            
            return df, analysis
        except Exception as e:
            logger.error(f"Error processing renewable energy data: {e}")
            raise

    def get_correlation_analysis(self, start_year: Optional[int] = None) -> Dict:
        """
        Analyze correlations between different climate indicators.
        Returns correlation analysis summary.
        """
        try:
            # Get data for all indicators
            temp_df, _ = self.get_temperature_trends(start_year=start_year)
            emissions_df, _ = self.get_emissions_analysis(start_year=start_year)
            sea_level_df, _ = self.get_sea_level_analysis(start_year=start_year)
            
            # Merge dataframes on year
            merged_df = pd.merge(temp_df, emissions_df, on='year', how='inner')
            merged_df = pd.merge(merged_df, sea_level_df, on='year', how='inner')
            
            # Calculate correlations
            correlations = {
                'temperature_emissions': merged_df['temperature'].corr(merged_df['total_emissions']),
                'temperature_sea_level': merged_df['temperature'].corr(merged_df['sea_level']),
                'emissions_sea_level': merged_df['total_emissions'].corr(merged_df['sea_level'])
            }
            
            return correlations
        except Exception as e:
            logger.error(f"Error performing correlation analysis: {e}")
            raise

    def get_regional_comparison(self, metric: str, regions: List[str], start_year: Optional[int] = None) -> Dict:
        """
        Compare specific climate metrics across different regions.
        Returns comparison analysis.
        """
        try:
            comparison = {}
            
            for region in regions:
                if metric == 'temperature':
                    df, analysis = self.get_temperature_trends(region, start_year)
                    comparison[region] = {
                        'mean_temperature': analysis['mean_temperature'],
                        'temperature_increase': analysis['temperature_increase']
                    }
                elif metric == 'sea_level':
                    df, analysis = self.get_sea_level_analysis(region, start_year)
                    comparison[region] = {
                        'mean_sea_level': analysis['mean_sea_level'],
                        'sea_level_rise': analysis['sea_level_rise']
                    }
                elif metric == 'extreme_weather':
                    df, analysis = self.get_extreme_weather_analysis(region, start_year)
                    comparison[region] = {
                        'total_events': analysis['total_events'],
                        'events_per_year': analysis['events_per_year']
                    }
            
            return comparison
        except Exception as e:
            logger.error(f"Error performing regional comparison: {e}")
            raise

def process_temperature_data():
    """Process global temperature data"""
    try:
        # Read the temperature data
        temp_df = pd.read_csv('data/Global Temperature Data.csv', skiprows=1)
        
        # Extract year and annual mean (J-D column)
        temp_data = temp_df[['Year', 'J-D']].copy()
        
        # Rename columns
        temp_data.columns = ['year', 'temperature']
        
        # Convert to numeric, replacing '***' with NaN
        temp_data['temperature'] = pd.to_numeric(temp_data['temperature'], errors='coerce')
        
        # Drop rows with missing values
        temp_data = temp_data.dropna()
        
        # Calculate baseline (pre-industrial average, 1880-1900)
        baseline = temp_data[(temp_data['year'] >= 1880) & (temp_data['year'] <= 1900)]['temperature'].mean()
        
        # Calculate temperature anomaly
        temp_data['anomaly'] = temp_data['temperature'] - baseline
        
        # Convert to dictionary format for JSON
        result = {
            'years': temp_data['year'].tolist(),
            'values': temp_data['temperature'].tolist(),
            'anomalies': temp_data['anomaly'].tolist(),
            'baseline': baseline
        }
        
        return result
    except Exception as e:
        print(f"Error processing temperature data: {e}")
        return {'years': [], 'values': [], 'anomalies': [], 'baseline': 0}

def process_greenhouse_gas_data():
    """Process greenhouse gas emissions data"""
    try:
        # Read the greenhouse gas data, skipping the header rows
        ghg_df = pd.read_csv('data/Greenhouse Gas Emissions Data.csv', skiprows=4)
        
        # Rename columns
        ghg_df.columns = ['year', 'hfcs_pfcs_sf6_nf3', 'nitrous_oxide', 'methane', 'carbon_dioxide']
        
        # Convert to numeric
        for col in ghg_df.columns[1:]:
            ghg_df[col] = pd.to_numeric(ghg_df[col], errors='coerce')
        
        # Drop rows with missing values
        ghg_df = ghg_df.dropna()
        
        # Calculate total emissions
        ghg_df['total_emissions'] = ghg_df['hfcs_pfcs_sf6_nf3'] + ghg_df['nitrous_oxide'] + ghg_df['methane'] + ghg_df['carbon_dioxide']
        
        # Calculate percentage change from baseline (1990)
        baseline = ghg_df[ghg_df['year'] == 1990]['total_emissions'].iloc[0]
        ghg_df['percent_change'] = ((ghg_df['total_emissions'] - baseline) / baseline) * 100
        
        # Convert to dictionary format for JSON
        result = {
            'years': ghg_df['year'].tolist(),
            'total_emissions': ghg_df['total_emissions'].tolist(),
            'percent_change': ghg_df['percent_change'].tolist(),
            'by_gas': {
                'carbon_dioxide': ghg_df['carbon_dioxide'].tolist(),
                'methane': ghg_df['methane'].tolist(),
                'nitrous_oxide': ghg_df['nitrous_oxide'].tolist(),
                'other': ghg_df['hfcs_pfcs_sf6_nf3'].tolist()
            },
            'baseline': baseline
        }
        
        return result
    except Exception as e:
        print(f"Error processing greenhouse gas data: {e}")
        return {
            'years': [], 
            'total_emissions': [], 
            'percent_change': [], 
            'by_gas': {
                'carbon_dioxide': [],
                'methane': [],
                'nitrous_oxide': [],
                'other': []
            },
            'baseline': 0
        }

def prepare_climate_events():
    """Prepare timeline of significant climate events"""
    events = [
        {
            'year': 1988,
            'event': 'IPCC Established',
            'description': 'The Intergovernmental Panel on Climate Change was established by the United Nations.'
        },
        {
            'year': 1997,
            'event': 'Kyoto Protocol',
            'description': 'First international treaty to reduce greenhouse gas emissions.'
        },
        {
            'year': 2015,
            'event': 'Paris Agreement',
            'description': 'Global agreement to limit temperature rise to well below 2°C.'
        },
        {
            'year': 2021,
            'event': 'Glasgow Climate Pact',
            'description': 'Updated commitments to reduce emissions and increase climate finance.'
        }
    ]
    return events

def prepare_impact_regions():
    """Prepare data about climate impact regions"""
    regions = [
        {
            'name': 'Arctic',
            'impact': 'Rapid warming, ice melt, and ecosystem disruption',
            'severity': 'High'
        },
        {
            'name': 'Small Island Nations',
            'impact': 'Sea level rise, coastal erosion, and freshwater scarcity',
            'severity': 'Critical'
        },
        {
            'name': 'Amazon Rainforest',
            'impact': 'Deforestation, biodiversity loss, and carbon sink reduction',
            'severity': 'High'
        },
        {
            'name': 'Sahel Region',
            'impact': 'Desertification, water scarcity, and agricultural stress',
            'severity': 'High'
        }
    ]
    return regions

def get_all_climate_data():
    """Get all processed climate data"""
    return {
        'temperature': process_temperature_data(),
        'greenhouse_gas': process_greenhouse_gas_data(),
        'climate_events': prepare_climate_events(),
        'impact_regions': prepare_impact_regions()
    }

if __name__ == "__main__":
    # Test the data processing
    climate_data = get_all_climate_data()
    print("Data processing completed successfully!")
    print(f"Temperature records: {len(climate_data['temperature']['years'])}")
    print(f"Greenhouse gas records: {len(climate_data['greenhouse_gas']['years'])}")
    print(f"Climate events: {len(climate_data['climate_events'])}")
    print(f"Impact regions: {len(climate_data['impact_regions'])}") 