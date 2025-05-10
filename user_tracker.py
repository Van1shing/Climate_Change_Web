import uuid
import time
from datetime import datetime
from typing import Dict, Optional, List
import json
from db_connection import DatabaseConnection
import logging
from user_agents import parse
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UserTracker:
    def __init__(self, db_path: str = "climate_data.db"):
        """Initialize the user tracker with database connection."""
        self.db = DatabaseConnection(db_path)
        self.db.connect()

    def __del__(self):
        """Clean up database connection."""
        if hasattr(self, 'db'):
            self.db.disconnect()

    def _generate_session_id(self, user_agent: str, ip_address: str) -> str:
        """Generate a unique session ID based on user agent and IP."""
        unique_string = f"{user_agent}{ip_address}{time.time()}"
        return hashlib.md5(unique_string.encode()).hexdigest()

    def start_session(self, request_data: Dict) -> str:
        """Start a new user session and return the session ID."""
        try:
            user_agent = request_data.get('user_agent', '')
            ip_address = request_data.get('ip_address', '')
            referrer = request_data.get('referrer', '')
            
            # Parse user agent
            ua = parse(user_agent)
            device_type = 'mobile' if ua.is_mobile else 'tablet' if ua.is_tablet else 'desktop'
            
            session_id = self._generate_session_id(user_agent, ip_address)
            
            query = """
            INSERT INTO user_sessions 
            (session_id, user_agent, ip_address, referrer, device_type, browser, os)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                session_id,
                user_agent,
                ip_address,
                referrer,
                device_type,
                ua.browser.family,
                ua.os.family
            )
            
            self.db.execute_query(query, params)
            return session_id
            
        except Exception as e:
            logger.error(f"Error starting session: {e}")
            raise

    def end_session(self, session_id: str):
        """End a user session."""
        try:
            query = """
            UPDATE user_sessions 
            SET end_time = CURRENT_TIMESTAMP
            WHERE session_id = ?
            """
            self.db.execute_query(query, (session_id,))
        except Exception as e:
            logger.error(f"Error ending session: {e}")
            raise

    def record_page_view(self, session_id: str, page_data: Dict):
        """Record a page view with associated metrics."""
        try:
            query = """
            INSERT INTO page_views 
            (session_id, page_url, time_spent, scroll_depth)
            VALUES (?, ?, ?, ?)
            """
            params = (
                session_id,
                page_data.get('url', ''),
                page_data.get('time_spent', 0),
                page_data.get('scroll_depth', 0)
            )
            
            self.db.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error recording page view: {e}")
            raise

    def record_interaction(self, session_id: str, interaction_data: Dict):
        """Record a user interaction."""
        try:
            query = """
            INSERT INTO user_interactions 
            (session_id, interaction_type, element_id, element_type)
            VALUES (?, ?, ?, ?)
            """
            params = (
                session_id,
                interaction_data.get('type', ''),
                interaction_data.get('element_id', ''),
                interaction_data.get('element_type', '')
            )
            
            self.db.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error recording interaction: {e}")
            raise

    def record_metric(self, session_id: str, metric_name: str, metric_value: float):
        """Record a user metric."""
        try:
            query = """
            INSERT INTO user_metrics 
            (session_id, metric_name, metric_value)
            VALUES (?, ?, ?)
            """
            params = (session_id, metric_name, metric_value)
            
            self.db.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error recording metric: {e}")
            raise

    def get_session_metrics(self, session_id: str) -> Dict:
        """Get all metrics for a specific session."""
        try:
            # Get session info
            session_query = """
            SELECT * FROM user_sessions WHERE session_id = ?
            """
            session_data = self.db.execute_query(session_query, (session_id,))[0]
            
            # Get page views
            page_views_query = """
            SELECT * FROM page_views WHERE session_id = ?
            """
            page_views = self.db.execute_query(page_views_query, (session_id,))
            
            # Get interactions
            interactions_query = """
            SELECT * FROM user_interactions WHERE session_id = ?
            """
            interactions = self.db.execute_query(interactions_query, (session_id,))
            
            # Get metrics
            metrics_query = """
            SELECT * FROM user_metrics WHERE session_id = ?
            """
            metrics = self.db.execute_query(metrics_query, (session_id,))
            
            return {
                'session': session_data,
                'page_views': page_views,
                'interactions': interactions,
                'metrics': metrics
            }
        except Exception as e:
            logger.error(f"Error getting session metrics: {e}")
            raise

    def get_aggregate_metrics(self, start_date: Optional[datetime] = None) -> Dict:
        """Get aggregate metrics across all sessions."""
        try:
            metrics = {}
            
            # Calculate average session duration
            duration_query = """
            SELECT AVG(strftime('%s', end_time) - strftime('%s', start_time)) as avg_duration
            FROM user_sessions
            WHERE end_time IS NOT NULL
            """
            if start_date:
                duration_query += " AND start_time >= ?"
                metrics['avg_session_duration'] = self.db.execute_query(duration_query, (start_date,))[0][0]
            else:
                metrics['avg_session_duration'] = self.db.execute_query(duration_query)[0][0]
            
            # Calculate bounce rate (sessions with only one page view)
            bounce_query = """
            SELECT 
                (COUNT(DISTINCT CASE WHEN page_count = 1 THEN session_id END) * 100.0 / COUNT(DISTINCT session_id)) as bounce_rate
            FROM (
                SELECT session_id, COUNT(*) as page_count
                FROM page_views
                GROUP BY session_id
            )
            """
            metrics['bounce_rate'] = self.db.execute_query(bounce_query)[0][0]
            
            # Calculate average time spent per page
            time_query = """
            SELECT AVG(time_spent) as avg_time_spent
            FROM page_views
            """
            if start_date:
                time_query += " WHERE view_time >= ?"
                metrics['avg_time_per_page'] = self.db.execute_query(time_query, (start_date,))[0][0]
            else:
                metrics['avg_time_per_page'] = self.db.execute_query(time_query)[0][0]
            
            # Calculate most common interactions
            interaction_query = """
            SELECT interaction_type, COUNT(*) as count
            FROM user_interactions
            GROUP BY interaction_type
            ORDER BY count DESC
            LIMIT 5
            """
            metrics['top_interactions'] = self.db.execute_query(interaction_query)

            # Calculate tab switching metrics
            tab_metrics_query = """
            SELECT 
                element_id as tab_name,
                COUNT(*) as visit_count,
                AVG(time_spent) as avg_time_spent,
                COUNT(DISTINCT session_id) as unique_visitors
            FROM user_interactions
            WHERE interaction_type = 'tab_switch'
            GROUP BY element_id
            ORDER BY visit_count DESC
            """
            metrics['tab_metrics'] = self.db.execute_query(tab_metrics_query)

            # Calculate tab sequence patterns
            tab_sequence_query = """
            WITH tab_sequences AS (
                SELECT 
                    session_id,
                    element_id as current_tab,
                    LAG(element_id) OVER (PARTITION BY session_id ORDER BY interaction_time) as previous_tab
                FROM user_interactions
                WHERE interaction_type = 'tab_switch'
            )
            SELECT 
                previous_tab,
                current_tab,
                COUNT(*) as transition_count
            FROM tab_sequences
            WHERE previous_tab IS NOT NULL
            GROUP BY previous_tab, current_tab
            ORDER BY transition_count DESC
            LIMIT 10
            """
            metrics['tab_sequences'] = self.db.execute_query(tab_sequence_query)
            
            return metrics
        except Exception as e:
            logger.error(f"Error getting aggregate metrics: {e}")
            raise 