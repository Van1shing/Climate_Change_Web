from flask import Flask, render_template, jsonify, request
from data_processor import get_all_climate_data
from user_tracker import UserTracker
import logging
from db_connection import DatabaseConnection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
user_tracker = UserTracker()

def initialize_database():
    try:
        db = DatabaseConnection("climate_data.db")
        db.connect()
        with open("schema.sql", "r") as f:
            schema = f.read()
            db.conn.executescript(schema)
        db.disconnect()
        print("✅ Database initialized successfully.")
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")

initialize_database()


@app.route('/')
def index():
    """Render the main page."""
    climate_data = get_all_climate_data()
    return render_template('index.html', climate_data=climate_data)

@app.route('/api/climate-data')
def climate_data():
    return jsonify(get_all_climate_data())

@app.route('/api/tracking/start-session', methods=['POST'])
def start_session():
    """Start a new user session."""
    try:
        request_data = {
            'user_agent': request.headers.get('User-Agent', ''),
            'ip_address': request.remote_addr,
            'referrer': request.json.get('referrer', '')
        }
        session_id = user_tracker.start_session(request_data)
        return jsonify({'session_id': session_id})
    except Exception as e:
        logger.error(f"Error starting session: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tracking/end-session', methods=['POST'])
def end_session():
    """End a user session and record final metrics."""
    try:
        session_id = request.json.get('session_id')
        if not session_id:
            return jsonify({'error': 'Session ID required'}), 400

        # Record final page view metrics
        page_data = {
            'url': request.json.get('url', request.referrer),
            'time_spent': request.json.get('time_spent', 0),
            'scroll_depth': request.json.get('scroll_depth', 0)
        }
        user_tracker.record_page_view(session_id, page_data)

        # End the session
        user_tracker.end_session(session_id)
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f"Error ending session: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tracking/interaction', methods=['POST'])
def record_interaction():
    """Record a user interaction."""
    try:
        session_id = request.json.get('session_id')
        if not session_id:
            return jsonify({'error': 'Session ID required'}), 400

        interaction_data = {
            'type': request.json.get('type'),
            'element_id': request.json.get('element_id'),
            'element_type': request.json.get('element_type')
        }
        user_tracker.record_interaction(session_id, interaction_data)
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f"Error recording interaction: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tracking/metric', methods=['POST'])
def record_metric():
    """Record a user metric."""
    try:
        session_id = request.json.get('session_id')
        if not session_id:
            return jsonify({'error': 'Session ID required'}), 400

        metric_name = request.json.get('metric_name')
        metric_value = request.json.get('metric_value')
        if not metric_name or metric_value is None:
            return jsonify({'error': 'Metric name and value required'}), 400

        user_tracker.record_metric(session_id, metric_name, metric_value)
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f"Error recording metric: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tracking/metrics', methods=['GET'])
def get_metrics():
    """Get aggregate metrics."""
    try:
        start_date = request.args.get('start_date')
        metrics = user_tracker.get_aggregate_metrics(start_date)
        return jsonify(metrics)
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/debug/init-user-tracking')
def debug_create_user_tracking_tables():
    try:
        from db_connection import DatabaseConnection
        import os

        # 让路径明确指向项目根目录的 climate_data.db
        db_path = os.path.join(os.path.dirname(__file__), 'climate_data.db')
        db = DatabaseConnection(db_path)
        db.connect()

        schema = """
        CREATE TABLE IF NOT EXISTS user_sessions (
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

        CREATE TABLE IF NOT EXISTS page_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id VARCHAR(100) NOT NULL,
            page_url TEXT NOT NULL,
            view_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            time_spent INTEGER,
            scroll_depth INTEGER,
            FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
        );

        CREATE TABLE IF NOT EXISTS user_interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id VARCHAR(100) NOT NULL,
            interaction_type VARCHAR(50) NOT NULL,
            element_id TEXT,
            element_type TEXT,
            interaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
        );

        CREATE TABLE IF NOT EXISTS user_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id VARCHAR(100) NOT NULL,
            metric_name VARCHAR(50) NOT NULL,
            metric_value REAL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
        );
        """

        db.conn.executescript(schema)
        db.disconnect()
        return jsonify({'status': '✅ User tracking tables created successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True) 

