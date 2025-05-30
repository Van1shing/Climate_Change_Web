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
        
        # Check if tables already exist
        cursor = db.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='regions'")
        if not cursor.fetchone():
            # Only create tables if they don't exist
            with open("schema.sql", "r") as f:
                schema = f.read()
                db.conn.executescript(schema)
            print("✅ Database initialized successfully.")
        else:
            print("✅ Database tables already exist.")
            
        db.disconnect()
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
        logger.info(f"Starting new session for IP: {request_data['ip_address']}")
        session_id = user_tracker.start_session(request_data)
        logger.info(f"Session started successfully with ID: {session_id}")
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
            logger.error("End session request missing session_id")
            return jsonify({'error': 'Session ID required'}), 400

        logger.info(f"Ending session: {session_id}")
        
        # Record final page view metrics
        page_data = {
            'url': request.json.get('url', request.referrer),
            'time_spent': request.json.get('time_spent', 0),
            'scroll_depth': request.json.get('scroll_depth', 0)
        }
        logger.info(f"Recording final page view for session {session_id}: {page_data}")
        user_tracker.record_page_view(session_id, page_data)

        # End the session
        user_tracker.end_session(session_id)
        logger.info(f"Session {session_id} ended successfully")
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
            logger.error("Interaction request missing session_id")
            return jsonify({'error': 'Session ID required'}), 400

        interaction_data = {
            'type': request.json.get('type'),
            'element_id': request.json.get('element_id'),
            'element_type': request.json.get('element_type')
        }
        logger.info(f"Recording interaction for session {session_id}: {interaction_data}")
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
            logger.error("Metric request missing session_id")
            return jsonify({'error': 'Session ID required'}), 400

        metric_name = request.json.get('metric_name')
        metric_value = request.json.get('metric_value')
        if not metric_name or metric_value is None:
            logger.error(f"Invalid metric data: name={metric_name}, value={metric_value}")
            return jsonify({'error': 'Metric name and value required'}), 400

        logger.info(f"Recording metric for session {session_id}: {metric_name}={metric_value}")
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

@app.route('/dashboard')
def dashboard():
    return render_template("dashboard.html")


if __name__ == '__main__':
    app.run(debug=True) 

