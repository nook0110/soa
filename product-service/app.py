from flask import Flask, jsonify
from datetime import datetime
import os

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'product-service',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'service': 'product-service',
        'version': '1.0.0',
        'endpoints': {
            'health': '/health'
        }
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)