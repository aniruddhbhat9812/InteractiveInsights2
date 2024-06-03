import os
# from app import app
from flask import request, jsonify, session
from flask_cors import CORS
from model.campaign_model import InteractiveInsightsDTO
from services.processing_campaign import genAIOutput
import logging

# CORS(app, resources={r"*": {"origins": "*"}})

logging.basicConfig(level=logging.INFO)

@app.route('/InteractiveInsights', methods=['POST'])
def InteractiveInsights_Output():
    try:
        body = request.json
        print("Step1 Successful")
        if InteractiveInsightsDTO.validatePayload(body):
            q = InteractiveInsightsDTO.instance_from_flask_body(body)
            logging.info(q)
            result = genAIOutput(q)
            if 'error' in result:
                return jsonify(result), 500
            return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': f'{str(e)}'}), 503