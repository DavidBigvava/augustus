from flask import Flask, render_template, request, jsonify
from typing import Dict, Any  # For compatibility
from customer_script import process_input

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/ask', methods=['POST'])
def ask():
    user_message = request.json['message']
    try:
        bot_response = process_input(user_message)
        return jsonify({'message': bot_response})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':  
    app.run(debug=True)
