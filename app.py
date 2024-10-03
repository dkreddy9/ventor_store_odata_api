from flask import Flask, request, jsonify 
from helpers import *

app = Flask(__name__)

# Define the GET API
@app.route('/process_odata', methods=['GET'])
def fetch_odata():
    odata_url = ""
    # Get the OData URL from the request parameters
    # odata_url = request.args.get('odata_url')
    # Get the full path from the request
    full_path = request.full_path
    
    # Split the string at '&odata_url=' and get the part after it
    if '&odata_url=' in full_path:
        odata_url = full_path.split('&odata_url=', 1)[1]  # Split and get the part after &odata_url=    

    # If no URL is provided, return an error message
    if not odata_url:
        return jsonify({'error': 'No OData URL provided. (Perameter &odata_url=)'}), 400

    try:         
            process_odata(odata_url)  
            return jsonify({'success':'OData Processed Successfully'}), 200
    except Exception as e:
        # Return an error message if there was an exception
        return jsonify({'error': str(e)}), 500

# Example of a global error handler
@app.errorhandler(Exception)
def handle_exception(e):
    # Generic handler for all exceptions raised in the app
    return jsonify({"error": str(e)}), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)