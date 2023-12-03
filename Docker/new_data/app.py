import os
import pickle
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/')
def load_model():
    pickle_file_path = os.environ.get('PICKLE_FILE_PATH')
    
    # Load your Pickle model here
    with open(pickle_file_path, 'rb') as f:
        model = pickle.load(f)

    # Do something with the model...

    return jsonify({'message': 'Model loaded successfully'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8888)))
