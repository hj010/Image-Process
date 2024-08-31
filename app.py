from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import os
import mysql.connector
import csv
import uuid
import requests
from PIL import Image
from io import BytesIO

# Load environment variables
load_dotenv()

# Flask application initialization
app = Flask(__name__)

# Configure MySQL database connection using environment variables
db_config = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

# Webhook URL
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

# Directory for storing images
IMAGE_FOLDER = '/tmp/images'
if not os.path.exists(IMAGE_FOLDER):
    os.makedirs(IMAGE_FOLDER)

# Upload API: Accept CSV files and return a unique request ID
@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400
    
    file = request.files['file']
    
    if file.filename == '' or not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file format. Please upload a CSV file.'}), 400
    
    filename = secure_filename(file.filename)
    file_path = os.path.join(IMAGE_FOLDER, filename)
    file.save(file_path)

    request_id = str(uuid.uuid4())

    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()

    cursor.execute('''
        INSERT INTO requests (request_id, status)
        VALUES (%s, %s)
    ''', (request_id, 'Processing'))

    with open(file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row

        for row in csv_reader:
            serial_number = row[0]
            product_name = row[1]
            input_image_urls = row[2]

            cursor.execute('''
                INSERT INTO products (request_id, serial_number, product_name, input_image_urls, output_image_urls)
                VALUES (%s, %s, %s, %s, %s)
            ''', (request_id, serial_number, product_name, input_image_urls, ''))
        
        db.commit()
        cursor.close()
        db.close()

    # Asynchronously process the images
    process_images(request_id)

    return jsonify({'request_id': request_id}), 200

def process_images(request_id):
    """Process images asynchronously (simulated for simplicity)."""
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()

    cursor.execute('SELECT id, input_image_urls FROM products WHERE request_id = %s', (request_id,))
    records = cursor.fetchall()

    for record in records:
        product_id, input_image_urls = record
        output_image_urls = []

        for url in input_image_urls.split(','):
            url = url.strip()
            response = requests.get(url)
            if response.status_code == 200:
                img = Image.open(BytesIO(response.content))
                img = img.convert("RGB")
                output_io = BytesIO()
                img.save(output_io, format='JPEG', quality=50)
                output_io.seek(0)

                output_filename = f"{uuid.uuid4()}.jpg"
                output_path = os.path.join(IMAGE_FOLDER, output_filename)
                with open(output_path, 'wb') as f:
                    f.write(output_io.read())
                
                output_image_urls.append(output_path)

        cursor.execute('''
            UPDATE products 
            SET output_image_urls = %s
            WHERE id = %s
        ''', (",".join(output_image_urls), product_id))

    cursor.execute('UPDATE requests SET status = %s WHERE request_id = %s', ('Completed', request_id))
    db.commit()

    # Notify webhook
    notify_webhook(request_id)

    cursor.close()
    db.close()

def notify_webhook(request_id):
    """Send a notification to the webhook URL after processing is complete."""
    webhook_payload = {
        'request_id': request_id,
        'status': 'Completed'
    }
    try:
        response = requests.post(WEBHOOK_URL, json=webhook_payload)
        if response.status_code == 200:
            print('Webhook notification sent successfully.')
        else:
            print(f'Failed to send webhook notification: {response.status_code}')
    except Exception as e:
        print(f'Error sending webhook notification: {e}')

@app.route('/status/<request_id>', methods=['GET'])
def check_status(request_id):
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()

    cursor.execute('''
        SELECT p.serial_number, p.product_name, p.input_image_urls, p.output_image_urls, r.status 
        FROM products p
        JOIN requests r ON p.request_id = r.request_id
        WHERE p.request_id = %s
    ''', (request_id,))
    results = cursor.fetchall()

    if not results:
        return jsonify({'error': 'Invalid request ID'}), 404

    response_data = []
    for result in results:
        response_data.append({
            'serial_number': result[0],
            'product_name': result[1],
            'input_image_urls': result[2],
            'output_image_urls': result[3] if result[3] else '',
            'status': result[4]
        })

    cursor.close()
    db.close()

    return jsonify({'status': response_data}), 200

@app.route('/notify', methods=['POST'])
def webhook_notify():
    data = request.json
    request_id = data.get('request_id')
    status = data.get('status')

    print(f'Received webhook notification for request ID: {request_id} with status: {status}')

    return jsonify({'message': 'Notification received successfully'}), 200  

if __name__ == '__main__':
    app.run(debug=True)
