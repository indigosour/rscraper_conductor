import json,pika,datetime
from flask import Flask, request, jsonify
from common import *
from peertube import *
from database import *

logging.basicConfig(filename='log.log', encoding='utf-8', format='%(asctime)s %(message)s', level=logging.DEBUG)

app = Flask(__name__)

def send_message_work(dlBatch, metadata):

    # Set up RabbitMQ connection and channel
    mq_cred = pika.PlainCredentials(get_az_secret('RMQ-CRED')['username'],get_az_secret('RMQ-CRED')['password'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',credentials=mq_cred))
    channel = connection.channel()

    # Declare the durable queue (create if not exists)
    queue_name = "work"
    channel.queue_declare(queue=queue_name, durable=True)

    # Send messages to the queue with message persistence and metadata
    message = json.dumps(dlBatch)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name, 
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
            headers=metadata  # Add metadata as headers
        )
    )

    # Close the connection
    connection.close()


def queue_dl_period(period, batch_size=100):
    today = datetime.today().strftime('%m-%d-%Y')
    peertube_auth()
    p_title = f'Top of the {period} for all subs as of {today}'
    p_id = create_playlist(p_title, 2)
    dlList = get_dl_list_period(period)

    print(f'Adding {len(dlList)} posts from {period} for all subreddits to the worker queue.')

    # Split dlList into batches
    batches = [dlList[i:i + batch_size] for i in range(0, len(dlList), batch_size)]

    for batch in batches:
        metadata = {
            "content_type": "application/json",
            "job_type": "dl_period",
            "period": period,
            "p_id": p_id,
            "version": "1.0"
        }
        send_message_work(batch,metadata)

    print(f'Sent {len(batches)} messages to worker queue.')


@app.route('/queue_dl_period', methods=['POST'])
def api_queue_dl_period():
    data = request.get_json()
    period = data.get('period', None)
    batch_size = data.get('batch_size', 100)

    if not period:
        return jsonify({'error': 'Please provide a period value'}), 400

    try:
        queue_dl_period(period, batch_size)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)