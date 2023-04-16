import json, pika, datetime, sched, time, logging, datetime, sys
from fastapi import FastAPI, Request
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from common import *
from peertube import *
from database import *

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.StreamHandler(sys.stderr)
    ]
)
app = FastAPI()

s = sched.scheduler(time.time, time.sleep)

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
    today = datetime.now().strftime('%m-%d-%Y')
    try:
        peertube_auth()
    except Exception as e:
        print(f"Error authenticating with Peertube: {e}")
        logging.error(f"Error authenticating with Peertube: {e}")
        sys.exit(1)
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
        try: 
            send_message_work(batch,metadata)
        except Exception as e:
            print(f"Error sending message: {e}")

    print(f'Sent {len(batches)} messages to worker queue.')


def process_subreddit_update():
    sublist = load_sublist()

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(store_reddit_posts, sub) for sub in sublist]

        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("Completed updating database with all posts from all tracked subreddits")


@app.post('/process_update')
async def api_process_update(request: Request):
    try:
        process_subreddit_update()
        logging.info(f"Updating subreddits with new posts...")
        return {'status': 'success'}
    except Exception as e:
        logging.error(f"Error updating subreddits with new posts...")
        return {'error': str(e)}, 500


@app.post('/queue_dl_period')
async def api_queue_dl_period(request: Request):
    data = await request.json()
    period = data.get('period', None)
    batch_size = data.get('batch_size', 100)

    if not period:
        return {'error': 'Please provide a period value'}, 400

    try:
        queue_dl_period(period, batch_size)
        logging.info(f"Queueing post download for period {period}")
        return {'status': 'success', 'message': f'Queued post download for period {period}'}
    except Exception as e:
        return {'error': str(e)}, 500


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=5000)