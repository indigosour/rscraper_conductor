import logging,requests,json,os,time
from concurrent.futures import ThreadPoolExecutor
from common import get_az_secret

# Variables
peertube_api_url = get_az_secret("TUBE-CRED")['url']
peertube_token = None


######################
##### Peertube #######
######################

def peertube_auth():
    global peertube_token
    peertube_api_user = get_az_secret("TUBE-CRED")['username']
    peertube_api_pass = get_az_secret("TUBE-CRED")['password']
    logging.info("peertube_auth: Logging into peertube")

    try:
        response = requests.get(peertube_api_url + '/oauth-clients/local')
        data = response.json()
        client_id = data['client_id']
        client_secret = data['client_secret']

        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'password',
            'response_type': 'code',
            'username': peertube_api_user,
            'password': peertube_api_pass
        }

        response = requests.post( peertube_api_url + '/users/token', data=data)
        data = response.json()
        peertube_token = data['access_token']
    except Exception as e:
        logging.error('peertube_auth: Error logging into peertube.',e)


def list_channels():
    global peertube_token
    peertube_auth()
    headers = {
	'Authorization': 'Bearer' + ' ' + peertube_token
    }
    params={'count': 50,'sort': '-createdAt'}
    channel_list = {}
    res = requests.get(url=f'{peertube_api_url}/video-channels', headers=headers, params=params)

    try:
        for i in res.json()['data']:
            channel_list[i['displayName'].replace("r/","")] = i['id']
    except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Response content: {res.content}")
        logging.error(f"list_channels: Error decoding JSON: {res.content} {e}")
    return channel_list


def check_plist():
    global peertube_token
    peertube_auth()
    headers = {
	'Authorization': 'Bearer' + ' ' + peertube_token
    }
    params={'count': 100,'sort': '-createdAt'}
    play_list = {}
    channelHandle = 'autoupload'
    res = requests.get(url=f'{peertube_api_url}/video-channels/{channelHandle}/video-playlists', headers=headers, params=params)

    for i in res.json()['data']:
        play_list['displayName'] = (i['displayName'])
        play_list['uuid'] = (i['uuid'])

    # try:
    #     for i in res.json()['data']:
    #         play_list['displayName'] = (i['displayName'])
    #         play_list['uuid'] = (i['uuid'])
    # except json.decoder.JSONDecodeError as e:
    #     print(f"Error decoding JSON: {e}")
    #     print(f"Response content: {res.content}")
    #     logging.error(f"list_channels: Error decoding JSON: {res.content} {e}")

    # for i in play_list:
    #     if i['displayName'] == p_name:
    #         p_uuid = 

    return print(play_list)


def upload_video(sub,title,video_path,description):
    global peertube_token
    try:
        videoChannelId = 2
        filenamevar = os.path.basename(video_path)
        title = f"r/{sub} - {title}"
        data = {'channelId': videoChannelId, 'name': title, 'description': description, 'privacy': 1}
        files = {
            'videofile': (filenamevar,open(video_path, 'rb'),'video/mp4',{'Expires': '0'})}
        headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
        
        # Upload a video
        res = requests.post(url=f'{peertube_api_url}/videos/upload', headers=headers, files=files, data=data)
        res.raise_for_status()
        v_id = res.json()['video']['uuid']
        print(f"Successfully uploaded video with id {v_id}")
        return v_id
    
    except Exception as e:
        print(f"Error occurred while uploading video: {e}")
        logging.error(f"upload_video: Error occurred while uploading video: {e}")
        return None


# Create playlist in peertube

def create_playlist(display_name,sub_pid):
    global peertube_token
    
    if type(sub_pid) == str:
        videoChannelId = list_channels()[sub_pid]
    elif type(sub_pid) == int:
        videoChannelId = sub_pid

    logging.info(f'create_playlist: Creating playlist {display_name}')
    privacy = 1
    headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
    data = {
        'displayName': (None, display_name),
        'videoChannelId': (None, videoChannelId),
        'privacy': (None, str(privacy))
    }

    try:
        # Create playlilst
            res = requests.post(url=f'{peertube_api_url}/video-playlists',headers=headers,files=data)
            p_id = res.json()['videoPlaylist']['id']
    except Exception as e:
        print("Exception when calling VideoApi->videos_upload_post: %s\n" % e)
        logging.error(f"create_playlist: Exception when calling VideoApi->videos_upload_post: {e}")
    return p_id


# # Add video to playlist

def add_video_playlist(v_id,p_id):
    global peertube_token
    headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
    data = {
        'videoId': v_id
    }
    try:
        # Create playlilst
            requests.post(url=f'{peertube_api_url}/video-playlists/{p_id}/videos',headers=headers,json=data)
    except Exception as e:
        print(f'Error adding video to playlist {p_id}')
        logging.error(f'add_video_playlist: Error adding video to playlist {p_id}')


def list_videos(page):
    global peertube_token
    headers = {
        'Authorization': 'Bearer' + ' ' + peertube_token
    }
    params = {'count': 100, 'sort': '-createdAt', 'page': page}
    video_list = []
    res = requests.get(url=f'{peertube_api_url}/videos', headers=headers, params=params)
    try:
        data = res.json()['data']
        for i in data:
            video_list.append(i['shortUUID'])
    except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Response content: {res.content}")
        logging.error(f"list_channels: Error decoding JSON: {res.content} {e}")
    # Add delay to avoid "too many requests" error
    time.sleep(1)
    return video_list

# Check video views by ID and return number of views

# def get_video_views(v_id):
#     global peertube_token


#     return views


def delete_video(v_id):
    global peertube_token
    headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
    try:
        # Create playlilst
            requests.delete(url=f'{peertube_api_url}/videos/{v_id}',headers=headers)
    except Exception as e:
        print(f'Error deleting video {v_id}')
        logging.error(f'delete_video: Error deleting video {v_id}')


def delete_all_videos():
    batch_size = 100
    peertube_auth()

    def delete_video_thread(v_id):
        while True:
            try:
                delete_video(v_id)
                break
            except Exception as e:
                if 'Too many requests' in str(e):
                    print("Too many requests, waiting for 60 seconds...")
                    time.sleep(60)
                else:
                    print(f"Error deleting video {v_id}: {e}")
                    break

    def delete_batch(batch):
        with ThreadPoolExecutor(max_workers=5) as executor:
            for v_id in batch:
                executor.submit(delete_video_thread, v_id)

    # Set the initial page number to 1
    page = 1

    while True:
        video_list = list_videos(page)
        if len(video_list) == 0:
            break
        
        # Divide videos into batches and create threads for each batch
        num_batches = (len(video_list) + batch_size - 1) // batch_size
        for i in range(num_batches):
            start = i * batch_size
            end = min(start + batch_size, len(video_list))
            batch = video_list[start:end]
            delete_batch(batch)
            time.sleep(2)

        # Wait for videos to be deleted before checking the remaining list
        time.sleep(1)

        # Check the remaining list of videos to see if any were deleted
        remaining_videos = list_videos(page)
        if set(remaining_videos) == set(video_list):
            print("Error: No videos were deleted.")
            break

        # Increment the page number to retrieve the next batch of videos
        page += 1
