import logging,requests
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


# Create playlist in peertube

def create_playlist(display_name,ch_id):
    global peertube_token

    logging.info(f'create_playlist: Creating playlist {display_name}')
    privacy = 1
    headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
    data = {
        'displayName': (None, display_name),
        'videoChannelId': (None, ch_id),
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