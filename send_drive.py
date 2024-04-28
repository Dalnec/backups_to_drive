from __future__ import print_function
import pickle
import os.path
import io
from googleapiclient import errors
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.appdata', 'https://www.googleapis.com/auth/drive.file'] #.metadata.readonly']

def getCredentials():
    creds = None
    # if os.path.exists('token.pickle'):
    #     with open('token.pickle', 'rb') as token:
    #         creds = pickle.load(token)
    # if not creds or not creds.valid:
    #     if creds and creds.expired and creds.refresh_token:
    #         creds.refresh(Request())
    #     else:
    #         flow = InstalledAppFlow.from_client_secrets_file(
    #             'credenciales/credentials_faqture.json', SCOPES)
    #         creds = flow.run_local_server(port=0)
    #     with open('token.pickle', 'wb') as token:
    #         pickle.dump(creds, token)
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credenciales/credentials.json', SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds, cache_discovery=False, static_discovery=False)
    return service

def list_items():
    services = getCredentials()
    folder_id = '144sUdLku04IQPpOgcvEaNTDwYs8y3vQy'
    query = f"'{folder_id}' in parents"
    results = services.files().list(q=query, pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))

def uploadFile(filename,filepath,mimetype):
    services = getCredentials()
    folder_id = '144sUdLku04IQPpOgcvEaNTDwYs8y3vQy'
    file_metadata = {'name': filename, 'parents': [folder_id]}
    media = MediaFileUpload(filepath, mimetype = mimetype, resumable = True)
    file = services.files().create(body = file_metadata, media_body = media, fields='id').execute()
    return file.get('id')

def searchFile(size, queryname, filename, back):
    services = getCredentials()
    mimeType='application/x-rar-compressed'
    folder_id = '144sUdLku04IQPpOgcvEaNTDwYs8y3vQy'
    q = f"'{folder_id}' in parents and name='{queryname}'"
    results = services.files().list(q=q, spaces='drive', pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    print(items)
    
    if not items:
        print('No files found.')        
        uploadFile(filename, filename, mimeType)
    else:
        print('Files found.')
        if back == 'id':
            return items[0]['id'], items[0]['name']

        for item in items:            
            fileId = item['id']
        updateFile(fileId, filename, mimeType)
        return item
    
def updateFile(file_id, new_filename, new_mime_type):
    try:
        services = getCredentials()
        file = {}
        media_body = MediaFileUpload(new_filename, mimetype=new_mime_type, resumable=True)
        updated_file = services.files().update(fileId=file_id, body=file, media_body=media_body).execute()
        return print('Updated: %s' % (updated_file['id']))
    except errors.HttpError as error:
        print('%s An error occurred: %s' % (error)) 
        return None

def download_file(file_id, file_name):
    try:
        services = getCredentials()
        request = services.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(F'Download {int(status.progress() * 100)}.')
        
        file.seek(0)
        with open(os.path.join('./bk', file_name), 'wb') as f:
            f.write(file.read())
            f.close

    except error.HttpError as error:
        print(F'An error occurred: {error}')
        file = None

    return file.getvalue()