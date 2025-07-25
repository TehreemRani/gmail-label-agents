import base64
import pandas as pd
from googleapiclient.errors import HttpError

def read_csv_emails(path='approved_emails.csv'):
    df = pd.read_csv(path)
    return set(df['Email'].str.lower())

def get_message_sender(message):
    headers = message.get('payload', {}).get('headers', [])
    for header in headers:
        if header['name'].lower() == 'from':
            return header['value'].split('<')[-1].replace('>', '').strip().lower()
    return None

def get_message_text(message):
    try:
        parts = message.get('payload', {}).get('parts', [])
        for part in parts:
            if part['mimeType'] == 'text/plain':
                return base64.urlsafe_b64decode(part['body']['data']).decode()
        return base64.urlsafe_b64decode(message['payload']['body']['data']).decode()
    except Exception:
        return ""

def create_label_if_not_exists(service, label_name):
    labels = service.users().labels().list(userId='me').execute().get('labels', [])
    for label in labels:
        if label['name'].lower() == label_name.lower():
            return label['id']
    # Else create
    label_obj = {
        'name': label_name,
        'labelListVisibility': 'labelShow',
        'messageListVisibility': 'show',
    }
    label = service.users().labels().create(userId='me', body=label_obj).execute()
    return label['id']

def apply_label_to_message(service, msg_id, label_id):
    try:
        service.users().messages().modify(
            userId='me',
            id=msg_id,
            body={'addLabelIds': [label_id]}
        ).execute()
    except HttpError as e:
        print(f"Error applying label: {e}")
