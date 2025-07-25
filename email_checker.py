import base64
import csv
import json
from googleapiclient.errors import HttpError

def load_allowed_emails(csv_path='approved_emails.csv'):
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        return [row['Email'].strip().lower() for row in reader]

def load_keywords(json_path='keywords.json'):
    with open(json_path, 'r') as file:
        return json.load(file)['labels_to_check']

def get_email_threads(service):
    results = service.users().messages().list(userId='me', q="is:inbox").execute()
    return results.get('messages', [])

def get_message_payload(service, msg_id):
    msg = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
    headers = msg['payload'].get('headers', [])
    payload = msg['payload']
    data = ''
    
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain':
                data = part['body'].get('data')
                break
    else:
        data = payload['body'].get('data')

    body = base64.urlsafe_b64decode(data.encode('ASCII')).decode('utf-8', errors='ignore') if data else ""
    
    sender = ''
    for header in headers:
        if header['name'] == 'From':
            sender = header['value'].lower()
            break

    return sender, body

def create_label_if_not_exists(service, label_name):
    labels = service.users().labels().list(userId='me').execute().get('labels', [])
    for label in labels:
        if label['name'].lower() == label_name.lower():
            return label['id']
    new_label = {'name': label_name, 'labelListVisibility': 'labelShow', 'messageListVisibility': 'show'}
    result = service.users().labels().create(userId='me', body=new_label).execute()
    return result['id']

def apply_label(service, msg_id, label_id):
    service.users().messages().modify(
        userId='me',
        id=msg_id,
        body={'addLabelIds': [label_id]}
    ).execute()

def process_emails(service):
    allowed_emails = load_allowed_emails()
    keywords = load_keywords()
    threads = get_email_threads(service)

    for msg in threads:
        msg_id = msg['id']
        try:
            sender, body = get_message_payload(service, msg_id)

            # Label as "Cleared" if sender is in CSV
            if any(email in sender for email in allowed_emails):
                cleared_id = create_label_if_not_exists(service, "Cleared")
                apply_label(service, msg_id, cleared_id)

            # Search for city/keywords in body
            for keyword in keywords:
                if keyword.lower() in body.lower():
                    label_id = create_label_if_not_exists(service, keyword)
                    apply_label(service, msg_id, label_id)

        except HttpError as error:
            print(f"An error occurred: {error}")
