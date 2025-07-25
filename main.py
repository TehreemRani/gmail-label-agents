import os
import csv
import time
import random
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.utils import parseaddr

# ------------------------- CONFIG -------------------------
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_PATH = os.path.join(BASE_DIR, 'token.json')
CSV_PATH = os.path.join(BASE_DIR, 'approved_emails.csv')

# ------------------ Load Gmail API Credentials ------------------
try:
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
except Exception as e:
    print(f"❌ Failed to load token.json: {e}")
    exit(1)

# ------------------ Load Approved Emails ------------------
approved_emails = {}

try:
    with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            email = row.get('Email', '').strip().lower()
            city = row.get('City', '').strip()
            if email and city:
                approved_emails[email] = city
except FileNotFoundError:
    print(f"❌ CSV file not found at: {CSV_PATH}")
    exit(1)

# ------------------ Gmail API Setup ------------------
try:
    service = build('gmail', 'v1', credentials=creds)
except Exception as e:
    print(f"❌ Failed to build Gmail API service: {e}")
    exit(1)


def get_threads(max_threads=100):
    """Fetch only last N Gmail threads"""
    try:
        response = service.users().threads().list(userId='me', maxResults=max_threads).execute()
        return response.get('threads', [])
    except HttpError as error:
        print(f"Error fetching threads: {error}")
        return []


def get_email_addresses_from_thread(thread, retries=3):
    """Extract incoming and all emails from a thread with retry & delay"""
    for attempt in range(retries):
        try:
            thread_data = service.users().threads().get(userId='me', id=thread['id']).execute()
            messages = thread_data.get('messages', [])
            incoming_emails = set()
            all_emails = set()

            for msg in messages:
                headers = msg['payload']['headers']
                for header in headers:
                    if header['name'] == 'From':  # incoming only
                        for addr in header['value'].split(','):
                            _, email = parseaddr(addr)
                            email = email.strip().lower()
                            if email:
                                incoming_emails.add(email)

                    if header['name'] in ['From', 'To', 'Cc', 'Delivered-To']:  # full thread
                        for addr in header['value'].split(','):
                            _, email = parseaddr(addr)
                            email = email.strip().lower()
                            if email:
                                all_emails.add(email)

            # Add small delay to avoid API throttling
            time.sleep(random.uniform(0.2, 0.5))
            return incoming_emails, all_emails

        except (HttpError, ConnectionResetError) as error:
            print(f"⚠️ Retry {attempt+1}/{retries} for thread {thread['id']} due to: {error}")
            time.sleep(3)

    print(f"❌ Failed to fetch thread {thread['id']} after {retries} attempts.")
    return set(), set()


def get_or_create_label(label_name):
    """Find or create label"""
    try:
        labels_response = service.users().labels().list(userId='me').execute()
        for label in labels_response.get('labels', []):
            if label['name'].lower() == label_name.lower():
                return label['id']

        new_label = {
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show"
        }
        created_label = service.users().labels().create(userId='me', body=new_label).execute()
        return created_label['id']
    except HttpError as error:
        print(f"Error creating/finding label '{label_name}': {error}")
        return None


def label_thread(thread_id, label_name):
    """Apply label to a thread"""
    label_id = get_or_create_label(label_name)
    if not label_id:
        return
    try:
        service.users().threads().modify(
            userId='me',
            id=thread_id,
            body={'addLabelIds': [label_id]}
        ).execute()
        print(f"✅ Labeled thread {thread_id} with '{label_name}'")
    except HttpError as error:
        print(f"Error labeling thread {thread_id}: {error}")


# ------------------ Main Execution ------------------
def main():
    threads = get_threads(max_threads=100)
    print(f"🔍 Checking last {len(threads)} threads...\n")

    cleared_count = 0
    city_count = 0

    for thread in threads:
        incoming_emails, all_emails = get_email_addresses_from_thread(thread)

        # Step 1: Cleared Label (only for incoming)
        for email in incoming_emails:
            if email in approved_emails:
                label_thread(thread['id'], "Cleared")
                cleared_count += 1
                break

        # Step 2: City Labels (full thread)
        for email in all_emails:
            if email in approved_emails:
                city_label = approved_emails[email]
                label_thread(thread['id'], city_label)
                city_count += 1

    print(f"\n✅ Summary: Cleared labeled: {cleared_count}, City labeled: {city_count}")


if __name__ == '__main__':
    main()
