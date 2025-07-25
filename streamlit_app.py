import streamlit as st
import os
import csv
import time
import random
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.utils import parseaddr

# ------------------ STREAMLIT UI ------------------
st.title("📧 Gmail Label Agent")
st.write("Automatically label Gmail threads based on approved emails.")

# ------------------ CONFIG ------------------
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

# ✅ Load credentials from Streamlit secrets
if "GOOGLE_CREDENTIALS" not in st.secrets:
    st.error("❌ Credentials not found in Streamlit secrets!")
    st.stop()

try:
    creds = Credentials.from_authorized_user_info(
        json.loads(st.secrets["GOOGLE_CREDENTIALS"]),
        SCOPES
    )
    service = build('gmail', 'v1', credentials=creds)
except Exception as e:
    st.error(f"❌ Failed to load Gmail API credentials: {e}")
    st.stop()

# ------------------ CSV Upload ------------------
uploaded_file = st.file_uploader("📂 Upload Approved Emails CSV", type=["csv"])
approved_emails = {}

if uploaded_file:
    reader = csv.DictReader(uploaded_file.read().decode("utf-8").splitlines())
    for row in reader:
        email = row.get('Email', '').strip().lower()
        city = row.get('City', '').strip()
        if email and city:
            approved_emails[email] = city
    st.success(f"✅ Loaded {len(approved_emails)} approved emails.")

# ------------------ Gmail Functions ------------------
def get_threads(max_threads=100):
    try:
        response = service.users().threads().list(userId='me', maxResults=max_threads).execute()
        return response.get('threads', [])
    except HttpError as error:
        st.error(f"Error fetching threads: {error}")
        return []

def get_email_addresses_from_thread(thread):
    incoming_emails, all_emails = set(), set()
    try:
        thread_data = service.users().threads().get(userId='me', id=thread['id']).execute()
        messages = thread_data.get('messages', [])
        for msg in messages:
            headers = msg['payload']['headers']
            for header in headers:
                if header['name'] == 'From':
                    for addr in header['value'].split(','):
                        _, email = parseaddr(addr)
                        incoming_emails.add(email.strip().lower())
                if header['name'] in ['From', 'To', 'Cc', 'Delivered-To']:
                    for addr in header['value'].split(','):
                        _, email = parseaddr(addr)
                        all_emails.add(email.strip().lower())
        time.sleep(random.uniform(0.2, 0.5))
    except HttpError as error:
        st.warning(f"Error fetching thread {thread['id']}: {error}")
    return incoming_emails, all_emails

def get_or_create_label(label_name):
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
        st.warning(f"Error creating label '{label_name}': {error}")
        return None

def label_thread(thread_id, label_name):
    label_id = get_or_create_label(label_name)
    if label_id:
        service.users().threads().modify(
            userId='me',
            id=thread_id,
            body={'addLabelIds': [label_id]}
        ).execute()

# ------------------ Main Button ------------------
if st.button("🚀 Run Labeling"):
    if not approved_emails:
        st.error("❌ Please upload approved_emails.csv first.")
    else:
        threads = get_threads(100)
        cleared_count, city_count = 0, 0

        progress = st.progress(0)
        for i, thread in enumerate(threads):
            incoming_emails, all_emails = get_email_addresses_from_thread(thread)
            for email in incoming_emails:
                if email in approved_emails:
                    label_thread(thread['id'], "Cleared")
                    cleared_count += 1
                    break
            for email in all_emails:
                if email in approved_emails:
                    label_thread(thread['id'], approved_emails[email])
                    city_count += 1
            progress.progress((i + 1) / len(threads))

        st.success(f"✅ Done! Cleared labeled: {cleared_count}, City labeled: {city_count}")
