# FYI: This whole code takes about an hour to run 
# Starting from json file from Assignment 1 and the original text email corpus

import pandas as pd
import json
import re
from bs4 import BeautifulSoup
from tika import parser



# Define functions

def get_ttr_text(txt):
    # Initialize an empty text-to-tag array for our data
    ttr_array = []

    # Initialize an empty array for the text data
    text_data = []

    # Go thru each line in the soup and compute the TTR
    for line in txt.split('\n'):

        # If the line isn't empty, compute the TTR
        if len(str(line)) > 0:

            total_len = len(str(line)) # Length of the total string
        
            # Get length of the text portion of the line
            try:
                just_text = re.sub('<[^>]+>', '', line)    # Get rid of the <...> tag elements
                text_len = len(str(just_text))
            except:
                text_len = 0    # All tag elements

            # Compute tag length as the difference between total length and text length
            tag_len = total_len - text_len

            #print(f"Total length: {total_len}\nText length: {text_len}\nTag length: {tag_len}")

            # Add the TTR to the ttr_array
            if tag_len > 0:
                ratio = text_len / tag_len
            else:
                ratio = text_len

            ttr_array.append(ratio)

            if ratio > 1:
                text_data.append(line)
        else:
            ttr_array.append(0)
    
    string = '\n'
    ttr_text = string.join(text_data)
    
    return(ttr_array, ttr_text)

def flatten(nested, flat):
    for key, value in nested.items():
        if not type(value) == dict:
            flat[key] = value
        else:
            # Add key to value's keys
            new_keys = [f'{key}-{valkey}' for valkey in value.keys()]
            value = dict(zip(new_keys, list(value.values())))
            flatten(value, flat)
    return flat



# Re-parse with Tika to get the HTML content of each email

# Import in the original Nigerian Prince email set and parse thru it with Tika
parsed = parser.from_file('/Users/madeleine/Desktop/DSCI_550/Assignment_2/TEAM_BANANA_DSCI550_HW_BIGDATA/data/fraudulent_emails.txt', xmlContent=True)

# Save the entire parsed corpus to an intermediate json file
with open('fraudulent_emails_re_parsed.json', 'w') as outfile:
    json.dump(parsed, outfile, indent=2)
    outfile.close()

# parsed is a dictionary with keys "metadata" and "content"
content = parsed['content']

# Save the emails as a list, store in a json file
emails = content.split("</html>")
emails = emails[1:]

new_emails = []
for email in emails:
    # Add the ending </html> tag back into the string
    email = email + '</html>'

    # Use BeautifulSoup to parse the email message
    soup = BeautifulSoup(email, 'html.parser')

    # Fint the 'from' email address and message ID if they are in the message
    try:
        from_email = str(soup.find('meta', attrs={'name':'Message:From-Email'})['content'])
    except TypeError:
        from_email = ''
    try:
        message_id = str(soup.find('meta', attrs={'name':'Message:Raw-Header:Message-Id'})['content'])
    except TypeError:
        message_id = ''

    # Add dictionary to new_emails with keys 'email', 'from', and 'message_id'
    new_emails.append({'email': email, 'from': from_email, 'message_id': message_id})

# Save the re-parsed HTML emails to emails.json
with open('emails.json', 'w') as out_emails:
    json.dump(new_emails, out_emails, indent=2)
    out_emails.close()



# Compare re-parsed HTML with originally parsed messages and run TTR algorithm

# Open the originally parsed fraudulent_emails.json
with open('/Users/madeleine/Desktop/DSCI_550/Assignment_2/TEAM_BANANA_DSCI550_HW_BIGDATA/data/fraudulent_emails.json', 'r') as f:
    messages = json.load(f)

# Iterate through the messages from the fraudulent_emails.json and the HTML parsed emails in emails.json
for message in messages:
    # Add new columns
    message['GPT2_gen_images'] = {'Phish_Iris_image': {'Image_caption': ''}, 'Attack_persona_repr': ''}
    message['Falsified'] = 'No'

    # Extract the 'from' email address and message_id fields from the message
    try:
        fraud_from_email = str(message["Message:From-Email"])
    except KeyError:
        fraud_from_email = ''
    try:
        fraud_message_id = str(message["Message:Raw-Header:Message-Id"])
    except KeyError:
        fraud_message_id = ''

    # Go through each email in the newly parsed emails list
    for email in new_emails:

        # Extract 'from' and 'message_id' keys
        from_email = email['from']
        message_id = email['message_id']

        if fraud_from_email == from_email and (fraud_message_id == message_id or fraud_message_id == ''):
            # If the email addresses and message id's match, pass the HTML content of the
            # email to the get_ttr_text function
            ttr_array, text_data = get_ttr_text(email['email'])
        else:
            # Can't match the email addresses and message id's, pass the content of the
            # originally parsed email message to the get_ttr_text function
            ttr_array, text_data = get_ttr_text(message["X-TIKA:content"])
        # Save the TTR'ed text to the message json blob
        message['TTR_text'] = {'ttr_text': text_data, 'ttr_array': ttr_array}



# Save updated data

# Save new messages to a new json file
with open('fraudulent_emails_v2.json', 'w') as outjson:
    json.dump(messages, outjson, indent=2)

# Save "flattened" message blobs to new tsv file
flat_messages = []

# These are our TSV headers
keys = [
    'se_tags',
    'author_titles',
    'attacker_offering',
    'urgency_score',
    'attacker_offering',
    'TIKA-GeoLocationParser',
    'timestamp',
    'relationship',
    'Sentiment_Analysis',
    'misspelled_ratio',
    'capitalization_ratio',
    'IPInfo-data',
    'scamalytics',
    'GlobalUnemployment',
    'InternationalDebt',
    'Enron',
    'subject',
    'X-TIKA:content',
    'MboxParser-from',
    'MboxParser-status',
    'MboxParser-x-mailer',
    'MboxParser-x-sieve',
    'Content-Type',
    'GPT2_gen_images',
    'Falsified',
    'TTR_text'
]

# Iterate and "flatten" all messages
for message in messages:
    flat_message = {}
    for key in keys:
        if key in message:
            # Flatten and save
            if isinstance(message[key], dict):
                flat_one = flatten(message[key], {})
                for k in flat_one:
                    if key not in k:
                        newk = f'{key}-{k}'
                    else:
                        newk = k
                    flat_message[newk] = flat_one[k]
            else:
                flat_message[key] = message[key]
    flat_messages.append(flat_message)

# Json to dataframe
final_df = pd.DataFrame(flat_messages)
final_df

# Write to TSV
final_df.to_csv('fraudulent_emails_v2.tsv', sep='\t')