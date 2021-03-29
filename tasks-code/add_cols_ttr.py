import pandas as pd
import json
import re

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

# Open the fraudulent emails json
with open('/Users/madeleine/Desktop/DSCI_550/Assignment_2/TEAM_BANANA_DSCI550_HW_BIGDATA/data/fraudulent_emails.json', 'r') as f:
    messages = json.load(f)

for message in messages:
    message['GPT2_gen_images'] = {'Phish_Iris_image': {'Image_caption': ''}, 'Attack_persona_repr': ''}
    message['Falsified'] = 'No'

    # Get the text of the message from the parsed X-TIKA:content section
    txt = message["X-TIKA:content"]

    # Pass the text to the get_ttr_text function
    ttr_array, text_data = get_ttr_text(txt)

    message['TTR_text'] = {'ttr_text': text_data, 'ttr_array': ttr_array}

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