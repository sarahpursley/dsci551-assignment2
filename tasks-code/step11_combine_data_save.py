import json
import os
import pandas as pd


####################################################
# Q11 - Generate new rows and save data


DIR = os.getcwd()

JSON_file = "fraudulent_emails_v2.json"
with open(JSON_file, 'r') as injson:
    messages = json.load(injson)

# Each of these files are generated messages from GPT-2 of different types
malware_emails = {'emails': "gpt2_gentext_mal.txt", 'type': 'malware'}
phishing_emails = {'emails': "gpt2_gentext_phish.txt", 'type': 'phishing'}
recon_emails = {'emails': "gpt2_gentext_recon.txt", 'type': 'recon'}
se_emails = {'emails': "gpt2_gentext_se.txt", 'type': 'social_engineering'}
reply_emails = {'emails': "gpt2_gentext_reply.txt", 'type': 'reply'}

gpt2_emails = []
for corpus in [malware_emails, phishing_emails, recon_emails, se_emails]:
    with open(os.path.join(DIR, "new_attacks", corpus['emails']), 'r') as f:
        data = f.read()
        # Split email .txt files on === line that marks separate emails
        emails = data.split('====================')
        f.close()

    for email in emails[:-1]:    # Last item is empty
        if email != '':
            gpt2_emails.append({"X-TIKA:content": email, "se_tag": corpus['type'], "Falsified": "Yes", "GPT2_gen_images": {"Phish_Iris_image": {}}})


with open(os.path.join(DIR, "new_attacks", reply_emails['emails']), 'r') as f:
    data = f.read()
    # Split email .txt files on === line that marks separate emails
    emails = data.split('====================')
    f.close()

# Load replies to append to dataset
reply_emails = []
for email in emails[:-1]:
    if email != '' and email != ' ':
        reply_emails.append(email)
    

# Load image captions to append to dataset
with open('img_captions.json', 'r') as fj:
    img_captions = json.load(fj)
    fj.close

IMAGE_CAPTIONS = []
for image in img_captions:
    for key, value in image.items():
        filename = key.split('/')[-1] # should be filename
        IMAGE_CAPTIONS.append({"Image_filename": filename, "Image_caption": value})


# Save face image filenames to append to dataset
PATH = os.path.join(DIR, 'new_attacks', 'faces800')
directory = os.listdir(PATH)

faces = []
for file in directory:
    faces.append(file)



for i in range(800):
    # Add image data to 800 rows
    gpt2_emails[i]["GPT2_gen_images"]["Face_image"] = faces[i]
    gpt2_emails[i]["GPT2_gen_images"]["Phish_Iris_image"] = IMAGE_CAPTIONS[i]

gpt2_emails = gpt2_emails[:-1]    # Last message is junk

for j in range(2400):
    # Add replies to dataset
    gpt2_emails.append({"X-TIKA:content": reply_emails[j], "se_tag": "reply", "Falsified": "Yes"})


for email in gpt2_emails:
    messages.append(email)



####################################################
# SAVE FINAL DATA

# Save to JSON
JSON_file = "fraudulent_emails_vFINAL.json"
with open(JSON_file, 'w') as outjson:
    json.dump(messages, outjson, indent=2)

if os.path.exists(JSON_file):
    print(f"Saved {JSON_file}")

# Save to TSV
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

# Save "flattened" message blobs to new tsv file
flat_messages = []

# These are our TSV headers
keys = [
    'se_tags',
    'se_tag',
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
TSV = "fraudulent_emails_vFINAL.tsv"
final_df.to_csv(TSV, sep='\t')
if os.path.exists(TSV):
    print(f"Saved {TSV}")