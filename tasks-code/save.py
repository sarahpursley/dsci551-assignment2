import json

with open('fraudulent_emails_v2.json', 'r') as f:
    messages = json.load(f)

to_text = []

for message in messages:
    to_append = str(message['TTR_text']['ttr_text'])
    if len(to_append) > 0:
        try:
            to_text.append(to_append)
        except:
            continue

with open('ttr_text.txt', 'w') as outfile:
    string = "\n\n============\n\n"
    new_text = string.join(to_text)
    outfile.write(new_text)
    outfile.close()
