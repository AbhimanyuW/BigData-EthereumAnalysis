import json

# Executing this code creates a copy of scams.json file as a .csv file

with open('scams.json') as jfile:
    json = json.load(jfile)


op = open('scams.csv', 'w')

for address in json['result'].keys():
    for i in json['result'][address]['addresses']:
        
        identity = str(json['result'][address]['id'])
        name = str(json['result'][address]['name'])
        url = str(json['result'][address]['url'])
        coin = str(json['result'][address]['coin'])
        if json['result'][address]['category'] != 'Scam':
            category = str(json['result'][address]['category'])
        else:
            category = 'Scamming'

        if 'subcategory' in json['result'][address]:
            subcategory = str(json['result'][address]['subcategory'])
        else:
            subcategory = ""

        index = str(i)
        status = str(json['result'][address]['status'])

        csv = identity + ',' + name + ',' + url + ',' + coin + ',' + category + ',' + subcategory + ',' + index + ',' + status + '\n'

        op.write(csv)

op.close()
