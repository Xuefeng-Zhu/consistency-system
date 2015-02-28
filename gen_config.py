import json

config = {}
serverA = {
    'delay': 3,
    'port': 2001
}
serverB = {
    'delay': 2,
    'port': 2002
}

serverC = {
    'delay': 4,
    'port': 2003
}

serverD = {
    'delay': 3,
    'port': 2004
}

config['A'] = serverA
config['B'] = serverB
config['C'] = serverC
config['D'] = serverD

with open('config.json', 'w') as f:
    json.dump(config, f)
