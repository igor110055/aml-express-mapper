import requests

access_key_id = 'KASKJUUAYPANP181GIMP80DL'
secret_access_key = 'b2500ZPWlEocHmh3np5BlEq9fJ6HHzZmxL4cfsIv'
bs = 'Basic S0FTS0pVVUFZUEFOUDE4MUdJTVA4MERMOmIyNTAwWlBXbEVvY0htaDNucDVCbEVxOWZKNkhIelpteEw0Y2ZzSXY='

def get_klaytn_block():
    headers = {
        'x-chain-id': '8217',
    }

    json_data = {
        'jsonrpc': '2.0',
        'method': 'klay_blockNumber',
        'params': [],
        'id': 1,
    }

    response = requests.post(url='https://node-api.klaytnapi.com/v1/klaytn', headers=headers, json=json_data,
                             auth=(access_key_id, secret_access_key))
    print(response.json())


def get_klaytn_balance(address):
    headers = {
        'x-chain-id': '8217',
    }

    json_data = {
        'jsonrpc': '2.0',
        'method': 'klay_getBalance',
        'params': [address, 'latest'],
        'id': 1,
    }

    response = requests.post('https://node-api.klaytnapi.com/v1/klaytn', headers=headers, json=json_data,
                             auth=(access_key_id, secret_access_key))
    response_data = response.json()
    print(response_data)
    return response_data['result']


def get_kip7_balance(contract_address, account_address):
    token_balance_url = 'https://kip7-api.klaytnapi.com/v1/contract/' + contract_address + '/account/' + account_address + '/balance'
    headers = {
        'x-chain-id': '8217',
    }
    response = requests.get(token_balance_url, headers=headers, auth=(access_key_id, secret_access_key))
    response_data = response.json()
    print(response_data)
    return response_data  # balance, decimals

if __name__ == '__main__':
    get_klaytn_balance('0xb761923932cecd043acc320d122548a34a15d417')
    get_kip7_balance('0xfc8c4f0fe129bc070a474c1983c64efd2dce264b', '0x664acbd914f4091e670311ea555d62cdcb059a76')
