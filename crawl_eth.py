import requests
from web3 import Web3
from web3.middleware import geth_poa_middleware
from tronpy import Tron
from utils.abi import ERC20_ABI
from time import sleep
from decimal import Decimal
import csv
from utils.klaytn import get_klaytn_balance, get_kip7_balance

LOOPER_ETH_ENDPOINT = 'https://mainnet.infura.io/v3/e09363759e0a4d09b0a7ac39d83e0b14'
LOOPER_ETH_GAS_FEE_API_KEY = 'ab67e284eb9ba9481c6b9e237b21c6cfb4cb69e94e9760360b8119da7a11'
LOOPER_ETH_DEFAULT_GAS_PRICE = 1100
LOOPER_BSC_ENDPOINT = 'https://bsc-dataseed.binance.org'
LOOPER_TRON_ENDPOINT = 'https://api.trongrid.io'
LOOPER_NIT_ENDPOINT = 'https://node01.nitwallet.io'

w3 = Web3(Web3.HTTPProvider(LOOPER_ETH_ENDPOINT))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)
isConnected = w3.isConnected()

bw3 = Web3(Web3.HTTPProvider(LOOPER_BSC_ENDPOINT))
bw3.middleware_onion.inject(geth_poa_middleware, layer=0)
b_isConnected = bw3.isConnected()

nw3 = Web3(Web3.HTTPProvider(LOOPER_NIT_ENDPOINT))
nw3.middleware_onion.inject(geth_poa_middleware, layer=0)
n_isConnected = nw3.isConnected()


def check_contract(contract_address):
    checksum_address = w3.toChecksumAddress(contract_address)
    contract = w3.eth.contract(checksum_address, abi=ERC20_ABI)

    return contract.address

def replace_uncompleted_crawling():
    with open('./processed/wallet_balances.txt', 'r') as file:
        # read a list of lines into data
        data = file.readlines()
    f = open('./processed/wallet_addresses.csv', 'r+')
    csv_data = csv.reader(f)
    index = 0
    for row in csv_data:
        address = row[1]
        contract_address = row[9]
        balance = -1
        if 'KLAYTN' in row[3]:
            print(address, contract_address)
            if contract_address.__len__() > 2 and contract_address != 'MAINNET':
                rdata = get_kip7_balance(contract_address, address)
                raw_balance = int(rdata['balance'], 16)
                DECIMALS = Decimal('10') ** Decimal(rdata['decimals'])
                balance = raw_balance // DECIMALS
            else:
                result = get_klaytn_balance(address)
                raw_balance = int(result, 16)
                DECIMALS = Decimal('10') ** Decimal('18')
                balance = raw_balance // DECIMALS
            print(balance)
            data[index] = (balance.__str__() + '\n')

        index += 1

    f.close()
    with open('./processed/wallet_balances.txt', 'w') as file:
        file.writelines(data)


def validate_tron():
    replace_uncompleted_crawling()
    f = open('./processed/wallet_addresses.csv', 'r+')
    output = open('./processed/tron_balances.txt', 'w', newline='')
    csv_data = csv.reader(f)
    for row in csv_data:
        address = row[1]
        contract_address = row[9]
        balance = -1
        if 'TRON' in row[3]:
            client = Tron()
            try:
                trx_in_possession = client.get_account_balance(address)
            except Exception as e:
                trx_in_possession = 0
            if contract_address.__len__() > 2:
                contract = client.get_contract(contract_address)
                tron_balance = contract.functions.balanceOf(address)
                DECIMALS = 10 ** int(row[10])
                balance = tron_balance // DECIMALS
            else:
                tron_balance = int(Decimal(trx_in_possession))  # client.get_account_balance(received_address)
                balance = tron_balance

        print(balance)
        output.write(f'[{address}]({row[4]}){balance}' + '\n')

    f.close()
    output.close()


if __name__ == '__main__':
    f = open('./processed/wallet_addresses.csv', 'r+')
    output = open('./processed/wallet_balances.txt', 'w', newline='')
    csv_data = csv.reader(f)
    for row in csv_data:
        address = row[1]
        contract_address = row[9]
        balance = -1
        if 'ETHEREUM' in row[3]:
            if contract_address.__len__() > 2:
                checksum_address = w3.toChecksumAddress(contract_address)
                contract = w3.eth.contract(checksum_address, abi=ERC20_ABI)
                raw_balances = contract.functions.balanceOf(address).call()
                DECIMALS = 10 ** int(row[10])
                balance = raw_balances // DECIMALS
            else:
                raw_balances = w3.eth.getBalance(address)
                DECIMALS = 10 ** int(row[10])
                balance = raw_balances // DECIMALS
        elif 'BSC' in row[3]:
            if contract_address.__len__() > 2:
                checksum_address = bw3.toChecksumAddress(contract_address)
                contract = bw3.eth.contract(checksum_address, abi=ERC20_ABI)
                raw_balances = contract.functions.balanceOf(address).call()
                DECIMALS = 10 ** int(row[10])
                balance = raw_balances // DECIMALS
            else:
                raw_balances = bw3.eth.getBalance(address)
                DECIMALS = 10 ** int(row[10])
                balance = raw_balances // DECIMALS
        elif 'TRON' in row[3]:
            client = Tron()
            try:
                trx_in_possession = client.get_account_balance(address)
            except Exception as e:
                trx_in_possession = 0
            if contract_address.__len__() > 2:
                contract = client.get_contract(contract_address)
                tron_balance = contract.functions.balanceOf(address)
                DECIMALS = 10 ** int(row[10])
                balance = tron_balance // DECIMALS
            else:
                tron_balance = int(Decimal(trx_in_possession))  # client.get_account_balance(received_address)
                balance = tron_balance
        elif 'NIT' in row[3]:
            if contract_address.__len__() > 2 and contract_address != 'MAINNET':
                checksum_address = nw3.toChecksumAddress(contract_address)
                contract = nw3.eth.contract(checksum_address, abi=ERC20_ABI)
                raw_balances = contract.functions.balanceOf(address).call()
                DECIMALS = 10 ** int(row[10])
                balance = raw_balances // DECIMALS
            else:
                raw_balances = nw3.eth.getBalance(address)
                DECIMALS = 10 ** int(row[10])
                balance = raw_balances // DECIMALS
            sleep(1.5)
        elif 'BITCOIN' in row[3]:
            res = requests.request(url=f'https://api.blockcypher.com/v1/btc/main/addrs/{address}/balance', method='GET')
            data = res.json()
            balance = data['balance']
        elif 'KLAYTN' in row[3]:
            if contract_address.__len__() > 2 and contract_address != 'MAINNET':
                rdata = get_kip7_balance(contract_address, address)
                raw_balance = int(rdata['balance'], 16)
                DECIMALS = Decimal('10') ** Decimal(rdata['decimals'])
                balance = raw_balance // DECIMALS
            else:
                result = get_klaytn_balance(address)
                raw_balance = int(result, 16)
                DECIMALS = Decimal('10') ** Decimal('18')
                balance = raw_balance // DECIMALS

        print(balance)
        output.write(balance.__str__() + '\n')

    f.close()
    output.close()

