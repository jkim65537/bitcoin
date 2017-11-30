import sys
sys.path.append("/Users/junkim/Documents/crypto/crypto/code/")
from utilities.crypto_data import crypto
import time

def update_crypto_data():
    '''
    Declaring a crypto class variable updates the crypto data.
    Using this, update the crypto data.
    '''
    b = crypto("eth")

def collect_crypto_data(days):
    '''
    Collect crypto data.
    Make sure to not do it too much too fast to avoid being banned by bitstamp.
    Arguments
    '''
    seconds = 86400 * days
    while seconds > 0:
        update_crypto_data()
        seconds -= 10
        time.sleep(10)
    print("crypto data updated")

if __name__ == '__main__':
    days = sys.argv[0]
    collect_crypto_data(days)
