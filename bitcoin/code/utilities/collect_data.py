import sys
sys.path.append("C:/Users/jkim118/Documents/bitcoin/code/")
from utilities.bitcoin_data import bitcoin
import time

def update_bitcoin_data():
    '''
    Declaring a bitcoin class variable updates the bitcoin data.
    Using this, update the bitcoin data.
    '''
    b = bitcoin()

def collect_bitcoin_data(days):
    '''
    Collect bitcoin data.
    Make sure to not do it too much too fast to avoid being banned by bitstamp.
    Arguments
    '''
    seconds = 86400 * days
    while seconds > 0:
        update_bitcoin_data()
        seconds -= 10
        time.sleep(10)
    print("bitcoin data updated")

if __name__ == '__main__':
    days = sys.argv[0]
    collect_bitcoin_data(days)
