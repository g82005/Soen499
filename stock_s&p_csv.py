import os
import requests


def download_file(url, filename):
    ''' Downloads file from the url and save it as filename '''
    # check if file already exists
    if not os.path.isfile(filename):
        print('Downloading File')
        response = requests.get(url)
        # Check if the response is ok (200)
        if response.status_code == 200:
            # Open file and write the content
            with open(filename, 'wb') as file:
                # A chunk of 128 bytes
                for chunk in response:
                    file.write(chunk)
    else:
        print('File exists')


if __name__ == '__main__':
    """ s&p index """
    s_and_p = '.INX'
    api_key = '193RYP0HN0HC1569'

    """ Download s&p index csv file"""
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_price.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p macd csv file"""
    url = 'https://www.alphavantage.co/query?function=MACD&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)

    filename = './s&p/{}_macd.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p rsi csv file"""
    url = 'https://www.alphavantage.co/query?function=RSI&time_period=60&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_rsi.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p adx csv file"""
    url = 'https://www.alphavantage.co/query?function=ADX&time_period=60&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_adx.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p BBANDS csv file"""
    url = 'https://www.alphavantage.co/query?function=BBANDS&time_period=60&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_bbands.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p STOCH csv file"""
    url = 'https://www.alphavantage.co/query?function=STOCH&time_period=60&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_stoch.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p SMA5 csv file"""
    url = 'https://www.alphavantage.co/query?function=SMA&time_period=5&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_sma5.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p SMA10 csv file"""
    url = 'https://www.alphavantage.co/query?function=SMA&time_period=10&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_sma10.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p SMA20 csv file"""
    url = 'https://www.alphavantage.co/query?function=SMA&time_period=20&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_sma20.csv'.format(s_and_p)
    download_file(url, filename)

    """ Download s&p SMA40 csv file"""
    url = 'https://www.alphavantage.co/query?function=SMA&time_period=40&symbol={}&apikey={}&interval=daily&series_type=close&datatype=csv&outputsize=full'.format(
        s_and_p, api_key)
    filename = './s&p/{}_sma40.csv'.format(s_and_p)
    download_file(url, filename)

