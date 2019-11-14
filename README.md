# NASDAQ ITCH 5.0 Parser

## NASDAQ Stock Exchange Itch 5.0 Protocol
In algorithmic trading, it is mission-critical to be able to obtain and process information with as little latency as possible, and to this end, the NASDAQ Stock Exchange currently offers firms the outbound ITCH 5.0 protocol for efficiently sending byte-by-byte messages encompassing all of the market's data on securities, including changes to the order book and successful executions of trades and under what conditions.

Information on this protocol can be found here: http://www.nasdaqtrader.com/Trader.aspx?id=Totalview2

## Parsing Trades for a Volume Weighted Average Price (VWAP)
In my script, I specifically seek out any orders that impact the order book or result in a normal in-market trade to occur, and use the resulting information on quantities and prices of executed orders on each security to derive a VWAP for each trading hour (during standard market hours).

Normal market hours are from 9:30 AM to 4:00 PM EST; in the output CSV file I create 1 hour windows from 10:00 AM to 4:00 PM.

Specifications of the formatting of each type of byte-wise message can be found in NASDAQ's PDF here: http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf

The most important features to note are that each message's type is defined by a character in its first byte, which dictates the length of its associated data and the fields included with it. Every message has a timestamp in nanoseconds from the beginning of transmission for the day, each type of trade or order message has differing rules on associated quantities and prices.

## Required Input
At the bottom of the file, the file name must be provided of a matching ITCH 5.0 file. Sample files, and the information about them, can be found here: https://www.nasdaqtrader.com/TraderNews.aspx?id=dtn2019-12

Download a desired data file and keep it in the same directory, set fileName accordingly.

My implementation also requires one non-standard external package: pandas. Installation information can be found here: https://pandas.pydata.org/getpandas.html

## Output
After a successful run, the resulting data will be output as a CSV file in the same directory
