# Author: Marcus Horn

# This script takes a desired NASDAQ Itch 5.0 file as input and parses
# trades each hour of the trading day covered, and outputs a CSV file with a
# VWAP (Volume-Weighted Average Price) over each hour for each security.

# The formatting of the messages is very specific to NASDAQ, and the 
# specifications can be found here: 
# http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf

import sys
import os
import gzip
import pandas as pd

# To parse the message bytes according to the specifications, I will be using
# Python's struct library to differentiate the desired datatypes for each field
# according to the documentation: https://docs.python.org/2/library/struct.html
import struct
# Note: timestamps, despite being integers, do not have a direct equivalent
# type for the struct unpacking function as they have size of 6; I will treat
# them as a character array to then convert to an integer

def messageMap():
    # This function defines the lengths of each type of message according to
    # NASDAQ specifications; used to properly parse every message
    m_map = dict()
    m_map["S"] = 11 # System messages, important to set start/stop timestamps
    m_map["R"] = 38 # Stock Directory, use to set keys in map of stocks map
    m_map["H"] = 24
    m_map["Y"] = 19
    m_map["L"] = 25
    m_map["V"] = 34
    m_map["W"] = 11
    m_map["K"] = 27
    m_map["J"] = 34
    m_map["h"] = 20
    m_map["A"] = 35 # Added orders
    m_map["F"] = 39 # Added orders
    m_map["E"] = 30 # Executed orders, linked to the previously added orders
    m_map["C"] = 35 # Executed orders without linked added orders
    m_map["X"] = 22
    m_map["D"] = 18
    m_map["U"] = 34 # Modifications to added orders
    m_map["P"] = 43 # Undisplayable non-cross orders executed
    m_map["Q"] = 39
    m_map["B"] = 18
    m_map["I"] = 49
    m_map["N"] = 19
    return m_map

def read_bytes(file, n):
    # Advances the file by n number of bytes and return the data in the sequence
    message = file.read(n)
    return message

def decodeTimestamp(timestamp):
    # Given a 6 byte integer, returns an 8 bit unsigned long long
    new_bytes = struct.pack('>2s6s',b'\x00\x00', timestamp) # Add padding bytes
    new_timestamp = struct.unpack('>Q', new_bytes)
    return new_timestamp[0]

def hourlyMap(stockIDs, openTime, endTime):
    # Using a complete list of stock IDs, sets up an empty list of trades for
    # each trading hour on each security, along with cutoff times in nanoseconds
    trades = dict()
    nsPerHour = (10**9) * (60*60) # Nanoseconds per hour
    for ID in stockIDs:
        stock_trades = dict()
        # Market hours: 9:30 AM - 4:00 PM
        # Subtract (N * nsPerHour) from the endTime to make 1 hour increments
        # with total of 7 market hours counted
        for n in range(10, 17):
            # 12-hr times, excluding AM/PM since they wouldn't overlap
            hour = "%d:00" % ((n + 11) % 12 + 1)
            tradeTuple = (0, 0) # (Sum of Prices*Quantities, Total Quantity)
            stock_trades[hour] = tradeTuple
        trades[ID] = stock_trades
    return trades

def calculateHour(time, endTime):
    # Returns a string for which hour's bucket a timestamp fits in
    nsPerHour = (10**9) * (60*60) # Nanoseconds per hour
    hour = "%d:00" % ((min(16 - ((endTime - time) // nsPerHour),
        16) + 11) % 12 + 1)
    return hour

def parseOrders(trades, orders, endTime):
    # Using a list of orders, creates a total value and total quantity of each
    # hour's trades, and stores them at the proper hour
    for order in orders:
        stock, price, quantity, time = order
        hour = calculateHour(time, endTime)

        curValue, curQuantity = trades[stock][hour]
        value = price * quantity
        curValue += value
        curQuantity += quantity

        trades[stock][hour] = (curValue, curQuantity)
    print("Test 1")
    return trades

def parseTrades(file, m_map):
    # First, we want to extract all relevant messages and their information:
    # System "S" - to determine timestamps of market open and close
    openTime = 0 # Measured in nanoseconds to split trades by the hour
    endTime = 0

    # Stock Directory "R" - to map IDs to each stock ticker
    stock_map = dict() # Maps tickers to stock IDs

    # Add Order W/ MPID "F", W/O MPID "A" - New orders placed on the book with
    # a reference number and price defined to match with an execution order
    added_orders = dict() # Maps sale price to reference number

    # Order Executed "E" - Message that a corresponding added order was filled
    # in part or in full for the price in the original add order. Linked to an
    # added order via the reference number field to get price
    # ONLY COUNT ORDERS DURING NORMAL TRADING HOURS

    # Order Executed W/ Price "C" - An abnormal filled order that doesn't have
    # a matching added order, price is defined in the message. If Non-printable,
    # don't count it in the volume (counted in Cross Trades)

    # Order Replace Messages "U" - Events where the details of an existing added
    # order are overwritten, stock ID is the same, reference number is updated
    # Removing the mapping for the previous added order is unnecessary, as new
    # messages will reference this new number. 
    # We will modify the existing map of added orders.

    # Trade Message (Non-Cross) "P" - Matches made for non-displayed orders

    # Cross trades shouldn't be included, as they don't involve the wider market

    filled_orders = []

    m_type = file.read(1)
    totalCount = 0 # Counters for providing feedback on parsing progress
    runningCount = 0
    megaByte = 1000000 # Bytes per MB
    updateFreq = 100000000 # .1 GB
    started = False # Tracks if the market opened yet
    
    while m_type:
        m_type = m_type.decode()
        if(runningCount > updateFreq):
            print("%d MB parsed..." % (totalCount / megaByte))
            runningCount = runningCount % updateFreq
        if m_type in m_map.keys():
            steps = m_map[m_type]
            msg = read_bytes(file, steps)
            totalCount += steps
            runningCount += steps
            if m_type == "S":
                data = struct.unpack('>HH6sc',msg)
                if data[3].decode() == "Q": # Start of Market hours
                    openTime = decodeTimestamp(data[2])
                    print("Market opened at %d nanoseconds: " % openTime)
                    started = True
                elif data[3].decode() == "M": # End of Market hours
                    endTime = decodeTimestamp(data[2])
                    print("Market closed at %d nanoseconds: " % endTime)
                    break
            elif m_type == "R":
                data = struct.unpack('>HH6s8sccIcc2scccccIc',msg)
                stockID = data[0]
                # Converts to string, removes trailing spaces
                ticker = data[3].decode().strip()
                stock_map[stockID] = ticker
            elif m_type == "A":
                data = struct.unpack('>HH6sQcI8sI',msg)
                reference = data[3]
                price = data[7] / (10 ** 4) # 4 decimal points
                added_orders[reference] = price
            elif m_type == "F":
                data = struct.unpack('>HH6sQcI8sI4s',msg)
                reference = data[3]
                price = data[7] / (10 ** 4) # 4 decimal points
                added_orders[reference] = price
            elif m_type == "E" and started == True:
                data = struct.unpack('>HH6sQIQ',msg)
                stockID = data[0]
                quantity = data[4]
                time = decodeTimestamp(data[2])
                reference = data[3]
                price = added_orders[reference]
                orderTuple = (stockID,price,quantity,time)
                filled_orders.append(orderTuple)
            elif m_type == "C" and started == True:
                data = struct.unpack('>HH6sQIQcI',msg)
                printable = data[6]
                if printable.decode() == "Y": # Only count Printable
                    stockID = data[0]
                    price = data[7] / (10 ** 4) # 4 decimal points
                    quantity = data[4]
                    time = decodeTimestamp(data[2])
                    orderTuple = (stockID,price,quantity,time)
                    filled_orders.append(orderTuple)
            elif m_type == "U":
                data = struct.unpack('>HH6sQQII',msg)
                reference = data[4] # New reference number created
                price = data[6] / (10 ** 4) # 4 decimal points
                added_orders[reference] = price
            elif m_type == "P" and started == True:
                data = struct.unpack('>HH6sQcIQIQ',msg)
                stockID = data[0]
                price = data[7] / (10 ** 4) # 4 decimal points
                quantity = data[5]
                time = decodeTimestamp(data[2])
                orderTuple = (stockID,price,quantity,time)
                filled_orders.append(orderTuple)
        m_type = file.read(1) # Advances to next byte
        totalCount += 1
        runningCount += 1

    #Combine fulfilled orders into a map of trades per stock per hour
    trades = hourlyMap(stock_map.keys(), openTime, endTime) # Setup
    trades = parseOrders(trades, filled_orders, endTime) # Calculates trades
    print("Test 2")
    return stock_map, trades

def VWAP(tradeTuple, runningValue = 0, runningQuantity = 0):
    # Takes in a tuple of hourly trade values and quantities, per 
    # security per hour, and calculates a running VWAP as output
    # Format of trades: (Total Value, Total Quantity)

    # Since a running VWAP is desired, I will be counting the prices and
    # quantities made that day up to the current hour, represented by the
    # optional fields of this function
    totalValue, totalQuantity = tradeTuple
    totalValue += runningValue
    totalQuantity += runningQuantity
    if totalQuantity != 0:
        average = totalValue/totalQuantity
    else:
        average = 0
    return totalValue, totalQuantity, average

def main(fileName):
    print("Parsing NASDAQ file %s: " % fileName)
    file = gzip.open(fileName, 'rb')
    m_map = messageMap() # Sets up lengths of each message type
    stock_map, trades = parseTrades(file, m_map)
    print("Done parsing!")

    print("Calculating VWAPs and exporting to CSV file: ")
    stocks = []
    splitName = fileName.split(',')
    outName = splitName[0] + ".csv"
    output = pd.DataFrame()
    vwap_map = dict() # VWAPs by hour, starting at 10:00 AM through 4:00 PM
    for i in range(10,17):
        hour = "%d:00" % (((i + 11) % 12) + 1)
        vwap_map[hour] = []

    for ID in trades.keys():
        stocks.append(stock_map[ID])
        curValue = 0 # Running totals from VWAP calculation
        curQuantity = 0
        for i in range(10,17):
            hour = "%d:00" % (((i + 11) % 12) + 1)
            curValue, curQuantity, vwap = VWAP(trades[ID][hour],
                curValue, curQuantity)
            vwap_map[hour].append(vwap)

    output["Stock Ticker"] = stocks
    for i in range(10,17):
        hour = "%d:00" % (((i + 11) % 12) + 1)
        vwaps = vwap_map[hour]
        output["%s Running VWAP" % hour] = vwaps

    output.to_csv(outName)
    print("File exported as %s" % outName)
    pass

def testCalculateHour():
    # Small function to test the correctness of the calculateHour helper 
    # function, the values are copy-pasted timestamps from parsing the real data
    startTime = 34200000036157
    print(startTime)
    endTime = 57600000113132
    testTime = 57500000113132
    print(endTime, testTime)
    print(calculateHour(testTime, endTime))
    return

#testCalculateHour()
#print(os.listdir())

# Set the desired file to parse here
fileName = ''
main(fileName)

