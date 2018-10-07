# msmquote
**msmquote** is a Java application for updating Microsoft Money files with quote data and currency exchange rates retrieved from Yahoo's JSON quote API.
## Getting Started
Download the latest **msmquote** [JAR](https://github.com/36bits/msmquote/releases) to your machine from and run as follows:

`java -cp msmquote-1.0.0-beta.jar uk.co.pueblo.msmquote.OnlineUpdate moneyfile.mny password "https://query2.finance.yahoo.com/v7/finance/quote?symbols=WMT,TSCO.L,CA.PA,^FTSE,GBPEUR=X`"

Where _moneyfile.mny_ is the MS Money file you wish to update and _password_ is the file password if applicable; omit the password if the file is not password protected. Replace the symbols after the _symbols=_ statement with the those for the quotes you want to update.

Symbol examples:
* Walmart Inc.: WMT
* Tesco PLC: TSCO.L  
* Carrefour SA: CA.PA
* FTSE-100 Index: ^FTSE
* Pound Sterling/Euro exchange rate: GBPEUR=X

It should be possible to include any symbol in the symbol list for which a quote is available on [Yahoo Finance](https://finance.yahoo.com/).

The following exit codes are returned by **msmquote**:

* 0 Execution completed successfully
* 1 Execution completed with warnings
* 2 Execution terminated due to errors 

## Currently Updated Quote Data
* **Equities:** price, open, high, low, volume, day change, 52 week high, 52 week low, bid, ask, capitalisation, shares outstanding, PE, dividend yield.
* **Bonds, funds and indices:** price, open, high, low, volume, day change, 52 week high, 52 week low, bid, ask.
* **Currencies:** exchange rate.

## Tested Environments
**msmquote** has been tested with the following MS Money versions and operating systems:
* MS Money 2005 (14.0), UK version, on MS Windows 10.
* MS Money 2004 (12.0), UK version, on MS Windows 10.

## Prerequisites
This project requires a Java runtime environment (JRE) of version 8 or later.
## Author
Jonathan Casiot.
## Licence
This project is licenced under the GNU GPL Version 3 - see the [LICENCE](./LICENSE) file.
## With Thanks To
* Hung Le for Sunriise, the original MS Money quote updater.
* Yahoo for a decent stock quote API.