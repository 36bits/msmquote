# msmquote
**msmquote** is a Java application for updating Microsoft Money files with quote data and currency exchange rates retrieved from Yahoo's JSON quote API.
## Getting Started
Download the latest **msmquote** [JAR](https://github.com/36bits/msmquote/releases) to your machine and run as follows:

`java -cp msmquote-1.0.1-beta.jar uk.co.pueblo.msmquote.OnlineUpdate moneyfile.mny password quoteurl`

Where:
* **moneyfile.mny** is the MS Money file you wish to update
* **password** is the file password if applicable (omit this if the file is not password protected)
* **quoteurl** is the URL for the Yahoo Finance quote API, for example to retrieve quotes for Walmart Inc., Tesco PLC, Carrefour SA, the FTSE-100 index and the Pound Sterling/Euro exchange rate you would use: `"https://query2.finance.yahoo.com/v7/finance/quote?symbols=WMT,TSCO.L,CA.PA,^FTSE,GBPEUR=X"`

Replace the symbols after the _symbols=_ statement in the quote URL with those for the quotes you want to update. It should be possible to include any symbol in the symbol list for which a quote is available on [Yahoo Finance](https://finance.yahoo.com/). The symbols must match those defined in your Money file.

The following exit codes are returned by **msmquote**:

* 0 Execution completed successfully
* 1 Execution completed with warnings
* 2 Execution terminated due to errors 

## Currently Updated Quote Data
* **Equities:** price, open, high, low, volume, day change, 52 week high, 52 week low, bid, ask, capitalisation, shares outstanding, PE, dividend yield.
* **Bonds and indices:** price, open, high, low, volume, day change, 52 week high, 52 week low, bid, ask.
* **Mutual funds:** price, day change.
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