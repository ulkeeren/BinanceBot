import pandas as pd
import shutil
import os


class FileIO:
    def __init__(self, fileName: str = "whitelist.txt"):
        self.fileName = fileName
        self.cwd = os.getcwd()
        self.intervals = ["1m", "5m", "15m", "1h", "4h"]
        self.marketDataDirectory = os.path.join(self.cwd, "MarketData")
        self.createMarketDataDirectories()

    def readWhitelist(self):
        file = open(self.fileName, 'r')
        return file.readlines()

    def openCsvFile(self):
        df = pd.read_csv(self.fileName)
        return df

    def updateCsvFile(self, rowsToAppend):
        df = self.openCsvFile()
        for row in rowsToAppend:
            df = pd.concat([df, row])

    def createMarketDataDirectories(self):
        if not os.path.exists(self.marketDataDirectory):
            os.mkdir(self.marketDataDirectory)
        self.createPairDirectories()

    def createPairDirectories(self):
        pairs = self.readWhitelist()
        for pair in pairs:
            pairDirectoryPath = os.path.join(self.marketDataDirectory, pair)
            if not os.path.exists(pairDirectoryPath):
                os.mkdir(pairDirectoryPath)

    def cutAndPasteFilesIntoDirectories(self):
        # pairs example = "BTCUSDT"
        pairs = self.readWhitelist()
        for pair in pairs:
            for interval in self.intervals:
                self.moveCsvFilesToDirectories(pair[:-1], interval) # [:-1] to exclude \n

    def moveCsvFilesToDirectories(self, pair, interval):
        csvFileName = f"{pair}_{interval}.csv."
        csvFilePath = os.path.join(self.cwd, csvFileName)
        pairPath = os.path.join(self.marketDataDirectory, pair)
        newCsvPath = os.path.join(pairPath, csvFileName)
        os.rename(csvFilePath, newCsvPath)
