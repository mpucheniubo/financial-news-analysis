import logging

import json
import pandas as pd
import pyodbc

from . import Model

def GetAnalysis(ConnectionString, Language, NewsGuid):

    tr4w = Model.TextRank4Keyword()

    Analysis = []

    News = pd.DataFrame()

    SqlInput = "SELECT [Guid],[Text] FROM [News].[Values] WHERE [Guid] = '" + NewsGuid + "' AND [Lang] = '" + Language + "'"

    try:
        cnxn = pyodbc.connect(ConnectionString)
        News = pd.read_sql(SqlInput,cnxn)
        cnxn.close()
    except Exception as e:
        logging.info(e)
        logging.info(SqlInput)

    for index, row in News.iterrows():
        NewsGuid = row["Guid"]
        NewsText = row["Text"]

        try:

            tr4w.analyze(NewsText, candidate_pos = ['NOUN', 'PROPN'], window_size=4, lower=False)
            
            ModelOutput = tr4w.get_keywords()
            
            for output in ModelOutput:
                
                if len(output[0])>2:

                    Analysis.append([output[0].replace("'","''"), str(output[1])])

        except Exception as e:
            logging.info(e)
            logging.info(SqlInput)

    return Analysis

def GetSymbols(ConnectionString,StockExchange):

    SqlInput = "SELECT CONCAT([Symbol], '|', [Name]) AS [Values] FROM [Stock].[Symbols] WHERE [StockExchange] = '" + StockExchange + "'"

    StockValues = []

    try:
        cnxn = pyodbc.connect(ConnectionString)
        Stock = pd.read_sql(SqlInput,cnxn)
        StockValues = Stock["Values"].to_list()
        cnxn.close()
    except Exception as e:
        logging.info(e)
        logging.info(SqlInput)

    return StockValues
    

def GetNewsKeywords(ConnectionString,Language,NewsGuid):

    NewsKeywords = []

    SqlInput = "SELECT [Keywords] FROM [News].[Values] WHERE [Guid] = '" + NewsGuid + "' AND [Lang] = '" + Language + "'"
    try:
        cnxn = pyodbc.connect(ConnectionString)
        News = pd.read_sql(SqlInput,cnxn)
        NewsKeywords = News["Keywords"].to_list()
        cnxn.close()
    except Exception as e:
        logging.info(e)
        logging.info(SqlInput)

    return NewsKeywords

def MatchNewsWithAssets(Analysis,Symbols,NewsKeywords,ForbiddenWords):

    match = [] 

    try:

        for rows in Analysis:

            word = rows[0]
            score = rows[1]

            PotentialSymbols = ""
            ChosenSymbols = ""

            if word not in ForbiddenWords:

                for symbols in Symbols:

                    if word in symbols:

                        SymbolInfo = symbols.split('|')
                        Symbol = SymbolInfo[0]
                        Name = SymbolInfo[1]

                        PotentialSymbols += '{"Symbol":"' +  Symbol.replace("'","''") + '","Name":"' + Name.replace("'","''") + '"},'

                if len(PotentialSymbols) > 1 :

                    PotentialSymbols = '[' + PotentialSymbols[:-1] + ']'

                    JsonSymbols = json.loads(PotentialSymbols)

                    for symbol in JsonSymbols:

                        for newsKeyword in NewsKeywords:

                            if newsKeyword in symbol["Name"]:

                                ChosenSymbols += json.dumps(symbol) + ","

                    if len(ChosenSymbols)>1:

                        ChosenSymbols = "[" + ChosenSymbols[:-1] + "]"

                    else:

                        ChosenSymbols = ""

                else:

                    PotentialSymbols = ""

            match.append([word,score,PotentialSymbols,ChosenSymbols])
            
    except Exception as e:

        logging.info(e)

    return match


def CommitAnalysis(ConnectionString,NewsGuid,AnalysisMethod,Language,StockExchange,MatchList):

            SqlInput = "INSERT INTO [News].[Analysis]([NewsGuid],[AnalysisMethod],[Language],[Keywords],[Score],[StockExchange],[PotentialSymbols],[ChosenSymbols]) VALUES "

            if MatchList:

                for match in MatchList:
                    
                    word = match[0]
                    score = match[1]
                    potentialSymbols = match[2]
                    chosenSymbols = match[3]

                    SqlInput += "('" + NewsGuid + "','" + AnalysisMethod + "','" + Language + "','" + word + "'," + score + ",'" + StockExchange + "','" + potentialSymbols + "','" + chosenSymbols + "'),"

                SqlInput = SqlInput[:-1]

                try:
                    cnxn = pyodbc.connect(ConnectionString)
                    cursor = cnxn.cursor()
                    cursor.execute(SqlInput)
                    cnxn.commit()
                    cnxn.close()

                except Exception as e:
                    logging.info(e)
                    logging.info(SqlInput)
        






