import logging

import azure.functions as func

import os
import datetime

from . import Functions

def main(req: func.HttpRequest) -> func.HttpResponse:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info('Python HTTP trigger function execution started: %s', utc_timestamp)

    NewsGuid = str(req.params.get('guid'))

    logging.info(NewsGuid)

    if NewsGuid:

        Language = "de"

        AnalysisMethod = "spacy.de_core_news_sm"

        StockExchange = "GER"
        ForbiddenWords = [""," ", "Aktie","Aktien","Deutsche","Deutschland","EUR","Euro","Germany","Industrie","Partner","Partners","Reuters","the","Thomson","Wert","Werte","Wirtschaft"]

        ConnectionStringInfo = str(os.environ["string_sqldb_information"])
        
        Analysis = Functions.GetAnalysis(ConnectionStringInfo,Language,NewsGuid)

        if Analysis:
        
            Symbols = Functions.GetSymbols(ConnectionStringInfo,StockExchange)

            NewsKeywords = Functions.GetNewsKeywords(ConnectionStringInfo,Language,NewsGuid)

            Match = Functions.MatchNewsWithAssets(Analysis,Symbols,NewsKeywords,ForbiddenWords)

            Functions.CommitAnalysis(ConnectionStringInfo,NewsGuid,AnalysisMethod,Language,StockExchange,Match)

        utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        logging.info('Python timer trigger function execution concluded: %s', utc_timestamp)

        return func.HttpResponse(
             "Execution successfully completed",
             status_code=200
        )

    else:

        utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        logging.info('Python timer trigger function execution concluded: %s', utc_timestamp)

        return func.HttpResponse(
             "Please pass a valid guid parameter",
             status_code=400
        )
    

