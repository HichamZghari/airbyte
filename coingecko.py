import json
from datetime import datetime
from typing import Dict, Generator
from datetime import datetime, timedelta
from pycoingecko import CoinGeckoAPI
import requests

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source

# Defining coin_geko Api
cg = CoinGeckoAPI()

def market_data(crypto):
    
  try:
    market = requests.get(f'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids={crypto}&order=market_cap_desc&per_page=100&page=1&sparkline=false').json()
    analyse = requests.get(f'https://api.coingecko.com/api/v3/coins/{crypto}?localization=false&tickers=false&market_data=false&community_data=true&developer_data=true&sparkline=false').json()
    data =  [{"symbol": (market[0]['symbol'].upper() + 'USDT'),
            "time": market[0]['last_updated'],
            "total_supply": market[0]['total_supply'],
            "circulating_supply": market[0]['circulating_supply'],
            "max_supply": market[0]['max_supply'],
            "total_volume": market[0]['total_volume'],
            "price_change_24h": market[0]['price_change_24h'],
            "price_change_percentage_24h": market[0]['price_change_percentage_24h'],
            "market_cap": market[0]['market_cap'],
            "market_cap_change_24h": market[0]['market_cap_change_24h'],
            "market_cap_change_percentage_24h": market[0]['market_cap_change_percentage_24h'],
            "market_cap_rank": market[0]['market_cap_rank'],
            "sentiment_votes_up_percentage": analyse["sentiment_votes_up_percentage"],
            "sentiment_votes_down_percentage": analyse["sentiment_votes_down_percentage"],
            "community_score": analyse['community_score'],
            "twitter_followers": analyse['community_data']['twitter_followers'],
            "reddit_average_posts_48h": analyse['community_data']['reddit_average_posts_48h'],
            "reddit_average_comments_48h": analyse['community_data']['reddit_average_comments_48h'],
            "reddit_subscribers": analyse['community_data']['reddit_subscribers'],
            "telegram_channel_user_count": analyse['community_data']['telegram_channel_user_count'],
            "subscribers_developers": analyse['developer_data']['subscribers'],
            "pull_request_contributors": analyse['developer_data']['pull_request_contributors']

            }] 
    return data
  except ValueError:
    return []

list = requests.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false').json()
cryptos = [crypto['id'] for crypto in list if crypto['market_cap_rank'] != None and crypto['market_cap_rank'] < 1000]

def all_market_data():
  data=[]
  for crypto in cryptos:
    data = data + market_data(crypto)
  
  return data

class SourceBinance(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.
        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file
        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            # Not Implemented

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.
        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file
        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []

        stream_name = "TableName"  # Example
        json_schema = {  # Example
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"columnName": {"type": "string"}},
        }

        # Not Implemented

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema))
        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.
        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.json file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.
        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """
        stream_name = "TableName" 

        data = {"data": all_market_data()}

        # Airbyte message with data 
        yield AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=int(datetime.now().timestamp()) * 1000),
        )
