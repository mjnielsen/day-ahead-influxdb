import json, xmltodict, requests
from dateutil.parser import parse
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from decouple import config


def main():
    INFLUX_URL = config('INFLUX_URL')
    INFLUX_TOKEN = config('INFLUX_TOKEN')
    INFLUX_ORG = config('INFLUX_ORG')
    INFLUX_BUCKET = config('INFLUX_BUCKET')
    ENTSOE_TOKEN = config('ENTSOE_TOKEN')

    period_start = (datetime.now()).strftime('%Y%m%d') + "2200"
    period_end = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d') + "2200"
    request_url = ("https://web-api.tp.entsoe.eu/api?"
                "securityToken={entsoe_token}"
                "&documentType=A44"
                "&in_Domain=10YFI-1--------U"
                "&out_Domain=10YFI-1--------U"
                "&periodStart={period_start}"
                "&periodEnd={period_end}").format(
                    entsoe_token=ENTSOE_TOKEN,
                    period_start=period_start, 
                    period_end=period_end)

    data = xmltodict.parse(requests.get(request_url).text)
  
    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) \
            as influx_client:
        
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        try:
            day = data['Publication_MarketDocument']['TimeSeries']
        except KeyError:
            raise Exception(data['Acknowledgement_MarketDocument']['Reason']['text'])

        first_hour = parse(day['Period']['timeInterval']['start'])
        for hour in day['Period']['Point']:
            print(hour)
            write_api.write(INFLUX_BUCKET, INFLUX_ORG, Point.from_dict(
                {
                    "measurement": "hourlySpotPrice",
                    "fields": {
                        "price": float(hour['price.amount'])
                    },
                    "time": first_hour + timedelta(hours=int(hour['position'])-1)
                }))

        influx_client.close()


if __name__ == "__main__":
    main()