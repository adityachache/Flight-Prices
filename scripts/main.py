import os
import pymongo
from datetime import datetime
from amadeus import Client, ResponseError
from event_logger import EventLogger
from dateutil.relativedelta import relativedelta

HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Content-Type": "application/x-www-form-urlencoded",
    "Connection": "keep-alive"
}

BASE_URL_TOKEN = "https://test.api.amadeus.com/v1/security/oauth2/token/"

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')

amadeus = Client(
    client_id=API_KEY,
    client_secret=API_SECRET
)

# Get the current date and time
CURRENT_DATE = datetime.now()


class DatabaseHandler:
    def __init__(self, db_name, collection_name):
        self.client = pymongo.MongoClient(
            f"mongodb+srv://adityachache:{MONGO_PASSWORD}@etlcluster.2itnsyc.mongodb.net/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_document(self, document):
        self.collection.insert_one(document)
        print('data inserted in MongoDB')

    def find_documents(self, query):
        return self.collection.find(query)

    def find_one_document(self, query):
        return self.collection.find_one(query)

    def insert_many_documents(self, documents_list):
        self.collection.insert_many(documents_list, ordered=True)
        print('data inserted in MongoDB')


class FlightOffers:

    def __init__(self):
        self.headers = HEADERS
        self.resData = {}  # response from api will be loaded here
        self.itineraries = list()
        self.dataToSave = list()

    @staticmethod
    def get_configvariables():
        handler = DatabaseHandler("ETL_Config", "ETL_ConfigVariables")
        handler_to_insert = DatabaseHandler("ETL_Data", "Event_Log")

        source_airport = ""
        destination_airport = ""
        max_offers = ""

        try:
            source_airport = handler.find_one_document({"name": "source_IATA"})
            destination_airport = handler.find_one_document({"name": "destination_IATA"})
            max_offers = handler.find_one_document({"name": "maxOffers"})
        except Exception as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)

        obj = {
            'source': source_airport["value"],
            'destination': destination_airport["value"],
            'max': max_offers["value"]
        }

        return obj

    def get_flight_data(self):
        handler_to_insert = DatabaseHandler("ETL_Data", "Event_Log")

        # Calculate the date one month from now
        one_month_from_now = CURRENT_DATE + relativedelta(months=1)
        one_month_from_now_str = one_month_from_now.strftime("%Y-%m-%d")

        config_values = self.get_configvariables()

        try:
            response = amadeus.shopping.flight_offers_search.get(
                originLocationCode=config_values['source'],
                destinationLocationCode=config_values['destination'],
                departureDate=one_month_from_now_str,
                adults=1,
                max=config_values['max'],
                nonStop='false',
                currencyCode="INR")
            # print(response.result)
            self.resData = response.result
        except ResponseError as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)

    def format_data(self):
        handler_to_insert = DatabaseHandler("ETL_Data", "Event_Log")

        try:
            self.get_flight_data()
            offerData = self.resData['data']
        except KeyError as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)
            return  # Exit the function if there is a problem with the flight data

        try:
            for itineraries in offerData:
                try:
                    obj = {
                        'itineraries': itineraries['itineraries'],
                        'price': itineraries['price']
                    }
                    self.itineraries.append(obj)
                except KeyError as e:
                    event = EventLogger.log_event(e)
                    handler_to_insert.insert_document(event)
                    continue  # Skip to the next itinerary if there is a problem with the current one

            for itinerary in self.itineraries:
                try:
                    newObj = {
                        'totalDuration': itinerary['itineraries'][0]['duration'],
                        'flights': itinerary['itineraries'][0]['segments'],
                        'totalCost': {'currency': itinerary['price']['currency'], 'total': itinerary['price']['total']}
                    }
                    self.dataToSave.append(newObj)
                except (KeyError, IndexError) as e:
                    event = EventLogger.log_event(e)
                    handler_to_insert.insert_document(event)
                    continue  # Skip to the next itinerary if there is a problem with the current one
        except Exception as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)
            return  # Exit the function if there is an unexpected error

    def replace_codes(self):
        handler_to_insert = DatabaseHandler("ETL_Data", "Event_Log")

        for entry in self.dataToSave:
            for flight in entry['flights']:
                try:
                    # Replace carrier code with carrier name
                    carrier_code = flight['carrierCode']
                    flight['carrierCode'] = self.resData["dictionaries"]['carriers'][carrier_code]
                    flight['operating']['carrierCode'] = self.resData["dictionaries"]['carriers'][
                        flight['operating']['carrierCode']]
                except KeyError as e:
                    event = EventLogger.log_event(e)
                    handler_to_insert.insert_document(event)

                try:
                    # Replace aircraft code with aircraft name
                    aircraft_code = flight['aircraft']['code']
                    flight['aircraft']['code'] = self.resData["dictionaries"]['aircraft'][aircraft_code]
                except KeyError as e:
                    event = EventLogger.log_event(e)
                    handler_to_insert.insert_document(event)

        return self.dataToSave

    @staticmethod
    def save_data_in_db(data):
        handler_to_add = DatabaseHandler("ETL_Data", "Airline_Data")
        handler_to_add.insert_many_documents(data)

# test commit in mac

if __name__ == "__main__":
    finder = FlightOffers()
    finder.format_data()
    finalData = finder.replace_codes()
    print(finalData)
    finder.save_data_in_db(finalData)































