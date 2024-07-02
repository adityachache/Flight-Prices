from bs4 import BeautifulSoup
import requests
import lxml
import pymongo
from datetime import datetime
from event_logger import EventLogger

HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
    "Accept-Encoding": "gzip, deflate"
}


class DatabaseHandler:
    def __init__(self, db_name, collection_name):
        self.client = pymongo.MongoClient(
            "mongodb+srv://adityachache:Slark_26@etlcluster.2itnsyc.mongodb.net/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_document(self, document):
        self.collection.insert_one(document)

    def find_documents(self, query):
        return self.collection.find(query)

    def find_one_document(self, query):
        return self.collection.find_one(query)

    def insert_many_documents(self, documents_list):
        self.collection.insert_many(documents_list, ordered=True)
        print('data inserted in MongoDB')


class Scraper:
    """Class to scrape flight details from skyscanner website"""

    def __init__(self):
        self.headers = HEADERS

    @staticmethod
    def extract_data(data: list):
        """extracts airline info from the scraped data and adds it into a python dictionary to save in mongoDB"""

        handler = DatabaseHandler("ETL_Config", "ETL_ConfigVariables")
        handler_to_insert = DatabaseHandler("ETL_Data", "Event_Log")

        span_texts = []
        data_to_save = []
        source_city = {}
        destination_city = {}

        try:
            source_city = handler.find_one_document({"name": "source_airport"})
            destination_city = handler.find_one_document({"name": "destination_airport"})
        except Exception as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)

        if len(data) != 0:
            try:
                for obj in data:
                    span_texts.append([span.get_text(strip=True) for span in obj.find_all('span')])
            except Exception as e:
                event = EventLogger.log_event(e)
                handler_to_insert.insert_document(event)

        try:
            for item in span_texts:
                # print(item)
                price_per_passenger = "INR " + item[1].split()[1]  # ₹ 16,325 per passenger.
                departure_date = item[2].split(',')[1].strip().replace('.', '')  # Tue, 7 May.
                airline = item[3].split()[3:]  # Saudia.
                airline = " ".join(airline).replace(".", "")
                flight_type = item[3].split()[0]  # One-way.
                airport = item[4].split(',')[-1].replace("arriving in ", "").replace(".", "").strip()  # Malpensa.
                source = source_city['value']
                destination = destination_city['value']
                current_date = str(datetime.now().date())

                data_obj = {'ticketCost': price_per_passenger, "departureDate": departure_date, "airline": airline,
                            "flightType": flight_type, 'airport': airport, 'source': source,
                            'destination': destination,
                            'currentDate': current_date}
                data_to_save.append(data_obj)

        except Exception as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)

        return data_to_save

    @staticmethod
    def save_data_to_db(data_array):
        """Saves data into the database"""

        handler_to_add = DatabaseHandler("ETL_Data", "Airline_Data")
        handler_to_insert = DatabaseHandler("ETL_Data", "Event_Log")
        try:
            handler_to_add.insert_many_documents(data_array)
        except Exception as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)

    def get_deals(self):
        """scrape data from the skyscanner website"""

        source_iata_code = ""
        destination_iata_code = ""
        scraped_data = ""

        handler = DatabaseHandler("ETL_Config", "ETL_ConfigVariables")
        handler_to_insert = DatabaseHandler("ETL_Data", "Event_Log")

        try:
            source_iata_code = handler.find_one_document({"name": "source_IATA"})
            destination_iata_code = handler.find_one_document({"name": "destination_IATA"})
            print(destination_iata_code)
        except Exception as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)

        try:
            web_url = f"https://www.skyscanner.co.in/routes/{source_iata_code['value']}/{destination_iata_code['value']}/"
            response = requests.get(url=web_url, headers=self.headers)
            webpage = response.text

            soup = BeautifulSoup(webpage, "lxml")
            scraped_data = soup.find_all("div", class_="DealARIADescriptor_DealARIADescriptor__YWUwN")
            print(scraped_data)
        except Exception as e:
            event = EventLogger.log_event(e)
            handler_to_insert.insert_document(event)

        data_to_save = self.extract_data(scraped_data)
        print(data_to_save)

        self.save_data_to_db(data_to_save)


if __name__ == "__main__":
    scraperObj = Scraper()
    scraperObj.get_deals()
