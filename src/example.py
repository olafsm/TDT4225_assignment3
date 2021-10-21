from pprint import pprint 
from DbConnector import DbConnector


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_documents(self, collection_name):
        Users = [
            {
            "_id": 1,
            "has_labels": True,
            "Activityies": [
                {"ActivityID":  '001'},
                {"ActivityID":  '002'},
                {"ActivityID":  '003'},
            ]
            }
        ]
        Activities = [
            {
                "_id": 1,
                "Transportation:mode": "Walk",
                "start_date_time": "2020-12-12 00:00:00",
                "end_date_time": "2020-1212 00:00:05",
                "Trackpoints": [
                    {"_id": 1, "lat":25.222, "lon": 26.111, "alt":777, "date_time": "2020-12-12 00:00:00"}
                    {"_id": 1, "lat": 25.222, "lon": 26.111, "alt": 777, "date_time": "2020-12-12 00:00:00"}
                    {"_id": 1, "lat": 25.222, "lon": 26.111, "alt": 777, "date_time": "2020-12-12 00:00:00"}
                ]
            }
        ]
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'OI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_coll(collection_name="Person")
        program.show_coll()
        program.insert_documents(collection_name="Person")
        program.fetch_documents(collection_name="Person")
        #program.drop_coll(collection_name="Person")
        # program.drop_coll(collection_name='person')
        # program.drop_coll(collection_name='users')
        # Check that the table is dropped
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
