from werkzeug.utils import secure_filename
from flask import Flask, render_template, request, jsonify,flash
import pandas as pd
from pymongo import MongoClient
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from apscheduler.schedulers.background import BackgroundScheduler
import pickle
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import os

# model = None

database_url = os.environ['DATABASE_URL']
client = MongoClient(database_url)

secret_key = os.environ['SECRET_KEY']

def run_aggregation():
    db = client['test']
    collection = db['patients']

    pipeline =[
    {
        '$match': {
            'health_record': {
                '$elemMatch': {
                    '$or': [
                        {
                            'collection_name': 'symptoms'
                        }, {
                            'collection_name': 'diseases'
                        }, {
                            'collection_name': 'medications'
                        }
                    ]
                }
            }
        }
    }, {
        '$project': {
            'health_record': {
                '$filter': {
                    'input': '$health_record',
                    'as': 'record',
                    'cond': {
                        '$or': [
                            {
                                '$eq': [
                                    '$$record.collection_name', 'symptoms'
                                ]
                            }, {
                                '$eq': [
                                    '$$record.collection_name', 'diseases'
                                ]
                            }, {
                                '$eq': [
                                    '$$record.collection_name', 'medications'
                                ]
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$match': {
            'health_record': {
                '$elemMatch': {
                    '$or': [
                        {
                            'collection_name': 'symptoms'
                        }, {
                            'collection_name': 'diseases'
                        }, {
                            'collection_name': 'medications'
                        }
                    ]
                }
            }
        }
    }, {
        '$match': {
            'health_record': {
                '$elemMatch': {
                    '$or': [
                        {
                            'collection_name': 'symptoms'
                        }, {
                            'collection_name': 'diseases'
                        }, {
                            'collection_name': 'medications'
                        }
                    ]
                }
            }
        }
    }, {
        '$match': {
            'health_record': {
                '$elemMatch': {
                    '$or': [
                        {
                            'collection_name': 'symptoms'
                        }, {
                            'collection_name': 'diseases'
                        }, {
                            'collection_name': 'medications'
                        }
                    ]
                }
            }
        }
    }, {
        '$project': {
            '_id': 0,
            'health_record': {
                '$map': {
                    'input': '$health_record',
                    'as': 'record',
                    'in': {
                        'collection_name': '$$record.collection_name',
                        'names': {
                            '$map': {
                                'input': '$$record.data',
                                'as': 'item',
                                'in': '$$item.name'
                            }
                        }
                    }
                }
            }
        }
    }
]

    return list(collection.aggregate(pipeline))

def run_aggregation_certain_id(string_id):
        db = client['test']
        collection = db['patients']
        pipeline = [
            {
                '$match': {
                    'profile.nationalId': string_id
                }
            }, {
                '$project': {
                    '_id': 0,
                    'health_record': {
                        '$filter': {
                            'input': '$health_record',
                            'as': 'record',
                            'cond': {
                                '$in': [
                                    '$$record.collection_name', [
                                        'symptoms', 'diseases'
                                    ]
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'health_record': {
                        '$map': {
                            'input': '$health_record',
                            'as': 'record',
                            'in': {
                                'collection_name': '$$record.collection_name',
                                'names': '$$record.data.name'
                            }
                        }
                    }
                }
            }
        ]
        return list(collection.aggregate(pipeline))

    # # Create a BackgroundScheduler object
    # scheduler = BackgroundScheduler()
    #
    # # Schedule your aggregation function to run every hour
    # scheduler.add_job(run_aggregation, 'interval', hours=1)
    #
    # # Start the scheduler
    # scheduler.start()







app = Flask(__name__)


# CORS(app, origins='www.google.com')
# Limiter(get_remote_address, app=app, default_limits=["10/minute"])

@app.before_request
def authenticate():
    if request.get_json().get("key") != secret_key:
        return jsonify({"message":'invalid API key'})


@app.route('/train',  methods=['POST'])
def handle_train():
    try:
        result = run_aggregation()
        data = [record['health_record'] for record in list(result)]
        values_only = []

        for lst in data:
            new_dict = {}
            for item in lst:
                collection_name = item['collection_name']
                names = item['names']
                if names:
                    new_dict[collection_name] = names
            if new_dict:
                values_only.append(new_dict)
        print(values_only)


        symptoms = pd.DataFrame()
        rows = []
        for doc in values_only:
            array = []
            for key in doc:
                if key == 'medications':
                    continue
                else:

                    for i in doc[key]:
                        array.append(i)
            rows.append(array)

        symptoms = pd.DataFrame(rows)
        print(symptoms)
        medications = pd.DataFrame()
        rows = []
        for doc in values_only:
            array = []
            for key in doc:
                if key == 'medications':

                    for i in doc[key]:
                        array.append(i)
            rows.append(array)

        medications = pd.DataFrame(rows)
        print(medications)
################################################################## convert to 0s and 1s for symptoms
        all_symptoms = []

        # iterate over each row of the dataframe
        for index, row in symptoms.iterrows():
            # iterate over each value in the row and append it to the list
            for symptom in row:
                all_symptoms.append(str(symptom).strip())

        all_symptoms = set(all_symptoms)
        if 'None' in all_symptoms:
            all_symptoms.remove('None')
        print(all_symptoms)

        new_symptoms = pd.DataFrame(columns=list(all_symptoms))

        new_row = [0] * new_symptoms.shape[1]
        counter = -1
        for index, row in symptoms.iterrows():

            for symptom in list(row):
                symptom = str(symptom).strip()
                if symptom == 'nan':
                    counter = -1
                    break

                for column in new_symptoms.columns:
                    counter += 1
                    if symptom == column:
                        new_row[counter] = 1
                        counter = -1
                        break

            new_symptoms.loc[len(new_symptoms)] = new_row

            new_row = [0] * new_symptoms.shape[1]
            counter = -1
################################################################## convert to 0s and 1s for medications
        all_medications = []

        # iterate over each row of the dataframe
        for index, row in medications.iterrows():
            # iterate over each value in the row and append it to the list
            for medication in row:
                all_medications.append(str(medication).strip())

        all_medications = set(all_medications)
        if 'None' in all_medications:
            all_medications.remove('None')
        print(all_medications)

        new_medications = pd.DataFrame(columns=list(all_medications))

        new_row = [0] * new_medications.shape[1]
        counter = -1
        for index, row in medications.iterrows():

            for medication in list(row):
                medication = str(medication).strip()
                if medication == 'nan':
                    counter = -1
                    break

                for column in new_medications.columns:
                    counter += 1
                    if medication == column:
                        new_row[counter] = 1
                        counter = -1
                        break

            new_medications.loc[len(new_medications)] = new_row

            new_row = [0] * new_medications.shape[1]
            counter = -1
#######################################
        percentages_dict = {medication: {symptom: {1: None, 0: None} for symptom in all_symptoms} for medication in
                            all_medications}

        data = pd.concat([new_symptoms, new_medications], axis=1)
        for medication in percentages_dict:
            data_filtered_by_current_medication = data[data[medication] == 1]
            for symptom in all_symptoms:
                current_symptom = percentages_dict[medication][symptom]
                current_symptom[1] = (data_filtered_by_current_medication[symptom] == 1).sum() /  data_filtered_by_current_medication.shape[0]
                current_symptom[0] = 1 - current_symptom[1]

        print(percentages_dict)
        pickle.dump(percentages_dict, open('percentages_dict.sav', 'wb'))
        return jsonify({"message": 'trained successfully!!'})
    except:
        return jsonify({"message":'there was a problem'})








@app.route('/predict',  methods=['POST'])
def handle_predict():
    try:

        body = request.get_json()

#########################################
        if not body.get('id'):
            return jsonify({'message':"please enter id"})
        input_id = body.get('id')


        result = run_aggregation_certain_id(input_id)
        data = [record['health_record'] for record in list(result)]
        values_only = []
        for lst in data:
            new_dict = {}
            for item in lst:
                collection_name = item['collection_name']
                names = item['names']
                if names:
                    new_dict[collection_name] = names
            if new_dict:
                values_only.append(new_dict)
        print(values_only)
        input_patient_symptoms_from_id = []

        for item in values_only:
            for key, value in item.items():
                input_patient_symptoms_from_id.extend(value)

        print('from id input',input_patient_symptoms_from_id)
        if len(input_patient_symptoms_from_id) == 0:
            input_patient_symptoms_from_id.append("")
        ###############################################
        percentages_dict = pickle.load(open('percentages_dict.sav', 'rb'))
        result_dict = {}
        for medication in percentages_dict.keys():
            percentage_of_medication_presence_absence = 0
            percentage_of_medication_presence_only = 0
            for symptom in percentages_dict[medication].keys():
                if symptom in input_patient_symptoms_from_id:
                    percentage_of_medication_presence_absence += percentages_dict[medication][symptom][1]
                    percentage_of_medication_presence_only += percentages_dict[medication][symptom][1]
                else:
                    percentage_of_medication_presence_absence += percentages_dict[medication][symptom][0]

            Prescribed_for_such_symptoms_and_such_absence_of_symptoms_by = int(percentage_of_medication_presence_absence / len(percentages_dict[medication]) * 100)
            Prescribed_for_such_case_by =int(percentage_of_medication_presence_only / len(input_patient_symptoms_from_id) * 100)
            result_dict[medication] = [Prescribed_for_such_symptoms_and_such_absence_of_symptoms_by,Prescribed_for_such_case_by]
        return jsonify({'message':result_dict})
    except:
        return jsonify({'message':'there was a problem'})






if __name__ == '__main__':
    app.run()