from flask import Flask, jsonify
from flask_cors import CORS
from app.db.psql.database import engine
from app.db.psql.init_data import standardize_data, seed_database
from app.db.psql.models import Base
from app.service.elastic_service.csv_processor import process_secondary_csv, process_main_csv
from app.service.elastic_service.elastic_service import save_events_for_terror
from app.service.elastic_service.init_elastic import create_index
from app.service.elastic_service.scheduler import setup_scheduler
import os
from app.utils.csv_reader import read_and_process_files

app = Flask(__name__)
CORS(app)
scheduler = None

def import_historic_data(main_csv_path: str, secondary_csv_path: str) -> None:
    # Process main CSV
    if os.path.exists(main_csv_path):
        try:
            print("Processing main CSV file...")
            main_events = process_main_csv(main_csv_path)
            save_events_for_terror(main_events)
            print(f"Saved {len(main_events)} events from main CSV")
        except Exception as e:
            print(f"Error processing main CSV: {e}")
    else:
        print(f"Warning: Main CSV file not found at {main_csv_path}")

    # Process secondary CSV
    if os.path.exists(secondary_csv_path):
        try:
            print("Processing secondary CSV file...")
            secondary_events = process_secondary_csv(secondary_csv_path)
            save_events_for_terror(secondary_events)
            print(f"Saved {len(secondary_events)} events from secondary CSV")
        except Exception as e:
            print(f"Error processing secondary CSV: {e}")
    else:
        print(f"Warning: Secondary CSV file not found at {secondary_csv_path}")

def init_psql_db():
    print("Initializing database...")
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    print("Reading and processing files...")
    df_gtd, df_rand = read_and_process_files()
    print("Standardizing data...")
    df_merged = standardize_data(df_gtd, df_rand)
    print("Seeding database...")
    seed_database(df_merged)
    print("Database initialization complete!")

def init_elastic_db():
    global scheduler
    print("Initializing Elasticsearch...")
    create_index()
    scheduler = setup_scheduler()
    import_historic_data(
        main_csv_path='./data/globalterrorismdb_1000.csv',
        secondary_csv_path='./data/RAND_Database_of_Worldwide_Terrorism_Incidents_5000.csv'
    )
    print("Elasticsearch initialization complete!")

@app.route('/init_data')
def init_data():
    try:
        init_psql_db()
        if not hasattr(app, 'initialized_elastic'):
            init_elastic_db()
            app.initialized_elastic = True
            return jsonify({
                "status": "success",
                "message": "Databases initialized successfully"
            })
        return jsonify({
            "status": "info",
            "message": "Databases already initialized"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error initializing databases: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5002)