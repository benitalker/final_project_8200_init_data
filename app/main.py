from flask import Flask, jsonify
from flask_cors import CORS
from app.db.psql.database import engine
from app.db.psql.init_data import standardize_data, seed_database
from app.db.psql.models import Base
from app.service.sql_to_elastic_service import transfer_data_to_elastic
from app.service.init_elastic import create_index
from app.utils.csv_reader import read_and_process_files

app = Flask(__name__)
CORS(app)
scheduler = None

def init_psql_db():
    print("Initializing PostgreSQL database...")
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    print("Reading and processing files...")
    df_gtd, df_rand = read_and_process_files()
    print("Standardizing data...")
    df_merged = standardize_data(df_gtd, df_rand)
    print("Seeding database...")
    seed_database(df_merged)
    print("PostgreSQL database initialization complete!")

def init_elastic_db():
    print("Initializing Elasticsearch...")
    create_index()
    print("Transferring data from PostgreSQL to Elasticsearch...")
    transfer_data_to_elastic()
    print("Elasticsearch initialization complete!")

@app.route('/init_data')
def init_data():
    try:
        # init_psql_db()
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
