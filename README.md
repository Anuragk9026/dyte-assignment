# dyte-assignment
from flask import Flask, request
from flask_restful import Api, Resource, reqparse
import sqlite3
from datetime import datetime

app = Flask(__name__)
api = Api(app)

# SQLite Database Initialization
conn = sqlite3.connect('logs.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        level TEXT,
        message TEXT,
        resourceId TEXT,
        timestamp TEXT,
        traceId TEXT,
        spanId TEXT,
        `commit` TEXT,  -- Use backticks to wrap the reserved keyword
        parentResourceId TEXT
    )
''')
conn.commit()
conn.close()

# Log Ingestor Resource
class LogIngestor(Resource):
    def post(self):
        data = request.get_json()
        conn = sqlite3.connect('logs.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO logs
            (level, message, resourceId, timestamp, traceId, spanId, `commit`, parentResourceId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('level'),
            data.get('message'),
            data.get('resourceId'),
            data.get('timestamp'),
            data.get('traceId'),
            data.get('spanId'),
            data.get('commit'),
            data.get('metadata', {}).get('parentResourceId', None)
        ))
        conn.commit()
        conn.close()
        return {'message': 'Log ingested successfully'}, 201

# Query Interface Resource
class QueryInterface(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('level', type=str)
        parser.add_argument('message', type=str)
        parser.add_argument('resourceId', type=str)
        parser.add_argument('timestamp', type=str)
        parser.add_argument('traceId', type=str)
        parser.add_argument('spanId', type=str)
        parser.add_argument('commit', type=str)
        parser.add_argument('parentResourceId', type=str)
        args = parser.parse_args()

        conditions = []
        values = []
        for arg, value in args.items():
            if value:
                conditions.append(f"{arg} = ?")
                values.append(value)

        query = f"SELECT * FROM logs WHERE {' AND '.join(conditions)}" if conditions else "SELECT * FROM logs"
        
        conn = sqlite3.connect('logs.db')
        cursor = conn.cursor()
        cursor.execute(query, tuple(values))
        result = cursor.fetchall()
        conn.close()

        logs = []
        for row in result:
            logs.append({
                'id': row[0],
                'level': row[1],
                'message': row[2],
                'resourceId': row[3],
                'timestamp': row[4],
                'traceId': row[5],
                'spanId': row[6],
                'commit': row[7],
                'parentResourceId': row[8],
            })

        return {'logs': logs}

# API Routes
api.add_resource(LogIngestor, '/ingest')
api.add_resource(QueryInterface, '/query')

if __name__ == '__main__':
    app.run(port=3000)
