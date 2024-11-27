from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import sqlite3
from starlette.responses import JSONResponse
import requests
from joblib import Parallel, delayed
import random

# Define the Azure URL where your FastAPI application is hosted
AZURE_URL = "http://20.217.132.103/process_event"

# Create the FastAPI application instance
app = FastAPI()

# Define the database filename
DB_FILENAME = "events.db"

# Create a SQLite connection and cursor
conn = sqlite3.connect(DB_FILENAME)
cursor = conn.cursor()


# Create the events table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        eventtimestamputc TEXT,
        userid TEXT,
        eventname TEXT
    )
''')
conn.commit()


# Define the EventRequest model
class EventRequest(BaseModel):
    userid: str
    eventname: str


# Define the EventDB model
class EventDB(EventRequest):
    eventtimestamputc: datetime


# Define the shutdown event handler
@app.on_event("shutdown")
async def shutdown_event():
    # Close the SQLite connection when the FastAPI app shuts down
    conn.close()

@app.get("/")
async def get_message():
    return {"message": "Welcome to our app"}

# Define the process_event endpoint
@app.post("/process_event")
async def process_event(event: EventRequest):
    # Get the current UTC time
    event_timestamp_utc = datetime.now(timezone.utc)

    # Insert event into the SQLite database
    cursor.execute('''
            INSERT INTO events (eventtimestamputc, userid, eventname)
            VALUES (?, ?, ?)
        ''', (event_timestamp_utc.isoformat(), event.userid, event.eventname))
    conn.commit()
    return JSONResponse(content={"message": "Event processed successfully"}, status_code=201)

@app.get("/get_reports")
async def get_reports(request: Request):
    # Get parameters from the request body
    lastseconds = int(request.query_params.get("lastseconds", 0))
    userid = request.query_params.get("userid", "")

    # Calculate the datetime threshold based on lastseconds
    threshold_datetime = datetime.now(timezone.utc) - timedelta(seconds=lastseconds)

    # Fetch events from the database that match the criteria
    cursor.execute('''
        SELECT * FROM events
        WHERE userid = ? AND eventtimestamputc >= ?
    ''', (userid, threshold_datetime.isoformat()))
    events = cursor.fetchall()

    if not events:
        raise HTTPException(status_code=404, detail=f"No events found for userid {userid} in the last {lastseconds} seconds")

    # Convert the result to a list of dictionaries for JSON response
    result = [{"id": event[0], "eventtimestamputc": event[1], "userid": event[2], "eventname": event[3]} for event in events]
    return JSONResponse(content=result, status_code=200)


# Function to make HTTP requests to the process_event endpoint

def make_request():
    # Generate random data for the event
    userid = str(random.randint(1, 1000))
    eventname = f"Event_{random.randint(1, 1000)}"

    # Prepare the payload for the POST request
    payload = {"userid": userid, "eventname": eventname}

    # Make the HTTP POST request to the Azure URL
    response = requests.post(AZURE_URL, json=payload)

    # Check if the request was successful
    if response.status_code == 201:
        print(f"Event processed successfully: {payload}")
    else:
        print(f"Failed to process event: {payload}")


# Number of HTTP requests to make
NUM_REQUESTS = 1000

# Use joblib to execute HTTP requests in parallel
Parallel(n_jobs=-1, backend="threading")(delayed(make_request)() for _ in range(NUM_REQUESTS))










