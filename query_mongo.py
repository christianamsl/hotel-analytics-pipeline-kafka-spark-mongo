from pymongo import MongoClient
from datetime import datetime
from bson.son import SON
from collections import defaultdict

#Σύνδεση με MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["clickstream"]
collection = db["raw_events"]

# Ορισμός χρονικού διαστήματος
start_date = datetime(2025, 5, 29, 0, 0)
end_date = datetime(2025, 6, 1, 23, 59)


# Ερώτημα 1: Πόλη με τις περισσότερες κρατήσεις
bookings = collection.aggregate([
    {"$match": {
        "event_type": "complete_booking",
        "event_time": {"$gte": start_date, "$lte": end_date},
        "location": {"$ne": ""}
    }},
    {"$group": {"_id": "$location", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])

print("1. Πόλη με τις περισσότερες κρατήσεις:")
for doc in bookings:
    print(f"   {doc['_id']} με {doc['count']} κρατήσεις")

# -------------------------

# Ερώτημα 2: Πόλη με τις περισσότερες αναζητήσεις
searches = collection.aggregate([
    {"$match": {
        "event_type": "search_hotels",
        "event_time": {"$gte": start_date, "$lte": end_date},
        "location": {"$ne": ""}
    }},
    {"$group": {"_id": "$location", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])

print("\n2. Πόλη με τις περισσότερες αναζητήσεις:")
for doc in searches:
    print(f"   {doc['_id']} με {doc['count']} αναζητήσεις")

# -------------------------

# Ερώτημα 3: Μέση διάρκεια παραμονής ανά πόλη (σε μέρες)
bookings_cursor = collection.find({
    "event_type": "complete_booking",
    "event_time": {"$gte": start_date, "$lte": end_date},
    "location": {"$ne": ""},
    "check_in_date": {"$ne": ""},
    "check_out_date": {"$ne": ""}
})

durations = defaultdict(list)

for doc in bookings_cursor:
    try:
        check_in = datetime.strptime(doc["check_in_date"], "%Y-%m-%d")
        check_out = datetime.strptime(doc["check_out_date"], "%Y-%m-%d")
        stay_length = (check_out - check_in).days
        if stay_length > 0:
            durations[doc["location"]].append(stay_length)
    except:
        continue  

print("\n3. Μέση διάρκεια παραμονής ανά πόλη:")
for city, stays in durations.items():
    avg = sum(stays) / len(stays)
    print(f"   {city}: {avg:.2f} μέρες")