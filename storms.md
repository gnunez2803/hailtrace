# Storms

Used to collect storm data on a given date and location.

**URL** : `/storm/`

**Method** : `GET`

**Auth required** : NO

**Query constraints**

```json
{
    "location": "[valid location]",
    "date": "[date in FORMAT YYYY-MM-DD]"
}
```

**Data example**

```json
{
    "total_elements": 1,
    "wind_events": [
        {
            "speed": "60",
            "event_time": "2024-09-13 17:13:00",
            "location": "Cactus Flat",
            "county": "Jackson",
            "state": "SD",
            "lat": 43.84,
            "lon": -101.9,
            "comments": "pea sized hail (UNR)"
        }
    ]
}
```

## Success Response

**Code** : `200 OK`

## Error Response

**Condition** : If 'date' format is wrong.

**Code** : `400 BAD REQUEST`

**Content** :

```json
{
    "errors": [
        "date format must be YYYY-MM-DD."
    ]
}
```