import csv
import json
from metaflow.cards import VegaChart

SPEC = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "width": 400,
    "height": 500,
    "layer": [
        {   
            "data": {"url": "https://data.sfgov.org/resource/wamw-vt4s.geojson"},
            "format": {"type": "json", "property": "features"},
            "projection": {"type": "mercator"},
            "mark": {"type": "geoshape", "fill": "#eee", "stroke": "white"},
        },
        {
            "background": None,
            "data": {"values": None},
            "mark": "circle",
            "encoding": {
                "longitude": {
                    "field": "lon",
                    "type": "quantitative"
                },
                "latitude": {
                    "field": "lat",
                    "type": "quantitative"
                },
                "size": {"value": 200},
                "color": {
                    "field": "temperature",
                    "type": "quantitative",
                    "scale": {
                        "domain": [32, 50, 68, 86, 104],
                        "range": ["blue", "lightblue", "yellow", "orange", "red"]
                    },
                    "title": "Temperature (°F)"
                }
            }
        }
    ]
}

def read_latlon():
    with open('data/zipcodes.csv') as csvfile:
        return {zipcode: (lat, lon)
                for zipcode, lat, lon in csv.reader(csvfile)}
    
def dataset(rows):
    latlon = read_latlon()
    for zipcode, temp in rows:
        if zipcode in latlon:
            lat, lon = latlon[zipcode]
            yield {'lat': lat, 'lon': lon, 'temperature': float(temp)}

def make_vegachart(rows):
    spec = SPEC.copy()
    spec['layer'][1]['data']['values'] = list(dataset(rows))
    return VegaChart(spec)