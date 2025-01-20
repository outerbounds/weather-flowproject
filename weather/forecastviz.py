
def vegaspec(past, future):
    cutoff = future[0][0]
    data = [{'time': time, 'temp': temp} for time, temp in past + future]
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": 500,
        "height": 300,
        "data": {"values": data},
        "layer": [
        {
            "transform": [
            {
                "filter": f"toDate(datum.time) <= toDate('{cutoff}')"
            }
            ],
            "mark": "line",
            "encoding": {
            "x": {"field": "time", "type": "temporal", "title": "Time"},
            "y": {"field": "temp", "type": "quantitative", "title": "Temperature"},
            "color": {"value": "blue"}
            }
        },
        {
            "transform": [
            {
                "filter": f"toDate(datum.time) >= toDate('{cutoff}')"
            }
            ],
            "mark": "line",
            "encoding": {
            "x": {"field": "time", "type": "temporal"},
            "y": {"field": "temp", "type": "quantitative"},
            "color": {"value": "orange"}
            }
        }
        ]
    }
