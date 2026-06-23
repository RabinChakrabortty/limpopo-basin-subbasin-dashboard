import asyncio
import csv
import io
import json
import math
import time
from collections import defaultdict
from datetime import date, timedelta

import httpx
import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI(
    title="Limpopo Basin and Sub-basin Explorer",
    version="1.2.0",
    description="Basin-scale and sub-basin-scale hydroclimate, flood, drought, LULC, population, prediction and download dashboard."
)

CACHE = {}
CACHE_TTL_SECONDS = 6 * 60 * 60
LATEST_DATA = None

# Strategic monitoring nodes. Static population and LULC values are reference layers.
LOCATIONS = {
    "upper_limpopo": {
        "name": "Upper Limpopo Headwaters", "lat": -25.20, "lon": 26.90,
        "country": "South Africa / Botswana", "subbasin": "upper_limpopo", "population": 45000,
        "lulc": {"urban": 6, "cropland": 28, "grassland": 42, "forest": 8, "water": 2, "bare": 14}
    },
    "gaborone": {
        "name": "Gaborone Catchment", "lat": -24.65, "lon": 25.91,
        "country": "Botswana", "subbasin": "upper_limpopo", "population": 230000,
        "lulc": {"urban": 24, "cropland": 12, "grassland": 38, "forest": 4, "water": 3, "bare": 19}
    },
    "shashe": {
        "name": "Francistown / Shashe Sub-Basin", "lat": -21.17, "lon": 27.51,
        "country": "Botswana / Zimbabwe", "subbasin": "shashe", "population": 95000,
        "lulc": {"urban": 12, "cropland": 18, "grassland": 44, "forest": 5, "water": 2, "bare": 19}
    },
    "polokwane": {
        "name": "Polokwane Regional Platform", "lat": -23.90, "lon": 29.45,
        "country": "South Africa", "subbasin": "mogalakwena", "population": 510000,
        "lulc": {"urban": 22, "cropland": 30, "grassland": 28, "forest": 5, "water": 1, "bare": 14}
    },
    "mokopane": {
        "name": "Mokopane / Mogalakwena System", "lat": -24.19, "lon": 29.01,
        "country": "South Africa", "subbasin": "mogalakwena", "population": 185000,
        "lulc": {"urban": 10, "cropland": 36, "grassland": 32, "forest": 5, "water": 1, "bare": 16}
    },
    "beitbridge": {
        "name": "Beitbridge Gateway", "lat": -22.22, "lon": 30.00,
        "country": "Zimbabwe / South Africa", "subbasin": "middle_limpopo", "population": 120000,
        "lulc": {"urban": 8, "cropland": 20, "grassland": 40, "forest": 4, "water": 1, "bare": 27}
    },
    "olifants": {
        "name": "Olifants River Node", "lat": -24.00, "lon": 31.50,
        "country": "South Africa / Mozambique", "subbasin": "olifants", "population": 620000,
        "lulc": {"urban": 10, "cropland": 34, "grassland": 24, "forest": 12, "water": 4, "bare": 16}
    },
    "massingir": {
        "name": "Massingir Dam Zone", "lat": -23.88, "lon": 32.16,
        "country": "Mozambique", "subbasin": "lower_limpopo", "population": 35000,
        "lulc": {"urban": 4, "cropland": 18, "grassland": 31, "forest": 18, "water": 12, "bare": 17}
    },
    "chokwe": {
        "name": "Chokwe Irrigation Zone", "lat": -24.53, "lon": 32.98,
        "country": "Mozambique", "subbasin": "lower_limpopo", "population": 180000,
        "lulc": {"urban": 7, "cropland": 58, "grassland": 16, "forest": 5, "water": 5, "bare": 9}
    },
    "xai_xai": {
        "name": "Xai-Xai Estuary", "lat": -25.05, "lon": 33.65,
        "country": "Mozambique", "subbasin": "lower_limpopo", "population": 140000,
        "lulc": {"urban": 14, "cropland": 32, "grassland": 16, "forest": 10, "water": 14, "bare": 14}
    }
}

# Illustrative planning polygons. Replace with official HydroBASINS boundaries for formal analysis.
SUBBASINS = {
    "upper_limpopo": {
        "name": "Upper Limpopo Planning Zone", "countries": "South Africa / Botswana",
        "polygon": [[24.8, -26.2], [28.1, -26.2], [28.1, -23.6], [24.8, -23.6], [24.8, -26.2]]
    },
    "shashe": {
        "name": "Shashe Tributary Planning Zone", "countries": "Botswana / Zimbabwe",
        "polygon": [[25.8, -23.7], [29.2, -23.7], [29.2, -20.1], [25.8, -20.1], [25.8, -23.7]]
    },
    "mogalakwena": {
        "name": "Mogalakwena Planning Zone", "countries": "South Africa",
        "polygon": [[27.8, -25.4], [30.3, -25.4], [30.3, -22.7], [27.8, -22.7], [27.8, -25.4]]
    },
    "middle_limpopo": {
        "name": "Middle Limpopo Planning Zone", "countries": "South Africa / Zimbabwe",
        "polygon": [[28.8, -23.9], [31.7, -23.9], [31.7, -21.0], [28.8, -21.0], [28.8, -23.9]]
    },
    "olifants": {
        "name": "Olifants Planning Zone", "countries": "South Africa / Mozambique",
        "polygon": [[30.0, -25.4], [32.4, -25.4], [32.4, -22.4], [30.0, -22.4], [30.0, -25.4]]
    },
    "lower_limpopo": {
        "name": "Lower Limpopo and Delta Planning Zone", "countries": "Mozambique",
        "polygon": [[31.0, -26.0], [34.5, -26.0], [34.5, -23.0], [31.0, -23.0], [31.0, -26.0]]
    }
}


def clean(values):
    return [v for v in (values or []) if isinstance(v, (int, float)) and not math.isnan(v)]


def total(values):
    values = clean(values)
    return round(sum(values), 2) if values else 0.0


def mean(values):
    values = clean(values)
    return round(sum(values) / len(values), 2) if values else 0.0


def maximum(values):
    values = clean(values)
    return round(max(values), 2) if values else 0.0


def minimum(values):
    values = clean(values)
    return round(min(values), 2) if values else 0.0


def clamp(value, low, high):
    return max(low, min(high, value))


def risk_class(score):
    if score >= 80:
        return "Very high"
    if score >= 60:
        return "High"
    if score >= 35:
        return "Moderate"
    return "Low"


def cache_key(url, params):
    return str((url, tuple(sorted(params.items()))))


async def fetch_json(client, url, params):
    key = cache_key(url, params)
    now = time.time()
    if key in CACHE:
        saved_at, saved_data = CACHE[key]
        if now - saved_at < CACHE_TTL_SECONDS:
            return saved_data
    try:
        response = await client.get(url, params=params, timeout=60.0)
        response.raise_for_status()
        data = response.json()
        CACHE[key] = (now, data)
        return data
    except Exception as error:
        return {"error": str(error), "daily": {}}


def soil_proxy(rainfall, et0):
    balance = total(rainfall) - total(et0)
    if balance >= 60:
        return 0.45
    if balance >= 30:
        return 0.39
    if balance >= 10:
        return 0.33
    if balance >= 0:
        return 0.28
    if balance >= -25:
        return 0.22
    if balance >= -60:
        return 0.16
    return 0.10


def drought_score(soil, water_balance, recent_rain):
    score = 0
    if soil < 0.14:
        score += 45
    elif soil < 0.22:
        score += 30
    elif soil < 0.30:
        score += 15
    if water_balance < -80:
        score += 30
    elif water_balance < -35:
        score += 20
    elif water_balance < 0:
        score += 10
    if recent_rain < 45:
        score += 25
    elif recent_rain < 90:
        score += 15
    return round(clamp(score, 0, 100), 2)


def flood_score(peak_discharge):
    if peak_discharge >= 120:
        return 95
    if peak_discharge >= 50:
        return 75
    if peak_discharge >= 20:
        return 50
    return 20


def climate_score(rainfall, et0, max_temp, wind, radiation):
    balance = rainfall - et0
    score = 0
    if balance < -80:
        score += 35
    elif balance < -40:
        score += 25
    elif balance < 0:
        score += 12
    if max_temp >= 42:
        score += 25
    elif max_temp >= 38:
        score += 17
    elif max_temp >= 34:
        score += 10
    if wind >= 45:
        score += 15
    elif wind >= 30:
        score += 8
    if radiation >= 180:
        score += 15
    elif radiation >= 120:
        score += 8
    return round(clamp(score, 0, 100), 2)


def lulc_score(lulc):
    score = (
        lulc["urban"] * 0.45 +
        lulc["cropland"] * 0.35 +
        lulc["bare"] * 0.28 -
        lulc["forest"] * 0.18 -
        lulc["water"] * 0.08
    )
    return round(clamp(score, 0, 100), 2)


def exposure(population, flood, drought, climate, lulc):
    percentage = flood * 0.34 + drought * 0.26 + climate * 0.20 + lulc * 0.20
    percentage = round(clamp(percentage, 0, 100), 2)
    return int(population * percentage / 100), percentage


def climatology_prediction(history_daily, prediction_days):
    history_dates = history_daily.get("time", []) or []
    rain = history_daily.get("precipitation_sum", []) or []
    temperature = history_daily.get("temperature_2m_mean", []) or []
    et0 = history_daily.get("et0_fao_evapotranspiration", []) or []
    grouped = defaultdict(lambda: {"rain": [], "temp": [], "et0": []})
    for index, item_date in enumerate(history_dates):
        try:
            month_day = item_date[5:10]
            if index < len(rain) and isinstance(rain[index], (int, float)):
                grouped[month_day]["rain"].append(rain[index])
            if index < len(temperature) and isinstance(temperature[index], (int, float)):
                grouped[month_day]["temp"].append(temperature[index])
            if index < len(et0) and isinstance(et0[index], (int, float)):
                grouped[month_day]["et0"].append(et0[index])
        except Exception:
            continue
    fallback_rain = mean(rain)
    fallback_temp = mean(temperature)
    fallback_et0 = mean(et0)
    dates, predicted_rain, predicted_temp, predicted_et0, predicted_balance = [], [], [], [], []
    start = date.today() + timedelta(days=1)
    for day_number in range(prediction_days):
        future_day = start + timedelta(days=day_number)
        key = future_day.isoformat()[5:10]
        rain_value = mean(grouped[key]["rain"]) or fallback_rain
        temp_value = mean(grouped[key]["temp"]) or fallback_temp
        et0_value = mean(grouped[key]["et0"]) or fallback_et0
        dates.append(future_day.isoformat())
        predicted_rain.append(round(rain_value, 2))
        predicted_temp.append(round(temp_value, 2))
        predicted_et0.append(round(et0_value, 2))
        predicted_balance.append(round(rain_value - et0_value, 2))
    return {
        "dates": dates,
        "rainfall_mm": predicted_rain,
        "temperature_c": predicted_temp,
        "et0_mm": predicted_et0,
        "water_balance_mm": predicted_balance,
    }


async def fetch_node(client, node_id, meta, forecast_days, flood_days):
    today = date.today()
    history_start = (today - timedelta(days=90)).isoformat()
    history_end = (today - timedelta(days=1)).isoformat()
    forecast_params = {
        "latitude": meta["lat"], "longitude": meta["lon"],
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min,et0_fao_evapotranspiration,wind_speed_10m_max,shortwave_radiation_sum",
        "forecast_days": forecast_days, "timezone": "auto"
    }
    historical_params = {
        "latitude": meta["lat"], "longitude": meta["lon"],
        "start_date": history_start, "end_date": history_end,
        "daily": "precipitation_sum,temperature_2m_mean,et0_fao_evapotranspiration", "timezone": "auto"
    }
    flood_params = {
        "latitude": meta["lat"], "longitude": meta["lon"],
        "daily": "river_discharge", "forecast_days": flood_days, "timezone": "auto"
    }
    forecast, history, flood = await asyncio.gather(
        fetch_json(client, "https://api.open-meteo.com/v1/forecast", forecast_params),
        fetch_json(client, "https://archive-api.open-meteo.com/v1/archive", historical_params),
        fetch_json(client, "https://flood-api.open-meteo.com/v1/flood", flood_params),
    )
    fd, hd, fl = forecast.get("daily", {}) or {}, history.get("daily", {}) or {}, flood.get("daily", {}) or {}
    rainfall = fd.get("precipitation_sum", []) or []
    et0 = fd.get("et0_fao_evapotranspiration", []) or []
    temp_max = fd.get("temperature_2m_max", []) or []
    temp_min = fd.get("temperature_2m_min", []) or []
    wind = fd.get("wind_speed_10m_max", []) or []
    radiation = fd.get("shortwave_radiation_sum", []) or []
    discharge = fl.get("river_discharge", []) or []
    recent_rain = hd.get("precipitation_sum", []) or []
    rainfall_total = total(rainfall)
    et0_total = total(et0)
    balance = round(rainfall_total - et0_total, 2)
    soil = soil_proxy(rainfall, et0)
    drought = drought_score(soil, balance, total(recent_rain))
    flood_risk = flood_score(maximum(discharge))
    climate = climate_score(rainfall_total, et0_total, maximum(temp_max), maximum(wind), total(radiation))
    lulc = lulc_score(meta["lulc"])
    people_exposed, exposure_percent = exposure(meta["population"], flood_risk, drought, climate, lulc)
    composite = round(climate * 0.25 + flood_risk * 0.25 + drought * 0.20 + lulc * 0.15 + exposure_percent * 0.15, 2)
    return {
        "id": node_id,
        "name": meta["name"],
        "country": meta["country"],
        "subbasin": meta["subbasin"],
        "coordinates": {"lat": meta["lat"], "lon": meta["lon"]},
        "population": {"total": meta["population"], "exposed": people_exposed, "exposure_percent": exposure_percent},
        "lulc": {"profile": meta["lulc"], "pressure_score": lulc},
        "climate": {
            "rainfall_mm": rainfall_total, "et0_mm": et0_total, "water_balance_mm": balance,
            "mean_temperature_c": round((mean(temp_max) + mean(temp_min)) / 2, 2),
            "max_temperature_c": maximum(temp_max), "wind_speed_kmh": maximum(wind),
            "solar_radiation_mj_m2": total(radiation), "recent_90d_rainfall_mm": total(recent_rain),
            "soil_saturation_proxy": soil, "risk_score": climate,
        },
        "flood": {"peak_discharge_m3s": maximum(discharge), "mean_discharge_m3s": mean(discharge), "risk_score": flood_risk, "risk_class": risk_class(flood_risk)},
        "drought": {"risk_score": drought, "risk_class": risk_class(drought)},
        "risk": {"composite_score": composite, "composite_class": risk_class(composite)},
        "time_series": {
            "forecast_dates": fd.get("time", []), "rainfall_mm": rainfall, "et0_mm": et0,
            "temperature_max_c": temp_max, "temperature_min_c": temp_min,
            "wind_speed_kmh": wind, "solar_radiation_mj_m2": radiation,
            "flood_dates": fl.get("time", []), "discharge_m3s": discharge,
        },
        "errors": {"forecast": forecast.get("error"), "history": history.get("error"), "flood": flood.get("error")},
    }


def average_lulc(nodes):
    categories = ["urban", "cropland", "grassland", "forest", "water", "bare"]
    return {category: mean([node["lulc"]["profile"][category] for node in nodes]) for category in categories}


def aggregate_subbasin(subbasin_id, nodes):
    info = SUBBASINS[subbasin_id]
    return {
        "id": subbasin_id, "name": info["name"], "countries": info["countries"], "node_ids": [node["id"] for node in nodes],
        "population": {"total": int(sum(node["population"]["total"] for node in nodes)), "exposed": int(sum(node["population"]["exposed"] for node in nodes))},
        "climate": {
            "rainfall_mm": mean([node["climate"]["rainfall_mm"] for node in nodes]),
            "et0_mm": mean([node["climate"]["et0_mm"] for node in nodes]),
            "water_balance_mm": mean([node["climate"]["water_balance_mm"] for node in nodes]),
            "mean_temperature_c": mean([node["climate"]["mean_temperature_c"] for node in nodes]),
            "soil_saturation_proxy": mean([node["climate"]["soil_saturation_proxy"] for node in nodes]),
            "risk_score": mean([node["climate"]["risk_score"] for node in nodes]),
        },
        "flood": {"peak_discharge_m3s": maximum([node["flood"]["peak_discharge_m3s"] for node in nodes]), "risk_score": mean([node["flood"]["risk_score"] for node in nodes])},
        "drought": {"risk_score": mean([node["drought"]["risk_score"] for node in nodes])},
        "lulc": {"profile": average_lulc(nodes), "pressure_score": mean([node["lulc"]["pressure_score"] for node in nodes])},
        "risk": {"composite_score": mean([node["risk"]["composite_score"] for node in nodes])},
    }


def subbasin_geojson(subbasins):
    features = []
    for subbasin in subbasins:
        features.append({
            "type": "Feature",
            "properties": {
                "id": subbasin["id"], "name": subbasin["name"],
                "risk": subbasin["risk"]["composite_score"],
                "rainfall": subbasin["climate"]["rainfall_mm"],
                "balance": subbasin["climate"]["water_balance_mm"],
                "flood": subbasin["flood"]["risk_score"],
                "drought": subbasin["drought"]["risk_score"],
                "population_exposed": subbasin["population"]["exposed"],
            },
            "geometry": {"type": "Polygon", "coordinates": [SUBBASINS[subbasin["id"]]["polygon"]]},
        })
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/dashboard")
async def dashboard_data(forecast_days: int = Query(16, ge=1, le=16), flood_days: int = Query(30, ge=1, le=30)):
    global LATEST_DATA
    async with httpx.AsyncClient() as client:
        tasks = [fetch_node(client, node_id, meta, forecast_days, flood_days) for node_id, meta in LOCATIONS.items()]
        nodes = await asyncio.gather(*tasks)
    grouped = defaultdict(list)
    for node in nodes:
        grouped[node["subbasin"]].append(node)
    subbasins = [aggregate_subbasin(key, grouped[key]) for key in SUBBASINS if key in grouped]
    response = {
        "metadata": {
            "basin": "Limpopo River Basin", "generated_on": date.today().isoformat(),
            "forecast_days": forecast_days, "flood_days": flood_days,
            "boundary_note": "Sub-basin polygons are illustrative planning zones. Replace with official HydroBASINS boundaries for formal analysis.",
        },
        "basin_summary": {
            "total_population": int(sum(node["population"]["total"] for node in nodes)),
            "population_exposed": int(sum(node["population"]["exposed"] for node in nodes)),
            "mean_composite_risk": mean([node["risk"]["composite_score"] for node in nodes]),
            "mean_water_balance_mm": mean([node["climate"]["water_balance_mm"] for node in nodes]),
            "mean_peak_discharge_m3s": mean([node["flood"]["peak_discharge_m3s"] for node in nodes]),
        },
        "nodes": nodes, "subbasins": subbasins, "subbasins_geojson": subbasin_geojson(subbasins),
    }
    LATEST_DATA = response
    return response


@app.get("/api/prediction/{scope_id}")
async def prediction(scope_id: str, prediction_days: int = Query(365, ge=30, le=365), history_years: int = Query(3, ge=3, le=10)):
    if scope_id == "basin":
        selected_nodes = list(LOCATIONS.values())
        title = "Entire Limpopo Basin"
    elif scope_id in SUBBASINS:
        selected_nodes = [meta for meta in LOCATIONS.values() if meta["subbasin"] == scope_id]
        title = SUBBASINS[scope_id]["name"]
    else:
        return {"error": "Invalid prediction scope"}
    end_day = date.today() - timedelta(days=1)
    start_day = end_day - timedelta(days=365 * history_years)
    async with httpx.AsyncClient() as client:
        tasks = []
        for item in selected_nodes:
            params = {
                "latitude": item["lat"], "longitude": item["lon"],
                "start_date": start_day.isoformat(), "end_date": end_day.isoformat(),
                "daily": "precipitation_sum,temperature_2m_mean,et0_fao_evapotranspiration", "timezone": "auto",
            }
            tasks.append(fetch_json(client, "https://archive-api.open-meteo.com/v1/archive", params))
        historical_data = await asyncio.gather(*tasks)
    predictions = [climatology_prediction(item.get("daily", {}) or {}, prediction_days) for item in historical_data]
    if not predictions:
        return {"error": "No historical prediction data available"}
    dates = predictions[0]["dates"]
    rainfall, temperature, et0, balance = [], [], [], []
    for index in range(len(dates)):
        rain_value = mean([item["rainfall_mm"][index] for item in predictions if index < len(item["rainfall_mm"])])
        temperature_value = mean([item["temperature_c"][index] for item in predictions if index < len(item["temperature_c"])])
        et0_value = mean([item["et0_mm"][index] for item in predictions if index < len(item["et0_mm"])])
        rainfall.append(rain_value)
        temperature.append(temperature_value)
        et0.append(et0_value)
        balance.append(round(rain_value - et0_value, 2))
    return {"scope": title, "prediction_days": prediction_days, "history_years": history_years, "method": "Historical daily climatology prediction using calendar-day averages.", "dates": dates, "rainfall_mm": rainfall, "temperature_c": temperature, "et0_mm": et0, "water_balance_mm": balance}


@app.get("/api/locations")
def locations():
    return LOCATIONS


@app.get("/health")
def health():
    return {"status": "ok"}


def csv_download(filename, rows):
    if not rows:
        rows = [{"message": "No data available"}]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.get("/download/subbasins.csv")
async def download_subbasins():
    data = LATEST_DATA or await dashboard_data()
    rows = []
    for item in data["subbasins"]:
        rows.append({
            "subbasin": item["name"], "countries": item["countries"],
            "population": item["population"]["total"], "population_exposed": item["population"]["exposed"],
            "rainfall_mm": item["climate"]["rainfall_mm"], "et0_mm": item["climate"]["et0_mm"],
            "water_balance_mm": item["climate"]["water_balance_mm"],
            "peak_discharge_m3s": item["flood"]["peak_discharge_m3s"],
            "flood_risk_score": item["flood"]["risk_score"], "drought_risk_score": item["drought"]["risk_score"],
            "lulc_pressure_score": item["lulc"]["pressure_score"], "composite_risk_score": item["risk"]["composite_score"],
        })
    return csv_download("limpopo_subbasin_summary.csv", rows)


@app.get("/download/nodes.csv")
async def download_nodes():
    data = LATEST_DATA or await dashboard_data()
    rows = []
    for item in data["nodes"]:
        rows.append({
            "node": item["name"], "country": item["country"], "subbasin": item["subbasin"],
            "latitude": item["coordinates"]["lat"], "longitude": item["coordinates"]["lon"],
            "population": item["population"]["total"], "population_exposed": item["population"]["exposed"],
            "rainfall_mm": item["climate"]["rainfall_mm"], "et0_mm": item["climate"]["et0_mm"],
            "water_balance_mm": item["climate"]["water_balance_mm"],
            "peak_discharge_m3s": item["flood"]["peak_discharge_m3s"],
            "drought_risk_score": item["drought"]["risk_score"], "flood_risk_score": item["flood"]["risk_score"],
            "composite_risk_score": item["risk"]["composite_score"],
        })
    return csv_download("limpopo_monitoring_nodes.csv", rows)


@app.get("/download/subbasins.geojson")
async def download_geojson():
    data = LATEST_DATA or await dashboard_data()
    text = json.dumps(data["subbasins_geojson"], indent=2)
    return StreamingResponse(iter([text]), media_type="application/geo+json", headers={"Content-Disposition": 'attachment; filename="limpopo_subbasins.geojson"'})


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return '''
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Limpopo Basin and Sub-basin Explorer</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
:root { --dark:#0f172a; --blue:#2563eb; --green:#10b981; --amber:#f59e0b; --orange:#f97316; --red:#e11d48; }
body { margin:0; font-family:Arial,sans-serif; background:#f1f5f9; color:#0f172a; }
header { background:linear-gradient(135deg,#020617,#1e3a8a,#0f766e); color:white; padding:28px 34px; }
header h1 { margin:0; font-size:29px; } header p { margin:7px 0 0; color:#cbd5e1; }
.layout { display:flex; min-height:calc(100vh - 100px); } .sidebar { width:330px; background:white; padding:20px; box-sizing:border-box; border-right:1px solid #e2e8f0; } .main { flex:1; padding:22px; box-sizing:border-box; }
.card { background:white; border-radius:13px; padding:18px; margin-bottom:18px; box-shadow:0 4px 14px rgba(15,23,42,.06); }
label { display:block; font-size:12px; font-weight:bold; margin-bottom:6px; color:#475569; text-transform:uppercase; }
select,button { width:100%; box-sizing:border-box; padding:10px; border-radius:8px; border:1px solid #cbd5e1; margin-bottom:14px; }
button { background:var(--dark); color:white; border:none; font-weight:bold; cursor:pointer; }
.metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:16px; } .metric { font-size:28px; font-weight:900; margin-top:7px; } .small { color:#64748b; font-size:12px; line-height:1.5; }
#map { height:560px; border-radius:12px; } .tabs { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:18px; } .tab { width:auto; background:#e2e8f0; color:#0f172a; margin:0; } .tab.active { background:#0f172a; color:white; } .panel { display:none; } .panel.active { display:block; }
table { width:100%; border-collapse:collapse; font-size:13px; } th,td { padding:10px; border-bottom:1px solid #e2e8f0; text-align:left; } th { background:#f8fafc; color:#475569; font-size:11px; text-transform:uppercase; }
.status { color:#2563eb; font-size:13px; font-weight:bold; } .download { display:block; padding:10px; margin-bottom:8px; background:#eff6ff; color:#1d4ed8; text-decoration:none; border-radius:7px; font-size:13px; font-weight:bold; }
@media(max-width:900px) { .layout { flex-direction:column; } .sidebar { width:100%; } }
</style>
</head>
<body>
<header><h1>Limpopo Basin and Sub-basin Explorer</h1><p>Basin-wide climate, flood, drought, LULC, population exposure, prediction and data download dashboard</p></header>
<div class="layout">
<aside class="sidebar">
<label>Climate forecast period</label><select id="forecastDays"><option value="3">3 days</option><option value="7">7 days</option><option value="16" selected>16 days</option></select>
<label>Flood forecast period</label><select id="floodDays"><option value="7">7 days</option><option value="14">14 days</option><option value="30" selected>30 days</option></select>
<label>Map layer</label><select id="mapLayer" onchange="drawMap()"><option value="risk">Composite risk</option><option value="rain">Rainfall</option><option value="balance">Water balance</option><option value="flood">Flood risk</option><option value="drought">Drought risk</option><option value="population">Population exposed</option><option value="lulc">LULC pressure</option></select>
<button onclick="loadData()">Refresh dashboard</button><p id="status" class="status">Loading dashboard...</p>
<div class="card"><strong>Download data</strong><br><br><a class="download" href="/download/subbasins.csv">Download Sub-basin CSV</a><a class="download" href="/download/nodes.csv">Download Nodes CSV</a><a class="download" href="/download/subbasins.geojson">Download Sub-basin GeoJSON</a></div>
<div class="card"><strong>Important note</strong><p class="small">Current polygons are illustrative planning zones. Replace them with official HydroBASINS boundaries for research or formal analysis.</p></div>
</aside>
<main class="main">
<div class="metrics"><div class="card"><label>Population exposed</label><div id="popMetric" class="metric">---</div><div class="small">People</div></div><div class="card"><label>Mean composite risk</label><div id="riskMetric" class="metric">---</div><div class="small">0–100 scale</div></div><div class="card"><label>Mean water balance</label><div id="balanceMetric" class="metric">---</div><div class="small">Rainfall minus ET0</div></div><div class="card"><label>Mean peak discharge</label><div id="floodMetric" class="metric">---</div><div class="small">m³/s</div></div></div>
<div class="tabs"><button class="tab active" onclick="openTab('mapPanel',this)">Basin Map</button><button class="tab" onclick="openTab('summaryPanel',this)">Sub-basin Summary</button><button class="tab" onclick="openTab('plotPanel',this)">Plots</button><button class="tab" onclick="openTab('predictionPanel',this)">Prediction</button></div>
<section id="mapPanel" class="panel active"><div class="card"><div id="map"></div></div></section>
<section id="summaryPanel" class="panel"><div class="card" style="overflow-x:auto"><table><thead><tr><th>Sub-basin</th><th>Population</th><th>Exposed</th><th>Rain</th><th>ET0</th><th>Balance</th><th>Flood</th><th>Drought</th><th>Risk</th></tr></thead><tbody id="summaryRows"></tbody></table></div></section>
<section id="plotPanel" class="panel"><div class="card"><div id="climatePlot"></div></div><div class="card"><div id="riskPlot"></div></div><div class="card"><div id="populationPlot"></div></div></section>
<section id="predictionPanel" class="panel"><div class="card"><label>Prediction scope</label><select id="predictionScope"></select><label>Prediction horizon</label><select id="predictionDays"><option value="30">30 days</option><option value="90">90 days</option><option value="180">180 days</option><option value="365" selected>365 days</option></select><label>Historical baseline</label><select id="historyYears"><option value="3">3 years</option><option value="5">5 years</option><option value="10">10 years</option></select><button onclick="loadPrediction()">Generate prediction</button><p id="predictionStatus" class="status"></p></div><div class="card"><div id="predictionPlot"></div></div><div class="card"><div id="predictionBalancePlot"></div></div></section>
</main></div>
<script>
let map, polygons, markers, latestData = null;
function format(v){ return v===null || v===undefined ? 'N/A' : Number(v).toFixed(2); }
function color(value,maxValue,layer){ if(layer==='balance'){ if(value < -100)return '#e11d48'; if(value < -30)return '#f97316'; if(value < 0)return '#f59e0b'; return '#10b981'; } let ratio=maxValue===0?0:value/maxValue; if(ratio>=.75)return '#e11d48'; if(ratio>=.5)return '#f97316'; if(ratio>=.25)return '#f59e0b'; return '#10b981'; }
function layerValue(item,layer){ if(layer==='risk')return item.risk.composite_score; if(layer==='rain')return item.climate.rainfall_mm; if(layer==='balance')return item.climate.water_balance_mm; if(layer==='flood')return item.flood.risk_score; if(layer==='drought')return item.drought.risk_score; if(layer==='population')return item.population.exposed; if(layer==='lulc')return item.lulc.pressure_score; return 0; }
function initMap(){ map=L.map('map').setView([-23.8,30.0],6); L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'OpenStreetMap'}).addTo(map); polygons=L.layerGroup().addTo(map); markers=L.layerGroup().addTo(map); }
function drawMap(){ if(!latestData||!map)return; polygons.clearLayers();markers.clearLayers();const layer=document.getElementById('mapLayer').value;const values=latestData.subbasins.map(x=>Math.abs(layerValue(x,layer)));const maxValue=Math.max(...values,1);L.geoJSON(latestData.subbasins_geojson,{style:function(feature){const item=latestData.subbasins.find(x=>x.id===feature.properties.id);return {color:'#334155',weight:1.5,fillColor:color(layerValue(item,layer),maxValue,layer),fillOpacity:.45};},onEachFeature:function(feature,mapLayer){const item=latestData.subbasins.find(x=>x.id===feature.properties.id);mapLayer.bindPopup('<strong>'+item.name+'</strong><br><b>Composite risk:</b> '+format(item.risk.composite_score)+'<br><b>Rainfall:</b> '+format(item.climate.rainfall_mm)+' mm<br><b>Water balance:</b> '+format(item.climate.water_balance_mm)+' mm<br><b>Population exposed:</b> '+item.population.exposed.toLocaleString());}}).addTo(polygons);latestData.nodes.forEach(node=>{L.circleMarker([node.coordinates.lat,node.coordinates.lon],{radius:6+node.risk.composite_score/12,color:'#0f172a',fillColor:'#ffffff',fillOpacity:.9,weight:2}).bindPopup('<strong>'+node.name+'</strong><br><b>Risk:</b> '+format(node.risk.composite_score)+'<br><b>Population exposed:</b> '+node.population.exposed.toLocaleString()).addTo(markers);});if(polygons.getBounds().isValid())map.fitBounds(polygons.getBounds(),{padding:[25,25]}); }
function openTab(id,button){document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));document.getElementById(id).classList.add('active');button.classList.add('active');if(id==='mapPanel')setTimeout(()=>map.invalidateSize(),200);}
function drawPlots(){const names=latestData.subbasins.map(x=>x.name);Plotly.newPlot('climatePlot',[{x:names,y:latestData.subbasins.map(x=>x.climate.rainfall_mm),type:'bar',name:'Rainfall'},{x:names,y:latestData.subbasins.map(x=>x.climate.et0_mm),type:'bar',name:'ET0'},{x:names,y:latestData.subbasins.map(x=>x.climate.water_balance_mm),type:'bar',name:'Water balance'}],{title:'Sub-basin climate water budget',barmode:'group'},{responsive:true});Plotly.newPlot('riskPlot',[{x:names,y:latestData.subbasins.map(x=>x.risk.composite_score),type:'bar',name:'Composite risk'},{x:names,y:latestData.subbasins.map(x=>x.flood.risk_score),type:'bar',name:'Flood risk'},{x:names,y:latestData.subbasins.map(x=>x.drought.risk_score),type:'bar',name:'Drought risk'}],{title:'Sub-basin risk components',barmode:'group'},{responsive:true});Plotly.newPlot('populationPlot',[{x:names,y:latestData.subbasins.map(x=>x.population.total),type:'bar',name:'Population'},{x:names,y:latestData.subbasins.map(x=>x.population.exposed),type:'bar',name:'Exposed population'}],{title:'Population exposure by sub-basin',barmode:'group'},{responsive:true});}
function fillSummary(){let html='';latestData.subbasins.forEach(item=>{html+='<tr><td><strong>'+item.name+'</strong></td><td>'+item.population.total.toLocaleString()+'</td><td>'+item.population.exposed.toLocaleString()+'</td><td>'+format(item.climate.rainfall_mm)+'</td><td>'+format(item.climate.et0_mm)+'</td><td>'+format(item.climate.water_balance_mm)+'</td><td>'+format(item.flood.risk_score)+'</td><td>'+format(item.drought.risk_score)+'</td><td><strong>'+format(item.risk.composite_score)+'</strong></td></tr>';});document.getElementById('summaryRows').innerHTML=html;}
function fillPredictionScope(){const select=document.getElementById('predictionScope');select.innerHTML='<option value="basin">Entire Limpopo Basin</option>';latestData.subbasins.forEach(item=>{select.innerHTML+='<option value="'+item.id+'">'+item.name+'</option>';});}
async function loadPrediction(){const scope=document.getElementById('predictionScope').value;const days=document.getElementById('predictionDays').value;const years=document.getElementById('historyYears').value;const status=document.getElementById('predictionStatus');status.textContent='Generating prediction...';try{const response=await fetch('/api/prediction/'+scope+'?prediction_days='+days+'&history_years='+years);const data=await response.json();if(data.error){status.textContent=data.error;return;}Plotly.newPlot('predictionPlot',[{x:data.dates,y:data.rainfall_mm,type:'scatter',mode:'lines',name:'Predicted rainfall'},{x:data.dates,y:data.et0_mm,type:'scatter',mode:'lines',name:'Predicted ET0'},{x:data.dates,y:data.temperature_c,type:'scatter',mode:'lines',name:'Predicted temperature'}],{title:'Climatology prediction: '+data.scope},{responsive:true});Plotly.newPlot('predictionBalancePlot',[{x:data.dates,y:data.water_balance_mm,type:'scatter',mode:'lines',fill:'tozeroy',name:'Predicted water balance'}],{title:'Predicted water balance: '+data.scope},{responsive:true});status.textContent='Prediction completed using '+data.history_years+'-year historical climatology.';}catch(error){status.textContent='Prediction error: '+error;}}
async function loadData(){const status=document.getElementById('status');status.textContent='Loading online basin data. First request may take a few minutes...';const forecastDays=document.getElementById('forecastDays').value;const floodDays=document.getElementById('floodDays').value;try{const response=await fetch('/api/dashboard?forecast_days='+forecastDays+'&flood_days='+floodDays);latestData=await response.json();document.getElementById('popMetric').textContent=latestData.basin_summary.population_exposed.toLocaleString();document.getElementById('riskMetric').textContent=format(latestData.basin_summary.mean_composite_risk)+' / 100';document.getElementById('balanceMetric').textContent=format(latestData.basin_summary.mean_water_balance_mm)+' mm';document.getElementById('floodMetric').textContent=format(latestData.basin_summary.mean_peak_discharge_m3s)+' m³/s';fillSummary();fillPredictionScope();drawMap();drawPlots();status.textContent='Dashboard updated successfully.';}catch(error){status.textContent='Error: '+error;}}
document.addEventListener('DOMContentLoaded',function(){initMap();loadData();});
</script>
</body>
</html>
    '''


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
