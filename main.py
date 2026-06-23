```python
import asyncio
import csv
import io
import json
import math
import time
import zipfile
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List

import httpx
import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse


app = FastAPI(
    title="Limpopo Basin and Sub-basin Explorer",
    version="1.0.0",
    description="Basin-scale and sub-basin-scale hydroclimate, risk, prediction and download dashboard."
)

# ============================================================
# CACHE
# ============================================================

CACHE: Dict[str, tuple] = {}
CACHE_TTL_SECONDS = 6 * 60 * 60
LATEST_STATE = None


# ============================================================
# STRATEGIC NODES
# ============================================================

LOCATIONS = {
    "upper_limpopo": {
        "name": "Upper Limpopo Headwaters",
        "lat": -25.20,
        "lon": 26.90,
        "country": "South Africa / Botswana",
        "subbasin": "upper_limpopo",
        "population": 45000,
        "dam_capacity_m3": 120000000,
        "irrigation_pressure": 42,
        "urban_pressure": 18,
        "groundwater_dependency": 64,
        "ecosystem_sensitivity": 66,
        "lulc": {"urban": 6, "cropland": 28, "grassland": 42, "forest": 8, "water": 2, "bare": 14}
    },
    "gaborone": {
        "name": "Gaborone Catchment",
        "lat": -24.65,
        "lon": 25.91,
        "country": "Botswana",
        "subbasin": "upper_limpopo",
        "population": 230000,
        "dam_capacity_m3": 141100000,
        "irrigation_pressure": 30,
        "urban_pressure": 82,
        "groundwater_dependency": 72,
        "ecosystem_sensitivity": 54,
        "lulc": {"urban": 24, "cropland": 12, "grassland": 38, "forest": 4, "water": 3, "bare": 19}
    },
    "shashe": {
        "name": "Francistown / Shashe Sub-Basin",
        "lat": -21.17,
        "lon": 27.51,
        "country": "Botswana / Zimbabwe",
        "subbasin": "shashe",
        "population": 95000,
        "dam_capacity_m3": 85000000,
        "irrigation_pressure": 38,
        "urban_pressure": 46,
        "groundwater_dependency": 70,
        "ecosystem_sensitivity": 59,
        "lulc": {"urban": 12, "cropland": 18, "grassland": 44, "forest": 5, "water": 2, "bare": 19}
    },
    "polokwane": {
        "name": "Polokwane Regional Platform",
        "lat": -23.90,
        "lon": 29.45,
        "country": "South Africa",
        "subbasin": "mogalakwena",
        "population": 510000,
        "dam_capacity_m3": 0,
        "irrigation_pressure": 48,
        "urban_pressure": 78,
        "groundwater_dependency": 84,
        "ecosystem_sensitivity": 62,
        "lulc": {"urban": 22, "cropland": 30, "grassland": 28, "forest": 5, "water": 1, "bare": 14}
    },
    "mokopane": {
        "name": "Mokopane / Mogalakwena System",
        "lat": -24.19,
        "lon": 29.01,
        "country": "South Africa",
        "subbasin": "mogalakwena",
        "population": 185000,
        "dam_capacity_m3": 0,
        "irrigation_pressure": 52,
        "urban_pressure": 50,
        "groundwater_dependency": 78,
        "ecosystem_sensitivity": 60,
        "lulc": {"urban": 10, "cropland": 36, "grassland": 32, "forest": 5, "water": 1, "bare": 16}
    },
    "beitbridge": {
        "name": "Beitbridge Gateway",
        "lat": -22.22,
        "lon": 30.00,
        "country": "Zimbabwe / South Africa",
        "subbasin": "middle_limpopo",
        "population": 120000,
        "dam_capacity_m3": 0,
        "irrigation_pressure": 40,
        "urban_pressure": 40,
        "groundwater_dependency": 68,
        "ecosystem_sensitivity": 58,
        "lulc": {"urban": 8, "cropland": 20, "grassland": 40, "forest": 4, "water": 1, "bare": 27}
    },
    "olifants": {
        "name": "Olifants River Transboundary Node",
        "lat": -24.00,
        "lon": 31.50,
        "country": "South Africa / Mozambique",
        "subbasin": "olifants",
        "population": 620000,
        "dam_capacity_m3": 2400000000,
        "irrigation_pressure": 72,
        "urban_pressure": 48,
        "groundwater_dependency": 65,
        "ecosystem_sensitivity": 82,
        "lulc": {"urban": 10, "cropland": 34, "grassland": 24, "forest": 12, "water": 4, "bare": 16}
    },
    "massingir": {
        "name": "Massingir Dam Control Zone",
        "lat": -23.88,
        "lon": 32.16,
        "country": "Mozambique",
        "subbasin": "lower_limpopo",
        "population": 35000,
        "dam_capacity_m3": 2844000000,
        "irrigation_pressure": 55,
        "urban_pressure": 20,
        "groundwater_dependency": 44,
        "ecosystem_sensitivity": 86,
        "lulc": {"urban": 4, "cropland": 18, "grassland": 31, "forest": 18, "water": 12, "bare": 17}
    },
    "chokwe": {
        "name": "Chokwe Irrigation and Floodplain",
        "lat": -24.53,
        "lon": 32.98,
        "country": "Mozambique",
        "subbasin": "lower_limpopo",
        "population": 180000,
        "dam_capacity_m3": 0,
        "irrigation_pressure": 92,
        "urban_pressure": 34,
        "groundwater_dependency": 52,
        "ecosystem_sensitivity": 76,
        "lulc": {"urban": 7, "cropland": 58, "grassland": 16, "forest": 5, "water": 5, "bare": 9}
    },
    "xai_xai": {
        "name": "Xai-Xai Estuary and Basin Outlet",
        "lat": -25.05,
        "lon": 33.65,
        "country": "Mozambique",
        "subbasin": "lower_limpopo",
        "population": 140000,
        "dam_capacity_m3": 0,
        "irrigation_pressure": 50,
        "urban_pressure": 58,
        "groundwater_dependency": 48,
        "ecosystem_sensitivity": 94,
        "lulc": {"urban": 14, "cropland": 32, "grassland": 16, "forest": 10, "water": 14, "bare": 14}
    }
}


# ============================================================
# ILLUSTRATIVE SUB-BASIN PLANNING ZONES
# Replace these with official HydroBASINS GeoJSON boundaries later.
# Coordinates are [longitude, latitude].
# ============================================================

SUBBASINS = {
    "upper_limpopo": {
        "name": "Upper Limpopo Planning Zone",
        "countries": "South Africa / Botswana",
        "node_ids": ["upper_limpopo", "gaborone"],
        "geometry": [
            [24.8, -26.2], [28.1, -26.2], [28.1, -23.6],
            [24.8, -23.6], [24.8, -26.2]
        ]
    },
    "shashe": {
        "name": "Shashe Tributary Planning Zone",
        "countries": "Botswana / Zimbabwe",
        "node_ids": ["shashe"],
        "geometry": [
            [25.8, -23.7], [29.2, -23.7], [29.2, -20.1],
            [25.8, -20.1], [25.8, -23.7]
        ]
    },
    "mogalakwena": {
        "name": "Mogalakwena and Polokwane Planning Zone",
        "countries": "South Africa",
        "node_ids": ["polokwane", "mokopane"],
        "geometry": [
            [27.8, -25.4], [30.3, -25.4], [30.3, -22.7],
            [27.8, -22.7], [27.8, -25.4]
        ]
    },
    "middle_limpopo": {
        "name": "Middle Limpopo Main Stem Planning Zone",
        "countries": "South Africa / Zimbabwe",
        "node_ids": ["beitbridge"],
        "geometry": [
            [28.8, -23.9], [31.7, -23.9], [31.7, -21.0],
            [28.8, -21.0], [28.8, -23.9]
        ]
    },
    "olifants": {
        "name": "Olifants Tributary Planning Zone",
        "countries": "South Africa / Mozambique",
        "node_ids": ["olifants"],
        "geometry": [
            [30.0, -25.4], [32.4, -25.4], [32.4, -22.4],
            [30.0, -22.4], [30.0, -25.4]
        ]
    },
    "lower_limpopo": {
        "name": "Lower Limpopo, Massingir and Delta Planning Zone",
        "countries": "Mozambique",
        "node_ids": ["massingir", "chokwe", "xai_xai"],
        "geometry": [
            [31.0, -26.0], [34.5, -26.0], [34.5, -23.0],
            [31.0, -23.0], [31.0, -26.0]
        ]
    }
}


# ============================================================
# BASIC UTILITY FUNCTIONS
# ============================================================

def clean_numbers(values):
    if not values:
        return []
    return [
        value for value in values
        if isinstance(value, (int, float)) and not math.isnan(value)
    ]


def safe_sum(values):
    values = clean_numbers(values)
    return round(sum(values), 2) if values else 0.0


def safe_mean(values):
    values = clean_numbers(values)
    return round(sum(values) / len(values), 2) if values else 0.0


def safe_max(values):
    values = clean_numbers(values)
    return round(max(values), 2) if values else 0.0


def safe_min(values):
    values = clean_numbers(values)
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


def class_score(name):
    return {
        "Low": 20,
        "Moderate": 50,
        "High": 75,
        "Very high": 95
    }.get(name, 0)


def cache_key(url, params):
    return str((url, tuple(sorted(params.items()))))


async def fetch_json(client, url, params):
    key = cache_key(url, params)
    now = time.time()

    if key in CACHE:
        saved_time, saved_data = CACHE[key]
        if now - saved_time < CACHE_TTL_SECONDS:
            return saved_data

    try:
        response = await client.get(url, params=params, timeout=60.0)
        response.raise_for_status()
        data = response.json()
        CACHE[key] = (now, data)
        return data
    except Exception as error:
        return {"error": str(error), "daily": {}}


# ============================================================
# RISK AND INDICATOR METHODS
# ============================================================

def estimate_soil_saturation(rainfall, et0):
    balance = safe_sum(rainfall) - safe_sum(et0)

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


def calculate_drought_risk(soil_saturation, water_balance, recent_rain):
    score = 0

    if soil_saturation < 0.14:
        score += 45
    elif soil_saturation < 0.22:
        score += 30
    elif soil_saturation < 0.30:
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


def calculate_flood_risk(peak_discharge):
    if peak_discharge >= 120:
        return 95
    if peak_discharge >= 50:
        return 75
    if peak_discharge >= 20:
        return 50
    return 20


def calculate_climate_risk(rainfall, et0, max_temp, wind, radiation):
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


def calculate_lulc_pressure(meta):
    lulc = meta["lulc"]

    score = (
        lulc["urban"] * 0.45 +
        lulc["cropland"] * 0.35 +
        lulc["bare"] * 0.28 -
        lulc["forest"] * 0.18 -
        lulc["water"] * 0.08 +
        meta["irrigation_pressure"] * 0.18 +
        meta["urban_pressure"] * 0.16 +
        meta["ecosystem_sensitivity"] * 0.10
    )

    return round(clamp(score, 0, 100), 2)


def calculate_reservoir_storage(capacity, discharge):
    if capacity <= 0:
        return None

    estimated_volume = safe_sum(discharge) * 86400
    storage_percent = (estimated_volume / capacity) * 100
    return round(clamp(storage_percent, 8, 100), 2)


def calculate_reservoir_stress(storage_percent, groundwater_dependency):
    if storage_percent is None:
        return round(groundwater_dependency * 0.45, 2)

    stress = ((100 - storage_percent) * 0.75) + (groundwater_dependency * 0.25)
    return round(clamp(stress, 0, 100), 2)


def calculate_population_exposure(population, flood_score, drought_score, climate_score, lulc_score):
    exposure_percent = (
        flood_score * 0.34 +
        drought_score * 0.26 +
        climate_score * 0.20 +
        lulc_score * 0.20
    )

    exposure_percent = round(clamp(exposure_percent, 0, 100), 2)
    exposed_people = int(population * exposure_percent / 100)

    return exposed_people, exposure_percent


# ============================================================
# CLIMATOLOGY-BASED PREDICTION
# ============================================================

def make_prediction(history_daily, prediction_days):
    dates = history_daily.get("time", []) or []
    rainfall = history_daily.get("precipitation_sum", []) or []
    temperature = history_daily.get("temperature_2m_mean", []) or []
    et0 = history_daily.get("et0_fao_evapotranspiration", []) or []

    seasonal = defaultdict(lambda: {"rain": [], "temp": [], "et0": []})

    for index, day in enumerate(dates):
        try:
            month_day = day[5:10]

            if index < len(rainfall) and isinstance(rainfall[index], (int, float)):
                seasonal[month_day]["rain"].append(rainfall[index])

            if index < len(temperature) and isinstance(temperature[index], (int, float)):
                seasonal[month_day]["temp"].append(temperature[index])

            if index < len(et0) and isinstance(et0[index], (int, float)):
                seasonal[month_day]["et0"].append(et0[index])
        except Exception:
            continue

    fallback_rain = safe_mean(rainfall)
    fallback_temp = safe_mean(temperature)
    fallback_et0 = safe_mean(et0)

    future_dates = []
    predicted_rainfall = []
    predicted_temperature = []
    predicted_et0 = []
    predicted_balance = []

    start_day = date.today() + timedelta(days=1)

    for offset in range(prediction_days):
        target_day = start_day + timedelta(days=offset)
        month_day = target_day.isoformat()[5:10]

        rain_value = safe_mean(seasonal[month_day]["rain"]) or fallback_rain
        temp_value = safe_mean(seasonal[month_day]["temp"]) or fallback_temp
        et0_value = safe_mean(seasonal[month_day]["et0"]) or fallback_et0

        future_dates.append(target_day.isoformat())
        predicted_rainfall.append(round(rain_value, 2))
        predicted_temperature.append(round(temp_value, 2))
        predicted_et0.append(round(et0_value, 2))
        predicted_balance.append(round(rain_value - et0_value, 2))

    return {
        "dates": future_dates,
        "rainfall_mm": predicted_rainfall,
        "temperature_c": predicted_temperature,
        "et0_mm": predicted_et0,
        "water_balance_mm": predicted_balance,
        "total_rainfall_mm": safe_sum(predicted_rainfall),
        "total_et0_mm": safe_sum(predicted_et0),
        "total_water_balance_mm": round(safe_sum(predicted_rainfall) - safe_sum(predicted_et0), 2)
    }


# ============================================================
# NODE DATA COLLECTION
# ============================================================

async def get_live_node_data(client, node_id, meta, forecast_days, flood_days):
    today = date.today()
    historical_start = (today - timedelta(days=90)).isoformat()
    historical_end = (today - timedelta(days=1)).isoformat()

    forecast_params = {
        "latitude": meta["lat"],
        "longitude": meta["lon"],
        "daily": ",".join([
            "precipitation_sum",
            "temperature_2m_max",
            "temperature_2m_min",
            "et0_fao_evapotranspiration",
            "wind_speed_10m_max",
            "shortwave_radiation_sum"
        ]),
        "forecast_days": forecast_days,
        "timezone": "auto"
    }

    history_params = {
        "latitude": meta["lat"],
        "longitude": meta["lon"],
        "start_date": historical_start,
        "end_date": historical_end,
        "daily": "precipitation_sum,temperature_2m_mean,et0_fao_evapotranspiration",
        "timezone": "auto"
    }

    flood_params = {
        "latitude": meta["lat"],
        "longitude": meta["lon"],
        "daily": "river_discharge",
        "forecast_days": flood_days,
        "timezone": "auto"
    }

    forecast, history, flood = await asyncio.gather(
        fetch_json(client, "https://api.open-meteo.com/v1/forecast", forecast_params),
        fetch_json(client, "https://archive-api.open-meteo.com/v1/archive", history_params),
        fetch_json(client, "https://flood-api.open-meteo.com/v1/flood", flood_params)
    )

    forecast_daily = forecast.get("daily", {}) or {}
    history_daily = history.get("daily", {}) or {}
    flood_daily = flood.get("daily", {}) or {}

    rainfall = forecast_daily.get("precipitation_sum", []) or []
    et0 = forecast_daily.get("et0_fao_evapotranspiration", []) or []
    temp_max = forecast_daily.get("temperature_2m_max", []) or []
    temp_min = forecast_daily.get("temperature_2m_min", []) or []
    wind = forecast_daily.get("wind_speed_10m_max", []) or []
    radiation = forecast_daily.get("shortwave_radiation_sum", []) or []
    discharge = flood_daily.get("river_discharge", []) or []

    rainfall_total = safe_sum(rainfall)
    et0_total = safe_sum(et0)
    water_balance = round(rainfall_total - et0_total, 2)
    soil_saturation = estimate_soil_saturation(rainfall, et0)

    drought_score = calculate_drought_risk(
        soil_saturation,
        water_balance,
        safe_sum(history_daily.get("precipitation_sum", []))
    )

    peak_discharge = safe_max(discharge)
    flood_score = calculate_flood_risk(peak_discharge)

    climate_score = calculate_climate_risk(
        rainfall_total,
        et0_total,
        safe_max(temp_max),
        safe_max(wind),
        safe_sum(radiation)
    )

    lulc_score = calculate_lulc_pressure(meta)
    reservoir_storage = calculate_reservoir_storage(meta["dam_capacity_m3"], discharge)
    reservoir_stress = calculate_reservoir_stress(
        reservoir_storage,
        meta["groundwater_dependency"]
    )

    exposed_population, exposure_percent = calculate_population_exposure(
        meta["population"],
        flood_score,
        drought_score,
        climate_score,
        lulc_score
    )

    composite_risk = round(
        climate_score * 0.20 +
        flood_score * 0.20 +
        drought_score * 0.20 +
        exposure_percent * 0.15 +
        lulc_score * 0.15 +
        reservoir_stress * 0.10,
        2
    )

    return {
        "id": node_id,
        "name": meta["name"],
        "country": meta["country"],
        "subbasin": meta["subbasin"],
        "coordinates": {"lat": meta["lat"], "lon": meta["lon"]},
        "reference": {
            "population": meta["population"],
            "dam_capacity_m3": meta["dam_capacity_m3"],
            "irrigation_pressure": meta["irrigation_pressure"],
            "urban_pressure": meta["urban_pressure"],
            "groundwater_dependency": meta["groundwater_dependency"],
            "ecosystem_sensitivity": meta["ecosystem_sensitivity"],
            "lulc": meta["lulc"]
        },
        "climate": {
            "rainfall_total_mm": rainfall_total,
            "et0_total_mm": et0_total,
            "water_balance_mm": water_balance,
            "mean_temperature_c": round((safe_mean(temp_max) + safe_mean(temp_min)) / 2, 2),
            "max_temperature_c": safe_max(temp_max),
            "min_temperature_c": safe_min(temp_min),
            "max_wind_kmh": safe_max(wind),
            "solar_radiation_mj_m2": safe_sum(radiation),
            "recent_90d_rainfall_mm": safe_sum(history_daily.get("precipitation_sum", [])),
            "soil_saturation_proxy": soil_saturation,
            "climate_risk_score": climate_score
        },
        "flood": {
            "peak_discharge_m3s": peak_discharge,
            "mean_discharge_m3s": safe_mean(discharge),
            "flood_risk_score": flood_score,
            "flood_risk_class": risk_class(flood_score)
        },
        "drought": {
            "drought_risk_score": drought_score,
            "drought_risk_class": risk_class(drought_score)
        },
        "lulc": {
            "lulc_pressure_score": lulc_score,
            "profile_percent": meta["lulc"]
        },
        "population": {
            "base_population": meta["population"],
            "population_exposed": exposed_population,
            "exposure_percent": exposure_percent
        },
        "reservoir": {
            "has_reservoir": meta["dam_capacity_m3"] > 0,
            "storage_percent": reservoir_storage,
            "reservoir_stress_score": reservoir_stress
        },
        "risk": {
            "composite_score": composite_risk,
            "composite_class": risk_class(composite_risk)
        },
        "time_series": {
            "forecast_dates": forecast_daily.get("time", []),
            "rainfall_mm": rainfall,
            "et0_mm": et0,
            "temperature_max_c": temp_max,
            "temperature_min_c": temp_min,
            "wind_speed_kmh": wind,
            "solar_radiation_mj_m2": radiation,
            "flood_dates": flood_daily.get("time", []),
            "river_discharge_m3s": discharge
        },
        "errors": {
            "forecast_error": forecast.get("error"),
            "history_error": history.get("error"),
            "flood_error": flood.get("error")
        }
    }


# ============================================================
# BASIN AND SUB-BASIN AGGREGATION
# ============================================================

def average_lulc(nodes):
    keys = ["urban", "cropland", "grassland", "forest", "water", "bare"]
    return {
        key: safe_mean([node["lulc"]["profile_percent"].get(key, 0) for node in nodes])
        for key in keys
    }


def aggregate_subbasin(subbasin_id, nodes):
    meta = SUBBASINS[subbasin_id]

    return {
        "id": subbasin_id,
        "name": meta["name"],
        "countries": meta["countries"],
        "node_count": len(nodes),
        "node_ids": [node["id"] for node in nodes],
        "population": {
            "total": int(sum(node["population"]["base_population"] for node in nodes)),
            "exposed": int(sum(node["population"]["population_exposed"] for node in nodes)),
            "exposure_percent": safe_mean([node["population"]["exposure_percent"] for node in nodes])
        },
        "climate": {
            "rainfall_mm": safe_mean([node["climate"]["rainfall_total_mm"] for node in nodes]),
            "et0_mm": safe_mean([node["climate"]["et0_total_mm"] for node in nodes]),
            "water_balance_mm": safe_mean([node["climate"]["water_balance_mm"] for node in nodes]),
            "mean_temperature_c": safe_mean([node["climate"]["mean_temperature_c"] for node in nodes]),
            "max_wind_kmh": safe_mean([node["climate"]["max_wind_kmh"] for node in nodes]),
            "radiation_mj_m2": safe_mean([node["climate"]["solar_radiation_mj_m2"] for node in nodes]),
            "soil_saturation_proxy": safe_mean([node["climate"]["soil_saturation_proxy"] for node in nodes]),
            "climate_risk_score": safe_mean([node["climate"]["climate_risk_score"] for node in nodes])
        },
        "flood": {
            "peak_discharge_m3s": safe_max([node["flood"]["peak_discharge_m3s"] for node in nodes]),
            "mean_flood_risk_score": safe_mean([node["flood"]["flood_risk_score"] for node in nodes]),
            "flood_risk_class": risk_class(safe_mean([node["flood"]["flood_risk_score"] for node in nodes]))
        },
        "drought": {
            "mean_drought_risk_score": safe_mean([node["drought"]["drought_risk_score"] for node in nodes]),
            "drought_risk_class": risk_class(safe_mean([node["drought"]["drought_risk_score"] for node in nodes]))
        },
        "lulc": {
            "profile_percent": average_lulc(nodes),
            "mean_lulc_pressure_score": safe_mean([node["lulc"]["lulc_pressure_score"] for node in nodes])
        },
        "reservoir": {
            "total_capacity_m3": int(sum(node["reference"]["dam_capacity_m3"] for node in nodes)),
            "mean_storage_percent": safe_mean([
                node["reservoir"]["storage_percent"]
                for node in nodes
                if node["reservoir"]["storage_percent"] is not None
            ]),
            "mean_reservoir_stress_score": safe_mean([
                node["reservoir"]["reservoir_stress_score"] for node in nodes
            ])
        },
        "risk": {
            "composite_score": safe_mean([node["risk"]["composite_score"] for node in nodes]),
            "composite_class": risk_class(safe_mean([node["risk"]["composite_score"] for node in nodes]))
        }
    }


def build_subbasin_geojson(subbasins):
    features = []

    for item in subbasins:
        geometry = SUBBASINS[item["id"]]["geometry"]

        features.append({
            "type": "Feature",
            "properties": {
                "id": item["id"],
                "name": item["name"],
                "countries": item["countries"],
                "composite_risk": item["risk"]["composite_score"],
                "flood_risk": item["flood"]["mean_flood_risk_score"],
                "drought_risk": item["drought"]["mean_drought_risk_score"],
                "population_exposed": item["population"]["exposed"],
                "rainfall_mm": item["climate"]["rainfall_mm"],
                "water_balance_mm": item["climate"]["water_balance_mm"],
                "lulc_pressure": item["lulc"]["mean_lulc_pressure_score"]
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [geometry]
            }
        })

    return {"type": "FeatureCollection", "features": features}


def build_nodes_geojson(nodes):
    features = []

    for node in nodes:
        features.append({
            "type": "Feature",
            "properties": {
                "id": node["id"],
                "name": node["name"],
                "country": node["country"],
                "subbasin": node["subbasin"],
                "composite_risk": node["risk"]["composite_score"],
                "flood_risk": node["flood"]["flood_risk_score"],
                "drought_risk": node["drought"]["drought_risk_score"],
                "population_exposed": node["population"]["population_exposed"],
                "rainfall_mm": node["climate"]["rainfall_total_mm"],
                "water_balance_mm": node["climate"]["water_balance_mm"]
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    node["coordinates"]["lon"],
                    node["coordinates"]["lat"]
                ]
            }
        })

    return {"type": "FeatureCollection", "features": features}


async def build_dashboard_payload(forecast_days=16, flood_days=30):
    global LATEST_STATE

    forecast_days = clamp(int(forecast_days), 1, 16)
    flood_days = clamp(int(flood_days), 1, 30)

    async with httpx.AsyncClient() as client:
        tasks = [
            get_live_node_data(client, node_id, meta, forecast_days, flood_days)
            for node_id, meta in LOCATIONS.items()
        ]
        nodes = await asyncio.gather(*tasks)

    grouped = defaultdict(list)

    for node in nodes:
        grouped[node["subbasin"]].append(node)

    subbasins = [
        aggregate_subbasin(subbasin_id, grouped[subbasin_id])
        for subbasin_id in SUBBASINS.keys()
        if subbasin_id in grouped
    ]

    basin_summary = {
        "total_nodes": len(nodes),
        "total_subbasins": len(subbasins),
        "total_population": int(sum(node["population"]["base_population"] for node in nodes)),
        "total_population_exposed": int(sum(node["population"]["population_exposed"] for node in nodes)),
        "mean_composite_risk": safe_mean([node["risk"]["composite_score"] for node in nodes]),
        "mean_climate_risk": safe_mean([node["climate"]["climate_risk_score"] for node in nodes]),
        "mean_drought_risk": safe_mean([node["drought"]["drought_risk_score"] for node in nodes]),
        "mean_flood_risk": safe_mean([node["flood"]["flood_risk_score"] for node in nodes]),
        "mean_lulc_pressure": safe_mean([node["lulc"]["lulc_pressure_score"] for node in nodes]),
        "mean_water_balance_mm": safe_mean([node["climate"]["water_balance_mm"] for node in nodes]),
        "mean_peak_discharge_m3s": safe_mean([node["flood"]["peak_discharge_m3s"] for node in nodes]),
        "mean_soil_saturation_proxy": safe_mean([node["climate"]["soil_saturation_proxy"] for node in nodes])
    }

    payload = {
        "metadata": {
            "title": "Limpopo Basin and Sub-basin Explorer",
            "generated_on": date.today().isoformat(),
            "forecast_days": forecast_days,
            "flood_days": flood_days,
            "boundary_note": "Sub-basin polygons are illustrative planning zones. Replace with official HydroBASINS polygons for formal basin delineation.",
            "data_note": "Climate and flood layers are fetched online. LULC, population, infrastructure and pressure layers are node-reference inputs."
        },
        "basin_summary": basin_summary,
        "nodes": nodes,
        "subbasins": subbasins,
        "nodes_geojson": build_nodes_geojson(nodes),
        "subbasins_geojson": build_subbasin_geojson(subbasins)
    }

    LATEST_STATE = payload
    return payload


# ============================================================
# PREDICTION ENDPOINT
# ============================================================

@app.get("/api/prediction/{scope_id}")
async def prediction_endpoint(
    scope_id: str,
    prediction_days: int = Query(365, ge=30, le=365),
    history_years: int = Query(3, ge=3, le=10)
):
    if scope_id == "basin":
        target_nodes = list(LOCATIONS.items())
        scope_name = "Limpopo Basin"
    elif scope_id in SUBBASINS:
        target_nodes = [
            (node_id, LOCATIONS[node_id])
            for node_id in SUBBASINS[scope_id]["node_ids"]
        ]
        scope_name = SUBBASINS[scope_id]["name"]
    else:
        return {"error": "Unknown scope ID"}

    end_day = date.today() - timedelta(days=1)
    start_day = end_day - timedelta(days=365 * history_years)

    async with httpx.AsyncClient() as client:
        tasks = []

        for node_id, meta in target_nodes:
            params = {
                "latitude": meta["lat"],
                "longitude": meta["lon"],
                "start_date": start_day.isoformat(),
                "end_date": end_day.isoformat(),
                "daily": "precipitation_sum,temperature_2m_mean,et0_fao_evapotranspiration",
                "timezone": "auto"
            }

            tasks.append(
                fetch_json(
                    client,
                    "https://archive-api.open-meteo.com/v1/archive",
                    params
                )
            )

        history_results = await asyncio.gather(*tasks)

    predictions = []

    for result in history_results:
        predictions.append(
            make_prediction(result.get("daily", {}) or {}, prediction_days)
        )

    if not predictions:
        return {"error": "No prediction data available"}

    dates = predictions[0]["dates"]
    rainfall = []
    temperature = []
    et0 = []
    balance = []

    for day_index in range(len(dates)):
        rainfall.append(safe_mean([
            prediction["rainfall_mm"][day_index]
            for prediction in predictions
            if day_index < len(prediction["rainfall_mm"])
        ]))

        temperature.append(safe_mean([
            prediction["temperature_c"][day_index]
            for prediction in predictions
            if day_index < len(prediction["temperature_c"])
        ]))

        et0.append(safe_mean([
            prediction["et0_mm"][day_index]
            for prediction in predictions
            if day_index < len(prediction["et0_mm"])
        ]))

        balance.append(round(rainfall[-1] - et0[-1], 2))

    return {
        "scope_id": scope_id,
        "scope_name": scope_name,
        "prediction_days": prediction_days,
        "history_years": history_years,
        "method": "Daily climatology prediction using historical daily rainfall, temperature and ET0 averages by calendar day.",
        "dates": dates,
        "predicted_rainfall_mm": rainfall,
        "predicted_temperature_c": temperature,
        "predicted_et0_mm": et0,
        "predicted_water_balance_mm": balance,
        "total_predicted_rainfall_mm": safe_sum(rainfall),
        "total_predicted_et0_mm": safe_sum(et0),
        "total_predicted_water_balance_mm": round(safe_sum(rainfall) - safe_sum(et0), 2)
    }


# ============================================================
# API ROUTES
# ============================================================

@app.get("/api/basin-dashboard")
async def basin_dashboard(
    forecast_days: int = Query(16, ge=1, le=16),
    flood_days: int = Query(30, ge=1, le=30)
):
    return await build_dashboard_payload(forecast_days, flood_days)


@app.get("/api/subbasin/{subbasin_id}")
async def get_subbasin(subbasin_id: str):
    payload = LATEST_STATE or await build_dashboard_payload()

    for subbasin in payload["subbasins"]:
        if subbasin["id"] == subbasin_id:
            node_ids = subbasin["node_ids"]
            selected_nodes = [
                node for node in payload["nodes"]
                if node["id"] in node_ids
            ]

            return {
                "subbasin": subbasin,
                "nodes": selected_nodes
            }

    return {"error": "Sub-basin not found"}


@app.get("/api/locations")
def locations_endpoint():
    return LOCATIONS


@app.get("/health")
def health():
    return {"status": "ok"}


# ============================================================
# DOWNLOAD ENDPOINTS
# ============================================================

def csv_response(filename, headers, rows):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


@app.get("/download/basin-summary.csv")
async def download_basin_summary():
    payload = LATEST_STATE or await build_dashboard_payload()

    row = payload["basin_summary"]

    return csv_response(
        "limpopo_basin_summary.csv",
        list(row.keys()),
        [row]
    )


@app.get("/download/subbasin-summary.csv")
async def download_subbasin_summary():
    payload = LATEST_STATE or await build_dashboard_payload()

    rows = []

    for subbasin in payload["subbasins"]:
        rows.append({
            "subbasin_id": subbasin["id"],
            "subbasin_name": subbasin["name"],
            "countries": subbasin["countries"],
            "node_count": subbasin["node_count"],
            "total_population": subbasin["population"]["total"],
            "population_exposed": subbasin["population"]["exposed"],
            "population_exposure_percent": subbasin["population"]["exposure_percent"],
            "rainfall_mm": subbasin["climate"]["rainfall_mm"],
            "et0_mm": subbasin["climate"]["et0_mm"],
            "water_balance_mm": subbasin["climate"]["water_balance_mm"],
            "mean_temperature_c": subbasin["climate"]["mean_temperature_c"],
            "peak_discharge_m3s": subbasin["flood"]["peak_discharge_m3s"],
            "flood_risk_score": subbasin["flood"]["mean_flood_risk_score"],
            "drought_risk_score": subbasin["drought"]["mean_drought_risk_score"],
            "lulc_pressure_score": subbasin["lulc"]["mean_lulc_pressure_score"],
            "composite_risk_score": subbasin["risk"]["composite_score"],
            "composite_risk_class": subbasin["risk"]["composite_class"]
        })

    return csv_response(
        "limpopo_subbasin_summary.csv",
        list(rows[0].keys()) if rows else [],
        rows
    )


@app.get("/download/nodes.csv")
async def download_nodes_csv():
    payload = LATEST_STATE or await build_dashboard_payload()

    rows = []

    for node in payload["nodes"]:
        rows.append({
            "node_id": node["id"],
            "node_name": node["name"],
            "country": node["country"],
            "subbasin": node["subbasin"],
            "latitude": node["coordinates"]["lat"],
            "longitude": node["coordinates"]["lon"],
            "population": node["population"]["base_population"],
            "population_exposed": node["population"]["population_exposed"],
            "rainfall_mm": node["climate"]["rainfall_total_mm"],
            "et0_mm": node["climate"]["et0_total_mm"],
            "water_balance_mm": node["climate"]["water_balance_mm"],
            "mean_temperature_c": node["climate"]["mean_temperature_c"],
            "peak_discharge_m3s": node["flood"]["peak_discharge_m3s"],
            "drought_risk_score": node["drought"]["drought_risk_score"],
            "flood_risk_score": node["flood"]["flood_risk_score"],
            "lulc_pressure_score": node["lulc"]["lulc_pressure_score"],
            "reservoir_stress_score": node["reservoir"]["reservoir_stress_score"],
            "composite_risk_score": node["risk"]["composite_score"]
        })

    return csv_response(
        "limpopo_monitoring_nodes.csv",
        list(rows[0].keys()) if rows else [],
        rows
    )


@app.get("/download/nodes.geojson")
async def download_nodes_geojson():
    payload = LATEST_STATE or await build_dashboard_payload()
    data = json.dumps(payload["nodes_geojson"], indent=2)

    return StreamingResponse(
        iter([data]),
        media_type="application/geo+json",
        headers={
            "Content-Disposition": 'attachment; filename="limpopo_monitoring_nodes.geojson"'
        }
    )


@app.get("/download/subbasins.geojson")
async def download_subbasins_geojson():
    payload = LATEST_STATE or await build_dashboard_payload()
    data = json.dumps(payload["subbasins_geojson"], indent=2)

    return StreamingResponse(
        iter([data]),
        media_type="application/geo+json",
        headers={
            "Content-Disposition": 'attachment; filename="limpopo_subbasin_planning_zones.geojson"'
        }
    )


@app.get("/download/project-data.zip")
async def download_project_zip():
    payload = LATEST_STATE or await build_dashboard_payload()

    memory = io.BytesIO()

    with zipfile.ZipFile(memory, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "basin_summary.json",
            json.dumps(payload["basin_summary"], indent=2)
        )

        archive.writestr(
            "subbasin_summary.json",
            json.dumps(payload["subbasins"], indent=2)
        )

        archive.writestr(
            "monitoring_nodes.geojson",
            json.dumps(payload["nodes_geojson"], indent=2)
        )

        archive.writestr(
            "subbasin_planning_zones.geojson",
            json.dumps(payload["subbasins_geojson"], indent=2)
        )

        archive.writestr(
            "full_dashboard_api_response.json",
            json.dumps(payload, indent=2)
        )

    memory.seek(0)

    return StreamingResponse(
        memory,
        media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="limpopo_basin_dashboard_downloads.zip"'
        }
    )


# ============================================================
# DASHBOARD HTML
# ============================================================

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Limpopo Basin and Sub-basin Explorer</title>

<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

<style>
:root {
    --dark: #0f172a;
    --blue: #2563eb;
    --teal: #0d9488;
    --green: #10b981;
    --amber: #f59e0b;
    --orange: #f97316;
    --red: #e11d48;
    --gray: #64748b;
}

body {
    margin: 0;
    font-family: Inter, Arial, sans-serif;
    background: #f1f5f9;
    color: #0f172a;
}

.hero {
    background: linear-gradient(135deg, #020617, #1e3a8a, #0f766e);
    color: white;
    padding: 28px 36px;
}

.hero h1 {
    margin: 0;
    font-size: 30px;
}

.hero p {
    margin: 7px 0 0 0;
    color: #cbd5e1;
}

.layout {
    display: flex;
    min-height: calc(100vh - 100px);
}

.sidebar {
    width: 350px;
    background: white;
    padding: 20px;
    box-sizing: border-box;
    border-right: 1px solid #e2e8f0;
}

.workspace {
    flex: 1;
    padding: 22px;
    box-sizing: border-box;
    overflow-y: auto;
}

.card {
    background: white;
    padding: 18px;
    margin-bottom: 18px;
    border-radius: 14px;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.06);
}

.controls {
    margin-bottom: 16px;
}

label {
    display: block;
    font-size: 12px;
    font-weight: 800;
    text-transform: uppercase;
    color: #475569;
    margin-bottom: 6px;
}

select, button {
    width: 100%;
    box-sizing: border-box;
    padding: 10px;
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    font-size: 14px;
}

button {
    background: var(--dark);
    color: white;
    cursor: pointer;
    font-weight: 800;
    border: none;
}

button:hover {
    background: #334155;
}

.metrics {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 16px;
}

.metric {
    font-size: 28px;
    font-weight: 900;
    margin-top: 5px;
}

.small {
    font-size: 12px;
    color: #64748b;
    line-height: 1.5;
}

.tabs {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 18px 0;
}

.tab {
    width: auto;
    background: #e2e8f0;
    color: #0f172a;
}

.tab.active {
    background: #0f172a;
    color: white;
}

.panel {
    display: none;
}

.panel.active {
    display: block;
}

#map {
    height: 590px;
    border-radius: 12px;
}

table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}

th, td {
    padding: 10px;
    border-bottom: 1px solid #e2e8f0;
    text-align: left;
}

th {
    background: #f8fafc;
    color: #475569;
    font-size: 11px;
    text-transform: uppercase;
}

.badge {
    padding: 4px 9px;
    border-radius: 999px;
    color: white;
    font-size: 11px;
    font-weight: 800;
}

.Low { background: var(--green); }
.Moderate { background: var(--amber); }
.High { background: var(--orange); }
.Veryhigh { background: var(--red); }

.status {
    color: var(--blue);
    font-weight: 800;
    font-size: 13px;
}

.download-link {
    display: block;
    margin-bottom: 10px;
    background: #eff6ff;
    color: #1d4ed8;
    text-decoration: none;
    border-radius: 7px;
    padding: 10px;
    font-size: 13px;
    font-weight: 700;
}

.legend {
    background: white;
    padding: 10px;
    border-radius: 8px;
    font-size: 12px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.12);
}

.legend span {
    display: inline-block;
    width: 13px;
    height: 13px;
    border-radius: 50%;
    margin-right: 6px;
}

pre {
    max-height: 450px;
    overflow: auto;
    background: #0f172a;
    color: #e2e8f0;
    padding: 14px;
    border-radius: 10px;
    font-size: 12px;
}

@media(max-width: 900px) {
    .layout {
        flex-direction: column;
    }

    .sidebar {
        width: 100%;
        border-right: none;
        border-bottom: 1px solid #e2e8f0;
    }
}
</style>
</head>

<body>
<div class="hero">
    <h1>Limpopo Basin and Sub-basin Explorer</h1>
    <p>Basin-wide and sub-basin-scale climate, flood, drought, LULC, population, risk, prediction and download dashboard</p>
</div>

<div class="layout">
    <aside class="sidebar">
        <div class="controls">
            <label>Climate forecast period</label>
            <select id="forecastDays">
                <option value="3">3 days</option>
                <option value="7">7 days</option>
                <option value="16" selected>16 days</option>
            </select>
        </div>

        <div class="controls">
            <label>Flood forecast period</label>
            <select id="floodDays">
                <option value="7">7 days</option>
                <option value="14">14 days</option>
                <option value="30" selected>30 days</option>
            </select>
        </div>

        <div class="controls">
            <label>Sub-basin map layer</label>
            <select id="mapLayer" onchange="drawMap()">
                <option value="composite">Composite risk</option>
                <option value="rainfall">Rainfall</option>
                <option value="et0">ET0</option>
                <option value="balance">Water balance</option>
                <option value="flood">Flood risk</option>
                <option value="drought">Drought risk</option>
                <option value="population">Population exposed</option>
                <option value="lulc">LULC pressure</option>
                <option value="temperature">Temperature</option>
                <option value="soil">Soil saturation proxy</option>
            </select>
        </div>

        <button onclick="loadDashboard()">Refresh basin dashboard</button>
        <p id="status" class="status">Initialising basin dashboard...</p>

        <div class="card">
            <strong>Boundary information</strong>
            <p class="small">
                The current sub-basin polygons are planning-zone placeholders for visual demonstration.
                Replace them with official HydroBASINS GeoJSON polygons before formal scientific analysis.
            </p>
        </div>

        <div class="card">
            <strong>Download Centre</strong>
            <br><br>
            <a class="download-link" href="/download/basin-summary.csv">Download Basin Summary CSV</a>
            <a class="download-link" href="/download/subbasin-summary.csv">Download Sub-basin Summary CSV</a>
            <a class="download-link" href="/download/nodes.csv">Download Monitoring Nodes CSV</a>
            <a class="download-link" href="/download/nodes.geojson">Download Monitoring Nodes GeoJSON</a>
            <a class="download-link" href="/download/subbasins.geojson">Download Sub-basin GeoJSON</a>
            <a class="download-link" href="/download/project-data.zip">Download Full Project ZIP</a>
        </div>
    </aside>

    <main class="workspace">
        <section class="metrics">
            <div class="card">
                <label>Total exposed population</label>
                <div class="metric" id="populationMetric">---</div>
                <div class="small">People exposed to combined risk conditions</div>
            </div>

            <div class="card">
                <label>Mean composite risk</label>
                <div class="metric" id="riskMetric">---</div>
                <div class="small">0 = low risk, 100 = critical</div>
            </div>

            <div class="card">
                <label>Mean water balance</label>
                <div class="metric" id="balanceMetric">---</div>
                <div class="small">Forecast rainfall minus ET0</div>
            </div>

            <div class="card">
                <label>Mean peak discharge</label>
                <div class="metric" id="floodMetric">---</div>
                <div class="small">m³/s screening indicator</div>
            </div>
        </section>

        <div class="tabs">
            <button class="tab active" onclick="openTab('mapPanel', this)">Basin Map</button>
            <button class="tab" onclick="openTab('overviewPanel', this)">Basin Overview</button>
            <button class="tab" onclick="openTab('subbasinPanel', this)">Sub-basin Explorer</button>
            <button class="tab" onclick="openTab('plotsPanel', this)">Climate and Risk Plots</button>
            <button class="tab" onclick="openTab('predictionPanel', this)">Prediction</button>
            <button class="tab" onclick="openTab('apiPanel', this)">API Data</button>
        </div>

        <section id="mapPanel" class="panel active">
            <div class="card">
                <div id="map"></div>
            </div>
        </section>

        <section id="overviewPanel" class="panel">
            <div class="card" style="overflow-x:auto;">
                <h3>Basin-scale sub-basin summary</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Sub-basin</th>
                            <th>Countries</th>
                            <th>Population</th>
                            <th>Exposed</th>
                            <th>Rain</th>
                            <th>ET0</th>
                            <th>Balance</th>
                            <th>Peak discharge</th>
                            <th>Drought</th>
                            <th>Flood</th>
                            <th>LULC</th>
                            <th>Composite risk</th>
                        </tr>
                    </thead>
                    <tbody id="subbasinRows"></tbody>
                </table>
            </div>
        </section>

        <section id="subbasinPanel" class="panel">
            <div class="card">
                <label>Select sub-basin</label>
                <select id="subbasinSelect" onchange="showSubbasin()"></select>
            </div>

            <div class="metrics">
                <div class="card">
                    <label>Sub-basin population exposed</label>
                    <div class="metric" id="subPopulationMetric">---</div>
                </div>

                <div class="card">
                    <label>Sub-basin composite risk</label>
                    <div class="metric" id="subRiskMetric">---</div>
                </div>

                <div class="card">
                    <label>Sub-basin water balance</label>
                    <div class="metric" id="subBalanceMetric">---</div>
                </div>

                <div class="card">
                    <label>Sub-basin peak discharge</label>
                    <div class="metric" id="subFloodMetric">---</div>
                </div>
            </div>

            <div class="card">
                <div id="subbasinClimatePlot"></div>
            </div>

            <div class="card">
                <div id="subbasinRiskPlot"></div>
            </div>

            <div class="card" style="overflow-x:auto;">
                <h3>Monitoring nodes inside selected sub-basin</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Node</th>
                            <th>Rainfall</th>
                            <th>Water balance</th>
                            <th>Flood risk</th>
                            <th>Drought risk</th>
                            <th>Population exposed</th>
                            <th>Composite risk</th>
                        </tr>
                    </thead>
                    <tbody id="subbasinNodeRows"></tbody>
                </table>
            </div>
        </section>

        <section id="plotsPanel" class="panel">
            <div class="card"><div id="climatePlot"></div></div>
            <div class="card"><div id="floodPlot"></div></div>
            <div class="card"><div id="droughtPlot"></div></div>
            <div class="card"><div id="populationPlot"></div></div>
            <div class="card"><div id="lulcPlot"></div></div>
            <div class="card"><div id="riskPlot"></div></div>
        </section>

        <section id="predictionPanel" class="panel">
            <div class="card">
                <label>Prediction spatial scope</label>
                <select id="predictionScope"></select>
                <br><br>

                <label>Prediction horizon</label>
                <select id="predictionDays">
                    <option value="30">30 days</option>
                    <option value="90">90 days</option>
                    <option value="180">180 days</option>
                    <option value="365" selected>365 days / 1 year</option>
                </select>
                <br><br>

                <label>Historical baseline</label>
                <select id="historyYears">
                    <option value="3">3 years</option>
                    <option value="5">5 years</option>
                    <option value="10">10 years</option>
                </select>
                <br><br>

                <button onclick="loadPrediction()">Generate prediction</button>
                <p id="predictionStatus" class="status"></p>
            </div>

            <div class="card"><div id="predictionClimatePlot"></div></div>
            <div class="card"><div id="predictionBalancePlot"></div></div>
        </section>

        <section id="apiPanel" class="panel">
            <div class="card">
                <p><a href="/api/basin-dashboard" target="_blank">Open Basin Dashboard API</a></p>
                <p><a href="/api/locations" target="_blank">Open Monitoring Nodes API</a></p>
                <p><a href="/docs" target="_blank">Open API Documentation</a></p>
                <pre id="apiBox">Loading API response...</pre>
            </div>
        </section>
    </main>
</div>

<script>
let map;
let subbasinLayer;
let nodeLayer;
let latestData = null;

function fmt(value, decimals = 2) {
    if (value === null || value === undefined || Number.isNaN(value)) return "N/A";
    return typeof value === "number" ? value.toFixed(decimals) : value;
}

function badge(label) {
    const className = label === "Very high" ? "Veryhigh" : label;
    return `<span class="badge ${className}">${label}</span>`;
}

function openTab(id, button) {
    document.querySelectorAll(".panel").forEach(panel => panel.classList.remove("active"));
    document.querySelectorAll(".tab").forEach(tab => tab.classList.remove("active"));

    document.getElementById(id).classList.add("active");
    button.classList.add("active");

    if (id === "mapPanel" && map) {
        setTimeout(() => map.invalidateSize(), 250);
    }
}

function initialiseMap() {
    map = L.map("map").setView([-23.7, 30.0], 6);

    const osm = L.tileLayer(
        "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        { attribution: "OpenStreetMap contributors" }
    ).addTo(map);

    const topo = L.tileLayer(
        "https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        { attribution: "OpenTopoMap contributors" }
    );

    L.control.layers({
        "OpenStreetMap": osm,
        "Topographic map": topo
    }).addTo(map);

    subbasinLayer = L.layerGroup().addTo(map);
    nodeLayer = L.layerGroup().addTo(map);

    const legend = L.control({ position: "bottomright" });

    legend.onAdd = function() {
        const div = L.DomUtil.create("div", "legend");
        div.innerHTML = `
            <strong>Risk intensity</strong><br>
            <span style="background:#10b981"></span> Low<br>
            <span style="background:#f59e0b"></span> Moderate<br>
            <span style="background:#f97316"></span> High<br>
            <span style="background:#e11d48"></span> Very high
        `;
        return div;
    };

    legend.addTo(map);
}

function mapValue(subbasin, layer) {
    if (layer === "composite") return subbasin.risk.composite_score;
    if (layer === "rainfall") return subbasin.climate.rainfall_mm;
    if (layer === "et0") return subbasin.climate.et0_mm;
    if (layer === "balance") return subbasin.climate.water_balance_mm;
    if (layer === "flood") return subbasin.flood.mean_flood_risk_score;
    if (layer === "drought") return subbasin.drought.mean_drought_risk_score;
    if (layer === "population") return subbasin.population.exposed;
    if (layer === "lulc") return subbasin.lulc.mean_lulc_pressure_score;
    if (layer === "temperature") return subbasin.climate.mean_temperature_c;
    if (layer === "soil") return subbasin.climate.soil_saturation_proxy;
    return 0;
}

function valueColor(value, maxValue, layer) {
    if (layer === "balance") {
        if (value < -150) return "#e11d48";
        if (value < -60) return "#f97316";
        if (value < 0) return "#f59e0b";
        return "#10b981";
    }

    if (layer === "soil") {
        const ratio = maxValue <= 0 ? 0 : value / maxValue;
        if (ratio < 0.35) return "#e11d48";
        if (ratio < 0.60) return "#f59e0b";
        return "#10b981";
    }

    const ratio = maxValue <= 0 ? 0 : value / maxValue;

    if (ratio >= 0.75) return "#e11d48";
    if (ratio >= 0.50) return "#f97316";
    if (ratio >= 0.25) return "#f59e0b";
    return "#10b981";
}

function drawMap() {
    if (!latestData || !map) return;

    subbasinLayer.clearLayers();
    nodeLayer.clearLayers();

    const selectedLayer = document.getElementById("mapLayer").value;
    const values = latestData.subbasins.map(item => Math.abs(mapValue(item, selectedLayer)));
    const maxValue = Math.max(...values, 1);

    L.geoJSON(latestData.subbasins_geojson, {
        style: function(feature) {
            const item = latestData.subbasins.find(
                s => s.id === feature.properties.id
            );

            const value = mapValue(item, selectedLayer);

            return {
                color: "#334155",
                weight: 1.4,
                fillColor: valueColor(value, maxValue, selectedLayer),
                fillOpacity: 0.48
            };
        },
        onEachFeature: function(feature, layer) {
            const item = latestData.subbasins.find(
                s => s.id === feature.properties.id
            );

            const value = mapValue(item, selectedLayer);

            layer.bindPopup(`
                <strong>${item.name}</strong><br>
                <small>${item.countries}</small><hr>
                <b>Selected layer:</b> ${fmt(value)}<br>
                <b>Composite risk:</b> ${fmt(item.risk.composite_score)}<br>
                <b>Rainfall:</b> ${fmt(item.climate.rainfall_mm)} mm<br>
                <b>ET0:</b> ${fmt(item.climate.et0_mm)} mm<br>
                <b>Water balance:</b> ${fmt(item.climate.water_balance_mm)} mm<br>
                <b>Peak discharge:</b> ${fmt(item.flood.peak_discharge_m3s)} m³/s<br>
                <b>Population exposed:</b> ${item.population.exposed.toLocaleString()}<br>
                <b>LULC pressure:</b> ${fmt(item.lulc.mean_lulc_pressure_score)}
            `);
        }
    }).addTo(subbasinLayer);

    latestData.nodes.forEach(node => {
        const marker = L.circleMarker(
            [node.coordinates.lat, node.coordinates.lon],
            {
                radius: 7 + node.risk.composite_score / 10,
                color: "#0f172a",
                fillColor: "#ffffff",
                fillOpacity: 0.9,
                weight: 2
            }
        ).addTo(nodeLayer);

        marker.bindPopup(`
            <strong>${node.name}</strong><br>
            <small>${node.country}</small><hr>
            <b>Composite risk:</b> ${fmt(node.risk.composite_score)}<br>
            <b>Drought:</b> ${badge(node.drought.drought_risk_class)}<br>
            <b>Flood:</b> ${badge(node.flood.flood_risk_class)}<br>
            <b>Population exposed:</b> ${node.population.population_exposed.toLocaleString()}<br>
            <b>Rainfall:</b> ${fmt(node.climate.rainfall_total_mm)} mm
        `);
    });

    const bounds = subbasinLayer.getBounds();

    if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [25, 25] });
    }
}

async function loadDashboard() {
    const status = document.getElementById("status");

    const forecastDays = document.getElementById("forecastDays").value;
    const floodDays = document.getElementById("floodDays").value;

    status.textContent = "Loading basin and sub-basin data. First run may take a few minutes...";

    try {
        const response = await fetch(
            `/api/basin-dashboard?forecast_days=${forecastDays}&flood_days=${floodDays}`
        );

        latestData = await response.json();

        document.getElementById("apiBox").textContent =
            JSON.stringify(latestData, null, 2);

        document.getElementById("populationMetric").textContent =
            latestData.basin_summary.total_population_exposed.toLocaleString();

        document.getElementById("riskMetric").textContent =
            fmt(latestData.basin_summary.mean_composite_risk) + " / 100";

        document.getElementById("balanceMetric").textContent =
            fmt(latestData.basin_summary.mean_water_balance_mm) + " mm";

        document.getElementById("floodMetric").textContent =
            fmt(latestData.basin_summary.mean_peak_discharge_m3s) + " m³/s";

        fillSubbasinTable();
        fillSubbasinSelector();
        fillPredictionSelector();
        drawMap();
        drawMainPlots();
        showSubbasin();

        status.textContent = "Basin dashboard updated successfully.";
    } catch (error) {
        status.textContent = "Dashboard loading error: " + error;
    }
}

function fillSubbasinTable() {
    let html = "";

    latestData.subbasins.forEach(item => {
        html += `
            <tr>
                <td><strong>${item.name}</strong></td>
                <td>${item.countries}</td>
                <td>${item.population.total.toLocaleString()}</td>
                <td>${item.population.exposed.toLocaleString()}</td>
                <td>${fmt(item.climate.rainfall_mm)}</td>
                <td>${fmt(item.climate.et0_mm)}</td>
                <td>${fmt(item.climate.water_balance_mm)}</td>
                <td>${fmt(item.flood.peak_discharge_m3s)}</td>
                <td>${badge(item.drought.drought_risk_class)}</td>
                <td>${badge(item.flood.flood_risk_class)}</td>
                <td>${fmt(item.lulc.mean_lulc_pressure_score)}</td>
                <td><strong>${fmt(item.risk.composite_score)}</strong></td>
            </tr>
        `;
    });

    document.getElementById("subbasinRows").innerHTML = html;
}

function fillSubbasinSelector() {
    const select = document.getElementById("subbasinSelect");
    select.innerHTML = "";

    latestData.subbasins.forEach(item => {
        const option = document.createElement("option");
        option.value = item.id;
        option.textContent = item.name;
        select.appendChild(option);
    });
}

function fillPredictionSelector() {
    const select = document.getElementById("predictionScope");
    select.innerHTML = "";

    const basinOption = document.createElement("option");
    basinOption.value = "basin";
    basinOption.textContent = "Entire Limpopo Basin";
    select.appendChild(basinOption);

    latestData.subbasins.forEach(item => {
        const option = document.createElement("option");
        option.value = item.id;
        option.textContent = item.name;
        select.appendChild(option);
    });
}

function plotLayout(title, bottom = 110) {
    return {
        title: title,
        paper_bgcolor: "white",
        plot_bgcolor: "white",
        font: { family: "Inter, Arial, sans-serif", color: "#0f172a" },
        margin: { l: 60, r: 30, t: 55, b: bottom },
        xaxis: { gridcolor: "#f1f5f9" },
        yaxis: { gridcolor: "#f1f5f9" }
    };
}

function drawMainPlots() {
    const subbasins = latestData.subbasins;
    const names = subbasins.map(item => item.name);

    Plotly.newPlot("climatePlot", [
        {
            x: names,
            y: subbasins.map(item => item.climate.rainfall_mm),
            type: "bar",
            name: "Rainfall"
        },
        {
            x: names,
            y: subbasins.map(item => item.climate.et0_mm),
            type: "bar",
            name: "ET0"
        },
        {
            x: names,
            y: subbasins.map(item => item.climate.water_balance_mm),
            type: "bar",
            name: "Water balance"
        }
    ], {
        ...plotLayout("Sub-basin climate water budget"),
        barmode: "group"
    }, { responsive: true });

    Plotly.newPlot("floodPlot", [
        {
            x: names,
            y: subbasins.map(item => item.flood.peak_discharge_m3s),
            type: "bar",
            name: "Peak discharge"
        },
        {
            x: names,
            y: subbasins.map(item => item.flood.mean_flood_risk_score),
            type: "bar",
            name: "Flood risk"
        }
    ], {
        ...plotLayout("Flood discharge and flood risk by sub-basin"),
        barmode: "group"
    }, { responsive: true });

    Plotly.newPlot("droughtPlot", [
        {
            x: names,
            y: subbasins.map(item => item.drought.mean_drought_risk_score),
            type: "bar",
            name: "Drought risk"
        },
        {
            x: names,
            y: subbasins.map(item => item.climate.soil_saturation_proxy),
            type: "bar",
            name: "Soil saturation proxy"
        }
    ], {
        ...plotLayout("Drought and soil saturation conditions"),
        barmode: "group"
    }, { responsive: true });

    Plotly.newPlot("populationPlot", [
        {
            x: names,
            y: subbasins.map(item => item.population.total),
            type: "bar",
            name: "Population"
        },
        {
            x: names,
            y: subbasins.map(item => item.population.exposed),
            type: "bar",
            name: "Population exposed"
        }
    ], {
        ...plotLayout("Population and exposure by sub-basin"),
        barmode: "group"
    }, { responsive: true });

    const lulcCategories = ["urban", "cropland", "grassland", "forest", "water", "bare"];

    Plotly.newPlot("lulcPlot",
        lulcCategories.map(category => ({
            x: names,
            y: subbasins.map(item => item.lulc.profile_percent[category]),
            type: "bar",
            name: category
        })),
        {
            ...plotLayout("Sub-basin LULC composition"),
            barmode: "stack",
            yaxis: { title: "Percent" }
        },
        { responsive: true }
    );

    Plotly.newPlot("riskPlot", [
        {
            x: names,
            y: subbasins.map(item => item.risk.composite_score),
            type: "bar",
            name: "Composite risk"
        },
        {
            x: names,
            y: subbasins.map(item => item.lulc.mean_lulc_pressure_score),
            type: "bar",
            name: "LULC pressure"
        },
        {
            x: names,
            y: subbasins.map(item => item.reservoir.mean_reservoir_stress_score),
            type: "bar",
            name: "Reservoir stress"
        }
    ], {
        ...plotLayout("Composite, LULC and reservoir risk"),
        barmode: "group"
    }, { responsive: true });
}

function showSubbasin() {
    if (!latestData) return;

    const id = document.getElementById("subbasinSelect").value;
    const item = latestData.subbasins.find(subbasin => subbasin.id === id);

    if (!item) return;

    document.getElementById("subPopulationMetric").textContent =
        item.population.exposed.toLocaleString();

    document.getElementById("subRiskMetric").textContent =
        fmt(item.risk.composite_score) + " / 100";

    document.getElementById("subBalanceMetric").textContent =
        fmt(item.climate.water_balance_mm) + " mm";

    document.getElementById("subFloodMetric").textContent =
        fmt(item.flood.peak_discharge_m3s) + " m³/s";

    Plotly.newPlot("subbasinClimatePlot", [
        {
            x: ["Rainfall", "ET0", "Water Balance", "Temperature", "Wind"],
            y: [
                item.climate.rainfall_mm,
                item.climate.et0_mm,
                item.climate.water_balance_mm,
                item.climate.mean_temperature_c,
                item.climate.max_wind_kmh
            ],
            type: "bar"
        }
    ], plotLayout(`Climate indicators: ${item.name}`, 70), { responsive: true });

    Plotly.newPlot("subbasinRiskPlot", [
        {
            x: ["Climate", "Flood", "Drought", "LULC", "Reservoir", "Composite"],
            y: [
                item.climate.climate_risk_score,
                item.flood.mean_flood_risk_score,
                item.drought.mean_drought_risk_score,
                item.lulc.mean_lulc_pressure_score,
                item.reservoir.mean_reservoir_stress_score,
                item.risk.composite_score
            ],
            type: "bar"
        }
    ], plotLayout(`Risk components: ${item.name}`, 70), { responsive: true });

    const relatedNodes = latestData.nodes.filter(node => node.subbasin === id);
    let html = "";

    relatedNodes.forEach(node => {
        html += `
            <tr>
                <td><strong>${node.name}</strong></td>
                <td>${fmt(node.climate.rainfall_total_mm)} mm</td>
                <td>${fmt(node.climate.water_balance_mm)} mm</td>
                <td>${badge(node.flood.flood_risk_class)}</td>
                <td>${badge(node.drought.drought_risk_class)}</td>
                <td>${node.population.population_exposed.toLocaleString()}</td>
                <td><strong>${fmt(node.risk.composite_score)}</strong></td>
            </tr>
        `;
    });

    document.getElementById("subbasinNodeRows").innerHTML = html;
}

async function loadPrediction() {
    const status = document.getElementById("predictionStatus");
    const scope = document.getElementById("predictionScope").value;
    const predictionDays = document.getElementById("predictionDays").value;
    const historyYears = document.getElementById("historyYears").value;

    status.textContent = "Preparing climatology prediction. This may take time on the first request...";

    try {
        const response = await fetch(
            `/api/prediction/${scope}?prediction_days=${predictionDays}&history_years=${historyYears}`
        );

        const prediction = await response.json();

        if (prediction.error) {
            status.textContent = prediction.error;
            return;
        }

        Plotly.newPlot("predictionClimatePlot", [
            {
                x: prediction.dates,
                y: prediction.predicted_rainfall_mm,
                type: "scatter",
                mode: "lines",
                name: "Predicted rainfall"
            },
            {
                x: prediction.dates,
                y: prediction.predicted_et0_mm,
                type: "scatter",
                mode: "lines",
                name: "Predicted ET0"
            },
            {
                x: prediction.dates,
                y: prediction.predicted_temperature_c,
                type: "scatter",
                mode: "lines",
                name: "Predicted temperature"
            }
        ], {
            ...plotLayout(`Prediction climate series: ${prediction.scope_name}`, 70),
            yaxis: { title: "Mixed units" }
        }, { responsive: true });

        Plotly.newPlot("predictionBalancePlot", [
            {
                x: prediction.dates,
                y: prediction.predicted_water_balance_mm,
                type: "scatter",
                mode: "lines",
                fill: "tozeroy",
                name: "Predicted water balance"
            }
        ], {
            ...plotLayout(`Prediction water balance: ${prediction.scope_name}`, 70),
            yaxis: { title: "Rainfall - ET0, mm/day" }
        }, { responsive: true });

        status.textContent =
            `Prediction completed: ${prediction.prediction_days} days using ${prediction.history_years}-year historical climatology.`;
    } catch (error) {
        status.textContent = "Prediction error: " + error;
    }
}

document.addEventListener("DOMContentLoaded", function() {
    initialiseMap();
    loadDashboard();
});
</script>
</body>
</html>
    """


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

