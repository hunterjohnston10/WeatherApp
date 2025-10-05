import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Dict, List, Optional, Tuple

import requests


# ------------------------------------------------------------
# Unified variable routing for Open-Meteo (weather/air quality/UV)
# Inputs: variable, location(lat,lon), mode(history/forecast/both), time range
# Output: unified schema {metadata, units, data}
# ------------------------------------------------------------


@dataclass(frozen=True)
class VariableSpec:
    category: str  # "weather" | "air_quality" | "uv"
    historical_url: str
    forecast_url: str
    param_kind: str  # "hourly" | "daily"
    api_var_name: str


# Open-Meteo endpoints
OPEN_METEO_WEATHER_FORECAST = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_WEATHER_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_AIR_QUALITY = "https://air-quality-api.open-meteo.com/v1/air-quality"


# Variable map (add more as needed)
VARIABLES: Dict[str, VariableSpec] = {
    # Weather (hourly)
    "temperature_2m": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="temperature_2m",
    ),
    "apparent_temperature": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="apparent_temperature",
    ),
    "relative_humidity_2m": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="relative_humidity_2m",
    ),
    "precipitation": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="precipitation",
    ),
    "precipitation_probability": VariableSpec(
        category='weather',
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="precipitation_probability",
    ),
    "snowfall": VariableSpec(
        category='weather',
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="snowfall",
    ),
    "pressure_msl": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="pressure_msl",
    ),
    "wind_speed_10m": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="wind_speed_10m",
    ),
    "wind_gusts_10m": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="wind_gusts_10m",
    ),
    "wind_direction_10m": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="wind_direction_10m",
    ),
    "visibility": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="visibility",
    ),
    "cloud_cover": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="cloud_cover",
    ),
    "evapotranspiration": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="evapotranspiration",
    ),
    "weather_code": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="hourly",
        api_var_name="weather_code",
    ),

    # Air quality (hourly)
    "pm2_5": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="pm2_5",
    ),
    "pm10": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="pm10",
    ),
    "nitrogen_dioxide": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="nitrogen_dioxide",
    ),
    "carbon_monoxide": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="carbon_monoxide",
    ),
    "ozone": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="ozone",
    ),
    "sulphur_dioxide": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="sulphur_dioxide",
    ),
    "carbon_dioxide": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="carbon_dioxide",
    ),
    "us_aqi": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="us_aqi",
    ),
    "us_aqi_pm2_5": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="us_aqi_pm2_5",
    ),
    "us_aqi_pm10": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="us_aqi_pm10",
    ),
    "us_aqi_nitrogen_dioxide": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="us_aqi_nitrogen_dioxide",
    ),
    "us_aqi_ozone": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="us_aqi_ozone",
    ),
    "us_aqi_sulphur_dioxide": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="us_aqi_sulphur_dioxide",
    ),
    "us_aqi_carbon_monoxide": VariableSpec(
        category="air_quality",
        historical_url=OPEN_METEO_AIR_QUALITY,
        forecast_url=OPEN_METEO_AIR_QUALITY,
        param_kind="hourly",
        api_var_name="us_aqi_carbon_monoxide",
    ),

    # UV (daily)
    "uv_index_max": VariableSpec(
        category="uv",
        historical_url=OPEN_METEO_WEATHER_FORECAST,  # Open-Meteo allows historical via forecast endpoint for ERA5
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="uv_index_max",
    ),

    # Weather (daily)
    "temperature_2m_max": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="temperature_2m_max",
    ),
    "temperature_2m_min": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="temperature_2m_min",
    ),
    "apparent_temperature_max": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="apparent_temperature_max",
    ),
    "apparent_temperature_min": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="apparent_temperature_min",
    ),
    "precipitation_sum": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="precipitation_sum",
    ),
    "rain_sum": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="rain_sum",
    ),
    "showers_sum": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="showers_sum",
    ),
    "snowfall_sum": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="snowfall_sum",
    ),
    "precipitation_hours": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="precipitation_hours",
    ),
    "precipitation_probability_max": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="precipitation_probability_max",
    ),
    "precipitation_probability_mean": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="precipitation_probability_mean",
    ),
    "precipitation_probability_min": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="precipitation_probability_min",
    ),
    "weather_code_daily": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="weather_code",
    ),
    "sunrise": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="sunrise",
    ),
    "sunset": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="sunset",
    ),
    "wind_speed_10m_max": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="wind_speed_10m_max",
    ),
    "wind_gusts_10m_max": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="wind_gusts_10m_max",
    ),
    "wind_direction_10m_dominant": VariableSpec(
        category="weather",
        historical_url=OPEN_METEO_WEATHER_ARCHIVE,
        forecast_url=OPEN_METEO_WEATHER_FORECAST,
        param_kind="daily",
        api_var_name="wind_direction_10m_dominant",
    ),
}


def _parse_location(location: str) -> Tuple[float, float]:
    lat_str, lon_str = location.split(",", 1)
    return float(lat_str.strip()), float(lon_str.strip())


def _request(url: str, params: Dict) -> Dict:
    resp = requests.get(url, params=params, timeout=90.0)
    resp.raise_for_status()
    return resp.json()


def _year_chunks(start: datetime, end: datetime) -> List[Tuple[datetime, datetime]]:
    chunks: List[Tuple[datetime, datetime]] = []
    cur = datetime(start.year, 1, 1, tzinfo=UTC)
    # first chunk may start after Jan 1
    first_start = start
    first_end = min(end, datetime(start.year, 12, 31, tzinfo=UTC))
    chunks.append((first_start, first_end))
    y = start.year + 1
    while y < end.year:
        chunks.append((datetime(y, 1, 1, tzinfo=UTC), datetime(y, 12, 31, tzinfo=UTC)))
        y += 1
    if end.year > start.year:
        chunks.append((datetime(end.year, 1, 1, tzinfo=UTC), end))
    # Coalesce trivial overlaps
    merged: List[Tuple[datetime, datetime]] = []
    for s, e in chunks:
        if not merged or s > merged[-1][1] + timedelta(days=0):
            merged.append((s, e))
        else:
            prev_s, prev_e = merged[-1]
            merged[-1] = (prev_s, max(prev_e, e))
    return merged


def _fetch_segment(lat: float, lon: float, spec: VariableSpec, start: datetime, end: datetime, is_history: bool) -> Dict:
    url = spec.historical_url if is_history else spec.forecast_url
    params: Dict[str, str] = {
        "latitude": f"{lat}",
        "longitude": f"{lon}",
        "timezone": "UTC",
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
    }
    if spec.param_kind == "hourly":
        params["hourly"] = spec.api_var_name
    else:
        params["daily"] = spec.api_var_name
    # For explicit historical air-quality, hint CAMS domain
    if spec.category == "air_quality" and is_history:
        params["domains"] = "cams_global"
    return _request(url, params)


def _merge_results(spec: VariableSpec, parts: List[Dict]) -> Dict:
    if not parts:
        return {}
    # Initialize with first part
    base = {k: v for k, v in parts[0].items()}
    if spec.param_kind == "hourly":
        key = "hourly"
        units_key = "hourly_units"
    else:
        key = "daily"
        units_key = "daily_units"
    if key not in base:
        base[key] = {}
    if units_key not in base:
        base[units_key] = {}
    # Merge others
    for p in parts[1:]:
        if key in p:
            for k, arr in p[key].items():
                if k in base[key]:
                    base[key][k].extend(arr)
                else:
                    base[key][k] = list(arr)
        if units_key in p:
            base[units_key].update(p[units_key])
    return base


def fetch_unified(variable: str, location: str, mode: str, start_date: str, end_date: str) -> Dict:
    var = variable.strip()
    if var not in VARIABLES:
        raise ValueError(f"Unknown variable '{var}'. Supported: {', '.join(sorted(VARIABLES.keys()))}")
    spec = VARIABLES[var]

    lat, lon = _parse_location(location)
    s = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
    e = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
    if e < s:
        raise ValueError("end_date is before start_date")

    today = datetime.now(UTC)
    parts: List[Dict] = []

    mode_norm = mode.strip().lower()
    if mode_norm not in ("history", "historical", "forecast", "both"):
        raise ValueError("mode must be 'history', 'historical', 'forecast', or 'both'")

    want_history = mode_norm in ("history", "historical", "both")
    want_forecast = mode_norm in ("forecast", "both")

    # Historical segment(s)
    if want_history and s < today:
        hist_end = min(e, today - timedelta(days=1))
        if s <= hist_end:
            for cs, ce in _year_chunks(s, hist_end):
                parts.append(_fetch_segment(lat, lon, spec, cs, ce, is_history=True))

    # Forecast segment
    if want_forecast and e >= today:
        fc_start = max(s, today)
        if fc_start <= e:
            parts.append(_fetch_segment(lat, lon, spec, fc_start, e, is_history=False))

    if not parts:
        return {"error": "Requested time range produced no segments to query."}

    merged = _merge_results(spec, parts)

    # Normalize to unified schema
    if spec.param_kind == "hourly":
        data_key = "hourly"
        units_key = "hourly_units"
        time_field = "time"
        value_field = spec.api_var_name
        times = merged.get(data_key, {}).get(time_field, [])
        values = merged.get(data_key, {}).get(value_field, [])
        rows = []
        for i, t in enumerate(times):
            rows.append({"timestamp_utc": t, value_field: values[i] if i < len(values) else None})
        units = merged.get(units_key, {})
    else:
        data_key = "daily"
        units_key = "daily_units"
        time_field = "time"
        value_field = spec.api_var_name
        times = merged.get(data_key, {}).get(time_field, [])
        values = merged.get(data_key, {}).get(value_field, [])
        rows = []
        for i, d in enumerate(times):
            rows.append({"date": d, value_field: values[i] if i < len(values) else None})
        units = merged.get(units_key, {})

    result = {
        "metadata": {
            "variable": var,
            "category": spec.category,
            "location": {"latitude": lat, "longitude": lon},
            "mode": mode_norm,
            "start_date": start_date,
            "end_date": end_date,
            "generated_at": datetime.now(UTC).isoformat(),
            "source": "open-meteo",
        },
        "units": units,
        "data": rows,
    }
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Unified fetch: variable + location + mode + time range",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("variable", type=str, help=f"Variable name. One of: {', '.join(sorted(VARIABLES.keys()))}")
    parser.add_argument("location", type=str, help="lat,lon (e.g., '33.94,-84.38')")
    parser.add_argument("mode", type=str, choices=["history", "historical", "forecast", "both"], help="Which data to pull")
    parser.add_argument("start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("end", type=str, help="End date YYYY-MM-DD")
    parser.add_argument("--out", type=str, default=None, help="Optional output JSON file path")

    args = parser.parse_args()

    res = fetch_unified(args.variable, args.location, args.mode, args.start, args.end)
    if "error" in res:
        print(json.dumps(res, indent=2))
        raise SystemExit(1)

    if args.out:
        outdir = os.path.dirname(args.out)
        if outdir:
            os.makedirs(outdir, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2)
        print(os.path.abspath(args.out))
    else:
        print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()


