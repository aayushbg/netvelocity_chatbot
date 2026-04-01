# NV Chatbot - Project Reference

A Rasa Open Source-based chatbot for network velocity KPI analysis and reporting.

---

## Architecture Overview

```
User Message → Rasa NLU (intent + entities) → Rasa Core (rules) → Action Server (actions.py) → MySQL Database
```

---

## Intents (`domain.yml`)

| Intent | Description |
|--------|-------------|
| `greet` | General greeting |
| `goodbye` | Farewell |
| `affirm` / `deny` | Confirmation/negation |
| `mood_great` / `mood_unhappy` | Mood detection |
| `bot_challenge` | Bot identity challenge |
| `get_metrics` | Fetch specific metric values |
| `rank_metrics` | Rank locations by metric |
| `threshold_metrics` | Find locations exceeding/falling below threshold |
| `compare_metrics` | Compare metrics across dimensions |
| `correlation` | Analyze correlation between metrics |
| `top_locations` | Get top N locations by tests/users |
| `breakdown_metrics` | Show metric breakdown by multiple dimensions |

---

## Entities (`domain.yml`)

| Entity | Type | Description |
|--------|------|-------------|
| `metric` | list | KPI to analyze (download, upload, latency, etc.) |
| `time` | list | Time period (last 7 days, last 30 days, etc.) |
| `geo` | list | Location (city, state, zone) |
| `app` | list | Test app (NV SpeedTest, HelloJio SpeedTest) |
| `tech` | list | Network technology (LTE, NR NSA, WiFi, 3G) |
| `band` | list | Frequency band (1800, 2300, n78, n28) |
| `agg` | list | Aggregation type (median, avg, max, min, p90) |
| `compare_time` | list | Comparison time period |
| `threshold` | list | Threshold value for filtering |
| `topN` | list | Number of results (top 5, bottom 10) |
| `dimension` | list | Grouping dimension (states, cities, zones, JCs) |
| `granularity` | list | Data granularity |
| `operator` | list | Comparison operator (<, >, <=, >=) |

---

## Slot Mappings

All slots are typed as `list` and mapped from entities:
- `type: from_entity`
- `entity: <entity_name>`

---

## Rules (`rules.yml`)

Each intent maps to `action_kpi_router` which delegates to specific handlers:

```yaml
- rule: Handle metric lookup requests
  steps:
    - intent: get_metrics
    - action: action_kpi_router
```

---

## Database Schema

### Tables Used
- `netvelocity_kpi_metrics` - KPI data (DLRATE, ULRATE, MINLATENCY, JITTER, etc.)
- `netvelocity_user_metrics` - User/test counts

### Key Columns
| Column | Description |
|--------|-------------|
| `GEOGRAPHY_NAME` | Location name |
| `CREATEDON` | Date of measurement |
| `TESTTYPE` | App name |
| `BAND` | Frequency band |
| `NETWORKTYPE` | Tech type (LTE, NR, etc.) |
| `DLRATE` | Download speed |
| `ULRATE` | Upload speed |
| `MINLATENCY` | Latency |
| `JITTER` | Jitter |
| `BROWSETIME` | Browse time |
| `PCKTLOSS` | Packet loss |
| `RSRP` | Reference signal received power |
| `SINR` | Signal to noise ratio |

---

## Action Handlers (`actions/actions.py`)

### 1. `ActionKpiRouter` (main router)
Routes requests to specific handlers based on intent.

### 2. `handle_get_metrics`
- **Purpose**: Fetch metric values with optional aggregations
- **Slots used**: metric, time, geo, app, band, tech, agg
- **Aggregations**: median, average, minimum, maximum, P90

### 3. `handle_rank_metrics`
- **Purpose**: Rank locations by metric
- **Slots used**: metric, topN, dimension, time
- **Output**: Ranked list with position

### 4. `handle_threshold_metrics`
- **Purpose**: Find locations where metric exceeds threshold
- **Slots used**: metric, threshold, dimension, time
- **Operators**: <, >, <=, >=, =

### 5. `handle_compare_metrics`
- **Purpose**: Compare metrics across dimensions (tech, band, app, time)
- **Slots used**: metric, time, geo, app, band, tech, compare_time
- **Supports**: Tech comparison (LTE vs NR), Band comparison, Time comparison

### 6. `handle_correlation`
- **Purpose**: Analyze correlation between two metrics
- **Slots used**: metric (list of 2), time, geo, app, band, tech

### 7. `handle_top_locations`
- **Purpose**: Get top N locations by users or tests
- **Slots used**: metric (Users/Tests), topN, dimension, time

### 8. `handle_breakdown_metrics`
- **Purpose**: Show metric breakdown by multiple dimensions
- **Slots used**: metric, time, geo, app, band, tech
- **Output**: Grouped data with averages

---

## Mapping Dictionaries

### METRIC_MAP
```python
"download" / "download speed" → DLRATE
"upload" / "upload speed"    → ULRATE
"latency"                    → MINLATENCY
"jitter"                     → JITTER
"browse time"                → BROWSETIME
"packet loss"                → PCKTLOSS
"rsrp"                       → RSRP
"sinr"                       → SINR
```

### DIMENSION_MAP
```python
"states" / "state"           → STATE
"cities" / "city"            → CITY
"circles" / "circle"         → CIRCLE
"pan india" / "pan_india"    → PANINDIA
"jc" / "jio centre" / "jcs"  → JIOCENTER
"zone" / "zones"             → ZONE
"r4g state" / "r4g"          → R4GSTATE
```

### AGG_MAP
```python
"median" → median
"avg" / "average" → average
"max" / "maximum" → maximum
"min" / "minimum" → minimum
"p90" → P90
```

### TOPN_MAP
```python
"top 3" → 3
"top 5" → 5
"top 10" → 10
```

---

## Time Condition Parsing (`parse_time_condition`)

Converts time phrases to SQL WHERE clauses:

| Input | Output |
|-------|--------|
| `today` | `CREATEDON = '2026-03-25'` |
| `yesterday` | `CREATEDON = '2026-03-24'` |
| `last week` | `CREATEDON >= '2026-03-18'` |
| `last month` | `CREATEDON >= '2026-02-23'` |
| `last 7 days` | `CREATEDON >= '2026-03-18'` |
| `last X days` | `CREATEDON >= '<date>'` |

---

## Response Format

All KPI responses use `build_nv_response()`:

```json
{
  "intent": "get_metrics",
  "confidence": 1.0,
  "technology": null,
  "module": "NV dash",
  "metric": "DLRATE",
  "message": "The average download speed for Ahmedabad during last 30 days is 45.2.",
  "data": 45.2,
  "is_report": false,
  "file_path": null,
  "export_type": null
}
```

---

## Database Connection

```python
host: localhost
user: root
password: Aayu#2520  # Should be moved to environment variables
database: jio
```

---

## Example Queries

### Get metrics
> "What is the download speed in Ahmedabad for last 30 days?"

### Rank metrics
> "Top 5 cities by download speed in last 7 days"

### Threshold
> "Which locations had download speed < 60 in last 7 days?"

### Compare
> "Compare download speed on LTE vs NR in Ahmedabad for last 7 days"

### Top locations
> "Top 10 cities by Number of Users in last 30 days"

### Breakdown
> "breakdown of download speed in Mumbai during last 7 days"