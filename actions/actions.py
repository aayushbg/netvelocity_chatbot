from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.events import AllSlotsReset
from rasa_sdk.executor import CollectingDispatcher
import mysql.connector
from datetime import datetime, timedelta
import logging
import json
import re
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# -------------------------------
# Metric → DB column mapping
# -------------------------------

METRIC_MAP = {
    "download": "DLRATE",
    "download speed": "DLRATE",
    "upload speed": "ULRATE",
    "upload": "ULRATE",
    "latency": "MINLATENCY",
    "jitter": "JITTER",
    "browse time": "BROWSETIME",
    "packet loss": "PCKTLOSS",
    "rsrp": "RSRP",
    "sinr": "SINR"
}

OUTPUT_METRIC_MAP = {
    "DLRATE" : "download speed",
    "ULRATE" : "upload speed",
    "MINLATENCY" : "latency",
    "BROWSETIME" : "browse time",
    "PCKTLOSS" : "packet loss",
    "RSRP" : "RSRP",
    "SINR" : "SINR"
}

AGG_MAP = {
    "median" : "median",
    "avg" : "average",
    "average" : "average",
    "maximum" : "maximum",
    "max" : "maximum",
    "minimum" : "minimum",
    "min" : "minimum",
    "p90" : "P90",
}

DIMENSION_MAP = {
    "states" : "STATE",
    "state" : "STATE",
    "cities" : "CITY",
    "city" : "CITY",
    "circles" : "CIRCLE",
    "circle" : "CIRCLE",
    "pan india" : "PANINDIA",
    "pan_india" : "PANINDIA",
    "pan ind" : "PANINDIA",
    "jc" : "JIOCENTER",
    "jio centre" : "JIOCENTER",
    "jio centres" : "JIOCENTER",
    "jio center" : "JIOCENTER",
    "jio centers" : "JIOCENTER",
    "jcs" : "JIOCENTER",
    "zone" : "ZONE",
    "zones" : "ZONE",
    "r4g state" : "R4GSTATE",
    "r4g states" : "R4GSTATE",
    "r4g" : "R4GSTATE",
}

TOPN_MAP = {
    "top 3" : "3",
    "top 5" : "5",
    "top 10" : "10",
}

# -------------------------------
# Time phrase → SQL condition
# -------------------------------

def parse_time_condition(time_text):

    if not time_text:
        return None

    today = datetime.today()
    time_text_lower = time_text.lower()

    # Handle "today"
    if "today" in time_text_lower:
        return f"CREATEDON = '{today.date()}'"

    # Handle "yesterday"
    if "yesterday" in time_text_lower:
        start = today - timedelta(days=1)
        return f"CREATEDON = '{start.date()}'"

    # Handle "last week"
    if "last week" in time_text_lower:
        start = today - timedelta(weeks=1)
        return f"CREATEDON >= '{start.date()}'"

    # Handle "last month"
    if "last month" in time_text_lower:
        # Approximate as 30 days
        start = today - timedelta(days=30)
        return f"CREATEDON >= '{start.date()}'"

    # Handle generic "last X day/days" pattern
    splt = str(time_text).split()
    if len(splt) >= 3 and splt[0].lower() == "last" and "day" in splt[2].lower():
        try:
            start = today - timedelta(days=int(splt[1]))
            return f"CREATEDON >= '{start.date()}'"
        except ValueError:
            pass

    return None

# -------------------------------
# MySQL Connection
# -------------------------------

def get_db_connection():

    return mysql.connector.connect(
        host="localhost",
        user="root",        # change if needed
        password="Aayu#2520",    # change if needed
        database="jio"
    )


def build_nv_response(tracker, metric, data, message):

    intent_name = tracker.latest_message["intent"].get("name")
    tech = tracker.get_slot("tech")[0] if tracker.get_slot("tech") is not None else None

    response = {
        "intent": intent_name,
        "confidence": 1.0,
        "technology": tech,
        "module": "NV dash",
        "metric": metric,
        "message": message,
        "data": data,
        "is_report": False,
        "file_path": None,
        "export_type": None
    }

    return json.dumps(response)


# ---------------------------------------------------
# KPI ROUTER ACTION
# ---------------------------------------------------

class ActionKpiRouter(Action):

    def name(self) -> Text:
        return "action_kpi_router"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]
    ) -> List[Dict[Text, Any]]:

        intent = tracker.latest_message["intent"].get("name")

        logger.info("======NEW REQUEST=======")
        logger.info(f"[USER]{tracker.latest_message.get('text')}")
        logger.info(f"[SLOTS]{tracker.current_slot_values()}")
        logger.info("========================")

        try:
            if intent == "get_metrics":
                return self.handle_get_metrics(dispatcher, tracker)

            if intent == "rank_metrics":
                return self.handle_rank_metrics(dispatcher, tracker)
            
            if intent == "threshold_metrics":
                return self.handle_threshold_metrics(dispatcher, tracker)
            
            if intent == "compare_metrics":
                return self.handle_compare_metrics(dispatcher, tracker)
            
            if intent == "correlation":
                return self.handle_correlation(dispatcher, tracker)
            
            if intent == "top_locations":
                return self.handle_top_locations(dispatcher, tracker)
            
            if intent == "breakdown_metrics":
                return self.handle_breakdown_metrics(dispatcher, tracker)
            
            

            dispatcher.utter_message(text="I couldn't understand the KPI request.")
        
        except Exception as E:
            logger.exception("ERROR")
            dispatcher.utter_message(text="Error processing request")


        return [AllSlotsReset()]



    # --------------------------------
    # GET METRICS
    # --------------------------------

    # def handle_get_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric") if tracker.get_slot("metric") is not None else ["empty"]
        time = tracker.get_slot("time") if tracker.get_slot("time") is not None else ["empty"]
        geo = tracker.get_slot("geo") if tracker.get_slot("geo") is not None else ["empty"]
        app = tracker.get_slot("app") if tracker.get_slot("app") is not None else ["empty"]
        band = tracker.get_slot("band") if tracker.get_slot("band") is not None else ["empty"]
        tech = tracker.get_slot("tech") if tracker.get_slot("tech") is not None else ["empty"]
        agg = tracker.get_slot("agg")[0] if tracker.get_slot("agg") is not None else None

        data=[]
        for i in metric:
            for j in time:
                for k in geo:
                    for m in app:
                        for n in band:
                            for p in tech:
                                metric_column = METRIC_MAP.get(i.lower()) if i != "empty" else i
                                if not metric:
                                    dispatcher.utter_message(text="Please specify the metric to compare.")
                                    return [AllSlotsReset()]
        
                                if not metric_column:
                                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                                    return [AllSlotsReset()]
                                output_metric = OUTPUT_METRIC_MAP.get(metric_column)
                                
                                time_condition = parse_time_condition(j) if j != "empty" else j

                                filters = " "
                                selectors = " "
                                if time_condition != "empty":
                                    filters += f""" AND {time_condition}"""
                                if k != "empty":
                                    filters += f" AND GEOGRAPHY_NAME = '{k}'"
                                    selectors += f"""GEOGRAPHY_NAME AS "Location","""
                                if m != "empty":
                                    filters += f" AND TESTTYPE = '{m}'"
                                    selectors += f"""TESTTYPE AS "App","""
                                if n != "empty":
                                    filters += f" AND BAND = '{n}'"
                                    selectors += f"""BAND AS Band,"""
                                if p != "empty":
                                    filters += f" AND NETWORKTYPE = '{p}'"
                                    selectors += f"""NETWORKTYPE AS "Network Type" """

                                if selectors[-1] == ',':
                                    selectors = selectors[:len(selectors)-1]

                                # query = f"""
                                #     SELECT 
                                #         AVG({metric_column}) AS "{output_metric}",
                                #         {selectors}
                                #     FROM netvelocity_kpi_metrics 
                                #     WHERE 1=1 {filters}
                                # """

                                query = ""

                                if agg:
                                    agg = AGG_MAP.get(agg.lower())

                                if agg == "median":
                                    query=f"""
                                    SELECT AVG({metric_column}) AS median
                                    FROM (
                                        SELECT 
                                            {metric_column},
                                            ROW_NUMBER() OVER (ORDER BY {metric_column}) AS rn,
                                            COUNT(*) OVER () AS cnt
                                        FROM netvelocity_kpi_metrics
                                        WHERE 1=1 {filters}
                                    ) t
                                    WHERE rn IN (FLOOR((cnt+1)/2), FLOOR((cnt+2)/2))
                                    """
                                elif agg == "minimum":
                                    query = f"SELECT MIN({metric_column}) FROM netvelocity_kpi_metrics WHERE 1=1 {filters}"
                                elif agg == "maximum":
                                    query = f"SELECT MAX({metric_column}) FROM netvelocity_kpi_metrics WHERE 1=1 {filters}"
                                elif agg == "p90":
                                    query=f"""
                                    SELECT AVG({metric_column}) AS p90
                                    FROM (
                                        SELECT 
                                            {metric_column},
                                            ROW_NUMBER() OVER (ORDER BY {metric_column}) AS rn,
                                            COUNT(*) OVER () AS cnt
                                        FROM netvelocity_kpi_metrics
                                        WHERE 1=1 {filters}
                                    ) t
                                    WHERE rn = CEIL(0.9 * cnt)
                                    """
                                else:
                                    query = f"SELECT AVG({metric_column}) FROM netvelocity_kpi_metrics WHERE 1=1 {filters}"


                                logger.debug(f"GetMetrics-Query: {query}")

                                cursor.execute(query)

                                rows = cursor.fetchall()

                                value = result[0]
                                if value is None:
                                    response = "No data found"
                                
                                else:
                                    value = round(value, 2)
                                    metric = OUTPUT_METRIC_MAP.get(metric_column)
                                    agg_text = str(agg) + " " if agg else ""
                                    response = f"The {agg_text}{metric}"
                                    if geo:
                                        response += f" for {geo}"
                                    if time:
                                        response += f" during {time}"
                                    if app:
                                        response += f" using {app}"
                                    response += f""" is {value}.\n\n"""

                                for r in rows:
                                    row_data = {}

                                    row_data[output_metric] = round(r[0], 2) if r[0] is not None else None

                                    idx = 1

                                    if k != "empty":
                                        row_data["Location"] = r[idx]
                                        idx += 1

                                    if m != "empty":
                                        row_data["App"] = r[idx]
                                        idx += 1

                                    if n != "empty":
                                        row_data["Band"] = r[idx]
                                        idx += 1

                                    if p != "empty":
                                        row_data["Network Type"] = r[idx]
                                        idx += 1

                                    data.append(row_data)
                                
        if not metric:
            dispatcher.utter_message(text="Please specify which metric you want.")
            return [AllSlotsReset()]

        metric_column = METRIC_MAP.get(metric.lower())
        if agg:
            agg = AGG_MAP.get(agg.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return [AllSlotsReset()]
        
        

        # if geo:
        #     query += f" AND GEOGRAPHY_NAME = '{geo}'"

        # if app and app != "All apps":
        #     query += f" AND TESTTYPE = '{app}'"

        # time_condition = parse_time_condition(time)

        # if time_condition:
        #     query += f" AND {time_condition}"

        try:

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(query)

            logger.debug(f"Query: {query}")

            result = cursor.fetchone()
            
            value = result[0]

            if value is None:
                response = "No data found"
            
            else:
                value = round(value, 2)
                metric = OUTPUT_METRIC_MAP.get(metric_column)
                agg_text = str(agg) + " " if agg else ""
                response = f"The {agg_text}{metric}"
                if geo:
                    response += f" for {geo}"
                if time:
                    response += f" during {time}"
                if app:
                    response += f" using {app}"
                response += f""" is {value}."""

            

            dispatcher.utter_message(text=build_nv_response(tracker, metric_column, value, response))

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]
    def handle_get_metrics(self, dispatcher, tracker):

        def ensure_list(slot):
            return slot if slot else ["empty"]

        metric_list = ensure_list(tracker.get_slot("metric"))
        time_list = ensure_list(tracker.get_slot("time"))
        geo_list = ensure_list(tracker.get_slot("geo"))
        app_list = ensure_list(tracker.get_slot("app"))
        band_list = ensure_list(tracker.get_slot("band"))
        tech_list = ensure_list(tracker.get_slot("tech"))

        agg = tracker.get_slot("agg")
        agg = AGG_MAP.get(agg[0].lower()) if agg else None

        # Validate metric early
        if metric_list == ["empty"]:
            dispatcher.utter_message(text="Please specify which metric you want.")
            return [AllSlotsReset()]

        conn = None
        cursor = None

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            data = []
            response = ""

            for metric in metric_list:
                metric_column = METRIC_MAP.get(metric.lower())

                if not metric_column:
                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                    return [AllSlotsReset()]

                output_metric = OUTPUT_METRIC_MAP.get(metric_column)

                for time in time_list:
                    for geo in geo_list:
                        for app in app_list:
                            for band in band_list:
                                for tech in tech_list:

                                    filters = ""
                                    selectors = ""

                                    # Time
                                    if time != "empty":
                                        time_condition = parse_time_condition(time)
                                        filters += f" AND {time_condition}"

                                    # Geo
                                    if geo != "empty":
                                        filters += f" AND GEOGRAPHY_NAME = '{geo}'"
                                        selectors += 'GEOGRAPHY_NAME AS "Location", '

                                    # App
                                    if app != "empty":
                                        filters += f" AND TESTTYPE = '{app}'"
                                        selectors += 'TESTTYPE AS "App", '

                                    # Band
                                    if band != "empty":
                                        filters += f" AND BAND = '{band}'"
                                        selectors += 'BAND AS "Band", '

                                    # Tech
                                    if tech != "empty":
                                        filters += f" AND NETWORKTYPE = '{tech}'"
                                        selectors += 'NETWORKTYPE AS "Network Type", '

                                    selectors = selectors.rstrip(", ")

                                    # Aggregation logic
                                    if agg == "median":
                                        query = f"""
                                            SELECT AVG({metric_column})
                                            FROM (
                                                SELECT {metric_column},
                                                    ROW_NUMBER() OVER (ORDER BY {metric_column}) AS rn,
                                                    COUNT(*) OVER () AS cnt
                                                FROM netvelocity_kpi_metrics
                                                WHERE 1=1 {filters}
                                            ) t
                                            WHERE rn IN (FLOOR((cnt+1)/2), FLOOR((cnt+2)/2))
                                        """
                                    elif agg == "minimum":
                                        query = f"SELECT MIN({metric_column}) FROM netvelocity_kpi_metrics WHERE 1=1 {filters}"
                                    elif agg == "maximum":
                                        query = f"SELECT MAX({metric_column}) FROM netvelocity_kpi_metrics WHERE 1=1 {filters}"
                                    elif agg == "p90":
                                        query = f"""
                                            SELECT AVG({metric_column})
                                            FROM (
                                                SELECT {metric_column},
                                                    ROW_NUMBER() OVER (ORDER BY {metric_column}) AS rn,
                                                    COUNT(*) OVER () AS cnt
                                                FROM netvelocity_kpi_metrics
                                                WHERE 1=1 {filters}
                                            ) t
                                            WHERE rn = CEIL(0.9 * cnt)
                                        """
                                    else:
                                        query = f"SELECT AVG({metric_column}) FROM netvelocity_kpi_metrics WHERE 1=1 {filters}"

                                    logger.debug(f"GetMetrics-Query: {query}")

                                    cursor.execute(query)
                                    result = cursor.fetchone()

                                    value = result[0] if result else None

                                    # Build response
                                    if value is None:
                                        response += "\n\nNo data found"
                                    else:
                                        value = round(value, 2)
                                        metric_name = OUTPUT_METRIC_MAP.get(metric_column)
                                        agg_text = f"{agg} " if agg else ""

                                        response += f"\n\nThe {agg_text}{metric_name}"

                                        if geo != "empty":
                                            response += f" for {geo}"
                                        if time != "empty":
                                            response += f" during {time}"
                                        if app != "empty":
                                            response += f" using {app}"

                                        response += f" is {value}."

                                    # Store structured data
                                    row_data = {
                                        output_metric: value
                                    }

                                    if geo != "empty":
                                        row_data["Location"] = geo
                                    if app != "empty":
                                        row_data["App"] = app
                                    if band != "empty":
                                        row_data["Band"] = band
                                    if tech != "empty":
                                        row_data["Network Type"] = tech

                                    data.append(row_data)

            dispatcher.utter_message(
                text=build_nv_response(tracker, metric_column, data, response)
            )

        except Exception as e:
            dispatcher.utter_message(text=f"Database error: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return [AllSlotsReset()]



    # --------------------------------
    # RANK METRICS
    # --------------------------------

    # def handle_rank_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric")[0] if tracker.get_slot("metric") is not None else None
        topN = tracker.get_slot("topN")[0] if tracker.get_slot("topN") is not None else None
        dimension = tracker.get_slot("dimension")[0] if tracker.get_slot("dimension") is not None else None
        time = tracker.get_slot("time")[0] if tracker.get_slot("time") is not None else None

        logger.debug(f"topN - {topN}")
        order = "DESC"
        if(topN.split()[0] in ['bottom', 'lowest'] and metric in ['latency']):
            order = "DESC"
        elif(topN.split()[0] in ['bottom', 'lowest'] and metric not in ['latency']):
            order = "ASC"
        elif(metric in ['latency']):
            order = "ASC"
        topN = int(topN.split()[1])
        logger.debug(f"dimension - {dimension}")

        time_condition = parse_time_condition(time)

        dimension_column = DIMENSION_MAP.get(dimension.lower())

        filters = f"""AND GEOGRAPHYNAME = '{dimension_column}'"""
        if time_condition:
            filters += "AND "
            filters += time_condition

        if not metric:
            dispatcher.utter_message(text="Please specify the metric to compare.")
            return [AllSlotsReset()]

        metric_column = METRIC_MAP.get(metric.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return [AllSlotsReset()]

        try:

            conn = get_db_connection()
            cursor = conn.cursor()

            
            query = f"""
                SELECT
                    GEOGRAPHY_NAME,
                    AVG({metric_column}) AS metric_value,
                    RANK() OVER (ORDER BY AVG({metric_column}) DESC) AS rank_position
                FROM netvelocity_kpi_metrics
                WHERE 1=1
                {filters}
                GROUP BY GEOGRAPHY_NAME
                ORDER BY metric_value {order}
                LIMIT {topN};
            """

            logger.debug(f"RankMetrics-Query: {query}")

            cursor.execute(query)

            rows = cursor.fetchall()

            if not rows:
                dispatcher.utter_message(text="No data found for ranking.")
                return []

            response = f"Ranking top {topN} {dimension} by {metric} in {time}\n\n"

            data = []
            metric = OUTPUT_METRIC_MAP.get(metric_column)
            for r in rows:
                data.append({
                    "Location": r[0],
                    f"{metric}": round(r[1], 2) if r[1] else None,
                    "Rank": r[2]
                })

            response += json.dumps(data)

            dispatcher.utter_message(text=build_nv_response(tracker, metric_column, json.dumps(data), response))

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]
    def handle_rank_metrics(self, dispatcher, tracker):

        def ensure_list(slot):
            return slot if slot else ["empty"]

        metric_list = ensure_list(tracker.get_slot("metric"))
        topN_list = ensure_list(tracker.get_slot("topN"))
        dimension_list = ensure_list(tracker.get_slot("dimension"))
        time_list = ensure_list(tracker.get_slot("time"))
        app_list = ensure_list(tracker.get_slot("app"))
        band_list = ensure_list(tracker.get_slot("band"))
        tech_list = ensure_list(tracker.get_slot("tech"))

        conn = None
        cursor = None

        final_response = ""
        final_data = []

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            for metric in metric_list:

                if metric == "empty":
                    dispatcher.utter_message(text="Please specify the metric to rank.")
                    return [AllSlotsReset()]

                metric_column = METRIC_MAP.get(metric.lower())
                if not metric_column:
                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                    return [AllSlotsReset()]

                output_metric = OUTPUT_METRIC_MAP.get(metric_column)

                for topN_raw in topN_list:
                    if topN_raw == "empty":
                        dispatcher.utter_message(text="Please specify top/bottom N.")
                        return [AllSlotsReset()]

                    # Parse topN
                    try:
                        parts = topN_raw.lower().split()
                        direction_word = parts[0]
                        topN = int(parts[1])
                    except Exception:
                        dispatcher.utter_message(
                            text="Invalid format for topN. Use like 'top 5' or 'bottom 10'."
                        )
                        return [AllSlotsReset()]

                    for dimension in dimension_list:

                        if dimension == "empty":
                            dispatcher.utter_message(
                                text="Please specify the dimension (city/state)."
                            )
                            return [AllSlotsReset()]

                        dimension_column = DIMENSION_MAP.get(dimension.lower())
                        if not dimension_column:
                            dispatcher.utter_message(
                                text=f"Dimension '{dimension}' is not supported."
                            )
                            return [AllSlotsReset()]

                        for time in time_list:
                            for app in app_list:
                                for band in band_list:
                                    for tech in tech_list:

                                        # ✅ Order logic
                                        if direction_word in ["bottom", "lowest"]:
                                            order = "DESC" if metric.lower() == "latency" else "ASC"
                                        else:
                                            order = "ASC" if metric.lower() == "latency" else "DESC"

                                        # ✅ Build filters
                                        filters = f" AND GEOGRAPHYNAME = '{dimension_column}'"

                                        if time != "empty":
                                            time_condition = parse_time_condition(time)
                                            if time_condition:
                                                filters += f" AND {time_condition}"

                                        if app != "empty":
                                            filters += f" AND TESTTYPE = '{app}'"

                                        if band != "empty":
                                            filters += f" AND BAND = '{band}'"

                                        if tech != "empty":
                                            filters += f" AND NETWORKTYPE = '{tech}'"

                                        query = f"""
                                            SELECT
                                                GEOGRAPHY_NAME,
                                                AVG({metric_column}) AS metric_value
                                            FROM netvelocity_kpi_metrics
                                            WHERE 1=1
                                            {filters}
                                            GROUP BY GEOGRAPHY_NAME
                                            ORDER BY metric_value {order}
                                            LIMIT {topN}
                                        """

                                        logger.debug(f"RankMetrics-Query: {query}")

                                        cursor.execute(query)
                                        rows = cursor.fetchall()

                                        if not rows:
                                            final_response += (
                                                f"No data found for {metric} ({dimension}).\n\n"
                                            )
                                            continue

                                        # ✅ Build response block per combination
                                        response = f"{direction_word.capitalize()} {topN} {dimension} by {output_metric}"

                                        if time != "empty":
                                            response += f" during {time}"
                                        if app != "empty":
                                            response += f" using {app}"
                                        if band != "empty":
                                            response += f" on {band}"
                                        if tech != "empty":
                                            response += f" ({tech})"

                                        response += ":\n"

                                        for idx, r in enumerate(rows, start=1):
                                            value = round(r[1], 2) if r[1] is not None else None

                                            response += f"{idx}. {r[0]} - {value}\n"

                                            row_data = {
                                                "Location": r[0],
                                                output_metric: value,
                                                "Rank": idx
                                            }

                                            # Attach context (important for multi-combo clarity)
                                            if time != "empty":
                                                row_data["Time"] = time
                                            if app != "empty":
                                                row_data["App"] = app
                                            if band != "empty":
                                                row_data["Band"] = band
                                            if tech != "empty":
                                                row_data["Network Type"] = tech
                                            if dimension != "empty":
                                                row_data["Dimension"] = dimension

                                            final_data.append(row_data)

                                        final_response += response + "\n\n"

            # ✅ Final dispatch
            dispatcher.utter_message(
                text=build_nv_response(
                    tracker,
                    metric_column,
                    final_data,
                    final_response.strip()
                )
            )

        except Exception as e:
            dispatcher.utter_message(text=f"Database error: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return [AllSlotsReset()]

    # --------------------------------
    # THRESHOLD METRICS
    # --------------------------------

    # def handle_threshold_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric")[0] if tracker.get_slot("metric") is not None else None
        threshold = tracker.get_slot("threshold")[0] if tracker.get_slot("threshold") is not None else None
        dimension = tracker.get_slot("dimension")[0] if tracker.get_slot("dimension") is not None else None
        time = tracker.get_slot("time")[0] if tracker.get_slot("time") is not None else None
        time_condition = parse_time_condition(time)

        user_message = tracker.latest_message.get("text")
        operator = '='
        for i in range(len(user_message)):
            if(i + 1 < len(user_message) and (user_message[i:i+2] == '<=' or user_message[i:i+2] == '>=')):
                operator = str(user_message[i:i+2])
                break
            elif(user_message[i] in ['<', '>', '=']):
                operator = str(user_message[i])
                break


        logger.debug(f"ThresholdMetrics-Metric: {metric}")
        logger.debug(f"ThresholdMetrics-Dimension: {dimension}")
        logger.debug(f"ThresholdMetrics-Operator: {operator}")
        logger.debug(f"ThresholdMetrics-Threshold: {threshold}")
        logger.debug(f"ThresholdMetrics-Time: {time_condition}")

        if not metric:
            dispatcher.utter_message(text="Please specify the metric to compare.")
            return [AllSlotsReset()]
        
        metric_column = METRIC_MAP.get(metric.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return [AllSlotsReset()]


        filters = f"AND {metric_column} {operator} {threshold}"

        if dimension:
            dimension_column = DIMENSION_MAP.get(dimension.lower())
            filters += f""" AND GEOGRAPHYNAME = '{dimension_column}'"""

        if time_condition:
            filters += " AND "
            filters += time_condition

        

        try:

            conn = get_db_connection()
            cursor = conn.cursor()

            
            query = f"""
                SELECT
                    GEOGRAPHY_NAME,
                    AVG({metric_column}) AS metric_value
                FROM netvelocity_kpi_metrics
                WHERE 1=1 {filters} 
                GROUP BY GEOGRAPHY_NAME
                ORDER BY metric_value ASC
            """

            logger.debug(f"ThresholdMetrics-Query: {query}")

            cursor.execute(query)

            rows = cursor.fetchall()

            if not rows:
                dispatcher.utter_message(text="No data found for ranking.")
                return []
            
            metric = OUTPUT_METRIC_MAP.get(metric_column)
            response = f"Locations with {metric} {operator} {threshold}"
            if time:
                response += f" during {time}\n\n"

            data = []
            
            for r in rows:
                data.append({
                    "Location": r[0],
                    f"{metric}": round(r[1], 2) if r[1] else None,
                })

            response += json.dumps(data)

            dispatcher.utter_message(text=build_nv_response(tracker, metric_column, json.dumps(data), response))

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]

    def handle_threshold_metrics(self, dispatcher, tracker):

        def ensure_list(slot):
            return slot if slot else ["empty"]

        metric_list = ensure_list(tracker.get_slot("metric"))
        threshold_list = ensure_list(tracker.get_slot("threshold"))
        dimension_list = ensure_list(tracker.get_slot("dimension"))
        time_list = ensure_list(tracker.get_slot("time"))
        app_list = ensure_list(tracker.get_slot("app"))
        band_list = ensure_list(tracker.get_slot("band"))
        tech_list = ensure_list(tracker.get_slot("tech"))

        user_message = tracker.latest_message.get("text", "")

        # ✅ Detect operator safely
        operator = "="
        for i in range(len(user_message)):
            if i + 1 < len(user_message) and user_message[i:i+2] in ["<=", ">="]:
                operator = user_message[i:i+2]
                break
            elif user_message[i] in ["<", ">", "="]:
                operator = user_message[i]
                break

        conn = None
        cursor = None

        final_response = ""
        final_data = []

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            for metric in metric_list:

                if metric == "empty":
                    dispatcher.utter_message(text="Please specify the metric.")
                    return [AllSlotsReset()]

                metric_column = METRIC_MAP.get(metric.lower())
                if not metric_column:
                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                    return [AllSlotsReset()]

                output_metric = OUTPUT_METRIC_MAP.get(metric_column)

                for threshold in threshold_list:

                    if threshold == "empty":
                        dispatcher.utter_message(text="Please specify the threshold value.")
                        return [AllSlotsReset()]

                    # Ensure numeric threshold
                    try:
                        threshold_val = float(threshold)
                    except Exception:
                        dispatcher.utter_message(text=f"Invalid threshold '{threshold}'.")
                        return [AllSlotsReset()]

                    for dimension in dimension_list:

                        dimension_filter = ""
                        if dimension != "empty":
                            dimension_column = DIMENSION_MAP.get(dimension.lower())
                            if not dimension_column:
                                dispatcher.utter_message(
                                    text=f"Dimension '{dimension}' is not supported."
                                )
                                return [AllSlotsReset()]
                            dimension_filter = f" AND GEOGRAPHYNAME = '{dimension_column}'"

                        for time in time_list:
                            for app in app_list:
                                for band in band_list:
                                    for tech in tech_list:

                                        # ✅ Build filters
                                        filters = f" AND {metric_column} {operator} {threshold_val}"
                                        filters += dimension_filter

                                        if time != "empty":
                                            time_condition = parse_time_condition(time)
                                            if time_condition:
                                                filters += f" AND {time_condition}"

                                        if app != "empty":
                                            filters += f" AND TESTTYPE = '{app}'"

                                        if band != "empty":
                                            filters += f" AND BAND = '{band}'"

                                        if tech != "empty":
                                            filters += f" AND NETWORKTYPE = '{tech}'"

                                        query = f"""
                                            SELECT
                                                GEOGRAPHY_NAME,
                                                AVG({metric_column}) AS metric_value
                                            FROM netvelocity_kpi_metrics
                                            WHERE 1=1 {filters}
                                            GROUP BY GEOGRAPHY_NAME
                                            ORDER BY metric_value ASC
                                        """

                                        logger.debug(f"ThresholdMetrics-Query: {query}")

                                        cursor.execute(query)
                                        rows = cursor.fetchall()

                                        if not rows:
                                            final_response += (
                                                f"No data found for {output_metric} {operator} {threshold_val}.\n\n"
                                            )
                                            continue

                                        # ✅ Build response block
                                        response = f"Locations with {output_metric} {operator} {threshold_val}"

                                        if time != "empty":
                                            response += f" during {time}"
                                        if app != "empty":
                                            response += f" using {app}"
                                        if band != "empty":
                                            response += f" on {band}"
                                        if tech != "empty":
                                            response += f" ({tech})"

                                        response += ":\n"

                                        for r in rows:
                                            value = round(r[1], 2) if r[1] is not None else None

                                            response += f"- {r[0]}: {value}\n"

                                            row_data = {
                                                "Location": r[0],
                                                output_metric: value,
                                            }

                                            # Attach context
                                            if time != "empty":
                                                row_data["Time"] = time
                                            if app != "empty":
                                                row_data["App"] = app
                                            if band != "empty":
                                                row_data["Band"] = band
                                            if tech != "empty":
                                                row_data["Network Type"] = tech
                                            if dimension != "empty":
                                                row_data["Dimension"] = dimension

                                            final_data.append(row_data)

                                        final_response += response + "\n\n"

            dispatcher.utter_message(
                text=build_nv_response(
                    tracker,
                    metric_column,
                    final_data,
                    final_response.strip()
                )
            )

        except Exception as e:
            dispatcher.utter_message(text=f"Database error: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return [AllSlotsReset()]


    # --------------------------------
    # COMPARE METRICS
    # --------------------------------

    # def handle_compare_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric") if tracker.get_slot("metric") is not None else ["empty"]
        time = tracker.get_slot("time") if tracker.get_slot("time") is not None else ["empty"]
        geo = tracker.get_slot("geo") if tracker.get_slot("geo") is not None else ["empty"]
        app = tracker.get_slot("app") if tracker.get_slot("app") is not None else ["empty"]
        band = tracker.get_slot("band") if tracker.get_slot("band") is not None else ["empty"]
        tech = tracker.get_slot("tech") if tracker.get_slot("tech") is not None else ["empty"]

        logger.debug(f"CompareMetrics-1")

        try:

            conn = get_db_connection()
            cursor = conn.cursor()

            logger.debug(f"CompareMetrics-2")
            data = []

            for i in metric:
                for j in time:
                    for k in geo:
                        for m in app:
                            for n in band:
                                for p in tech:
                                    metric_column = METRIC_MAP.get(i.lower()) if i != "empty" else i
                                    if not metric:
                                        dispatcher.utter_message(text="Please specify the metric to compare.")
                                        return [AllSlotsReset()]
            
                                    if not metric_column:
                                        dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                                        return [AllSlotsReset()]
                                    output_metric = OUTPUT_METRIC_MAP.get(metric_column)
                                    
                                    time_condition = parse_time_condition(j) if j != "empty" else j

                                    logger.debug(f"CompareMetrics-3")

                                    filters = " "
                                    selectors = " "
                                    if time_condition != "empty":
                                        filters += f""" AND {time_condition}"""
                                        logger.debug(f"CompareMetrics-31")
                                    if k != "empty":
                                        filters += f" AND GEOGRAPHY_NAME = '{k}'"
                                        selectors += f"""GEOGRAPHY_NAME AS "Location","""
                                        logger.debug(f"CompareMetrics-32")
                                    if m != "empty":
                                        filters += f" AND TESTTYPE = '{m}'"
                                        selectors += f"""TESTTYPE AS "App","""
                                    if n != "empty":
                                        filters += f" AND BAND = '{n}'"
                                        selectors += f"""BAND AS Band,"""
                                    if p != "empty":
                                        filters += f" AND NETWORKTYPE = '{p}'"
                                        selectors += f"""NETWORKTYPE AS "Network Type" """

                                    if selectors[-1] == ',':
                                        selectors = selectors[:len(selectors)-1]

                                    logger.debug(f"CompareMetrics-4")

                                    query = f"""
                                        SELECT 
                                            AVG({metric_column}) AS "{output_metric}",
                                            {selectors}
                                        FROM netvelocity_kpi_metrics 
                                        WHERE 1=1 {filters}
                                    """
                                    logger.debug(f"CompareMetrics-Query: {query}")

                                    cursor.execute(query)

                                    rows = cursor.fetchall()

                                    for r in rows:
                                        row_data = {}

                                        row_data[output_metric] = round(r[0], 2) if r[0] is not None else None

                                        idx = 1

                                        if k != "empty":
                                            row_data["Location"] = r[idx]
                                            idx += 1

                                        if m != "empty":
                                            row_data["App"] = r[idx]
                                            idx += 1

                                        if n != "empty":
                                            row_data["Band"] = r[idx]
                                            idx += 1

                                        if p != "empty":
                                            row_data["Network Type"] = r[idx]
                                            idx += 1

                                        data.append(row_data)

                                    # for r in rows:
                                    #     data.append({
                                    #         f"{output_metric}": round(r[0], 2) if r[0] else None,
                                    #         f"Date": r[1],
                                    #         f"Location": r[2],
                                    #         f"App": r[3],
                                    #         f"Band": r[4],
                                    #         f"Network Type": r[5],
                                    #     })
            
            if data == []:
                dispatcher.utter_message(text="No data found for ranking.")
                return [AllSlotsReset()]

                    
            response = f"Following is your requested comparison: \n\n"
            response += json.dumps(data)

            dispatcher.utter_message(text=build_nv_response(tracker, metric_column, json.dumps(data), response))

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]

    def handle_compare_metrics(self, dispatcher, tracker):

        def ensure_list(slot):
            return slot if slot else ["empty"]

        metric_list = ensure_list(tracker.get_slot("metric"))
        time_list = ensure_list(tracker.get_slot("time"))
        geo_list = ensure_list(tracker.get_slot("geo"))
        app_list = ensure_list(tracker.get_slot("app"))
        band_list = ensure_list(tracker.get_slot("band"))
        tech_list = ensure_list(tracker.get_slot("tech"))

        # ✅ Validate metric early
        if metric_list == ["empty"]:
            dispatcher.utter_message(text="Please specify the metric to compare.")
            return [AllSlotsReset()]

        conn = None
        cursor = None

        final_data = []
        final_response = "Following is your requested comparison:\n\n"

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            for metric in metric_list:

                metric_column = METRIC_MAP.get(metric.lower())
                if not metric_column:
                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                    return [AllSlotsReset()]

                output_metric = OUTPUT_METRIC_MAP.get(metric_column)

                for time in time_list:
                    for geo in geo_list:
                        for app in app_list:
                            for band in band_list:
                                for tech in tech_list:

                                    # ✅ Build filters
                                    filters = ""

                                    if time != "empty":
                                        time_condition = parse_time_condition(time)
                                        if time_condition:
                                            filters += f" AND {time_condition}"

                                    if geo != "empty":
                                        filters += f" AND GEOGRAPHY_NAME = '{geo}'"

                                    if app != "empty":
                                        filters += f" AND TESTTYPE = '{app}'"

                                    if band != "empty":
                                        filters += f" AND BAND = '{band}'"

                                    if tech != "empty":
                                        filters += f" AND NETWORKTYPE = '{tech}'"

                                    # ✅ Query (no selectors needed → avoids index bugs)
                                    query = f"""
                                        SELECT AVG({metric_column})
                                        FROM netvelocity_kpi_metrics
                                        WHERE 1=1 {filters}
                                    """

                                    logger.debug(f"CompareMetrics-Query: {query}")

                                    cursor.execute(query)
                                    result = cursor.fetchone()

                                    value = result[0] if result else None

                                    # ✅ Build response per combination
                                    response = f"{output_metric}"

                                    if geo != "empty":
                                        response += f" for {geo}"
                                    if time != "empty":
                                        response += f" during {time}"
                                    if app != "empty":
                                        response += f" using {app}"
                                    if band != "empty":
                                        response += f" on {band}"
                                    if tech != "empty":
                                        response += f" ({tech})"

                                    if value is None:
                                        response += " → No data\n"
                                    else:
                                        value = round(value, 2)
                                        response += f" → {value}\n"

                                    final_response += response

                                    # ✅ Structured data
                                    row_data = {
                                        "Metric": output_metric,
                                        "Value": value
                                    }

                                    if geo != "empty":
                                        row_data["Location"] = geo
                                    if time != "empty":
                                        row_data["Time"] = time
                                    if app != "empty":
                                        row_data["App"] = app
                                    if band != "empty":
                                        row_data["Band"] = band
                                    if tech != "empty":
                                        row_data["Network Type"] = tech

                                    final_data.append(row_data)

            if not final_data:
                dispatcher.utter_message(text="No data found for comparison.")
                return [AllSlotsReset()]

            # ✅ Final response
            final_response += "\n"
            final_response += json.dumps(final_data, indent=2)

            dispatcher.utter_message(
                text=build_nv_response(
                    tracker,
                    metric_column,
                    final_data,
                    final_response
                )
            )

        except Exception as e:
            dispatcher.utter_message(text=f"Database error: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return [AllSlotsReset()]

    # --------------------------------
    # CORRELATION METRICS
    # --------------------------------

    # def handle_correlation(self, dispatcher, tracker):

        metric = tracker.get_slot("metric") if tracker.get_slot("metric") is not None else ["empty"]
        time = tracker.get_slot("time") if tracker.get_slot("time") is not None else ["empty"]
        geo = tracker.get_slot("geo") if tracker.get_slot("geo") is not None else ["empty"]
        app = tracker.get_slot("app")[0] if tracker.get_slot("app") is not None else ["empty"]
        band = tracker.get_slot("band")[0] if tracker.get_slot("band") is not None else ["empty"]
        tech = tracker.get_slot("tech")[0] if tracker.get_slot("tech") is not None else ["empty"]

        logger.debug(f"CorrelationMetrics-1")

        try:

            conn = get_db_connection()
            cursor = conn.cursor()

            logger.debug(f"CorrelationMetrics-2")
            data = []

            for i in metric:
                metric_column = METRIC_MAP.get(i.lower()) if i != "empty" else i
                if not metric:
                    dispatcher.utter_message(text="Please specify the metric to compare.")
                    return [AllSlotsReset()]

                if not metric_column:
                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                    return [AllSlotsReset()]
                output_metric = OUTPUT_METRIC_MAP.get(metric_column)
                
                time_condition = parse_time_condition(time[0]) if time[0] != "empty" else "empty"

                logger.debug(f"CorrelationMetrics-3")

                filters = " "
                selectors = " "
                if time_condition != "empty":
                    filters += f""" AND {time_condition}"""
                    logger.debug(f"CorrelationMetrics-31")
                if geo[0] != "empty":
                    filters += f" AND GEOGRAPHY_NAME = '{geo[0]}'"
                    selectors += f"""GEOGRAPHY_NAME AS "Location","""
                    logger.debug(f"CompareMetrics-32")
                if app[0] != "empty":
                    filters += f" AND TESTTYPE = '{app[0]}'"
                    selectors += f"""TESTTYPE AS "App","""
                if band[0] != "empty":
                    filters += f" AND BAND = '{band[0]}'"
                    selectors += f"""BAND AS Band,"""
                if tech[0] != "empty":
                    filters += f" AND NETWORKTYPE = '{tech[0]}'"
                    selectors += f"""NETWORKTYPE AS "Network Type" """

                if selectors[-1] == ',':
                    selectors = selectors[:len(selectors)-1]

                logger.debug(f"CorrelationMetrics-4")

                query = f"""
                    SELECT 
                        AVG({metric_column}) AS "{output_metric}",
                        {selectors}
                    FROM netvelocity_kpi_metrics 
                    WHERE 1=1 {filters}
                """
                logger.info(f"CorrelationMetrics-Query: {query}")

                cursor.execute(query)

                rows = cursor.fetchall()

                for r in rows:
                    row_data = {}

                    row_data[output_metric] = round(r[0], 2) if r[0] is not None else None

                    data.append(row_data)
            
            if data == []:
                dispatcher.utter_message(text="No data found for correlation.")
                return [AllSlotsReset()]

                    
            response = f"Correlation between both the metrics: \n\n"
            response += json.dumps(data)

            dispatcher.utter_message(text=build_nv_response(tracker, metric_column, json.dumps(data), response))

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]
    def handle_correlation(self, dispatcher, tracker):

        def ensure_list(slot):
            return slot if slot else ["empty"]

        metric_list = ensure_list(tracker.get_slot("metric"))
        time_list = ensure_list(tracker.get_slot("time"))
        geo_list = ensure_list(tracker.get_slot("geo"))
        app_list = ensure_list(tracker.get_slot("app"))
        band_list = ensure_list(tracker.get_slot("band"))
        tech_list = ensure_list(tracker.get_slot("tech"))

        # ✅ Need at least 2 metrics for correlation
        valid_metrics = [m for m in metric_list if m != "empty"]

        if len(valid_metrics) < 2:
            dispatcher.utter_message(text="Please provide at least two metrics for correlation.")
            return [AllSlotsReset()]

        metric1, metric2 = valid_metrics[:2]

        col1 = METRIC_MAP.get(metric1.lower())
        col2 = METRIC_MAP.get(metric2.lower())

        if not col1 or not col2:
            dispatcher.utter_message(text=f"One of the metrics is not supported.")
            return [AllSlotsReset()]

        name1 = OUTPUT_METRIC_MAP.get(col1)
        name2 = OUTPUT_METRIC_MAP.get(col2)

        conn = None
        cursor = None

        final_response = ""
        final_data = []

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            for time in time_list:
                for geo in geo_list:
                    for app in app_list:
                        for band in band_list:
                            for tech in tech_list:

                                # ✅ Build filters
                                filters = ""

                                if time != "empty":
                                    time_condition = parse_time_condition(time)
                                    if time_condition:
                                        filters += f" AND {time_condition}"

                                if geo != "empty":
                                    filters += f" AND GEOGRAPHY_NAME = '{geo}'"

                                if app != "empty":
                                    filters += f" AND TESTTYPE = '{app}'"

                                if band != "empty":
                                    filters += f" AND BAND = '{band}'"

                                if tech != "empty":
                                    filters += f" AND NETWORKTYPE = '{tech}'"

                                # ✅ Correlation query (Pearson)
                                query = f"""
                                    SELECT 
                                        (
                                            AVG({col1} * {col2}) 
                                            - AVG({col1}) * AVG({col2})
                                        ) /
                                        (
                                            STDDEV({col1}) * STDDEV({col2})
                                        ) AS correlation
                                    FROM netvelocity_kpi_metrics
                                    WHERE 1=1 {filters}
                                    AND {col1} IS NOT NULL
                                    AND {col2} IS NOT NULL
                                """

                                logger.debug(f"Correlation-Query: {query}")

                                cursor.execute(query)
                                result = cursor.fetchone()

                                corr_value = result[0] if result else None

                                # ✅ Build response
                                response = f"Correlation between {name1} and {name2}"

                                if geo != "empty":
                                    response += f" for {geo}"
                                if time != "empty":
                                    response += f" during {time}"
                                if app != "empty":
                                    response += f" using {app}"
                                if band != "empty":
                                    response += f" on {band}"
                                if tech != "empty":
                                    response += f" ({tech})"

                                if corr_value is None:
                                    response += " → No data\n"
                                else:
                                    corr_value = round(corr_value, 4)
                                    response += f" → {corr_value}\n"

                                final_response += response

                                # ✅ Structured data
                                row_data = {
                                    "Metric 1": name1,
                                    "Metric 2": name2,
                                    "Correlation": corr_value
                                }

                                if geo != "empty":
                                    row_data["Location"] = geo
                                if time != "empty":
                                    row_data["Time"] = time
                                if app != "empty":
                                    row_data["App"] = app
                                if band != "empty":
                                    row_data["Band"] = band
                                if tech != "empty":
                                    row_data["Network Type"] = tech

                                final_data.append(row_data)

            if not final_data:
                dispatcher.utter_message(text="No data found for correlation.")
                return [AllSlotsReset()]

            final_response += "\n"
            final_response += json.dumps(final_data, indent=2)

            dispatcher.utter_message(
                text=build_nv_response(
                    tracker,
                    col1,
                    final_data,
                    final_response
                )
            )

        except Exception as e:
            dispatcher.utter_message(text=f"Database error: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return [AllSlotsReset()]


    # --------------------------------
    # TOP LOCATIONS
    # --------------------------------

    # def handle_top_locations(self, dispatcher, tracker):

        metric = tracker.get_slot("metric")[0] if tracker.get_slot("metric") is not None else None
        topN = tracker.get_slot("topN")[0] if tracker.get_slot("topN") is not None else None
        dimension = tracker.get_slot("dimension")[0] if tracker.get_slot("dimension") is not None else None
        time = tracker.get_slot("time")[0] if tracker.get_slot("time") is not None else None

        logger.debug(f"topN - {topN}")
        # order = "DESC"
        # if(topN.split()[0] in ['bottom', 'lowest'] and metric in ['latency']):
        #     order = "DESC"
        # elif(topN.split()[0] in ['bottom', 'lowest'] and metric not in ['latency']):
        #     order = "ASC"
        # elif(metric in ['latency']):
        #     order = "ASC"
        topN = int(topN.split()[1])
        logger.debug(f"dimension - {dimension}")

        time_condition = parse_time_condition(time)

        dimension_column = DIMENSION_MAP.get(dimension.lower())

        filters = f"""AND GEOGRAPHYNAME = '{dimension_column}'"""
        if time_condition:
            filters += "AND "
            filters += time_condition

        if not metric:
            dispatcher.utter_message(text="Please specify the metric to compare.")
            return [AllSlotsReset()]

        metric_column = METRIC_MAP.get(metric.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return [AllSlotsReset()]

        try:

            conn = get_db_connection()
            cursor = conn.cursor()

            query=""
            if metric_column == "Users":
                query = f"""
                    SELECT
                        GEOGRAPHY_NAME AS Location,
                        AVG(
                            COALESCE(ENTERPRISEUC, 0) +
                            COALESCE(ANDROIDUC, 0) +
                            COALESCE(IOSUC, 0) +
                            COALESCE(CONSUMERUC, 0)
                        ) AS User_Count
                    FROM netvelocity_user_metrics
                    WHERE 1=1 {filters} 
                    GROUP BY GEOGRAPHY_NAME
                    ORDER BY User_Count DESC
                    LIMIT {topN};
                """
            elif metric_column == "Tests":
                query = f"""
                    SELECT
                        GEOGRAPHY_NAME AS Location,
                        COUNT(*) AS Record_Count
                    FROM netvelocity_user_metrics
                    WHERE 1=1 {filters}
                    GROUP BY GEOGRAPHY_NAME
                    ORDER BY Record_Count DESC
                    LIMIT {topN};
                """
            

            logger.debug(f"TopLocations-Query: {query}")

            cursor.execute(query)

            rows = cursor.fetchall()

            if not rows:
                dispatcher.utter_message(text="No data found for top location.")
                return [AllSlotsReset()]

            response = f"Ranking top {topN} {dimension} by {metric} in {time}\n\n"

            data = []
            metric = OUTPUT_METRIC_MAP.get(metric_column)
            for r in rows:
                data.append({
                    "Location": r[0],
                    f"{metric}": round(r[1], 2) if r[1] else None
                })

            response += json.dumps(data)

            dispatcher.utter_message(text=build_nv_response(tracker, metric_column, json.dumps(data), response))

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]

    def handle_top_locations(self, dispatcher, tracker):

        def get_first(slot):
            return slot[0] if slot and len(slot) > 0 else None

        metric = get_first(tracker.get_slot("metric"))
        topN_raw = get_first(tracker.get_slot("topN"))
        dimension = get_first(tracker.get_slot("dimension"))
        time = get_first(tracker.get_slot("time"))
        app = get_first(tracker.get_slot("app"))
        band = get_first(tracker.get_slot("band"))
        tech = get_first(tracker.get_slot("tech"))

        # ✅ Validation
        if not metric:
            dispatcher.utter_message(text="Please specify the metric.")
            return [AllSlotsReset()]

        if not topN_raw:
            dispatcher.utter_message(text="Please specify top N (e.g., 'top 5').")
            return [AllSlotsReset()]

        if not dimension:
            dispatcher.utter_message(text="Please specify the dimension (zone/state/city/jc).")
            return [AllSlotsReset()]

        # ✅ Parse topN safely
        try:
            parts = topN_raw.lower().split()
            topN = int(parts[1])
        except Exception:
            dispatcher.utter_message(text="Invalid topN format. Use like 'top 5'.")
            return [AllSlotsReset()]

        # ✅ Metric mapping
        metric_column = METRIC_MAP.get(metric.lower())
        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return [AllSlotsReset()]

        output_metric = OUTPUT_METRIC_MAP.get(metric_column)

        # ✅ Dimension mapping
        dimension_column = DIMENSION_MAP.get(dimension.lower())
        if not dimension_column:
            dispatcher.utter_message(text=f"Dimension '{dimension}' is not supported.")
            return [AllSlotsReset()]

        # ✅ Filters
        filters = f" AND GEOGRAPHYNAME = '{dimension_column}'"

        if time:
            time_condition = parse_time_condition(time)
            if time_condition:
                filters += f" AND {time_condition}"

        if app:
            filters += f" AND TESTTYPE = '{app}'"

        if band:
            filters += f" AND BAND = '{band}'"

        if tech:
            filters += f" AND NETWORKTYPE = '{tech}'"

        conn = None
        cursor = None

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # ✅ Metric-specific queries
            if metric_column == "Users":
                query = f"""
                    SELECT
                        GEOGRAPHY_NAME AS Location,
                        AVG(
                            COALESCE(ENTERPRISEUC, 0) +
                            COALESCE(ANDROIDUC, 0) +
                            COALESCE(IOSUC, 0) +
                            COALESCE(CONSUMERUC, 0)
                        ) AS value
                    FROM netvelocity_user_metrics
                    WHERE 1=1 {filters}
                    GROUP BY GEOGRAPHY_NAME
                    ORDER BY value DESC
                    LIMIT {topN}
                """

            elif metric_column == "Tests":
                query = f"""
                    SELECT
                        GEOGRAPHY_NAME AS Location,
                        COUNT(*) AS value
                    FROM netvelocity_user_metrics
                    WHERE 1=1 {filters}
                    GROUP BY GEOGRAPHY_NAME
                    ORDER BY value DESC
                    LIMIT {topN}
                """

            else:
                dispatcher.utter_message(
                    text=f"Top locations not supported for metric '{metric}'."
                )
                return [AllSlotsReset()]

            logger.debug(f"TopLocations-Query: {query}")

            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                dispatcher.utter_message(text="No data found for top locations.")
                return [AllSlotsReset()]

            # ✅ Build chart-friendly + readable response
            labels = []
            values = []
            data = []

            for idx, r in enumerate(rows, start=1):
                location = r[0]
                value = round(r[1], 2) if r[1] is not None else None

                labels.append(location)
                values.append(value)

                data.append({
                    "Rank": idx,
                    "Location": location,
                    output_metric: value
                })

            # ✅ Text response
            response = f"Top {topN} {dimension} by {output_metric}"
            if time:
                response += f" during {time}"
            if app:
                response += f" using {app}"
            if band:
                response += f" on {band}"
            if tech:
                response += f" ({tech})"

            response += ":\n\n"
            response += json.dumps(data, indent=2)

            # ✅ Chart payload (important difference from rank_metrics)
            chart_payload = {
                "labels": labels,
                "values": values,
                "metric": output_metric,
                "dimension": dimension
            }

            dispatcher.utter_message(
                text=build_nv_response(
                    tracker,
                    metric_column,
                    chart_payload,
                    response
                )
            )

        except Exception as e:
            dispatcher.utter_message(text=f"Database error: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return [AllSlotsReset()]

    # --------------------------------
    # BREAKDOWN METRICS
    # --------------------------------

    # def handle_breakdown_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric") if tracker.get_slot("metric") is not None else ["empty"]
        time = tracker.get_slot("time") if tracker.get_slot("time") is not None else ["empty"]
        geo = tracker.get_slot("geo") if tracker.get_slot("geo") is not None else ["empty"]
        app = tracker.get_slot("app") if tracker.get_slot("app") is not None else ["empty"]
        band = tracker.get_slot("band") if tracker.get_slot("band") is not None else ["empty"]
        tech = tracker.get_slot("tech") if tracker.get_slot("tech") is not None else ["empty"]

        logger.debug(f"BreakdownMetrics-1")

        try:

            conn = get_db_connection()
            cursor = conn.cursor()

            logger.debug(f"BreakdownMetrics-2")
            data = []

            for i in metric:
                for j in time:
                    for k in geo:
                        for m in app:
                            for n in band:
                                for p in tech:
                                    metric_column = METRIC_MAP.get(i.lower()) if i != "empty" else i
                                    if not metric:
                                        dispatcher.utter_message(text="Please specify the metric to compare.")
                                        return [AllSlotsReset()]
            
                                    if not metric_column:
                                        dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                                        return [AllSlotsReset()]
                                    output_metric = OUTPUT_METRIC_MAP.get(metric_column)
                                    
                                    time_condition = parse_time_condition(j) if j != "empty" else j

                                    logger.debug(f"BreakdownMetrics-3")

                                    filters = " "
                                    selectors = " "
                                    if time_condition != "empty":
                                        filters += f""" AND {time_condition}"""
                                        logger.debug(f"BreakdownMetrics-31")
                                    if k != "empty":
                                        filters += f" AND GEOGRAPHY_NAME = '{k}'"
                                        selectors += f"""GEOGRAPHY_NAME AS "Location","""
                                        logger.debug(f"BreakdownMetrics-32")
                                    if m != "empty":
                                        filters += f" AND TESTTYPE = '{m}'"
                                        selectors += f"""TESTTYPE AS "App","""
                                    if n != "empty":
                                        filters += f" AND BAND = '{n}'"
                                        selectors += f"""BAND AS Band,"""
                                    if p != "empty":
                                        filters += f" AND NETWORKTYPE = '{p}'"
                                        selectors += f"""NETWORKTYPE AS "Network Type" """

                                    if selectors[-1] == ',':
                                        selectors = selectors[:len(selectors)-1]

                                    logger.debug(f"BreakdownMetrics-4")

                                    query = f"""
                                        SELECT 
                                            AVG({metric_column}) AS "{output_metric}",
                                            {selectors}
                                        FROM netvelocity_kpi_metrics 
                                        WHERE 1=1 {filters}
                                    """
                                    logger.debug(f"BreakdownMetrics-Query: {query}")

                                    cursor.execute(query)

                                    rows = cursor.fetchall()

                                    for r in rows:
                                        row_data = {}

                                        row_data[output_metric] = round(r[0], 2) if r[0] is not None else None

                                        idx = 1

                                        if k != "empty":
                                            row_data["Location"] = r[idx]
                                            idx += 1

                                        if m != "empty":
                                            row_data["App"] = r[idx]
                                            idx += 1

                                        if n != "empty":
                                            row_data["Band"] = r[idx]
                                            idx += 1

                                        if p != "empty":
                                            row_data["Network Type"] = r[idx]
                                            idx += 1

                                        data.append(row_data)

                                    # for r in rows:
                                    #     data.append({
                                    #         f"{output_metric}": round(r[0], 2) if r[0] else None,
                                    #         f"Date": r[1],
                                    #         f"Location": r[2],
                                    #         f"App": r[3],
                                    #         f"Band": r[4],
                                    #         f"Network Type": r[5],
                                    #     })
            
            if data == []:
                dispatcher.utter_message(text="No data found for ranking.")
                return [AllSlotsReset()]

                    
            response = f"Following is your requested comparison: \n\n"
            response += json.dumps(data)

            dispatcher.utter_message(text=build_nv_response(tracker, metric_column, json.dumps(data), response))

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]

    def handle_breakdown_metrics(self, dispatcher, tracker):

        def ensure_list(slot):
            return slot if slot else ["empty"]

        metric_list = ensure_list(tracker.get_slot("metric"))
        time_list = ensure_list(tracker.get_slot("time"))
        geo_list = ensure_list(tracker.get_slot("geo"))
        app_list = ensure_list(tracker.get_slot("app"))
        band_list = ensure_list(tracker.get_slot("band"))
        tech_list = ensure_list(tracker.get_slot("tech"))

        # ✅ Validate metric
        if metric_list == ["empty"]:
            dispatcher.utter_message(text="Please specify the metric.")
            return [AllSlotsReset()]

        conn = None
        cursor = None

        final_data = []
        final_response = ""

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            for metric in metric_list:

                metric_column = METRIC_MAP.get(metric.lower())
                if not metric_column:
                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                    return [AllSlotsReset()]

                output_metric = OUTPUT_METRIC_MAP.get(metric_column)

                for time in time_list:
                    for geo in geo_list:
                        for app in app_list:
                            for band in band_list:
                                for tech in tech_list:

                                    filters = ""

                                    # ✅ Filters (NOT grouping)
                                    if time != "empty":
                                        time_condition = parse_time_condition(time)
                                        if time_condition:
                                            filters += f" AND {time_condition}"

                                    if geo != "empty":
                                        filters += f" AND GEOGRAPHY_NAME = '{geo}'"

                                    # ❗ Breakdown dimension logic
                                    group_by = ""
                                    select_dim = ""

                                    if app == "empty":
                                        group_by = "TESTTYPE"
                                        select_dim = "TESTTYPE AS App"
                                    elif band == "empty":
                                        group_by = "BAND"
                                        select_dim = "BAND AS Band"
                                    elif tech == "empty":
                                        group_by = "NETWORKTYPE"
                                        select_dim = 'NETWORKTYPE AS "Network Type"'
                                    else:
                                        # fallback: no breakdown possible
                                        dispatcher.utter_message(
                                            text="Please leave at least one dimension unspecified for breakdown."
                                        )
                                        return [AllSlotsReset()]

                                    # Apply remaining filters
                                    if app != "empty":
                                        filters += f" AND TESTTYPE = '{app}'"
                                    if band != "empty":
                                        filters += f" AND BAND = '{band}'"
                                    if tech != "empty":
                                        filters += f" AND NETWORKTYPE = '{tech}'"

                                    # ✅ Proper breakdown query
                                    query = f"""
                                        SELECT 
                                            {select_dim},
                                            AVG({metric_column}) AS value
                                        FROM netvelocity_kpi_metrics
                                        WHERE 1=1 {filters}
                                        GROUP BY {group_by}
                                        ORDER BY value DESC
                                    """

                                    logger.debug(f"BreakdownMetrics-Query: {query}")

                                    cursor.execute(query)
                                    rows = cursor.fetchall()

                                    if not rows:
                                        final_response += "No data found for this breakdown.\n\n"
                                        continue

                                    # ✅ Build response block
                                    response = f"{output_metric} breakdown by {group_by}"

                                    if geo != "empty":
                                        response += f" for {geo}"
                                    if time != "empty":
                                        response += f" during {time}"

                                    response += ":\n"

                                    for r in rows:
                                        dim_value = r[0]
                                        value = round(r[1], 2) if r[1] is not None else None

                                        response += f"- {dim_value}: {value}\n"

                                        row_data = {
                                            "Dimension": group_by,
                                            "Category": dim_value,
                                            output_metric: value
                                        }

                                        if geo != "empty":
                                            row_data["Location"] = geo
                                        if time != "empty":
                                            row_data["Time"] = time

                                        final_data.append(row_data)

                                    final_response += response + "\n\n"

            if not final_data:
                dispatcher.utter_message(text="No data found for breakdown.")
                return [AllSlotsReset()]

            final_response += json.dumps(final_data, indent=2)

            dispatcher.utter_message(
                text=build_nv_response(
                    tracker,
                    metric_column,
                    final_data,
                    final_response
                )
            )

        except Exception as e:
            dispatcher.utter_message(text=f"Database error: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return [AllSlotsReset()]

# --------------------------------
# OPTIONAL DIRECT ACTIONS
# --------------------------------

class ActionGetMetrics(Action):

    def name(self):
        return "action_get_metrics"

    def run(self, dispatcher, tracker, domain):

        router = ActionKpiRouter()
        return router.handle_get_metrics(dispatcher, tracker)


class ActionCompareMetrics(Action):

    def name(self):
        return "action_compare_metrics"

    def run(self, dispatcher, tracker, domain):

        router = ActionKpiRouter()
        return router.handle_compare_metrics(dispatcher, tracker)