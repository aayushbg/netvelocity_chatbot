from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.events import AllSlotsReset
from rasa_sdk.executor import CollectingDispatcher
import mysql.connector
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)

# -------------------------------
# Metric → DB column mapping
# -------------------------------

METRIC_MAP = {
    "download speed": "DLRATE",
    "upload speed": "ULRATE",
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

    if "last 7 days" in time_text:
        start = today - timedelta(days=7)
        return f"CREATEDON >= '{start.date()}'"

    if "last 30 days" in time_text:
        start = today - timedelta(days=30)
        return f"CREATEDON >= '{start.date()}'"
    
    if "last 60 days" in time_text:
        start = today - timedelta(days=60)
        return f"CREATEDON >= '{start.date()}'"
    
    if "last 90 days" in time_text:
        start = today - timedelta(days=90)
        return f"CREATEDON >= '{start.date()}'"

    if "yesterday" in time_text:
        start = today - timedelta(days=1)
        return f"CREATEDON = '{start.date()}'"

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

        if intent == "get_metrics":
            return self.handle_get_metrics(dispatcher, tracker)

        if intent == "rank_metrics":
            return self.handle_rank_metrics(dispatcher, tracker)

        dispatcher.utter_message(text="I couldn't understand the KPI request.")
        return [AllSlotsReset()]



    # --------------------------------
    # GET METRICS
    # --------------------------------

    def handle_get_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric")
        geo = tracker.get_slot("geo")
        time = tracker.get_slot("time")
        app = tracker.get_slot("app")
        agg = tracker.get_slot("agg")

        if not metric:
            dispatcher.utter_message(text="Please specify which metric you want.")
            return []

        metric_column = METRIC_MAP.get(metric.lower())
        agg = AGG_MAP.get(agg.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return []
        
        query = ""
        filters = ""

        if geo:
            filters += f" AND GEOGRAPHY_NAME = '{geo}'"

        if app and app != "All apps":
            filters += f" AND TESTTYPE = '{app}'"

        time_condition = parse_time_condition(time)

        if time_condition:
            filters += f" AND {time_condition}"

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
                agg_text = agg if agg else ""
                response = f"The {agg_text} {metric}"
                if geo:
                    response += f" for {geo}"
                if time:
                    response += f" during {time}"
                if app:
                    response += f" using {app}"
                response += f""" is {value}"""

            

            dispatcher.utter_message(text=response)

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return []



    # --------------------------------
    # RANK METRICS
    # --------------------------------

    def handle_rank_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric")
        topN = tracker.get_slot("topN")
        logger.debug(f"topN - {topN}")
        order = "DESC"
        if(topN.split()[0] in ['bottom', 'lowest'] and metric in ['latency']):
            order = "DESC"
        elif(topN.split()[0] in ['bottom', 'lowest'] and metric not in ['latency']):
            order = "ASC"
        elif(metric in ['latency']):
            order = "ASC"
        topN = int(topN.split()[1])
        dimension = tracker.get_slot("dimension")
        logger.debug(f"dimension - {dimension}")
        time = tracker.get_slot("time")

        time_condition = parse_time_condition(time)

        dimension_column = DIMENSION_MAP.get(dimension.lower())

        filters = f"""AND GEOGRAPHYNAME = '{dimension_column}'"""
        if time_condition:
            filters += "AND "
            filters = time_condition

        if not metric:
            dispatcher.utter_message(text="Please specify the metric to compare.")
            return []

        metric_column = METRIC_MAP.get(metric.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return []

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
                    "dimension": r[0],
                    f"{metric}": round(r[1], 2) if r[1] else None,
                    "rank": r[2]
                })

            response += json.dumps(data)

            dispatcher.utter_message(text=response)

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return []



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