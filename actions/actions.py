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
    
    splt = str(time_text).split()
    if((splt[0] == "last") and ("day" in splt[2])):
        start = today - timedelta(days=int(splt[1]))
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
        
        if intent == "threshold_metrics":
            return self.handle_threshold_metrics(dispatcher, tracker)
        
        if intent == "compare_metrics":
            return self.handle_compare_metrics(dispatcher, tracker)
        
        if intent == "correlation":
            return self.handle_correlation(dispatcher, tracker)
        
        if intent == "top_locations":
            return self.handle_top_locations(dispatcher, tracker)

        dispatcher.utter_message(text="I couldn't understand the KPI request.")
        return [AllSlotsReset()]



    # --------------------------------
    # GET METRICS
    # --------------------------------

    def handle_get_metrics(self, dispatcher, tracker):

        metric = tracker.get_slot("metric")[0] if tracker.get_slot("metric") is not None else None
        geo = tracker.get_slot("geo")[0] if tracker.get_slot("geo") is not None else None
        time = tracker.get_slot("time")[0] if tracker.get_slot("time") is not None else None
        app = tracker.get_slot("app")[0] if tracker.get_slot("app") is not None else None
        agg = tracker.get_slot("agg")[0] if tracker.get_slot("agg") is not None else None

        if not metric:
            dispatcher.utter_message(text="Please specify which metric you want.")
            return []

        metric_column = METRIC_MAP.get(metric.lower())
        if agg:
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
                agg_text = str(agg) + " " if agg else ""
                response = f"The {agg_text}{metric}"
                if geo:
                    response += f" for {geo}"
                if time:
                    response += f" during {time}"
                if app:
                    response += f" using {app}"
                response += f""" is {value}."""

            

            dispatcher.utter_message(text=response)

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return [AllSlotsReset()]




    # --------------------------------
    # RANK METRICS
    # --------------------------------

    def handle_rank_metrics(self, dispatcher, tracker):

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
                    "Location": r[0],
                    f"{metric}": round(r[1], 2) if r[1] else None,
                    "Rank": r[2]
                })

            response += json.dumps(data)

            dispatcher.utter_message(text=response)

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return []


    # --------------------------------
    # THRESHOLD METRICS
    # --------------------------------

    def handle_threshold_metrics(self, dispatcher, tracker):

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
            return []
        
        metric_column = METRIC_MAP.get(metric.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return []


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

            dispatcher.utter_message(text=response)

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return []



    # --------------------------------
    # COMPARE METRICS
    # --------------------------------

    def handle_compare_metrics(self, dispatcher, tracker):

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
                                        return []
            
                                    if not metric_column:
                                        dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                                        return []
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
                return []

                    
            response = f"Following is your requested comparison: \n\n"
            response += json.dumps(data)

            dispatcher.utter_message(text=response)

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return []


    # --------------------------------
    # CORRELATION METRICS
    # --------------------------------

    def handle_correlation(self, dispatcher, tracker):

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
                    return []

                if not metric_column:
                    dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
                    return []
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
                logger.debug(f"CorrelationMetrics-Query: {query}")

                cursor.execute(query)

                rows = cursor.fetchall()

                for r in rows:
                    row_data = {}

                    row_data[output_metric] = round(r[0], 2) if r[0] is not None else None

                    data.append(row_data)
            
            if data == []:
                dispatcher.utter_message(text="No data found for correlation.")
                return []

                    
            response = f"Correlation between both the metrics: \n\n"
            response += json.dumps(data)

            dispatcher.utter_message(text=response)

            cursor.close()
            conn.close()

        except Exception as e:

            dispatcher.utter_message(text=f"Database error: {str(e)}")

        return []
    
    # --------------------------------
    # TOP LOCATIONS
    # --------------------------------

    def handle_top_locations(self, dispatcher, tracker):

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
            return []

        metric_column = METRIC_MAP.get(metric.lower())

        if not metric_column:
            dispatcher.utter_message(text=f"Metric '{metric}' is not supported.")
            return []

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
                return []

            response = f"Ranking top {topN} {dimension} by {metric} in {time}\n\n"

            data = []
            metric = OUTPUT_METRIC_MAP.get(metric_column)
            for r in rows:
                data.append({
                    "Location": r[0],
                    f"{metric}": round(r[1], 2) if r[1] else None
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