import itertools
import re

# ✅ Define entity values
ENTITY_VALUES = {
    "metric": [
        "download speed",
        "upload speed",
        "latency",
        "coverage",
        "packet loss",
        "jitter",
        "rsrp",
        "sinr"
    ],
    "metric2": [
        "download speed",
        "upload speed",
        "latency",
        "coverage",
        "packet loss",
        "jitter",
        "rsrp",
        "sinr"
    ],
    "rating": [
        "best",
        "worst",
        "maximum",
        "highest",
        "lowest",
        "minimum"
    ],
    "geo": [
        "Mumbai",
        "Delhi",
        "HYD-001",
        "Bangalore",
        "Maharashtra",
        "Pune",
        "GJ"
    ],
    "time": [
        "today",
        "yesterday",
        "last 7 days",
        "last 30 days"
    ],
    "app": [
        "HelloJio SpeedTest",
        "NV SpeedTest",
        "All apps"
    ],
    "dimension": [
        "city",
        "state",
        "jiocenter",
        "zone",
        "circle",
        "R4G State"
    ],
    "dimension2": [
        "cities",
        "states",
        "jiocenters",
        "zones",
        "circles",
        "R4G States"
    ],
    "tech": [
        "LTE",
        "NR"
    ],
    "tech2": [
        "LTE",
        "NR"
    ],
    "agg": [
        "minimum",
        "maximum",
        "average",
        "median",
        "P90"
    ]
}

# ✅ Tech → Band mapping
TECH_BAND_MAP = {
    "LTE": ["850", "1800", "2300", "All"],
    "NR": ["n28", "n78", "All"]
}


def extract_entities(template):
    return re.findall(r"\[(.*?)\]", template)


def generate_sentences(template):
    entities = extract_entities(template)

    # Prepare value lists (except band, handled separately)
    value_lists = []
    for e in entities:
        if e == "band":
            value_lists.append([None])  # placeholder
        else:
            value_lists.append(ENTITY_VALUES[e])

    sentences = []

    # Generate combinations excluding band
    for combo in itertools.product(*value_lists):

        combo_dict = dict(zip(entities, combo))

        # Handle band based on tech
        if "band" in entities:
            tech_val = combo_dict.get("tech")

            if tech_val:
                band_options = TECH_BAND_MAP.get(tech_val, ["All"])
            else:
                band_options = ["All"]
        else:
            band_options = [None]

        for band_val in band_options:

            sentence = template

            for entity in entities:
                value = combo_dict.get(entity)

                if entity == "band":
                    value = band_val

                if value is None:
                    continue

                annotated = f"[{value}]({entity})"
                sentence = sentence.replace(f"[{entity}]", annotated, 1)

            sentences.append(sentence)

    return sentences


def to_rasa_format(sentences, intent="get_metrics"):
    rasa_block = f"- intent: {intent}\n  examples: |\n"

    for s in sentences:
        rasa_block += f"    - {s}\n"

    return rasa_block


# 🔥 Example usage
if __name__ == "__main__":

    template = "Which [dimension] has the [rating] [metric]"

    sentences = generate_sentences(template)

    print(f"Generated {len(sentences)} sentences\n")

    rasa_output = to_rasa_format(sentences)

    with open("data.txt", "w") as f:
        f.write(rasa_output)

    print(rasa_output)