COUNTRY_MAP = {

    "US": {
        "bank": "Federal Reserve",
        "code": "FED",
        "currency": "USD",
        "document_type": "statement",
        "canonical": "data/geoscen/cb/fed",
        "output": "us_policy_language_latest.json",
    },

    "EU": {
        "bank": "European Central Bank",
        "code": "ECB",
        "currency": "EUR",
        "document_type": "policy_decision",
        "canonical": "data/geoscen/cb/ecb",
        "output": "eu_policy_language_latest.json",
    },

    "GB": {
        "bank": "Bank of England",
        "code": "BOE",
        "currency": "GBP",
        "document_type": "mpc",
        "canonical": "data/geoscen/cb/boe",
        "output": "gb_policy_language_latest.json",
    },

    "JP": {
        "bank": "Bank of Japan",
        "code": "BOJ",
        "currency": "JPY",
        "document_type": "policy_decision",
        "canonical": "data/geoscen/cb/boj",
        "output": "jp_policy_language_latest.json",
    },

    "CA": {
        "bank": "Bank of Canada",
        "code": "BOC",
        "currency": "CAD",
        "document_type": "rate_decision",
        "canonical": "data/geoscen/cb/boc",
        "output": "ca_policy_language_latest.json",
    },

    "AU": {
        "bank": "Reserve Bank of Australia",
        "code": "RBA",
        "currency": "AUD",
        "document_type": "statement",
        "canonical": "data/geoscen/cb/rba",
        "output": "au_policy_language_latest.json",
    },

}


def build_latest(jurisdiction):

    cfg = COUNTRY_MAP[jurisdiction]

    print("=" * 70)
    print(cfg["bank"])
    print("=" * 70)

    ####################################################
    # MOVE THE CURRENT USA NOTEBOOK BUILD LOGIC HERE
    #
    # Replace hardcoded:
    #
    #     US
    #     Federal Reserve
    #     USD
    #     statement
    #
    # with:
    #
    #     cfg["bank"]
    #     cfg["currency"]
    #     cfg["document_type"]
    #     cfg["canonical"]
    #     cfg["output"]
    #
    ####################################################

    cfg["bank"]
    cfg["code"]
    cfg["currency"]
    cfg["document_type"]
    cfg["canonical"]
    cfg["output"]
