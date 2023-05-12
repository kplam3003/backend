import json
from flask import Flask, url_for
from routes import configure_routes

def test_index():
    app = Flask(__name__)
    configure_routes(app)
    client = app.test_client()

    res = client.get('/')
    assert res.status_code == 200
    expected = {'message': 'Hello World'}
    assert expected == json.loads(res.get_data(as_text=True))

def test_sync_request():
    app = Flask(__name__)
    configure_routes(app)
    client = app.test_client()

    payload = {
        "case_study_id": 30000,
        "case_study_name": "Stripe",
        "dimension_config_id": 1,
        "polarities": [
            {
                "nlp_polarity": "N",
                "modified_polarity": -1.0
            },
            {
                "nlp_polarity": "N+",
                "modified_polarity": -2.0
            },
            {
                "nlp_polarity": "P",
                "modified_polarity": 1.0
            },
            {
                "nlp_polarity": "P+",
                "modified_polarity": 2.0
            },
            {
                "nlp_polarity": "NEU",
                "modified_polarity": 0.0
            },
            {
                "nlp_polarity": "NONE",
                "modified_polarity": 0.0
            }
        ],
        "companies": [
            {
                "company_id": 1,
                "company_name": "Temenos",
                "source_name": "Capterra",
                "source_id": 3,
                "nlp_pack": "VoC Banking",
                "nlp_type": "VoC",
                "is_target": True,
                "url": "https://www.capterra.com/p/111334/TEMENOS-T24/"
            },
            {
                "company_id": 2,
                "company_name": "VNPAY",
                "source_name": "GooglePlay",
                "source_id": 1,
                "nlp_pack": "VoC Banking",
                "nlp_type": "VoC",
                "is_target": False,
                "url": "https://play.google.com/store/apps/details?id=com.vnpay.namabank"
            },
            {
                "company_id": 3,
                "company_name": "Vigo",
                "source_name": "AppleStore",
                "source_id": 2,
                "nlp_pack": "VoC Banking",
                "nlp_type": "VoC",
                "is_target": False,
                "url": "https://apps.apple.com/us/app/vigo-send-cash-24-7/id1513772236"
            },
            {
                "company_id": 4,
                "company_name": "Stripe",
                "source_id": 4,
                "source_name": "G2",
                "nlp_pack": "VoC Banking",
                "nlp_type": "VoC",
                "is_target": True,
                "url": "https://www.g2.com/products/stripe-billing/reviews"
            },
        ]
    }
    res = client.post('/sync', headers={'Content-Type': 'application/json'}, data=json.dumps(payload))
    assert res.status_code == 200
    expected = payload
    actual = json.loads(res.data)
    assert expected == actual