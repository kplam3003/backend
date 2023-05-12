import config
import service
import helper

from flask import jsonify
from flask import request
from src.services.company_data_source import (
    validate_company_data_source,
    trigger_company_data_source,
    fetch_review_summary,
    fetch_rating_summary,
    fetch_polarity_summary
)


def configure_routes(app):
    @app.route('/', methods=['GET'])
    def hello():
        return jsonify({'message': 'Hello World'}), 200

    @app.route('/health', methods=['GET'])
    def health():
        return jsonify({'message': 'OK'}), 200

    @app.route('/chart', methods=['GET'])
    def getChart():
        params = request.args.to_dict()
        task = service.request_task(
            helper.get_sorted_query_string(request), params)
        return jsonify(task), 200

    @app.route('/sync', methods=['POST'])
    def sync():
        payload = request.json
        if not helper.is_valid_request_payload(payload):
            return jsonify(payload), 400
        result = service.sync_request(payload)
        if result:
            return jsonify(payload), 200
        else:
            return jsonify(payload), 500

    @app.route('/company-datasource/<company_datasource_id>/validate', methods=['POST'])
    def validate_company_datasource(company_datasource_id):
        payload = request.json
        payload["company_datasource_id"] = company_datasource_id
        result = validate_company_data_source(payload)
        if result:
            return jsonify(payload), 200
        else:
            return jsonify(payload), 500

    @app.route('/company-datasource/<company_datasource_id>/trigger', methods=['POST'])
    def trigger_company_datasource(company_datasource_id):
        payload = request.json
        result = trigger_company_data_source(payload)
        if result:
            return jsonify(payload), 200
        else:
            return jsonify(payload), 500

    @app.route('/company-datasource/<int:company_datasource_id>/summary', methods=['GET'])
    def fetch_company_datasource_summary(company_datasource_id):
        nlp_type = request.args.get("nlp_type")
        summary_info = request.args.get("info")
        status_code = 200

        if summary_info == 'num_reviews':
            total, yearly, quarterly = fetch_review_summary(
                company_datasource_id=company_datasource_id,
                nlp_type=nlp_type
            )
            return_data = {
                "num_reviews": {
                    'total': total,
                    'yearly': yearly,
                    'quarterly': quarterly
                },
            }
        elif summary_info == 'rating':
            total, yearly, quarterly = fetch_rating_summary(
                company_datasource_id=company_datasource_id,
                nlp_type=nlp_type
            )

            return_data = {
                "rating": {
                    'total': total,
                    'yearly': yearly,
                    'quarterly': quarterly
                },
            }
        elif summary_info == 'polarity':
            total, yearly, quarterly = fetch_polarity_summary(
                company_datasource_id=company_datasource_id,
                nlp_type=nlp_type
            )

            return_data = {
                "polarity": {
                    'total': total,
                    'yearly': yearly,
                    'quarterly': quarterly
                },
            }
        elif not summary_info or summary_info == 'all':
            num_reviews_total, num_reviews_yearly, num_reviews_quarterly = fetch_review_summary(
                company_datasource_id=company_datasource_id,
                nlp_type=nlp_type
            )

            rating_total, rating_yearly, rating_quarterly = fetch_rating_summary(
                company_datasource_id=company_datasource_id,
                nlp_type=nlp_type
            )

            polarity_total, polarity_yearly, polarity_quarterly = fetch_polarity_summary(
                company_datasource_id=company_datasource_id,
                nlp_type=nlp_type
            )

            return_data = {
                "num_reviews": {
                    'total': num_reviews_total,
                    'yearly': num_reviews_yearly,
                    'quarterly': num_reviews_quarterly,
                },
                "rating": {
                    'total': rating_total,
                    'yearly': rating_yearly,
                    'quarterly': rating_quarterly,
                },
                "polarity": {
                    'total': polarity_total,
                    'yearly': polarity_yearly,
                    'quarterly': polarity_quarterly,
                },
            }
        else:
            return_data = {}
            status_code = 400

        return_data = return_data

        return jsonify(return_data), status_code

    @app.route('/export-chart-mapping', methods=['GET'])
    def get_export_chart_mapping():
        nlp_type_id = request.args.get("nlp_type")
        result = service.get_export_chart_mapping_by_nlp_type_id(nlp_type_id)
        return jsonify(result), 200

    @app.route('/case-study/<case_study_id>/raw-data', methods=['GET'])
    def get_case_study_raw_data(case_study_id):

        params = request.args.to_dict()
        result = []
        if params.get("type").lower() == config.CASE_STUDY_NLP_OUTPUT_TYPE.lower():
            result = service.get_case_study_nlp_output(
                case_study_id=case_study_id, request_param_dict=params)

        return jsonify(result), 200
