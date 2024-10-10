from flask import Blueprint, jsonify, request

from repository.accidents_repository import *


accidents_bp = Blueprint('accidents', __name__)

@accidents_bp.route('/initialize_database', methods=['POST'])
def init_database():
    result = init_db_with_data()
    if result.is_success():
        return jsonify({
            "message": "Database initialized successfully",
            "details": result.unwrap()
        }), 200
    else:
        return jsonify({
            "error": "Failed to initialize database",
            "details": result.failure()
        }), 500


@accidents_bp.route('/total_accidents/<int:region_code>', methods=['GET'])
def get_total_accidents(region_code):
    result = get_total_accidents_by_region(region_code)
    return (result.map(lambda data: (jsonify({
                "region": region_code,
                "total_accidents": data.get('total_accidents', 0) if data else 0
            }), 200))
            .value_or((jsonify({'error': f'Failed to retrieve total accidents for region {region_code}'}), 400)))


@accidents_bp.route('/accidents_by_period', methods=['GET'])
def get_accidents_period():
    try:
        region = int(request.args.get('region'))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid region. Must be an integer'}), 400

    period_type = request.args.get('period_type')
    date_str = request.args.get('date')

    if not all([region, period_type, date_str]):
        return jsonify({'error': 'Missing required parameters'}), 400

    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        return jsonify({'error': f'Invalid date format. Use YYYY-MM-DD ::{e}'}), 400

    result = get_accidents_by_region_and_period(region, period_type, date)

    return (result.map(lambda data: (jsonify(data), 200 if data.get('total_accidents', 0) > 0 else 404))
            .value_or((jsonify({'error': 'Failed to retrieve accidents data'}), 400)))


@accidents_bp.route('/accidents_by_cause', methods=['GET'])
def get_accidents_cause():
    try:
        region = int(request.args.get('region'))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid region. Must be an integer'}), 400

    result = get_accidents_by_primary_cause(region)

    return (result.map(lambda data: (jsonify(data), 200 if data.get('accident_causes') else 404))
            .value_or((jsonify({'error': 'Failed to retrieve accidents data'}), 400)))



@accidents_bp.route('/injury_statistics', methods=['GET'])
def get_injury_stats():
    try:
        region = int(request.args.get('region'))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid region. Must be an integer'}), 400

    result = get_injury_statistics(region)

    return (result.map(lambda data: (jsonify(data), 200 if data.get('total_injuries', 0) > 0 else 404))
            .value_or((jsonify({'error': 'Failed to retrieve injury statistics'}), 400)))
