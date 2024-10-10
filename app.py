from flask import Flask
from controllers.accidents_controller import accidents_bp


def create_app():
    app = Flask(__name__)

    app.register_blueprint(accidents_bp, url_prefix='/accidents')

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
