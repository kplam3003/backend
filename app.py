from flask import Flask
import config
from routes import configure_routes

app = Flask(__name__)

configure_routes(app)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(config.PORT))    