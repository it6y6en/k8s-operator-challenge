import threading
# import time
import requests
import yaml
import sys
import os
import logging
from flask import Flask, jsonify

CONFIG_PATH = "/var/server/config.yaml"

app = Flask(__name__)

logger = logging.getLogger("flask_app")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

app.logger.handlers = []        # remove default Flask handlers
app.logger.propagate = False    # don't propagate to root logger
app.logger.addHandler(handler)  # use our handler
app.logger.setLevel(logging.INFO)

config = {"replicas": 0, "timer": 6000}
pod_prefix = ""
pod_number = 0
service_name = ""
stop_event = threading.Event()


def load_config():
    """Load configuration from YAML file."""
    global config
    try:
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f) or config
        app.logger.info(f"Config loaded: {config}")
    except Exception as e:
        app.logger.error(f"Failed to load config: {e}")


def define_cluster():
    """Get Statefulset set name and current's pod number"""
    global pod_prefix
    global pod_number
    global service_name
    hostname = os.environ['HOSTNAME']
    service_name = os.environ.get('HEADLESS_SERVICE', "")
    pod_prefix = hostname.rsplit("-", 1)[0]
    pod_number = int(hostname.rsplit("-", 1)[1])
    app.logger.info(f"Server #{pod_number} from Statefulset: {pod_prefix}")
    app.logger.info(f"Service name: {service_name}")


def ping_loop():
    """Background loop that pings replicas periodically."""
    while not stop_event.is_set():
        load_config()
        replicas = config.get("replicas")
        timer = config.get("timer")

        for i in range(0, replicas):
            if i == pod_number:
                #app.logger.info(f"same pod {pod_number} == {i}")
                continue

            url = f"http://{pod_prefix}-{i}{'.'+service_name if service_name else ''}/ping"
            #url = f"http://{pod_prefix}-{i}/ping"
            try:
                r = requests.get(url, timeout=1)
                app.logger.info(f"Ping from server {pod_number} to server {i} -> {r.text}")
            except Exception as e:
                app.logger.warning(f"Ping {url} failed: {e}")

        stop_event.wait(timer)


def start_background_thread():
    """Start ping thread once (called at import)."""
    #load_config()
    define_cluster()
    t = threading.Thread(target=ping_loop, daemon=True)
    t.start()
    app.logger.info("Background ping thread started")


start_background_thread()


@app.route('/')
def index():
    return "ok"


@app.route('/ping')
def ping():
    return "pong\n"


@app.route("/config")
def get_config():
    return config


@app.route("/reload")
def reload_config():
    """Reload config from file."""
    load_config()
    return "reloaded"
