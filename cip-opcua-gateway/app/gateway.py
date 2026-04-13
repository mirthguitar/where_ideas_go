"""
CIP/EtherNet-IP to OPC-UA Gateway with Web UI
Reads Rockwell PLC tags via pycomm3, serves them as OPC-UA nodes,
and provides a web dashboard on port 8088.
"""
import asyncio, logging, os, signal, json, threading
from dataclasses import dataclass, field
from datetime import datetime

from pycomm3 import LogixDriver, RequestError, CommError
from asyncua import Server, ua
from flask import Flask, jsonify, request
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("cip_opcua_gateway")
logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Shared state (read by Flask, written by poll loop)
# ---------------------------------------------------------------------------
state_lock = threading.Lock()
gateway_state = {
    "plc_connected": False,
    "plc_address": "",
    "opcua_endpoint": "",
    "poll_interval_ms": 1000,
    "last_poll": None,
    "poll_count": 0,
    "error_count": 0,
    "last_error": None,
    "tags": {},
    "restart_requested": False,
}

CONFIG_PATH = os.getenv("GATEWAY_CONFIG", "/config/tags.json")

# ---------------------------------------------------------------------------
# Config dataclasses
# ---------------------------------------------------------------------------
@dataclass
class TagConfig:
    name: str
    cip_tag: str
    ua_type: str
    description: str = ""
    scan_group: str = "default"

@dataclass
class GatewayConfig:
    plc_address: str
    plc_path: str
    opcua_endpoint: str
    opcua_namespace: str
    poll_interval_ms: int
    tags: list = field(default_factory=list)

def load_config(path=CONFIG_PATH):
    with open(path) as f:
        raw = json.load(f)
    tags = [
        TagConfig(**{k: v for k, v in t.items() if not k.startswith("_")})
        for t in raw.get("tags", [])
        if not t.get("_comment") and "cip_tag" in t
    ]
    return GatewayConfig(
        plc_address=raw["plc_address"],
        plc_path=raw.get("plc_path", "1,0"),
        opcua_endpoint=raw.get("opcua_endpoint", "opc.tcp://0.0.0.0:4840/gateway"),
        opcua_namespace=raw.get("opcua_namespace", "urn:cip-opcua-gateway"),
        poll_interval_ms=raw.get("poll_interval_ms", 1000),
        tags=tags,
    )

def save_config(data, path=CONFIG_PATH):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# ---------------------------------------------------------------------------
# OPC-UA helpers
# ---------------------------------------------------------------------------
UA_TYPE_MAP = {
    "Float":  ua.VariantType.Float,
    "Double": ua.VariantType.Double,
    "Int16":  ua.VariantType.Int16,
    "Int32":  ua.VariantType.Int32,
    "Int64":  ua.VariantType.Int64,
    "UInt16": ua.VariantType.UInt16,
    "UInt32": ua.VariantType.UInt32,
    "Bool":   ua.VariantType.Boolean,
    "String": ua.VariantType.String,
}

def cast_value(raw, ua_type):
    if raw is None:
        return None
    try:
        if ua_type in ("Float", "Double"):   return float(raw)
        if ua_type in ("Int16","Int32","Int64","UInt16","UInt32"): return int(raw)
        if ua_type == "Bool":   return bool(raw)
        if ua_type == "String": return str(raw)
    except (TypeError, ValueError):
        pass
    return raw

async def build_ua_server(cfg):
    server = Server()
    await server.init()
    server.set_endpoint(cfg.opcua_endpoint)
    server.set_server_name("CIP OPC-UA Gateway")
    # NOTE: set_security_policy is NOT awaitable in asyncua 1.1.x
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    idx = await server.register_namespace(cfg.opcua_namespace)
    plc_node = await server.nodes.objects.add_folder(idx, "PLC")
    groups, ua_nodes = {}, {}
    for tag in cfg.tags:
        grp = tag.scan_group
        if grp not in groups:
            groups[grp] = await plc_node.add_folder(idx, grp)
        vtype = UA_TYPE_MAP.get(tag.ua_type, ua.VariantType.Variant)
        default = cast_value(0, tag.ua_type) if tag.ua_type != "String" else ""
        node = await groups[grp].add_variable(idx, tag.name, ua.Variant(default, vtype))
        await node.set_writable(False)
        if tag.description:
            await node.write_attribute(
                ua.AttributeIds.Description,
                ua.DataValue(ua.Variant(ua.LocalizedText(tag.description))),
            )
        ua_nodes[tag.cip_tag] = node
        with state_lock:
            gateway_state["tags"][tag.cip_tag] = {
                "name": tag.name, "value": None, "ua_type": tag.ua_type,
                "group": tag.scan_group, "description": tag.description,
                "last_update": None, "error": None,
            }
        log.info("Registered UA node: %s/%s (%s)", grp, tag.name, tag.ua_type)
    return server, ua_nodes

# ---------------------------------------------------------------------------
# CIP poll loop
# ---------------------------------------------------------------------------
async def poll_loop(cfg, ua_nodes, stop_event):
    interval = cfg.poll_interval_ms / 1000.0
    cip_tags = [t.cip_tag for t in cfg.tags]
    type_map  = {t.cip_tag: t.ua_type for t in cfg.tags}
    with state_lock:
        gateway_state["plc_address"]     = cfg.plc_address
        gateway_state["opcua_endpoint"]  = cfg.opcua_endpoint
        gateway_state["poll_interval_ms"] = cfg.poll_interval_ms
    consecutive_errors = 0

    while not stop_event.is_set():
        if gateway_state.get("restart_requested"):
            stop_event.set()
            break
        try:
            # NOTE: LogixDriver takes only the IP — no path= kwarg in pycomm3 1.2.x
            with LogixDriver(cfg.plc_address) as plc:
                with state_lock:
                    gateway_state["plc_connected"] = True
                    gateway_state["last_error"] = None
                log.info("Connected to PLC at %s", cfg.plc_address)
                consecutive_errors = 0
                while not stop_event.is_set():
                    if gateway_state.get("restart_requested"):
                        stop_event.set()
                        break
                    try:
                        results = plc.read(*cip_tags)
                        if not isinstance(results, list):
                            results = [results]
                        now = datetime.utcnow().isoformat() + "Z"
                        for tr in results:
                            if tr.error:
                                with state_lock:
                                    if tr.tag in gateway_state["tags"]:
                                        gateway_state["tags"][tr.tag]["error"] = tr.error
                                log.warning("Read error %s: %s", tr.tag, tr.error)
                                continue
                            node = ua_nodes.get(tr.tag)
                            if not node:
                                continue
                            val = cast_value(tr.value, type_map[tr.tag])
                            vt  = UA_TYPE_MAP.get(type_map[tr.tag], ua.VariantType.Variant)
                            await node.write_value(ua.DataValue(
                                Value=ua.Variant(val, vt),
                                StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                            ))
                            with state_lock:
                                if tr.tag in gateway_state["tags"]:
                                    gateway_state["tags"][tr.tag]["value"]       = val
                                    gateway_state["tags"][tr.tag]["last_update"] = now
                                    gateway_state["tags"][tr.tag]["error"]       = None
                        with state_lock:
                            gateway_state["last_poll"]  = now
                            gateway_state["poll_count"] += 1
                        log.debug("Poll #%d OK", gateway_state["poll_count"])
                    except RequestError as e:
                        with state_lock:
                            gateway_state["error_count"] += 1
                            gateway_state["last_error"]   = str(e)
                        log.error("CIP RequestError: %s", e)
                    await asyncio.sleep(interval)

        except CommError as e:
            consecutive_errors += 1
            backoff = min(2.0 ** consecutive_errors, 30.0)
            with state_lock:
                gateway_state["plc_connected"] = False
                gateway_state["error_count"]  += 1
                gateway_state["last_error"]    = str(e)
            log.error("PLC CommError: %s — retry in %.0fs", e, backoff)
            await asyncio.sleep(backoff)
        except Exception as e:
            consecutive_errors += 1
            backoff = min(2.0 ** consecutive_errors, 30.0)
            with state_lock:
                gateway_state["plc_connected"] = False
                gateway_state["error_count"]  += 1
                gateway_state["last_error"]    = str(e)
            log.exception("Unexpected error — retry in %.0fs", backoff)
            await asyncio.sleep(backoff)

    with state_lock:
        gateway_state["plc_connected"] = False

# ---------------------------------------------------------------------------
# Flask web UI  (port 8088 — avoids conflicts with common services)
# ---------------------------------------------------------------------------
flask_app = Flask(__name__)
CORS(flask_app)
WEB_PORT = int(os.getenv("WEB_PORT", "8088"))

@flask_app.route("/")
def index():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    with open(html_path) as f:
        return f.read()

@flask_app.route("/api/state")
def api_state():
    with state_lock:
        return jsonify(dict(gateway_state))

@flask_app.route("/api/config", methods=["GET"])
def api_config_get():
    try:
        with open(CONFIG_PATH) as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@flask_app.route("/api/config", methods=["POST"])
def api_config_post():
    try:
        updates = request.json
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
        for k, v in updates.items():
            if k != "tags":
                raw[k] = v
        save_config(raw)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@flask_app.route("/api/tags", methods=["POST"])
def api_tags_post():
    try:
        tag = request.json
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
        raw.setdefault("tags", []).append({
            "name":        tag["name"],
            "cip_tag":     tag["cip_tag"],
            "ua_type":     tag.get("ua_type", "Float"),
            "description": tag.get("description", ""),
            "scan_group":  tag.get("scan_group", "default"),
        })
        save_config(raw)
        with state_lock:
            gateway_state["tags"][tag["cip_tag"]] = {
                "name": tag["name"], "value": None,
                "ua_type": tag.get("ua_type", "Float"),
                "group": tag.get("scan_group", "default"),
                "description": tag.get("description", ""),
                "last_update": None, "error": None,
            }
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@flask_app.route("/api/tags/<path:cip_tag>", methods=["DELETE"])
def api_tags_delete(cip_tag):
    try:
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
        raw["tags"] = [t for t in raw.get("tags", []) if t.get("cip_tag") != cip_tag]
        save_config(raw)
        with state_lock:
            gateway_state["tags"].pop(cip_tag, None)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@flask_app.route("/api/restart", methods=["POST"])
def api_restart():
    with state_lock:
        gateway_state["restart_requested"] = True
    return jsonify({"ok": True})

def run_flask():
    log.info("Web UI starting on http://0.0.0.0:%d", WEB_PORT)
    flask_app.run(host="0.0.0.0", port=WEB_PORT, debug=False, use_reloader=False)

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
async def main():
    # Start Flask in a background daemon thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    while True:
        with state_lock:
            gateway_state["restart_requested"] = False

        log.info("Loading config: %s", CONFIG_PATH)
        cfg = load_config(CONFIG_PATH)

        log.info("Starting OPC-UA server on %s", cfg.opcua_endpoint)
        server, ua_nodes = await build_ua_server(cfg)

        stop_event = asyncio.Event()

        def _shutdown(sig, _frame):
            log.info("Signal %s — shutting down", sig.name)
            stop_event.set()

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT,  _shutdown)

        async with server:
            log.info("Gateway running. Web UI: http://0.0.0.0:%d", WEB_PORT)
            await poll_loop(cfg, ua_nodes, stop_event)

        with state_lock:
            restart = gateway_state.get("restart_requested")
        if not restart:
            break
        log.info("Restarting in 1s...")
        await asyncio.sleep(1)

    log.info("Gateway stopped.")

if __name__ == "__main__":
    asyncio.run(main())
