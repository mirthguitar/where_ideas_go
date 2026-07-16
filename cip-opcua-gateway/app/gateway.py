"""
CIP/EtherNet-IP to OPC-UA Gateway with Web UI
- Auto-discovers tags from Rockwell PLC
- Web UI for full configuration
- Serves OPC-UA on port 4840, Web UI on port 8088
"""
import asyncio, logging, os, signal, json, threading, socket
from dataclasses import dataclass, field
from datetime import datetime

from pycomm3 import LogixDriver, RequestError, CommError
from asyncua import Server, ua
from flask import Flask, jsonify, request
from flask_cors import CORS

logging.basicConfig(
    level=logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("cip_opcua_gateway")
logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)

def _host_ip() -> str:
    """Return the primary non-loopback host IP."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.254.254.254", 1))
        return s.getsockname()[0]
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"
    finally:
        try: s.close()
        except Exception: pass

HOST_IP = _host_ip()

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
    "discovery_running": False,
    "discovered_tags": [],
}

CONFIG_PATH = os.getenv("GATEWAY_CONFIG", "/config/tags.json")

# ---------------------------------------------------------------------------
# CIP -> OPC-UA type mapping
# ---------------------------------------------------------------------------
# pycomm3 returns string type names — map them to OPC-UA types
CIP_TO_UA = {
    "REAL":   "Float",
    "LREAL":  "Double",
    "DINT":   "Int32",
    "INT":    "Int16",
    "SINT":   "Int16",
    "LINT":   "Int64",
    "UDINT":  "UInt32",
    "UINT":   "UInt16",
    "USINT":  "UInt16",
    "BOOL":   "Bool",
    "STRING": "String",
    "WORD":   "UInt16",
    "DWORD":  "UInt32",
    "LWORD":  "Int64",
}

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

# ---------------------------------------------------------------------------
# Config
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
    raw_endpoint = raw.get("opcua_endpoint", "opc.tcp://0.0.0.0:4840/gateway")
    # Replace 0.0.0.0 with the real host IP so OPC-UA clients know where to connect
    endpoint = raw_endpoint.replace("0.0.0.0", HOST_IP)
    return GatewayConfig(
        plc_address=raw.get("plc_address", ""),
        plc_path=raw.get("plc_path", "1,0"),
        opcua_endpoint=endpoint,
        opcua_namespace=raw.get("opcua_namespace", "urn:cip-opcua-gateway"),
        poll_interval_ms=raw.get("poll_interval_ms", 1000),
        tags=tags,
    )

def save_config(data, path=CONFIG_PATH):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def cast_value(raw, ua_type):
    if raw is None:
        return None
    try:
        if ua_type in ("Float", "Double"):                        return float(raw)
        if ua_type in ("Int16","Int32","Int64","UInt16","UInt32"): return int(raw)
        if ua_type == "Bool":   return bool(raw)
        if ua_type == "String": return str(raw)
    except (TypeError, ValueError):
        pass
    return raw

# ---------------------------------------------------------------------------
# Tag Discovery (runs in a thread so it doesn't block the event loop)
# ---------------------------------------------------------------------------
def discover_tags_thread(plc_address: str):
    """Connect to PLC and enumerate tags: controller-scoped atomics, struct
    members, and all program-scoped tags."""
    with state_lock:
        gateway_state["discovery_running"] = True
        gateway_state["discovered_tags"] = []
        gateway_state["last_error"] = None

    log.info("Discovering tags from %s ...", plc_address)

    def _tag_entry(tag_name, cip_type, source="controller", cip_prefix=""):
        ua_type = CIP_TO_UA.get(cip_type.upper())
        if not ua_type:
            return None
        parts = tag_name.replace("Program:", "").split("_")
        grp = parts[0] if len(parts) > 1 else source
        cip_tag = f"{cip_prefix}{tag_name}" if cip_prefix else tag_name
        return {
            "cip_tag":     cip_tag,
            "name":        cip_tag,
            "cip_type":    cip_type.upper(),
            "ua_type":     ua_type,
            "scan_group":  grp,
            "description": f"{cip_type.upper()} ({source})",
        }

    def _collect(raw_tags, source, discovered, cip_prefix=""):
        """Walk a raw tag list and append atomic entries, expanding struct members."""
        for t in raw_tags:
            tag_name      = t.get('tag_name', '')    if isinstance(t, dict) else t.tag_name
            tag_type      = t.get('tag_type', '')    if isinstance(t, dict) else getattr(t, 'tag_type', '')
            data_type     = t.get('data_type', {})   if isinstance(t, dict) else getattr(t, 'data_type', {})
            data_type_name= t.get('data_type_name','') if isinstance(t, dict) else getattr(t, 'data_type_name', '')
            if not tag_name:
                continue
            if tag_type == 'atomic' or (tag_type == '' and CIP_TO_UA.get((data_type_name or '').upper())):
                entry = _tag_entry(tag_name, data_type_name, source, cip_prefix)
                if entry:
                    discovered.append(entry)
            elif tag_type == 'struct' and isinstance(data_type, dict):
                # Expand atomic members of the struct (skip array members)
                for member_name, member in data_type.get('internal_tags', {}).items():
                    if not isinstance(member, dict):
                        continue
                    if member.get('array', 0) != 0:
                        continue   # skip array members
                    if member.get('tag_type', 'atomic') != 'atomic':
                        continue
                    m_type = member.get('data_type_name', '')
                    entry = _tag_entry(f"{tag_name}.{member_name}", m_type, source, cip_prefix)
                    if entry:
                        discovered.append(entry)

    try:
        with LogixDriver(plc_address) as plc:
            discovered = []

            # program='*' returns controller + all program-scoped tags in one call.
            # Program-scoped tag names are already fully qualified by pycomm3:
            # e.g. 'Program:FastProgram.MyTag' — no prefix manipulation needed.
            all_tags = plc.get_tag_list(program='*')
            log.info("Tag list (all scopes): %d raw tags", len(all_tags))
            _collect(all_tags, "all", discovered)

            with state_lock:
                gateway_state["discovered_tags"] = discovered
            log.info("Discovery complete: %d total tags", len(discovered))
    except Exception as e:
        log.exception("Discovery failed")
        with state_lock:
            gateway_state["last_error"] = f"Discovery failed: {e}"
    finally:
        with state_lock:
            gateway_state["discovery_running"] = False

# ---------------------------------------------------------------------------
# OPC-UA server
# ---------------------------------------------------------------------------
async def build_ua_server(cfg):
    server = Server()
    await server.init()
    server.set_endpoint(cfg.opcua_endpoint)
    server.set_server_name("CIP OPC-UA Gateway")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    idx = await server.register_namespace(cfg.opcua_namespace)
    plc_node = await server.nodes.objects.add_folder(idx, "PLC")
    groups, ua_nodes = {}, {}
    for tag in cfg.tags:
        grp = tag.scan_group
        if grp not in groups:
            groups[grp] = await plc_node.add_folder(idx, grp)
        vtype   = UA_TYPE_MAP.get(tag.ua_type, ua.VariantType.Variant)
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
        log.info("UA node: %s/%s (%s)", grp, tag.name, tag.ua_type)
    return server, ua_nodes

# ---------------------------------------------------------------------------
# CIP poll loop
# ---------------------------------------------------------------------------
async def poll_loop(cfg, ua_nodes, stop_event):
    interval  = cfg.poll_interval_ms / 1000.0
    cip_tags  = [t.cip_tag for t in cfg.tags]
    type_map  = {t.cip_tag: t.ua_type for t in cfg.tags}

    with state_lock:
        gateway_state["plc_address"]      = cfg.plc_address
        gateway_state["opcua_endpoint"]   = cfg.opcua_endpoint
        gateway_state["poll_interval_ms"] = cfg.poll_interval_ms

    if not cip_tags:
        log.info("No tags configured — poll loop idle (configure tags in the web UI)")
        while not stop_event.is_set():
            if gateway_state.get("restart_requested"):
                stop_event.set()
                break
            await asyncio.sleep(1)
        return

    consecutive_errors = 0

    while not stop_event.is_set():
        if gateway_state.get("restart_requested"):
            stop_event.set()
            break
        try:
            with LogixDriver(cfg.plc_address) as plc:
                with state_lock:
                    gateway_state["plc_connected"] = True
                    gateway_state["last_error"]    = None
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
                            gateway_state["last_poll"]   = now
                            gateway_state["poll_count"] += 1
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
# Flask Web UI
# ---------------------------------------------------------------------------
flask_app = Flask(__name__)
CORS(flask_app)
WEB_PORT = int(os.getenv("WEB_PORT", "8088"))

@flask_app.route("/")
def index():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    with open(path) as f:
        return f.read()

@flask_app.route("/api/state")
def api_state():
    with state_lock:
        s = dict(gateway_state)
    s["host_ip"] = HOST_IP
    return jsonify(s)

@flask_app.route("/api/config", methods=["GET"])
def api_config_get():
    try:
        with open(CONFIG_PATH) as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@flask_app.route("/api/config", methods=["POST"])
def api_config_post():
    """Save config fields (not tags). Optionally restart."""
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

@flask_app.route("/api/discover", methods=["POST"])
def api_discover():
    """Start async tag discovery from PLC."""
    body = request.json or {}
    plc_address = body.get("plc_address", "")
    if not plc_address:
        return jsonify({"error": "plc_address required"}), 400
    if gateway_state.get("discovery_running"):
        return jsonify({"error": "Discovery already running"}), 409
    # Save the address first
    try:
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
        raw["plc_address"] = plc_address
        save_config(raw)
    except Exception:
        pass
    t = threading.Thread(target=discover_tags_thread, args=(plc_address,), daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Discovery started"})

@flask_app.route("/api/discover/status")
def api_discover_status():
    with state_lock:
        return jsonify({
            "running":   gateway_state["discovery_running"],
            "tags":      gateway_state["discovered_tags"],
            "count":     len(gateway_state["discovered_tags"]),
            "last_error": gateway_state["last_error"],
        })

@flask_app.route("/api/discover/apply", methods=["POST"])
def api_discover_apply():
    """Take a list of selected discovered tags and write them to config, then restart."""
    try:
        selected = request.json.get("tags", [])   # list of tag dicts from discovered_tags
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
        raw["tags"] = [{
            "name":        t["name"],
            "cip_tag":     t["cip_tag"],
            "ua_type":     t["ua_type"],
            "description": t.get("description", ""),
            "scan_group":  t.get("scan_group", "default"),
        } for t in selected]
        save_config(raw)
        # Clear live tag state so UI reflects new set
        with state_lock:
            gateway_state["tags"] = {}
            gateway_state["restart_requested"] = True
        return jsonify({"ok": True, "count": len(selected)})
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
    log.info("Web UI on http://0.0.0.0:%d", WEB_PORT)
    flask_app.run(host="0.0.0.0", port=WEB_PORT, debug=False, use_reloader=False)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    while True:
        with state_lock:
            gateway_state["restart_requested"] = False

        log.info("Loading config: %s", CONFIG_PATH)
        cfg = load_config(CONFIG_PATH)

        log.info("Starting OPC-UA server: %s", cfg.opcua_endpoint)
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
