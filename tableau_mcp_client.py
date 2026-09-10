"""Minimal MCP stdio client: start a server command, initialize, list tools, call one tool.

Usage:
  python mcp_stdio_client.py --cmd "node path/to/server.js" [--env K=V ...] --list
  python mcp_stdio_client.py --cmd "..." --call publish-workbook --args '{"name": "...", ...}'
Env for the child is inherited plus --env pairs. Prints JSON results; never prints env values.
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import threading
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cmd", required=True)
    ap.add_argument("--env", action="append", default=[])
    ap.add_argument("--cwd")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--call")
    ap.add_argument("--args", default="{}")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--json-only", action="store_true", help="print only the tool result JSON")
    a = ap.parse_args()

    env = dict(os.environ)
    for kv in a.env:
        k, v = kv.split("=", 1)
        env[k] = v

    argv = shlex.split(a.cmd, posix=False) if os.name == "nt" else shlex.split(a.cmd)
    proc = subprocess.Popen(
        argv, cwd=a.cwd, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", bufsize=1,
    )
    stderr_lines = []

    def drain():
        for line in proc.stderr:
            stderr_lines.append(line.rstrip())

    threading.Thread(target=drain, daemon=True).start()

    next_id = [0]

    def send(method, params=None, notify=False):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        if not notify:
            next_id[0] += 1
            msg["id"] = next_id[0]
        proc.stdin.write(json.dumps(msg) + "\n")
        proc.stdin.flush()
        return None if notify else msg["id"]

    def recv(want_id, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    raise RuntimeError(f"server exited {proc.returncode}; stderr:\n" + "\n".join(stderr_lines[-30:]))
                time.sleep(0.05)
                continue
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("id") == want_id:
                return obj
        raise RuntimeError("timeout waiting for id %s; stderr:\n%s" % (want_id, "\n".join(stderr_lines[-30:])))

    try:
        rid = send("initialize", {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "wax-stdio-client", "version": "0.1"},
        })
        init = recv(rid, a.timeout)
        if not a.json_only:
            print("initialized:", json.dumps(init.get("result", {}).get("serverInfo")))
        send("notifications/initialized", {}, notify=True)

        if a.list or not a.call:
            rid = send("tools/list", {})
            res = recv(rid, a.timeout)
            tools = res.get("result", {}).get("tools", [])
            print(f"tools: {len(tools)}")
            for t in tools:
                print(" -", t["name"], "|", (t.get("description") or "").split("\n")[0][:110])
            if "error" in res:
                print("error:", json.dumps(res["error"]))

        if a.call:
            rid = send("tools/call", {"name": a.call, "arguments": json.loads(a.args)})
            res = recv(rid, a.timeout)
            payload = res.get("result", res.get("error"))
            print(json.dumps(payload) if a.json_only else json.dumps(payload, indent=2)[:6000])
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass
        proc.terminate()
        if stderr_lines:
            print("--- server stderr (tail) ---", file=sys.stderr)
            for l in stderr_lines[-15:]:
                print(l, file=sys.stderr)


if __name__ == "__main__":
    main()
