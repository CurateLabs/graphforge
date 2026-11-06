#!/usr/bin/env python3
"""Exercise installed PyPI/npm consoles over loopback HTTP and ReferenceHub.

Requires rebuilt hub-test-support bindings. The explicitly preloaded fixture
routes only https://hub.test, stores synthetic sessions in the server's memory,
and never reads the OS vault. No distribution build contains these hooks.
"""

from __future__ import annotations

import argparse
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import pty
import select
import subprocess
import sysconfig
import tempfile
import threading
import time

from graphforge import GraphForge
from graphforge import _graphforge_rs as native

ROOT = Path(__file__).resolve().parents[2]
HUB = "https://hub.test"


class Hub(HTTPServer):
    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.vault = {}
        self.requests = []
        self.approved = threading.Event()
        self.ready = threading.Event()
        self.token = None
        self.device = None
        self.expire_login = False

    def serve(self):
        self.token = native._test_hub_start()
        self.ready.set()
        try:
            self.serve_forever(poll_interval=0.05)
        finally:
            native._test_hub_stop()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        self.exchange()

    def do_POST(self):
        self.exchange()

    def do_PUT(self):
        self.exchange()

    def do_HEAD(self):
        self.exchange()

    def exchange(self):
        hub = self.server
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        hub.requests.append((self.command, self.path, self.headers.get("authorization")))
        status, headers = 200, []
        if self.path == "/_fixture/vault":
            record = json.loads(body)
            key, value = record["origin"], record["value"]
            if value is None:
                value = hub.vault.get(key)
            elif value:
                hub.vault[key] = value
                value = None
            else:
                hub.vault.pop(key, None)
                value = None
            body = json.dumps(value).encode()
        elif self.path == "/api/cli/device":
            body = json.dumps(
                {
                    "device_code": "synthetic-device",
                    "user_code": "TEST-CODE",
                    "verification_uri": HUB + "/device",
                    "expires_in": 300,
                    "interval": 1,
                }
            ).encode()
        elif self.path == "/api/cli/token":
            assert hub.approved.wait(15), "notice must arrive before login completes"
            if hub.expire_login:
                status = 400
                body = json.dumps({"error": "expired_token"}).encode()
            else:
                body = json.dumps(
                    {
                        "access_token": "synthetic-access",
                        "refresh_token": "synthetic-refresh",
                        "expires_in": 3600,
                        "token_type": "Bearer",
                    }
                ).encode()
        elif self.path == "/api/cli/session":
            assert self.headers["authorization"] == "Bearer synthetic-access"
            body = json.dumps(
                {
                    "authenticated": True,
                    "principal_id": "principal:test",
                    "expires_at": int(time.time()) + 3600,
                }
            ).encode()
        elif self.path == "/api/cli/repository-token":
            assert self.headers["authorization"] == "Bearer synthetic-access"
            assert json.loads(body) == {
                "owner": "curate",
                "name": "claims",
                "capability": "publish",
            }
            body = json.dumps(
                {
                    "access_token": hub.token,
                    "origin": HUB,
                    "token_type": "Bearer",
                    "expires_in": 900,
                    "repository_id": "fixture-repository",
                }
            ).encode()
        else:
            if self.path.endswith("/token") and hub.device and hub.approved.is_set():
                native._test_hub_approve(hub.device)
                hub.device = None
            status, headers, body = native._test_hub_exchange(
                self.command, HUB + self.path, list(self.headers.items()), body
            )
            if self.path.endswith("/device_authorization"):
                hub.device = json.loads(body)["user_code"]
        self.send_response(status)
        for name, value in headers:
            if name.lower() not in {"content-length", "connection"}:
                self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)


def run(command, arguments, env, expected=0, data=None):
    result = subprocess.run(
        [*command, *arguments], check=False, env=env, input=data, capture_output=True, timeout=60
    )
    assert result.returncode == expected, (
        arguments,
        result.returncode,
        result.stdout,
        result.stderr,
    )
    assert b"synthetic-access" not in result.stdout + result.stderr
    assert b"synthetic-refresh" not in result.stdout + result.stderr
    return result


def notice_before_approval(command, arguments, env, hub, *, terminal=False, expected=0):
    hub.approved.clear()
    master, slave = pty.openpty() if terminal else (None, None)
    with subprocess.Popen(
        [*command, *arguments],
        env=env,
        stdin=slave if terminal else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=slave if terminal else subprocess.PIPE,
    ) as process:
        try:
            descriptor = master if terminal else process.stderr.fileno()
            received = b""
            deadline = time.monotonic() + 10
            while b"enter code" not in received:
                remaining = deadline - time.monotonic()
                assert remaining > 0 and select.select([descriptor], [], [], remaining)[0], received
                chunk = os.read(descriptor, 4096)
                assert chunk, received
                received += chunk
            assert process.poll() is None, "notice was buffered until exit"
            hub.approved.set()
            stdout, stderr = process.communicate(timeout=60)
            assert process.returncode == expected, (received, stdout, stderr)
            if expected == 5:
                assert b"device authorization expired" in received + (stderr or b"")
            assert b"synthetic-access" not in received + stdout + (stderr or b"")
        finally:
            hub.approved.set()
            if process.poll() is None:
                process.kill()
            if terminal:
                os.close(master)
                os.close(slave)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--npm-root", required=True, type=Path)
    args = parser.parse_args()
    npm = args.npm_root.resolve()
    # This script must use an installed wheel, never source-package shadowing.
    source_package = ROOT / "crates/graphforge-bindings-py/python"
    assert not Path(native.__file__).resolve().is_relative_to(source_package)
    with (
        tempfile.TemporaryDirectory(prefix="graphforge-launcher-auth-") as directory,
        Hub() as hub,
    ):
        root = Path(directory)
        worker = threading.Thread(target=hub.serve, daemon=True)
        worker.start()
        assert hub.ready.wait(5)
        try:
            port = hub.server_port
            (root / "sitecustomize.py").write_text(
                f"from graphforge import _graphforge_rs as n\nn._test_launcher_start({port})\n"
            )
            preload = root / "preload.cjs"
            binding = npm / "node_modules/@curatelabs/graphforge/index.js"
            preload.write_text(f"require({json.dumps(str(binding))})._testLauncherStart({port});\n")
            env = {
                key: value
                for key, value in os.environ.items()
                if key
                not in {
                    "GRAPHFORGE_HUB_PUBLISH_TOKEN",
                    "PYTHONPATH",
                    "NODE_OPTIONS",
                }
            }
            env.update(PYTHONPATH=str(root), NODE_OPTIONS=f"--require={preload}")
            project = root / "project"
            with closing(GraphForge(str(project))) as graph:
                graph.execute("CREATE (:Item {score: 1})")
            scripts = Path(sysconfig.get_path("scripts"))
            commands = [[str(scripts / name)] for name in ("gf", "graphforge")]
            commands += [[str(npm / "node_modules/.bin" / name)] for name in ("gf", "graphforge")]
            run(
                commands[0],
                ["--project", str(project), "branch", "main", "--author", "Fixture"],
                env,
            )
            for command in commands:
                hub.vault.clear()
                notice_before_approval(
                    command, ["auth", "login", "--hub", HUB, "--no-browser"], env, hub
                )
                status = run(command, ["auth", "status", "--hub", HUB], env)
                assert b"principal:test" in status.stdout
                before = len(hub.requests)
                for verb in ("publish", "push"):
                    run(
                        command,
                        ["--project", str(project), verb, "curate/claims", "--hub", HUB, "--json"],
                        env,
                    )
                assert (
                    sum(path == "/api/cli/repository-token" for _, path, _ in hub.requests[before:])
                    == 2
                )
                # No saved session: only real terminal stdin AND stderr admit
                # the native device fallback. This also proves live stderr.
                hub.vault.clear()
                notice_before_approval(
                    command,
                    ["--project", str(project), "publish", "curate/claims", "--hub", HUB],
                    env,
                    hub,
                    terminal=True,
                )
                hub.expire_login = True
                notice_before_approval(
                    command,
                    ["auth", "login", "--hub", HUB, "--no-browser"],
                    env,
                    hub,
                    expected=5,
                )
                hub.expire_login = False
                run(
                    command,
                    ["--project", str(project), "push", "curate/claims", "--hub", HUB],
                    env | {"GRAPHFORGE_HUB_PUBLISH_TOKEN": hub.token},
                )
                print(
                    f"PASS {command[0]}: live login, saved-session publish/push, "
                    "terminal device flow"
                )
            # Actual inherited stdin carries a portable package through Rust.
            package = ROOT / "tests/fixtures/hub/generated/v1/objects/opendata-openalex.gfpb"
            for command in (commands[0], commands[2]):
                run(
                    command,
                    ["portable", "verify", "--input", "-", "--json"],
                    env,
                    data=package.read_bytes(),
                )
                run(command, ["--json", "--unknown-option"], env, expected=2)
            # Process APIs return errors without exiting their library hosts;
            # subsequent captured calls still run normally.
            assert native._cli_execute_process(["gf", "--json", "--unknown-option"]) == 2
            assert native._cli_execute(["gf", "--version"])[0] == 0
            node = subprocess.run(
                [
                    "node",
                    "-e",
                    f"const n=require({json.dumps(str(binding))}); "
                    "if(n.runCliProcess(['--json','--unknown-option'])!==2 || "
                    "n.runCli(['--version']).exitCode!==0) process.exit(9);",
                ],
                check=False,
                env=env,
                capture_output=True,
                timeout=10,
            )
            assert node.returncode == 0, node.stderr
            assert any(method == "PUT" for method, _, _ in hub.requests), (
                "real graph objects were uploaded"
            )
            print(
                "PASS inherited stdin, parser exit propagation, library host survival, "
                "ReferenceHub uploads"
            )
        finally:
            hub.shutdown()
            worker.join(timeout=5)


if __name__ == "__main__":
    main()
