#!/usr/bin/env python3
"""Exercise installed PyPI/npm consoles over loopback HTTP and ReferenceHub.

Requires rebuilt, installed hub-test-support bindings (wheel or editable).
The explicitly preloaded fixture routes only https://hub.test, stores synthetic
sessions in server memory, and never reads the OS vault. No distribution build
contains these hooks.
"""

from __future__ import annotations

import argparse
import array
from contextlib import closing, contextmanager
import fcntl
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib.metadata import distribution
import json
import os
from pathlib import Path
import pty
import select
import signal
import subprocess
import sys
import sysconfig
import tempfile
import termios
import threading
import time
from urllib.parse import urlsplit
from urllib.request import url2pathname

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
        self.login_waiting = threading.Event()
        self.login_responded = threading.Event()
        self.cancelled_login = False

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
            hub.login_waiting.set()
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
        try:
            self.send_response(status)
            for name, value in headers:
                if name.lower() not in {"content-length", "connection"}:
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except ConnectionError:
            if self.path != "/api/cli/token" or not hub.cancelled_login:
                raise
        finally:
            if self.path == "/api/cli/token":
                hub.login_responded.set()


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


@contextmanager
def console_process(command, arguments, env, *, terminal, stdin_data=None):
    if terminal:
        master, slave = pty.openpty()
        reader, writer = slave, master
    else:
        master = slave = None
        reader, writer = os.pipe()
    invocation = [*command, *arguments]
    if terminal:
        # Set up a controlling terminal after exec, avoiding Python preexec_fn
        # callbacks in this multithreaded fixture. The real console keeps its PID.
        invocation = [
            sys.executable,
            "-c",
            "import fcntl,os,sys,termios; "
            "fcntl.ioctl(0,termios.TIOCSCTTY,0); os.execv(sys.argv[1],sys.argv[1:])",
            *invocation,
        ]
    try:
        if stdin_data is not None:
            assert os.write(writer, stdin_data) == len(stdin_data)
            # PTY line delivery is asynchronous. Prove input is queued before
            # starting its reader, so a later empty buffer proves consumption.
            assert select.select([reader], [], [], 10)[0], "stdin was never queued"
        with subprocess.Popen(
            invocation,
            env=env,
            stdin=reader,
            stdout=subprocess.PIPE,
            stderr=slave if terminal else subprocess.PIPE,
            start_new_session=True,
        ) as process:
            try:
                yield process, master, (reader, writer)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
    finally:
        os.close(reader)
        os.close(writer)


def interrupt_console(process, master):
    if master is None:
        process.send_signal(signal.SIGINT)
    else:
        os.write(master, b"\x03")
    assert process.wait(timeout=10) == -signal.SIGINT, "console did not exit by SIGINT"


def interrupted_stdin(command, env, *, terminal):
    with console_process(
        command,
        ["portable", "verify", "--input", "-", "--json"],
        env,
        terminal=terminal,
        stdin_data=b"input\n",
    ) as (process, master, (reader, _writer)):
        # The buffer was readable before process startup. Its consumption now
        # proves Rust is reading stdin, with the writer still open to block EOF.
        pending = array.array("i", [0])
        deadline = time.monotonic() + 10
        while True:
            assert process.poll() is None, "console exited before the blocking stdin read"
            fcntl.ioctl(reader, termios.FIONREAD, pending, True)
            if pending[0] == 0:
                break
            assert time.monotonic() < deadline, "console never consumed stdin"
            time.sleep(0.01)
        interrupt_console(process, master)


def read_login_notice(process, descriptor):
    received = b""
    deadline = time.monotonic() + 10
    while b"enter code" not in received:
        remaining = deadline - time.monotonic()
        assert remaining > 0 and select.select([descriptor], [], [], remaining)[0], received
        chunk = os.read(descriptor, 4096)
        assert chunk, received
        received += chunk
    assert process.poll() is None, "notice was buffered until exit"
    return received


def interrupted_login(command, env, hub):
    hub.approved.clear()
    hub.login_waiting.clear()
    hub.login_responded.clear()
    hub.cancelled_login = True
    try:
        with console_process(
            command, ["auth", "login", "--hub", HUB, "--no-browser"], env, terminal=True
        ) as (process, master, _input):
            read_login_notice(process, master)
            assert hub.login_waiting.wait(5), "console never polled for its credential"
            interrupt_console(process, master)
        assert not hub.vault, "interrupted login saved a credential"
    finally:
        hub.approved.set()
        if hub.login_waiting.is_set():
            assert hub.login_responded.wait(5), "interrupted token request did not settle"
        hub.cancelled_login = False


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
            received = read_login_notice(process, descriptor)
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
    # Both wheel and documented maturin develop installs are valid. An
    # out-of-tree extension must belong to the installed editable distribution,
    # rather than an unrelated source package shadowing the wheel.
    installed = distribution("graphforge")
    extension = Path(native.__file__).resolve()
    if not extension.is_relative_to(Path(installed.locate_file("")).resolve()):
        origin = json.loads(installed.read_text("direct_url.json") or "{}")
        assert origin.get("dir_info", {}).get("editable") is True
        source = urlsplit(origin["url"])
        assert source.scheme == "file" and source.netloc in {"", "localhost"}
        assert extension.is_relative_to(Path(url2pathname(source.path)).resolve())
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
                interrupted_stdin(command, env, terminal=False)
                interrupted_stdin(command, env, terminal=True)
                hub.vault.clear()
                interrupted_login(command, env, hub)
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
                    "terminal device flow, PID/TTY Ctrl-C and interrupted-login recovery"
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
            seen = []

            def host_interrupt(signum, _frame):
                seen.append(signum)

            previous = signal.signal(signal.SIGINT, host_interrupt)
            try:
                from graphforge import cli

                assert cli.main(["gf", "--version"]) == 0
                assert native._cli_execute_process(["gf", "--version"]) == 0
                assert signal.getsignal(signal.SIGINT) is host_interrupt
                signal.raise_signal(signal.SIGINT)
                assert seen == [signal.SIGINT]
            finally:
                signal.signal(signal.SIGINT, previous)
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
