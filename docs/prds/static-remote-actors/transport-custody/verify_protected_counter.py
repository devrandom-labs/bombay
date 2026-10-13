#!/usr/bin/env python3
"""Controlled TLS subprocess fixture; process guards are not actor outcomes."""
import argparse, hashlib, json, os, selectors, socket, subprocess, time
from pathlib import Path

arguments = argparse.ArgumentParser()
arguments.add_argument("executable", type=Path)
arguments.add_argument("evidence", type=Path)
arguments.add_argument("--certificates", type=Path, required=True)
arguments.add_argument("--source-root", type=Path, required=True)
arguments.add_argument("--grant-release", choices=("grant-stop", "release-query", "eof"), required=True)
arguments.add_argument("--evidence-source", choices=("target", "sibling-processing", "sibling-read"), required=True)
configuration = arguments.parse_args()
configuration.evidence.mkdir(parents=True, exist_ok=True)
source_paths = ("Cargo.toml", "Cargo.lock", "flake.lock", "examples/counter/Cargo.toml",
    "docs/prds/static-remote-actors/transport-custody/Cargo.toml", "docs/prds/static-remote-actors/transport-custody/Cargo.lock",
    "examples/counter/src/bin/authorized.rs", "examples/counter/src/bin/authorized/remote_counter.rs",
    "examples/counter/src/bin/authorized/protected/mod.rs", "examples/counter/src/bin/authorized/protected/tests.rs")
sources = {name: hashlib.sha256((configuration.source_root / name).read_bytes()).hexdigest() for name in source_paths}
certificates = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
    for path in sorted(configuration.certificates.glob("*.pem"))}
executable_digest = hashlib.sha256(configuration.executable.read_bytes()).hexdigest()
results = []
for layout, target in (("peer", "root"), ("peer", "child"), ("router", "root"), ("router", "child")):
    if configuration.evidence_source != "target" and target != "child":
        continue
    for provider in ("recorded", "scheduled"):
        directory = configuration.evidence / f"{target}-{layout}-{provider}"
        directory.mkdir(exist_ok=True)
        workers, observations, logs, fragments, provenance = {}, [], {}, {}, {}
        selector = selectors.DefaultSelector()
        deadline = time.monotonic() + 90
        with socket.socket() as address:
            address.bind(("127.0.0.1", 0))
            endpoint = f"tls/localhost:{address.getsockname()[1]}"
        def launch(role, binding=None):
            environment = dict(os.environ, BOMBAY_COUNTER_ROLE=("child-recipient" if role == "recipient" and target == "child" else role), BOMBAY_COUNTER_LAYOUT=layout,
                BOMBAY_COUNTER_PROVIDER=provider, BOMBAY_COUNTER_ADDRESS=endpoint,
                BOMBAY_COUNTER_CERTIFICATES=str(configuration.certificates.resolve()),
                BOMBAY_COUNTER_EVIDENCE_SOURCE=configuration.evidence_source, BOMBAY_COUNTER_GRANT_EXPECTATION=("accepted" if configuration.grant_release == "grant-stop" else "refused"))
            if binding is not None:
                environment.update(BOMBAY_COUNTER_RUNTIME_GENERATION=binding[0],
                    BOMBAY_COUNTER_ACTOR_GENERATION=binding[1])
            command = [str(configuration.executable.resolve()), "--ignored", "--exact",
                "remote_counter::remote_counter_query_preserves_admission_and_processing", "--nocapture"]
            worker = subprocess.Popen(command, env=environment, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
            workers[role], logs[role] = worker, (directory / f"{role}.log").open("wb")
            provenance[role] = dict(argv=command, binding=binding,
                environment={key: value for key, value in environment.items() if key.startswith("BOMBAY_COUNTER_")})
            fragments[role] = b""
            selector.register(worker.stdout, selectors.EVENT_READ, role)
        def wait(role, marker):
            while True:
                for observed_role, line in observations:
                    if observed_role == role and marker in line:
                        return line
                if time.monotonic() >= deadline:
                    raise RuntimeError("external process guard elapsed; no semantic credit")
                for ready, _ in selector.select(1):
                    chunk = os.read(ready.fileobj.fileno(), 65536)
                    if chunk:
                        logs[ready.data].write(chunk); logs[ready.data].flush()
                        lines = (fragments[ready.data] + chunk).split(b"\n")
                        fragments[ready.data] = lines.pop()
                        if len(fragments[ready.data]) > 16384:
                            raise RuntimeError("bounded control-line tail exceeded; no semantic credit")
                        if any(len(line) > 16384 for line in lines):
                            raise RuntimeError("bounded completed control line exceeded; no semantic credit")
                        observations.extend((ready.data, line.decode("utf-8", errors="replace")) for line in lines
                            if any(marker in line for marker in (b"BOUND ", b"READY", b"RECEIVED", b"PENDING_LOCAL_READ2", b"SEND_ONLY_REFUSED", b"STOP_REFUSAL_SETTLED", b"FINISHED")))
                    else:
                        selector.unregister(ready.fileobj)
                        code = workers[ready.data].wait(timeout=10)
                        if code != 0:
                            raise RuntimeError(f"actual worker {ready.data} exited {code} before {marker}")
        failure = None
        try:
            if layout == "router":
                launch("router"); wait("router", "READY")
            launch("recipient")
            issued = wait("recipient", "BOUND ").split("BOUND ", 1)[1].strip().split()
            if len(issued) != 2 or any(not value.isdecimal() for value in issued):
                raise RuntimeError("invalid trusted recipient issuance projection")
            wait("recipient", "READY")
            launch("caller", issued)
            wait("recipient", "RECEIVED")
            wait("caller", "PENDING_LOCAL_READ2")
            workers["recipient"].stdin.write(b"release-query\n"); workers["recipient"].stdin.flush()
            wait("caller", "STOP_REFUSAL_SETTLED")
            authenticated_refusal = any(role == "caller" and line.strip() == "SEND_ONLY_REFUSED" for role, line in observations)
            if authenticated_refusal and configuration.grant_release != "eof":
                workers["recipient"].stdin.write((configuration.grant_release + "\n").encode()); workers["recipient"].stdin.flush()
            else:
                workers["recipient"].stdin.close(); workers["recipient"].stdin = None
            wait("recipient", "FINISHED"); wait("caller", "FINISHED")
            if layout == "router":
                workers["router"].stdin.write(b"release-router\n"); workers["router"].stdin.flush()
        except Exception as original:
            failure = str(original)
        finally:
            exits, cleanup_errors = {}, {}
            # Always settle recipient's trusted input; do not kill an actor because another exited.
            if failure is not None and "recipient" in workers and workers["recipient"].stdin is not None:
                try:
                    workers["recipient"].stdin.close(); workers["recipient"].stdin = None
                except OSError as original:
                    cleanup_errors["recipient"] = str(original)
            if failure is not None and "router" in workers and workers["router"].poll() is None:
                try:
                    workers["router"].stdin.write(b"release-router\n"); workers["router"].stdin.flush()
                except OSError as original:
                    cleanup_errors["router"] = str(original)
            for role, worker in workers.items():
                try:
                    output, _ = worker.communicate(timeout=10)
                except subprocess.TimeoutExpired as original:
                    cleanup_errors[role] = str(original)
                    try:
                        worker.kill()
                    except OSError as original:
                        cleanup_errors[role] += "; " + str(original)
                    try:
                        output, _ = worker.communicate(timeout=10)
                    except Exception as original:
                        cleanup_errors[role] += "; " + str(original)
                        output = b""
                except Exception as original:
                    cleanup_errors[role], output = str(original), b""
                exits[role] = worker.poll()
                try:
                    logs[role].write(output); logs[role].close()
                except OSError as original:
                    cleanup_errors[role] = cleanup_errors.get(role, "") + "; " + str(original)
            if cleanup_errors:
                failure = failure or "actual child cleanup failed; no semantic credit"
            selector.close()
        executed = {}
        for role in workers:
            raw = (directory / f"{role}.log").read_bytes()
            executed[role] = (b"remote_counter::remote_counter_query_preserves_admission_and_processing ..." in raw
                and b"test result: ok. 1 passed; 0 failed; 0 ignored;" in raw)
        if not all(executed.values()):
            failure = failure or "exact native test did not execute once successfully in every role"
        results.append(dict(target=target, layout=layout, provider=provider, grant_release=configuration.grant_release, evidence_source=configuration.evidence_source, failure=failure, exits=exits,
            executable_sha256=executable_digest, sources=sources, certificates=certificates, provenance=provenance, cleanup_errors=cleanup_errors, executed=executed,
            logs={role: hashlib.sha256((directory / f"{role}.log").read_bytes()).hexdigest() for role in workers}))
        (configuration.evidence / "campaign.json").write_text(json.dumps(results, indent=2) + "\n")
raise SystemExit(0 if len(results) == (8 if configuration.evidence_source == "target" else 4) and all(case["failure"] is None and all(code == 0 for code in case["exits"].values()) for case in results) else 1)
