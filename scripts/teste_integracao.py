#!/usr/bin/env python3
"""Executa os testes com RabbitMQ, dados e chaves temporários, sem usar a feat-seg."""
import argparse
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time


def porta_livre():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default=shutil.which("go"))
    parser.add_argument("--rabbitmq", default="/usr/lib/rabbitmq/bin/rabbitmq-server")
    args = parser.parse_args()
    if not args.go or not Path(args.rabbitmq).is_file():
        parser.error("informe --go e --rabbitmq com os caminhos dos executáveis instalados")
    raiz = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory(prefix="sd-rabbitmq-") as temp:
        pasta = Path(temp)
        (pasta / "env.conf").touch()
        (pasta / "enabled_plugins").write_text("[].\n")
        porta, distribuicao = porta_livre(), porta_livre()
        (pasta / "rabbitmq.conf").write_text(
            f"listeners.tcp.1 = 127.0.0.1:{porta}\n"
            "disk_free_limit.absolute = 50MB\n"
        )
        env = os.environ.copy()
        env.update({
            "RABBITMQ_CONF_ENV_FILE": str(pasta / "env.conf"),
            "RABBITMQ_CONFIG_FILE": str(pasta / "rabbitmq.conf"),
            "RABBITMQ_ENABLED_PLUGINS_FILE": str(pasta / "enabled_plugins"),
            "RABBITMQ_MNESIA_BASE": str(pasta / "mnesia"),
            "RABBITMQ_LOG_BASE": str(pasta / "log"),
            "RABBITMQ_PID_FILE": str(pasta / "rabbitmq.pid"),
            "RABBITMQ_NODENAME": f"sd_teste_{os.getpid()}@localhost",
            "RABBITMQ_NODE_PORT": str(porta),
            "RABBITMQ_DIST_PORT": str(distribuicao),
            "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS": "+S 2:2 -setcookie sd_teste_temporario",
        })
        log_path = pasta / "broker.log"
        with log_path.open("w") as log:
            broker = subprocess.Popen([args.rabbitmq], env=env, cwd=temp,
                                      stdout=log, stderr=subprocess.STDOUT,
                                      start_new_session=True)
            try:
                limite = time.monotonic() + 40
                while True:
                    if broker.poll() is not None or time.monotonic() > limite:
                        raise RuntimeError("RabbitMQ temporário não iniciou:\n" + log_path.read_text())
                    try:
                        with socket.create_connection(("127.0.0.1", porta), timeout=0.2):
                            break
                    except OSError:
                        time.sleep(0.2)
                teste_env = os.environ.copy()
                teste_env["RABBITMQ_TEST_URL"] = f"amqp://guest:guest@127.0.0.1:{porta}/"
                print("RabbitMQ temporário pronto. Executando integração...", flush=True)
                return subprocess.run(
                    [args.go, "test", "-race", "-count=1", "-v", "-timeout=120s", "./integracao"],
                    cwd=raiz, env=teste_env, check=False,
                ).returncode
            finally:
                if broker.poll() is None:
                    os.killpg(broker.pid, signal.SIGTERM)
                    try:
                        broker.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        os.killpg(broker.pid, signal.SIGKILL)
                        broker.wait()


if __name__ == "__main__":
    raise SystemExit(main())
