#!/usr/bin/env python3
"""Bateria automatizada de experimentos para o Trabalho 2 (MC714).

Para cada configuração (número de nós N e limiar de heartbeat T) o script:

  1. gera um ``compose.gen.yml`` com N nós;
  2. sobe o cluster e aguarda a estabilização (maior ID como líder);
  3. injeta uma falha *crash-stop* no líder com ``docker kill`` (SIGKILL,
     imediato) e mede o tempo de reconvergência a partir do instante do kill;
  4. reinicia o nó derrubado e mede o tempo de recuperação até que o maior ID
     reassuma a coordenação (*crash-recovery*);
  5. registra os resultados em CSV.

Os instantes são extraídos dos logs estruturados dos próprios nós
(``LEADER_ANNOUNCE``, ``HEARTBEAT_TIMEOUT`` e ``ELECTION_MSG``), todos com
timestamp epoch, o que elimina o erro de granularidade da sondagem.
"""

import os
import re
import csv
import time
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SRC = os.path.join(ROOT, "src")
DATA_DIR = os.path.join(HERE, "data")
COMPOSE_PATH = os.path.join(HERE, "compose.gen.yml")
PROJECT = "mc714exp"

os.makedirs(DATA_DIR, exist_ok=True)


# --------------------------------------------------------------------------- #
# Infraestrutura Docker Compose
# --------------------------------------------------------------------------- #
def gen_compose(n_nodes, hb_timeout, hb_interval=1.0, election_timeout=2.0):
    lines = ["services:"]
    for i in range(1, n_nodes + 1):
        peers = ",".join(
            f"{j}:node{j}:5000" for j in range(1, n_nodes + 1) if j != i
        )
        lines += [
            f"  node{i}:",
            f"    build: {SRC}",
            f"    container_name: {PROJECT}_node{i}",
            "    environment:",
            f"      - NODE_ID={i}",
            "      - PORT=5000",
            f"      - PEERS={peers}",
            f"      - HEARTBEAT_INTERVAL={hb_interval}",
            f"      - HEARTBEAT_TIMEOUT={hb_timeout}",
            f"      - ELECTION_TIMEOUT={election_timeout}",
            "    networks:",
            "      - ds-net",
        ]
    lines += ["networks:", "  ds-net:", "    driver: bridge"]
    with open(COMPOSE_PATH, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def dc(*args):
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "-f", COMPOSE_PATH, *args],
        capture_output=True,
        text=True,
    )


def cname(nid):
    return f"{PROJECT}_node{nid}"


def logs(nid):
    r = subprocess.run(["docker", "logs", cname(nid)], capture_output=True, text=True)
    return r.stdout + r.stderr


def parse_announces(nid):
    return [
        (int(m.group(1)), float(m.group(2)))
        for m in re.finditer(
            r"LEADER_ANNOUNCE node=(\d+) epoch=([\d.]+)", logs(nid)
        )
    ]


def parse_timeouts(nid):
    return [
        (float(m.group(1)), float(m.group(2)))
        for m in re.finditer(
            r"HEARTBEAT_TIMEOUT node=\d+ epoch=([\d.]+) elapsed=([\d.]+)", logs(nid)
        )
    ]


def count_election_msgs_since(n_nodes, t0):
    total = 0
    for nid in range(1, n_nodes + 1):
        for m in re.finditer(r"ELECTION_MSG .* epoch=([\d.]+)", logs(nid)):
            if float(m.group(1)) > t0:
                total += 1
    return total


def wait_for(fn, timeout, poll=0.2):
    t0 = time.time()
    while time.time() - t0 < timeout:
        val = fn()
        if val:
            return val
        time.sleep(poll)
    return None


def wait_stable_leader(n_nodes, timeout=25.0):
    """Aguarda o nó de maior ID se anunciar como líder e o cluster estabilizar."""
    def check():
        ann = parse_announces(n_nodes)
        if any(nid == n_nodes for nid, _ in ann):
            return True
        return False

    if not wait_for(check, timeout):
        return False
    time.sleep(2.5)  # deixa heartbeats/adoções assentarem
    return True


# --------------------------------------------------------------------------- #
# Um ciclo experimental: falha + recuperação
# --------------------------------------------------------------------------- #
def run_cycle(n_nodes, hb_timeout, exp_name, rep, writer_fail, writer_rec):
    leader = n_nodes  # o maior ID é o líder após a estabilização

    # --- Falha crash-stop -------------------------------------------------- #
    t_kill = time.time()
    subprocess.run(["docker", "kill", cname(leader)], capture_output=True)
    t_kill = (t_kill + time.time()) / 2.0

    def new_leader():
        best = None
        for nid in range(1, n_nodes + 1):
            if nid == leader:
                continue
            for _, ep in parse_announces(nid):
                if ep > t_kill:
                    if best is None or ep < best[1]:
                        best = (nid, ep)
        return best

    res = wait_for(new_leader, timeout=hb_timeout + 15)
    if res is None:
        print(f"    !! sem novo líder para {exp_name} n={n_nodes} T={hb_timeout}")
        return False
    new_id, t_announce = res
    convergence = t_announce - t_kill

    # Detecção (primeiro timeout após o kill) e duração da eleição.
    detection = None
    for nid in range(1, n_nodes + 1):
        if nid == leader:
            continue
        for ep, _elapsed in parse_timeouts(nid):
            if ep > t_kill and (detection is None or ep < detection):
                detection = ep
    detection_sec = (detection - t_kill) if detection else float("nan")
    election_sec = (t_announce - detection) if detection else float("nan")
    msgs = count_election_msgs_since(n_nodes, t_kill)

    writer_fail.writerow(
        [exp_name, n_nodes, hb_timeout, rep, f"{t_kill:.3f}",
         f"{convergence:.3f}", f"{detection_sec:.3f}", f"{election_sec:.3f}",
         new_id, msgs]
    )
    print(
        f"    falha: n={n_nodes} T={hb_timeout} rep={rep} "
        f"-> líder {new_id} em {convergence:.3f}s "
        f"(detecção {detection_sec:.3f}s + eleição {election_sec:.3f}s, {msgs} msgs)"
    )

    # --- Recuperação crash-recovery --------------------------------------- #
    t_start = time.time()
    subprocess.run(["docker", "start", cname(leader)], capture_output=True)

    def reclaimed():
        for nid, ep in parse_announces(leader):
            if ep > t_start:
                return (nid, ep)
        return None

    rec = wait_for(reclaimed, timeout=hb_timeout + 15)
    if rec is None:
        print(f"    !! nó {leader} não reassumiu a liderança")
        return False
    recovery = rec[1] - t_start
    writer_rec.writerow(
        [exp_name, n_nodes, hb_timeout, rep, f"{recovery:.3f}", leader]
    )
    print(f"    recuperação: nó {leader} reassumiu em {recovery:.3f}s")
    time.sleep(1.0)
    return True


def run_config(exp_name, n_nodes, hb_timeout, reps, wf, wr):
    print(f"\n=== {exp_name}: N={n_nodes} T={hb_timeout} ({reps} reps) ===")
    dc("down", "-v", "--remove-orphans")
    gen_compose(n_nodes, hb_timeout)
    r = dc("up", "-d", "--build")
    if r.returncode != 0:
        print("  erro no up:", r.stderr[-500:])
        return
    if not wait_stable_leader(n_nodes):
        print("  !! cluster não estabilizou")
        dc("down", "-v", "--remove-orphans")
        return
    for rep in range(1, reps + 1):
        run_cycle(n_nodes, hb_timeout, exp_name, rep, wf, wr)
    dc("down", "-v", "--remove-orphans")


def main():
    reps = 6
    fail_path = os.path.join(DATA_DIR, "failover_results.csv")
    rec_path = os.path.join(DATA_DIR, "recovery_results.csv")

    with open(fail_path, "w", newline="") as ff, open(rec_path, "w", newline="") as rf:
        wf = csv.writer(ff)
        wr = csv.writer(rf)
        wf.writerow(
            ["exp", "n_nodes", "hb_timeout", "rep", "kill_epoch",
             "convergence_sec", "detection_sec", "election_sec",
             "new_leader", "election_messages"]
        )
        wr.writerow(["exp", "n_nodes", "hb_timeout", "rep", "recovery_sec", "leader"])

        # Experimento A: variando o limiar de heartbeat (N fixo = 3).
        for T in [1.0, 2.0, 3.0, 5.0]:
            run_config("timeout", 3, T, reps, wf, wr)
            ff.flush()
            rf.flush()

        # Experimento B: variando o número de nós (T fixo = 3).
        for N in [3, 4, 5]:
            run_config("nodes", N, 3.0, reps, wf, wr)
            ff.flush()
            rf.flush()

    print("\nConcluído. Dados em", DATA_DIR)


if __name__ == "__main__":
    main()
