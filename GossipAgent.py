#!/usr/bin/env python3
import json
import socket
import threading
import time
import random
from datetime import datetime

# ------------------------------
# Message + simple versioning
# ------------------------------
def now_ms():
    return int(time.time() * 1000)

def make_entry(agent_id, device, capabilities, availability, contributions, version=None):
    return {
        "agent_id": agent_id,
        "device": device,
        "capabilities": capabilities,
        "availability": availability,
        "contributions": contributions,
        "version": version if version is not None else now_ms()
    }

# ------------------------------
# Agent (serverless) with gossip
# ------------------------------
class GossipAgent:
    def __init__(self, agent_id, port, peer_ports):
        self.agent_id = agent_id
        self.port = port
        self.peer_ports = [p for p in peer_ports if p != port]
        self.ledger = {}  # agent_id -> entry
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", self.port))
        self.sock.settimeout(0.2)
        self.lock = threading.Lock()
        self.running = False

        # Initialize with this agent's own entry
        initial = make_entry(
            agent_id=self.agent_id,
            device=random.choice(["RTX4090","M2","JetsonOrin"]),
            capabilities=random.sample(["math","python","planning","verify","web"], k=2),
            availability=random.choice(["online","busy"]),
            contributions=[]
        )
        self.ledger[self.agent_id] = initial

    def _send(self, data, dest_port):
        payload = json.dumps(data).encode("utf-8")
        self.sock.sendto(payload, ("127.0.0.1", dest_port))

    def _broadcast_gossip(self):
        with self.lock:
            # Send only a summary map agent_id->version to reduce traffic
            summary = {aid: ent["version"] for aid, ent in self.ledger.items()}
        msg = {"type": "GOSSIP_SUMMARY", "from": self.port, "summary": summary}
        for p in self.peer_ports:
            self._send(msg, p)

    def _send_full_entries(self, missing_ids, dest_port):
        with self.lock:
            subset = {aid: self.ledger[aid] for aid in missing_ids if aid in self.ledger}
        msg = {"type": "FULL_ENTRIES", "from": self.port, "entries": subset}
        self._send(msg, dest_port)

    def _merge_entries(self, entries):
        changed = False
        with self.lock:
            for aid, ent in entries.items():
                if (aid not in self.ledger) or (ent["version"] > self.ledger[aid]["version"]):
                    self.ledger[aid] = ent
                    changed = True
        return changed

    def _recv_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue

            mtype = msg.get("type")
            if mtype == "GOSSIP_SUMMARY":
                their_summary = msg.get("summary", {})
                # Determine which entries we are missing or have older versions for
                missing = []
                with self.lock:
                    for aid, ver in their_summary.items():
                        my_ver = self.ledger.get(aid, {}).get("version", -1)
                        if ver > my_ver:
                            missing.append(aid)
                if missing:
                    # Ask sender for those entries (by sending FULL_ENTRIES request)
                    req = {"type": "FULL_REQUEST", "from": self.port, "want": missing}
                    self._send(req, msg["from"])
            elif mtype == "FULL_REQUEST":
                want = msg.get("want", [])
                self._send_full_entries(want, msg["from"])
            elif mtype == "FULL_ENTRIES":
                entries = msg.get("entries", {})
                changed = self._merge_entries(entries)
                if changed:
                    # Optional: propagate changes faster by immediately gossiping
                    self._broadcast_gossip()
            else:
                # ignore unknown
                pass

    def _periodic_tasks(self):
        # periodically update our own status + gossip
        while self.running:
            # Randomly change availability to simulate liveness
            with self.lock:
                entry = self.ledger[self.agent_id].copy()
                if random.random() < 0.3:
                    entry["availability"] = random.choice(["online","busy"])
                    entry["version"] = now_ms()
                    self.ledger[self.agent_id] = entry
            self._broadcast_gossip()
            time.sleep(0.5)

    def start(self):
        self.running = True
        self.t_recv = threading.Thread(target=self._recv_loop, daemon=True)
        self.t_tick = threading.Thread(target=self._periodic_tasks, daemon=True)
        self.t_recv.start()
        self.t_tick.start()

    def stop(self):
        self.running = False
        try:
            self.t_recv.join(timeout=1.0)
            self.t_tick.join(timeout=1.0)
        except Exception:
            pass
        self.sock.close()

    def snapshot_ledger(self):
        with self.lock:
            return json.dumps(self.ledger, indent=2)

# ------------------------------
# Demo runner
# ------------------------------
def run_demo():
    # Three agents on three ports
    ports = [12000, 12001, 12002]
    agents = [
        GossipAgent(agent_id=f"agent_{i+1}", port=ports[i], peer_ports=ports)
        for i in range(3)
    ]
    for a in agents:
        a.start()

    print("Running gossip for ~8 seconds...")
    time.sleep(8)

    for a in agents:
        print(f"\n=== Ledger at {a.agent_id} (port {a.port}) ===")
        print(a.snapshot_ledger())

    for a in agents:
        a.stop()

if __name__ == "__main__":
    run_demo()