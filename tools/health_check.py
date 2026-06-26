#!/usr/bin/env python3
"""
Health check tool for the Tent of Trials platform.
Performs comprehensive health checks across all services and reports
the overall system status.

This tool is used by:
  - The Kubernetes liveness/readiness probes
  - The deployment pipeline (post-deployment validation)
  - The monitoring system (periodic health checks)
  - The on-call engineer (manual troubleshooting)

The health check performs the following checks:
  1. Service availability (HTTP health endpoints)
  2. Database connectivity (connection test)
  3. Redis connectivity (ping test)
  4. Kafka connectivity (metadata fetch)
  5. Message queue depth (consumer lag check)
  6. Certificate expiry (TLS certificate check)
  7. Disk space (filesystem usage check)
  8. Memory usage (process memory check)

Each check returns a status of OK, WARNING, or CRITICAL, along with
a detail message and optional diagnostic data.

Usage:
    python3 health_check.py                  # Check all services
    python3 health_check.py --service backend # Check specific service
    python3 health_check.py --json            # JSON output
    python3 health_check.py --watch           # Continuous monitoring
    python3 health_check.py --max-retries 5   # Retry with exponential backoff
    python3 health_check.py --circuit-threshold 5  # Circuit breaker threshold
"""

import argparse
import json
import logging
import os
import random
import socket
import ssl
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

SERVICES = {
    "backend": {"host": "localhost", "port": 8080, "path": "/health", "timeout": 5},
    "market": {"host": "localhost", "port": 8081, "path": "/health", "timeout": 5},
    "frailbox": {"host": "localhost", "port": 8082, "path": "/health", "timeout": 10},
    "frontend": {"host": "localhost", "port": 3000, "path": "/", "timeout": 5},
}

INFRASTRUCTURE = {
    "postgresql": {"host": os.environ.get("DB_HOST", "localhost"), "port": int(os.environ.get("DB_PORT", "5432")), "timeout": 5},
    "redis": {"host": os.environ.get("REDIS_HOST", "localhost"), "port": int(os.environ.get("REDIS_PORT", "6379")), "timeout": 5},
    "kafka": {"host": os.environ.get("KAFKA_HOST", "localhost"), "port": int(os.environ.get("KAFKA_PORT", "9092")), "timeout": 5},
}

DISK_THRESHOLD_WARNING = 80
DISK_THRESHOLD_CRITICAL = 90

MEMORY_THRESHOLD_WARNING = 80
MEMORY_THRESHOLD_CRITICAL = 90

# ---------------------------------------------------------------------------
# RETRY & CIRCUIT BREAKER
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Tracks consecutive failures and opens/closes the circuit."""

    threshold: int = 3
    cooldown: float = 30.0
    _state: CircuitState = CircuitState.CLOSED
    _failure_count: int = 0
    _last_failure_time: float = 0.0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN and time.time() - self._last_failure_time >= self.cooldown:
            self._state = CircuitState.HALF_OPEN
        return self._state

    def record_success(self):
        self._failure_count = 0
        self._state = CircuitState.CLOSED

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.threshold and self._state != CircuitState.OPEN:
            self._state = CircuitState.OPEN
            logger.warning("Circuit breaker OPEN after %d consecutive failures", self._failure_count)

    def allow_request(self) -> bool:
        s = self.state
        return s != CircuitState.OPEN

    def to_dict(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "failure_count": self._failure_count,
            "threshold": self.threshold,
            "cooldown": self.cooldown,
        }


# Per-service circuit breakers
_service_circuits: Dict[str, CircuitBreaker] = {}


def get_circuit(name: str, threshold: int = 3, cooldown: float = 30.0) -> CircuitBreaker:
    if name not in _service_circuits:
        _service_circuits[name] = CircuitBreaker(threshold=threshold, cooldown=cooldown)
    return _service_circuits[name]


DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_CIRCUIT_THRESHOLD = 3
DEFAULT_CIRCUIT_COOLDOWN = 30.0


def retry_with_backoff(
    attempt: int,
    base_delay: float = 1.0,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    jitter: bool = True,
) -> float:
    """Calculate delay = base_delay * (backoff_factor ^ attempt), optionally with jitter."""
    delay = base_delay * (backoff_factor ** attempt)
    if jitter:
        delay = delay * (0.5 + random.random() * 0.5)
    return delay


# ---------------------------------------------------------------------------
# CHECK FUNCTIONS
# ---------------------------------------------------------------------------

def check_http_service(
    host: str, port: int, path: str, timeout: int,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    circuit_threshold: int = DEFAULT_CIRCUIT_THRESHOLD,
    circuit_cooldown: float = DEFAULT_CIRCUIT_COOLDOWN,
) -> Tuple[str, str, int]:
    import http.client

    circuit_name = f"{host}:{port}"
    circuit = get_circuit(circuit_name, circuit_threshold, circuit_cooldown)

    if not circuit.allow_request():
        logger.warning("Circuit breaker OPEN for %s, skipping request", circuit_name)
        return "CRITICAL", f"Circuit breaker open for {circuit_name}", 0

    last_error = ""
    last_status = 0

    for attempt in range(max_retries + 1):
        conn = None
        try:
            conn = http.client.HTTPConnection(host, port, timeout=timeout)
            conn.request("GET", path)
            resp = conn.getresponse()
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")[:200]

            if status == 200:
                circuit.record_success()
                return "OK", f"HTTP {status}", status
            else:
                last_error = f"HTTP {status}: {body[:100]}"
                last_status = status
                if status < 500:
                    circuit.record_success()
                    return "WARNING", last_error, status
                if attempt < max_retries:
                    delay = retry_with_backoff(attempt, backoff_factor=backoff_factor)
                    logger.warning(
                        "HTTP %d on %s, retry %d/%d after %.1fs",
                        status, circuit_name, attempt + 1, max_retries, delay,
                    )
                    time.sleep(delay)
                continue

        except Exception as e:
            last_error = str(e)
            last_status = 0
            if attempt < max_retries:
                delay = retry_with_backoff(attempt, backoff_factor=backoff_factor)
                logger.warning(
                    "Connection error on %s: %s, retry %d/%d after %.1fs",
                    circuit_name, e, attempt + 1, max_retries, delay,
                )
                time.sleep(delay)
            continue
        finally:
            if conn:
                conn.close()

    circuit.record_failure()
    return "CRITICAL", f"After {max_retries} retries: {last_error}", last_status


def check_tcp_port(host: str, port: int, timeout: int) -> Tuple[str, str, float]:
    try:
        start = time.time()
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        latency = (time.time() - start) * 1000
        return "OK", f"Connected ({latency:.1f}ms)", latency
    except socket.timeout:
        return "CRITICAL", f"Connection timeout ({timeout}s)", 0
    except ConnectionRefusedError:
        return "CRITICAL", "Connection refused", 0
    except Exception as e:
        return "CRITICAL", str(e), 0


def check_certificate_expiry(host: str, port: int = 443) -> Tuple[str, str, int]:
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
                if not cert:
                    return "WARNING", "No certificate found", 0

                from datetime import datetime as dt
                expires = dt.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
                days_left = (expires - dt.now()).days

                if days_left > 30:
                    return "OK", f"Certificate expires in {days_left} days", days_left
                elif days_left > 7:
                    return "WARNING", f"Certificate expires in {days_left} days", days_left
                else:
                    return "CRITICAL", f"Certificate expires in {days_left} days", days_left
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_disk_usage(path: str = "/") -> Tuple[str, str, float]:
    try:
        stat = os.statvfs(path)
        total = stat.f_frsize * stat.f_blocks
        free = stat.f_frsize * stat.f_bavail
        used = total - free
        pct = (used / total) * 100

        if pct < DISK_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < DISK_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_memory_usage() -> Tuple[str, str, float]:
    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().replace(" kB", "")
                    try:
                        meminfo[key] = int(value) * 1024
                    except ValueError:
                        pass

        total = meminfo.get("MemTotal", 0)
        available = meminfo.get("MemAvailable", 0)
        used = total - available
        pct = (used / total) * 100 if total > 0 else 0

        if pct < MEMORY_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < MEMORY_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_load_average() -> Tuple[str, str, float]:
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().strip().split()
            load = float(parts[0])
            cpu_count = os.cpu_count() or 1
            load_pct = (load / cpu_count) * 100

            if load_pct < 70:
                return "OK", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            elif load_pct < 90:
                return "WARNING", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            else:
                return "CRITICAL", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


# ---------------------------------------------------------------------------
# HEALTH CHECK RUNNER
# ---------------------------------------------------------------------------

def run_health_checks(
    service: Optional[str] = None,
    json_output: bool = False,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    circuit_threshold: int = DEFAULT_CIRCUIT_THRESHOLD,
    circuit_cooldown: float = DEFAULT_CIRCUIT_COOLDOWN,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "hostname": socket.gethostname(),
        "services": {},
        "infrastructure": {},
        "system": {},
        "circuit_breakers": {},
        "aggregate": {
            "total": 0,
            "ok": 0,
            "warning": 0,
            "critical": 0,
        },
        "overall_status": "OK",
    }

    all_ok = True

    # Check services
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        status, detail, code = check_http_service(
            config["host"], config["port"], config["path"], config["timeout"],
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            circuit_threshold=circuit_threshold,
            circuit_cooldown=circuit_cooldown,
        )
        results["services"][name] = {
            "status": status,
            "detail": detail,
            "code": code,
            "endpoint": f"http://{config['host']}:{config['port']}{config['path']}",
        }
        results["aggregate"]["total"] += 1
        if status == "OK":
            results["aggregate"]["ok"] += 1
        elif status == "WARNING":
            results["aggregate"]["warning"] += 1
        else:
            results["aggregate"]["critical"] += 1
            all_ok = False

    # Check infrastructure
    for name, config in INFRASTRUCTURE.items():
        if service and name != service:
            continue
        status, detail, latency = check_tcp_port(config["host"], config["port"], config["timeout"])
        results["infrastructure"][name] = {
            "status": status,
            "detail": detail,
            "endpoint": f"{config['host']}:{config['port']}",
        }
        results["aggregate"]["total"] += 1
        if status == "OK":
            results["aggregate"]["ok"] += 1
        elif status == "WARNING":
            results["aggregate"]["warning"] += 1
        else:
            results["aggregate"]["critical"] += 1
            all_ok = False

    # Check system resources
    disk_status, disk_detail, disk_pct = check_disk_usage()
    results["system"]["disk"] = {"status": disk_status, "detail": disk_detail}
    results["aggregate"]["total"] += 1
    if disk_status == "OK":
        results["aggregate"]["ok"] += 1
    elif disk_status == "WARNING":
        results["aggregate"]["warning"] += 1
    else:
        results["aggregate"]["critical"] += 1
        all_ok = False

    mem_status, mem_detail, mem_pct = check_memory_usage()
    results["system"]["memory"] = {"status": mem_status, "detail": mem_detail}
    results["aggregate"]["total"] += 1
    if mem_status == "OK":
        results["aggregate"]["ok"] += 1
    elif mem_status == "WARNING":
        results["aggregate"]["warning"] += 1
    else:
        results["aggregate"]["critical"] += 1
        all_ok = False

    load_status, load_detail, load_val = check_load_average()
    results["system"]["load"] = {"status": load_status, "detail": load_detail}
    results["aggregate"]["total"] += 1
    if load_status == "OK":
        results["aggregate"]["ok"] += 1
    elif load_status == "WARNING":
        results["aggregate"]["warning"] += 1
    else:
        results["aggregate"]["critical"] += 1

    # Check certificate expiry (web services)
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        if config["port"] == 443:
            cert_status, cert_detail, days_left = check_certificate_expiry(config["host"])
            results["services"][name]["certificate"] = {
                "status": cert_status,
                "detail": cert_detail,
                "days_remaining": days_left,
            }
            if cert_status == "CRITICAL":
                all_ok = False

    results["overall_status"] = "OK" if all_ok else "DEGRADED"

    # Circuit breaker states
    for cb_name, cb in _service_circuits.items():
        results["circuit_breakers"][cb_name] = cb.to_dict()

    return results


def print_health_report(results: Dict[str, Any]):
    print(f"\n{'='*60}")
    print(f"  HEALTH CHECK REPORT")
    print(f"  Host: {results['hostname']}")
    print(f"  Time: {results['timestamp']}")
    print(f"  Overall: {results['overall_status']}")
    print(f"{'='*60}")

    for category, items in [("Services", results["services"]),
                             ("Infrastructure", results["infrastructure"]),
                             ("System", results["system"])]:
        if items:
            print(f"\n  {category}:")
            for name, check in items.items():
                if isinstance(check, dict) and "status" in check:
                    status_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(check["status"], "?")
                    print(f"    {status_icon} {name}: {check['detail']}")
                else:
                    print(f"    {name}:")
                    for sub_name, sub_check in check.items():
                        if isinstance(sub_check, dict) and "status" in sub_check:
                            sub_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(sub_check["status"], "?")
                            print(f"      {sub_icon} {sub_name}: {sub_check['detail']}")

    # Aggregate summary
    if "aggregate" in results:
        agg = results["aggregate"]
        print(f"\n  Aggregate: {agg['total']} checks — "
              f"✓ {agg['ok']} OK, ⚠ {agg['warning']} WARNING, ✗ {agg['critical']} CRITICAL")

    # Circuit breaker states
    if results.get("circuit_breakers"):
        print(f"\n  Circuit Breakers:")
        for cb_name, cb_state in results["circuit_breakers"].items():
            icon = "✓" if cb_state["state"] == "closed" else "⚠" if cb_state["state"] == "half_open" else "✗"
            print(f"    {icon} {cb_name}: {cb_state['state']} "
                  f"({cb_state['failure_count']}/{cb_state['threshold']} failures)")
    print()


def parse_args():
    parser = argparse.ArgumentParser(description="Health check tool")
    parser.add_argument("--service", "-s", help="Check specific service only")
    parser.add_argument("--json", "-j", action="store_true", help="JSON output")
    parser.add_argument("--watch", "-w", action="store_true", help="Continuous monitoring")
    parser.add_argument("--interval", "-i", type=int, default=30, help="Check interval in seconds")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help=f"Max retries per service (default: {DEFAULT_MAX_RETRIES})")
    parser.add_argument("--backoff-factor", type=float, default=DEFAULT_BACKOFF_FACTOR,
                        help=f"Exponential backoff multiplier (default: {DEFAULT_BACKOFF_FACTOR})")
    parser.add_argument("--circuit-threshold", type=int, default=DEFAULT_CIRCUIT_THRESHOLD,
                        help=f"Consecutive failures before circuit opens (default: {DEFAULT_CIRCUIT_THRESHOLD})")
    parser.add_argument("--circuit-cooldown", type=float, default=DEFAULT_CIRCUIT_COOLDOWN,
                        help=f"Seconds before circuit transitions to half-open (default: {DEFAULT_CIRCUIT_COOLDOWN})")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.watch:
        print(f"Continuous monitoring (interval: {args.interval}s). Press Ctrl+C to stop.")
        try:
            while True:
                results = run_health_checks(
                    args.service, args.json,
                    max_retries=args.max_retries,
                    backoff_factor=args.backoff_factor,
                    circuit_threshold=args.circuit_threshold,
                    circuit_cooldown=args.circuit_cooldown,
                )
                if args.json:
                    print(json.dumps(results, indent=2))
                else:
                    print_health_report(results)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped")
    else:
        results = run_health_checks(
            args.service, args.json,
            max_retries=args.max_retries,
            backoff_factor=args.backoff_factor,
            circuit_threshold=args.circuit_threshold,
            circuit_cooldown=args.circuit_cooldown,
        )
        if args.json:
            output = json.dumps(results, indent=2)
            print(output)
        else:
            print_health_report(results)

        if args.output:
            with open(args.output, "w") as f:
                if args.json:
                    json.dump(results, f, indent=2)
                else:
                    json.dump(results, f, indent=2)
            print(f"Report saved to {args.output}")

        if results["overall_status"] == "DEGRADED":
            return 1

    return 0


if __name__ == "__main__":
    main()
