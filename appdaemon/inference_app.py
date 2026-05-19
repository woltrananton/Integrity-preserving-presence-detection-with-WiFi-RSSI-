"""inference_app.py — AppDaemon-app för WiFi-RSSI hybridmodell.

Multivariat arkitektur (ersätter tidigare V1-V7-iterationer):
  - rssi_logger.py skriver CSV till /share/data/
  - Denna app tail:ar CSV var 10:e sek, beräknar features, anropar ml-server
    för IF-prediktion, kör rule-based detektorer A/B/C lokalt och slår
    ihop till hybrid presence-state. Publicerar till binary_sensor via MQTT.
  - period_logger.py lyssnar separat på state-change → loggar perioder.

Modellen: tränad med Projekt/test/train_if_production.py på säkert-tomma
helger. 100% TPR på 9 annoterade testfönster, 17 FP per 3 dygn helg.
"""

from __future__ import annotations

import csv
import json
import statistics
import urllib.error
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import appdaemon.plugins.hass.hassapi as hass


PRIMARY_ANCHORS = ["led_router", "prokord_vh", "shelly_vv", "cleverio_vm"]
ALL_ANCHORS = PRIMARY_ANCHORS + ["shelly_kh"]  # kh loggas men ej i primär logik

HISTORY_60S_SAMPLES = 6   # 60s rolling-fönster vid 10s sampling
HISTORY_30S_SAMPLES = 3   # 30s rolling-fönster


def _median(seq):
    return statistics.median(seq) if seq else 0.0


def _std(seq):
    return statistics.stdev(seq) if len(seq) >= 2 else 0.0


# ---------- Adaptiv baseline (ewma eller nightly) ----------
NIGHT_START_HOUR = 0
NIGHT_END_HOUR = 5
NIGHTLY_MIN_SAMPLES = 1000


class EWMABaseline:
    """Per-ankare adaptiv baseline för median + std.

    Två strategier:
      "ewma"    - EWMA-glidande som uppdateras vid tom-perioder (legacy).
      "nightly" - Hård recal kl 05:00 från 00-05-buffer, fryst dagtid.

    Default är "nightly" (fix 2026-05-13: löser baseline-drift-feedback).
    """

    HALF_LIFE_MIN = 30.0
    UPDATE_INTERVAL_S = 30.0
    EMPTY_REQUIRED_S = 300.0

    def __init__(self, anchors, strategy="nightly"):
        if strategy not in ("ewma", "nightly"):
            raise ValueError(f"okänd strategy: {strategy}")
        self.strategy = strategy
        self.anchors = list(anchors)
        self.alpha = 1.0 - 0.5 ** (self.UPDATE_INTERVAL_S / (self.HALF_LIFE_MIN * 60.0))
        self.median = {a: 0.0 for a in anchors}
        self.std = {a: 0.0 for a in anchors}
        self.initialized = {a: False for a in anchors}
        # EWMA-state
        self._empty_since = None
        self._last_update = None
        # Nightly-state
        self._night_buf_med = {a: [] for a in anchors}
        self._night_buf_std = {a: [] for a in anchors}
        self._prev_in_night = None
        self.last_recal_info = None

    def init_from(self, median_init: dict, std_init: dict) -> None:
        for a, m in median_init.items():
            if a in self.median:
                self.median[a] = float(m)
                self.std[a] = float(std_init.get(a, 0.0))
                self.initialized[a] = True

    def step(self, ts, is_empty_now, current_med, current_std):
        if self.strategy == "ewma":
            return self._step_ewma(ts, is_empty_now, current_med, current_std)
        else:
            return self._step_nightly(ts, current_med, current_std)

    def _step_ewma(self, ts, is_empty_now, current_med, current_std):
        if not is_empty_now:
            self._empty_since = None
            return False
        if self._empty_since is None:
            self._empty_since = ts
            return False
        if (ts - self._empty_since).total_seconds() < self.EMPTY_REQUIRED_S:
            return False
        if self._last_update and (ts - self._last_update).total_seconds() < self.UPDATE_INTERVAL_S:
            return False
        self._last_update = ts
        for a in self.median:
            if a not in current_med:
                continue
            if not self.initialized[a]:
                self.median[a] = current_med[a]
                self.std[a] = current_std.get(a, self.std[a])
                self.initialized[a] = True
            else:
                self.median[a] = self.alpha * current_med[a] + (1 - self.alpha) * self.median[a]
                self.std[a] = self.alpha * current_std.get(a, self.std[a]) + (1 - self.alpha) * self.std[a]
        return True

    def _step_nightly(self, ts, current_med, current_std):
        in_night = NIGHT_START_HOUR <= ts.hour < NIGHT_END_HOUR
        if in_night:
            for a in self.anchors:
                if a in current_med:
                    self._night_buf_med[a].append(current_med[a])
                if a in current_std:
                    self._night_buf_std[a].append(current_std[a])
            self._prev_in_night = True
            return False
        if self._prev_in_night is True:
            self._recal_from_buffer(ts)
            self._prev_in_night = False
            return True
        self._prev_in_night = False
        return False

    def _recal_from_buffer(self, ts):
        info = {"ts": ts.isoformat(), "recal": {}, "fallback": {}}
        for a in self.anchors:
            buf_med = self._night_buf_med[a]
            buf_std = self._night_buf_std[a]
            n = len(buf_med)
            if n >= NIGHTLY_MIN_SAMPLES:
                self.median[a] = statistics.median(buf_med)
                if buf_std:
                    self.std[a] = statistics.median(buf_std)
                self.initialized[a] = True
                info["recal"][a] = {"n": n, "med": round(self.median[a], 2)}
            else:
                info["fallback"][a] = {"n": n}
            self._night_buf_med[a] = []
            self._night_buf_std[a] = []
        self.last_recal_info = info


# ---------- Rule-based detektorer (motsvarar test/detectors.py) ----------
class DetectorA:
    """Median-shift: 1 person stillsittande."""
    NAME = "A"

    def __init__(self, ledr_drop=3.0, vh_drop=1.0, vv_drop=1.5, persistence_s=90.0):
        self.ledr_drop = ledr_drop
        self.vh_drop = vh_drop
        self.vv_drop = vv_drop
        self.persistence_s = persistence_s
        self._trig_since = None

    def update(self, ts, med60, baseline_med):
        ledr_d = baseline_med["led_router"] - med60.get("led_router", 0)
        vh_d = baseline_med["prokord_vh"] - med60.get("prokord_vh", 0)
        vv_d = baseline_med["shelly_vv"] - med60.get("shelly_vv", 0)
        cond = ledr_d >= self.ledr_drop and (vh_d >= self.vh_drop or vv_d >= self.vv_drop)
        if not cond:
            self._trig_since = None
            return False
        if self._trig_since is None:
            self._trig_since = ts
        return (ts - self._trig_since).total_seconds() >= self.persistence_s


class DetectorB:
    """Std-shift: flera personer."""
    NAME = "B"

    def __init__(self, ledr_jump=1.0, vh_jump=1.0, vv_jump=1.0, persistence_s=30.0):
        self.ledr_jump = ledr_jump
        self.vh_jump = vh_jump
        self.vv_jump = vv_jump
        self.persistence_s = persistence_s
        self._trig_since = None

    def update(self, ts, std30, baseline_std):
        ledr_j = std30.get("led_router", 0) - baseline_std["led_router"]
        vh_j = std30.get("prokord_vh", 0) - baseline_std["prokord_vh"]
        vv_j = std30.get("shelly_vv", 0) - baseline_std["shelly_vv"]
        cond = ledr_j >= self.ledr_jump and (vh_j >= self.vh_jump or vv_j >= self.vv_jump)
        if not cond:
            self._trig_since = None
            return False
        if self._trig_since is None:
            self._trig_since = ts
        return (ts - self._trig_since).total_seconds() >= self.persistence_s


class DetectorC:
    """Fidget-spike-räknare."""
    NAME = "C"

    def __init__(self, std_threshold=4.0, count_threshold=4, window_s=300.0):
        self.std_threshold = std_threshold
        self.count_threshold = count_threshold
        self.window_s = window_s
        self._spikes = deque()

    def update(self, ts, std30, med60, baseline_med):
        ledr_std = std30.get("led_router", 0.0)
        while self._spikes and (ts - self._spikes[0]).total_seconds() > self.window_s:
            self._spikes.popleft()
        if ledr_std >= self.std_threshold:
            if not self._spikes or (ts - self._spikes[-1]).total_seconds() >= 30:
                self._spikes.append(ts)
        med_ok = (baseline_med["led_router"] - med60.get("led_router", 0)) >= 0.3
        return len(self._spikes) >= self.count_threshold and med_ok


# ---------- Trigger latch (fix 2026-05-11) ----------
class TriggerLatch:
    """Håller lamp_on i N sekunder efter senaste trigger.

    Skyddar mot persistens-glap vid stillsittande där detektorerna
    triggar glest. Inspirerat av V7:s LatchTracker men längre.
    """
    def __init__(self, latch_s=600.0):
        self.latch_s = latch_s
        self.latch_until = None

    def fire(self, ts):
        new_until = ts + timedelta(seconds=self.latch_s)
        if self.latch_until is None or new_until > self.latch_until:
            self.latch_until = new_until

    def is_active(self, ts):
        return self.latch_until is not None and ts <= self.latch_until


# ---------- Hybrid detector wrapper ----------
class HybridDetector:
    """Slår ihop A/B/C/D + TriggerLatch + cooldown till lamp-state.

    Fix 2026-05-11: TriggerLatch (10 min) håller PÅ trots glesa triggers.

    Fix 2026-05-13: tre tillägg som löser baseline-drift-feedback:
      * bc_watchdog_s: max tid lampan får vara ON utan B/C-fyring. Tvingar
        release om bara D fyrar (vilket är typiskt vid möbel-skift).
      * motion_required efter watchdog: D får inte återtända ensam. Bara B/C
        (rörelse-bevis) får tända igen tills rörelse syns.
      * natt-guard 00-05: alla detektorer avstängda under buffer-fönstret för
        nightly-recal. Kontoret är stängt, så inga FP där.
    """

    def __init__(self, cooldown_s=120.0, latch_s=600.0, bc_watchdog_s=7200.0,
                 night_guard_end_hour=5):
        self.a = DetectorA()
        self.b = DetectorB()
        self.c = DetectorC()
        self.latch = TriggerLatch(latch_s=latch_s)
        self.cooldown_s = cooldown_s
        self.bc_watchdog_s = bc_watchdog_s
        self.night_guard_end_hour = night_guard_end_hour
        self.lamp_on = False
        self._all_off_since = None
        self.last_active = "-"
        self._bc_watchdog_ts = None
        self._motion_required = False

    def update(self, ts, med60, std30, baseline_med, baseline_std, d_anomaly):
        # Natt-guard: under 00:00-05:00 är detektorer avstängda
        if ts.hour < self.night_guard_end_hour:
            self.lamp_on = False
            self.latch.latch_until = None
            self._bc_watchdog_ts = None
            self._motion_required = False
            return {
                "lamp_on": False, "A": False, "B": False, "C": False,
                "D": False, "latch_active": False, "last_active": "-",
                "watchdog_release": False,
            }

        ra = self.a.update(ts, med60, baseline_med)
        rb = self.b.update(ts, std30, baseline_std)
        rc = self.c.update(ts, std30, med60, baseline_med)

        # B eller C = rörelse-evidens. Återställer motion-required-låset.
        if rb or rc:
            self._motion_required = False

        if self._motion_required:
            any_trigger = rb or rc  # bara rörelse-bevis får tända igen
        else:
            any_trigger = ra or rb or rc or d_anomaly

        was_on = self.lamp_on
        if any_trigger:
            self.latch.fire(ts)
            self._all_off_since = None
            if ra: self.last_active = "A"
            elif rb: self.last_active = "B"
            elif rc: self.last_active = "C"
            elif d_anomaly: self.last_active = "D"
            self.lamp_on = True
        elif self.latch.is_active(ts):
            self.lamp_on = True
        else:
            if self._all_off_since is None:
                self._all_off_since = ts
            elif (ts - self._all_off_since).total_seconds() >= self.cooldown_s:
                self.lamp_on = False

        # B/C-watchdog
        watchdog_release = False
        if self.bc_watchdog_s is not None and self.bc_watchdog_s > 0:
            if not was_on and self.lamp_on:
                self._bc_watchdog_ts = ts
            if rb or rc:
                self._bc_watchdog_ts = ts
            if self.lamp_on and self._bc_watchdog_ts is not None:
                if (ts - self._bc_watchdog_ts).total_seconds() >= self.bc_watchdog_s:
                    self.lamp_on = False
                    self.latch.latch_until = None
                    self._all_off_since = ts
                    self._bc_watchdog_ts = None
                    self._motion_required = True
                    watchdog_release = True
            if not self.lamp_on:
                self._bc_watchdog_ts = None

        return {
            "lamp_on": self.lamp_on,
            "A": ra,
            "B": rb,
            "C": rc,
            "D": d_anomaly,
            "latch_active": self.latch.is_active(ts),
            "last_active": self.last_active,
            "watchdog_release": watchdog_release,
        }


class InferenceApp(hass.Hass):

    def initialize(self):
        self.data_dir = Path(self.args.get("data_dir", "/share/data"))
        self.log_dir = Path(self.args.get("log_dir", "/share/data/inference_log"))
        self.sample_interval = int(self.args.get("sample_interval", 10))
        self.mqtt_topic = self.args.get("mqtt_topic", "office/room1/wifi_presence")
        self.ml_url = self.args.get("ml_url", "http://192.168.1.88:8765")
        self.persistence_d_s = float(self.args.get("if_persistence_s", 100.0))
        self.cooldown_s = float(self.args.get("cooldown_s", 120.0))
        self.latch_s = float(self.args.get("latch_s", 600.0))
        # Fix 2026-05-13
        self.baseline_strategy = self.args.get("baseline_strategy", "nightly")
        self.bc_watchdog_s = float(self.args.get("bc_watchdog_s", 7200.0))

        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Hämta modell-info från ml-server
        self.log(f"Hämtar modell-info från {self.ml_url}/info ...")
        try:
            with urllib.request.urlopen(f"{self.ml_url}/info", timeout=10) as resp:
                info = json.loads(resp.read())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            self.log(f"FEL: kunde inte nå ml-server: {e}", level="ERROR")
            return

        self.feature_cols = info["feature_cols"]
        self.score_threshold = info["score_threshold"]
        self.log(f"Modell {info['version']} laddad. score_thr={self.score_threshold} "
                 f"strategy={info.get('strategy')}")

        # Baseline från bundle som initial-värden (recal från 00-05 varje natt)
        self.baseline = EWMABaseline(PRIMARY_ANCHORS, strategy=self.baseline_strategy)
        self.baseline.init_from(info["baseline_init"], info.get("baseline_std_init", {}))
        self.log(f"Baseline initialiserad från bundle, strategy={self.baseline_strategy}: "
                 f"{info['baseline_init']}")

        # Hybrid + detektor D persistens-tracker
        self.hybrid = HybridDetector(cooldown_s=self.cooldown_s, latch_s=self.latch_s,
                                     bc_watchdog_s=self.bc_watchdog_s)
        self.d_trig_since = None

        # Rolling-fönster per ankare
        self.r60 = {a: deque(maxlen=HISTORY_60S_SAMPLES) for a in ALL_ANCHORS}
        self.r30 = {a: deque(maxlen=HISTORY_30S_SAMPLES) for a in ALL_ANCHORS}

        # CSV-tail-state
        self.current_file = None
        self.last_pos = 0
        self.header = None

        # Pre-fill från dagens CSV
        n_filled = self._read_new_rows()
        self.log(f"inference_app startad. Pre-fill: {n_filled} rader. Ticker var {self.sample_interval}s")

        # Trace-log state (per-tick CSV i /share/data/inference_log/)
        self._log_path = None
        self._log_header_written = False

        # Tidsstämpel för senaste detektor-aktiva tick (A/B/C/D). Exponeras
        # som attribut så period_logger kan mäta närvaroperioden på
        # detektor-span i stället för lampans latch-svans. Fix 2026-05-17.
        self._last_detector_active_ts = None

        self.last_published = None
        self.run_every(self.tick, "now", self.sample_interval)

    # --------- Trace-CSV ---------
    def _trace_log(self, ts, lamp_result, score, below_thr, med60, std30, bm, bs):
        """Skriver en rad per tick till /share/data/inference_log/inference_YYYY-MM-DD.csv.

        Kolumner: timestamp, lamp_on, detector, score, below_thr, A, B, C, D,
        latch_active, <m60×4>, <s30×4>, <active×4>, active_count.
        Active-flagga = std30[a] - baseline_std[a] >= 1.0 (rörelse-bevis per ankare).
        """
        day = ts.strftime("%Y-%m-%d")
        path = self.log_dir / f"inference_{day}.csv"
        new_day = path != self._log_path
        if new_day:
            self._log_path = path
            self._log_header_written = path.exists() and path.stat().st_size > 0

        active_flags = {a: int((std30.get(a, 0) - bs.get(a, 0)) >= 1.0)
                        for a in PRIMARY_ANCHORS}
        active_count = sum(active_flags.values())

        try:
            with path.open("a", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if not self._log_header_written:
                    cols = ["timestamp", "lamp_on", "detector", "score", "below_thr",
                            "A", "B", "C", "D", "latch_active"]
                    cols += [f"{a}_m60" for a in PRIMARY_ANCHORS]
                    cols += [f"{a}_s30" for a in PRIMARY_ANCHORS]
                    cols += [f"{a}_active" for a in PRIMARY_ANCHORS]
                    cols += ["active_count"]
                    w.writerow(cols)
                    self._log_header_written = True
                row = [
                    ts.isoformat(timespec="seconds"),
                    int(lamp_result["lamp_on"]),
                    lamp_result["last_active"] if lamp_result["lamp_on"] else "-",
                    f"{score:.4f}",
                    int(below_thr),
                    int(lamp_result["A"]),
                    int(lamp_result["B"]),
                    int(lamp_result["C"]),
                    int(lamp_result["D"]),
                    int(lamp_result["latch_active"]),
                ]
                row += [f"{med60.get(a, float('nan')):.2f}" for a in PRIMARY_ANCHORS]
                row += [f"{std30.get(a, float('nan')):.3f}" for a in PRIMARY_ANCHORS]
                row += [active_flags[a] for a in PRIMARY_ANCHORS]
                row += [active_count]
                w.writerow(row)
        except OSError as e:
            self.log(f"WARN: kunde inte skriva trace-log: {e}", level="WARNING")

    # --------- CSV-tail ---------
    def _get_today_path(self):
        for is_utc in (False, True):
            day = datetime.now(timezone.utc if is_utc else None).strftime("%Y-%m-%d")
            p = self.data_dir / f"rssi_{day}.csv"
            if p.exists():
                return p
        day = datetime.now().strftime("%Y-%m-%d")
        return self.data_dir / f"rssi_{day}.csv"

    def _read_new_rows(self):
        path = self._get_today_path()
        if not path.exists():
            return 0
        if self.current_file != path:
            self.current_file = path
            self.last_pos = 0
            self.header = None
        n_added = 0
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                if self.header is None:
                    header_line = f.readline()
                    if not header_line:
                        return 0
                    self.header = [h.strip() for h in header_line.rstrip("\r\n").split(",")]
                    self.last_pos = f.tell()
                f.seek(self.last_pos)
                reader = csv.DictReader(f, fieldnames=self.header)
                for row in reader:
                    self._handle_row(row)
                    n_added += 1
                self.last_pos = f.tell()
        except OSError:
            pass
        return n_added

    def _handle_row(self, row):
        try:
            ts = datetime.fromisoformat(row["timestamp"])
        except Exception:
            return
        entity = row.get("entity_id")
        rssi_str = row.get("rssi")
        if entity not in ALL_ANCHORS or not rssi_str:
            return
        try:
            rssi = float(rssi_str)
        except ValueError:
            return
        self.r60[entity].append(rssi)
        self.r30[entity].append(rssi)

    # --------- Tick ---------
    def tick(self, _):
        n = self._read_new_rows()
        if n == 0:
            return

        ts = datetime.now().astimezone()

        med60 = {a: _median(list(self.r60[a])) for a in PRIMARY_ANCHORS
                 if len(self.r60[a]) >= HISTORY_60S_SAMPLES}
        std30 = {a: _std(list(self.r30[a])) for a in PRIMARY_ANCHORS
                 if len(self.r30[a]) >= HISTORY_30S_SAMPLES}

        if len(med60) < 4 or len(std30) < 4:
            return  # vänta på fullt fönster

        # 8-dim feature-vektor: median-delta + std-delta per ankare
        bm = dict(self.baseline.median)
        bs = dict(self.baseline.std)
        feats = []
        for a in PRIMARY_ANCHORS:
            feats.append(med60[a] - bm[a])
            feats.append(std30[a] - bs[a])

        # Anropa ml-server
        d_anomaly = False
        score = 0.0
        below_thr = False
        try:
            req = urllib.request.Request(
                f"{self.ml_url}/predict",
                data=json.dumps({"features": feats}).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                result = json.loads(resp.read())
            score = float(result.get("score", 0.0))
            below_thr = bool(result["is_anomaly"])
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            self.log(f"WARN: ml-server unreachable: {e}", level="WARNING")

        # IF persistens (100s = 10 sampler) — separat från rule-based
        if below_thr:
            if self.d_trig_since is None:
                self.d_trig_since = ts
            d_anomaly = (ts - self.d_trig_since).total_seconds() >= self.persistence_d_s
        else:
            self.d_trig_since = None

        # Hybrid lampa-state
        lamp_result = self.hybrid.update(ts, med60, std30, bm, bs, d_anomaly)
        lamp = lamp_result["lamp_on"]

        # Detektor-aktiv span. När lampan är PÅ och minst en detektor faktiskt
        # fyrar uppdateras tidsstämpeln. period_logger använder den som
        # periodslut — latch (600 s) + cooldown (120 s) skulle annars lägga
        # ~12 min falsk svans på varje period som loggas till Azure.
        if lamp and (lamp_result["A"] or lamp_result["B"]
                     or lamp_result["C"] or lamp_result["D"]):
            self._last_detector_active_ts = ts

        # Skriv trace-rad (oavsett state-change, för retrospektiv analys)
        self._trace_log(ts, lamp_result, score, below_thr, med60, std30, bm, bs)

        # Uppdatera baseline (nightly: bara mellan 00-05 och recal kl 05;
        # ewma: vid tom-periode r enligt is_empty_now).
        is_empty_now = not lamp  # bara använd av ewma-pathen
        self.baseline.step(ts, is_empty_now, med60, std30)
        if self.baseline.last_recal_info and self.baseline.last_recal_info.get("ts") == ts.isoformat():
            self.log(f"Nightly recal kl {ts}: {self.baseline.last_recal_info}")

        # Publicera till HA om state-change
        new_state = "on" if lamp else "off"
        if new_state != self.last_published:
            self.set_state("binary_sensor.rummet_bemannat_wifi",
                           state=new_state,
                           attributes={
                               "detector": self.hybrid.last_active if lamp else "-",
                               "source": "wifi_multivariate",
                               "score_threshold": self.score_threshold,
                               # Senaste detektor-aktiva tick — period_logger
                               # mäter periodslut här, inte på lampans släck.
                               "detector_end_ts": (
                                   self._last_detector_active_ts.isoformat()
                                   if self._last_detector_active_ts else None),
                           })
            self.last_published = new_state
            self.log(f"inference_app → {new_state} (via {self.hybrid.last_active})")
