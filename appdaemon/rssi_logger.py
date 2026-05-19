"""
AppDaemon-version av RSSI-loggern för WiFi-närvarodetektering.

Pollar **routern** (Asus RT-AC51U+/AC52U_B1, MediaTek-chipset) över SSH
istället för varje plugg individuellt. Vi får därmed:

  • Enhetlig mottagare (samma chip-kalibrering för alla anchors)
  • Brand-agnostisk loggning (Shelly, Tuya, vad som helst på WiFi)
  • Ingen rådata lämnar lokalnätverket

**Mätstrategi (workaround för stock-firmware-begränsning):**

Asus stock-firmware på MT7620 exponerar inte per-klient-RSSI via något
tillförlitligt API: `iwpriv ra0 get_mac_table` är trasig, /proc-paths
saknas, AsusWRT-Merlin stöds inte. Däremot uppdaterar `iwpriv ra0 stat`
sin globala "Last RX RSSI" varje frame. Genom att SSH:a in och pinga
varje anchor i tur och ordning — och läsa stat direkt efter varje ping —
"selekterar" vi vilken klient som senast skickade en frame, och får
därmed ut den klientens RSSI vid routern.

Detta har en känd race: om en annan klient skickar en frame mellan vår
ping och vår stat-läsning får vi fel värde. På ett tyst nätverk
(WAN-kontrollerat, få aktiva klienter) är racet sällsynt men finns.
Mitigeras av rullande median i analyssteget snarare än här.

Konfigureras via apps.yaml — se exempel där.
"""

import csv
import logging
import re
from datetime import datetime
from pathlib import Path

import appdaemon.plugins.hass.hassapi as hass

# Tysta paramiko/transport-spam: vi vill INTE se "Connected (version 2.0)"
# och "Authentication successful" var 10:e sekund i AppDaemon-loggen.
logging.getLogger("paramiko").setLevel(logging.WARNING)
logging.getLogger("paramiko.transport").setLevel(logging.WARNING)


CSV_COLUMNS = [
    "timestamp",
    "entity_id",
    "rssi",
    "label",
    "camera_motion",
    "session_id",
    "mac",
    "rssi0",
    "rssi1",
    "idle_sec",
]

# Regex för rader som dyker upp i `iwpriv ra0 stat` output:
# "RSSI                            = -37 -44 0"
RSSI_LINE = re.compile(r"^RSSI\s*=\s*(-?\d+)\s+(-?\d+)\s+(-?\d+)")

# Markerar början av en ankarsektion i SSH-outputen så vi kan parsa
# flera ankare på en gång. Format: "===<MAC>===" på egen rad.
ANCHOR_HEADER = re.compile(r"^===\s*([0-9A-Fa-f:]{17})\s*===")

# Filen där SSH-lösenordet ligger (gitignored, samma mapp som denna fil).
SECRETS_PATH = Path(__file__).parent / "secrets.yaml"


class RssiLogger(hass.Hass):
    """SSH:ar mot routern, läser per-klient-RSSI, skriver CSV per anchor."""

    def initialize(self):
        # ---------- Läs config ----------
        self.sample_interval = int(self.args.get("sample_interval", 10))
        self.motion_entity = self.args.get("motion_entity", "").strip()
        self.heartbeat_entity = self.args.get("heartbeat_entity", "").strip()
        self.label_mode = self.args.get("label_mode", "auto").strip()
        self.session_id = self.args.get("session_id", "").strip()
        self.data_dir = Path(self.args.get("data_dir", "/share/data"))

        router_cfg = self.args.get("router", {})
        self.router_host = router_cfg.get("host", "").strip()
        self.router_user = router_cfg.get("user", "").strip()
        self.router_password = self._resolve_password(router_cfg)
        self.router_port = int(router_cfg.get("port", 22))

        if not (self.router_host and self.router_user and self.router_password):
            self.error("FEL: routern är inte fullständigt konfigurerad i apps.yaml.")
            return

        # ---------- Validera och normalisera ankarna ----------
        anchors = self.args.get("anchors", [])
        self.anchors = []
        for a in anchors:
            name = a.get("name", "").strip()
            mac = a.get("mac", "").strip().upper()
            ip = a.get("ip", "").strip()
            if not (name and mac and ip):
                self.error(f"VARNING: hoppar över ofullständigt ankare: {a}")
                continue
            self.anchors.append({"name": name, "mac": mac, "ip": ip})

        if not self.anchors:
            self.error("FEL: inga giltiga ankare i apps.yaml.")
            return

        self.anchor_by_mac = {a["mac"]: a for a in self.anchors}

        # ---------- Importera paramiko (kräver python_packages: paramiko i addon) ----------
        try:
            import paramiko  # noqa: F401
            self._paramiko = paramiko
        except ImportError:
            self.error(
                "FEL: paramiko saknas. Lägg till 'paramiko' under "
                "python_packages i AppDaemon-addonens config och starta om."
            )
            return

        # ---------- Förbered datakatalog ----------
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # ---------- Tillstånd ----------
        self.camera_motion = "unknown"
        self.sample_count = 0
        self.error_count = 0
        self.no_match_count = 0

        # ---------- Initial kamerastate + lyssna på framtida ändringar ----------
        if self.motion_entity:
            initial = self.get_state(self.motion_entity)
            self.camera_motion = initial if initial is not None else "unknown"
            self.listen_state(self.on_motion_change, self.motion_entity)

        # ---------- Starta sampling-loopen ----------
        self.run_every(self.sample_tick, "now+5", self.sample_interval)

        # ---------- Heartbeat var 5:e minut ----------
        if self.heartbeat_entity:
            self.run_every(self.heartbeat_tick, "now+30", 300)

        self.log(
            f"RSSI-logger startad: router={self.router_host} "
            f"({self.router_user}), {len(self.anchors)} ankare, "
            f"intervall {self.sample_interval}s, data → {self.data_dir}"
        )

    # ---------- Callbacks ----------

    def on_motion_change(self, entity, attribute, old, new, kwargs):
        self.camera_motion = new if new is not None else "unknown"

    def sample_tick(self, kwargs):
        ts = datetime.now().astimezone().isoformat(timespec="seconds")

        # Bestäm label
        if self.label_mode == "auto":
            row_label = (
                "occupied" if self.camera_motion == "on"
                else "empty" if self.camera_motion == "off"
                else "unknown"
            )
        else:
            row_label = self.label_mode

        # Hämta MAC-tabell från routern
        try:
            output = self._poll_router()
        except Exception as e:
            self.error_count += 1
            self.error(f"VARNING: routerpoll misslyckades: {e}")
            return

        rssi_by_mac = self._parse_router_output(output)

        # Nödfallsdump: om parsern returnerar 0 ankare misstänker vi att
        # routern svarar konstigt — då skriver vi sista rå-outputen för
        # felsökning. Skriver inte vid varje lyckat anrop.
        if not rssi_by_mac:
            try:
                (self.data_dir / "_debug_router_output.txt").write_text(
                    output, encoding="utf-8"
                )
            except Exception:
                pass
            self.log(
                f"VARNING: parsern fick 0 ankare av {len(output)} tecken — "
                f"se _debug_router_output.txt"
            )

        path = self._csv_path()
        self._ensure_csv_header(path)

        rows = []
        seen_macs = set()
        for mac, info in rssi_by_mac.items():
            anchor = self.anchor_by_mac.get(mac)
            if not anchor:
                continue  # ignorera klienter som inte är konfigurerade som anchors
            seen_macs.add(mac)
            rows.append([
                ts,
                anchor["name"],
                info["rssi_avg"],
                row_label,
                self.camera_motion,
                self.session_id,
                mac,
                info["rssi0"],
                info["rssi1"],
                info["idle"],
            ])

        # Skriv en "saknad"-rad för anchors som inte syntes i tabellen den här
        # cykeln (sov/utanför räckvidd) — så CSV behåller raster-struktur.
        for anchor in self.anchors:
            if anchor["mac"] not in seen_macs:
                self.no_match_count += 1
                rows.append([
                    ts,
                    anchor["name"],
                    "",  # rssi
                    row_label,
                    self.camera_motion,
                    self.session_id,
                    anchor["mac"],
                    "", "", "",
                ])

        with path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)

        self.sample_count += 1

        if self.sample_count % 60 == 0:
            self.log(
                f"{self.sample_count} samples skrivna "
                f"(fel={self.error_count}, anchor_miss={self.no_match_count}, "
                f"label={row_label})"
            )

    def heartbeat_tick(self, kwargs):
        try:
            self.set_state(
                self.heartbeat_entity,
                state="online",
                attributes={
                    "samples": self.sample_count,
                    "errors": self.error_count,
                    "anchor_miss": self.no_match_count,
                    "last_update": datetime.now().astimezone().isoformat(timespec="seconds"),
                    "anchors": len(self.anchors),
                },
            )
        except Exception as e:
            self.error(f"VARNING: heartbeat misslyckades: {e}")

    # ---------- Routerkommunikation ----------

    def _poll_router(self):
        """SSH:a mot routern, pinga varje anchor i tur och ordning, läs iwpriv-RSSI.

        Eftersom `iwpriv ra0 get_mac_table` är trasig på denna firmware använder
        vi `iwpriv ra0 stat`s globala "Last RX RSSI" och tvingar selektion genom
        att pinga rätt klient direkt innan vi läser. Routern uppdaterar då sin
        senaste-mottagna-RSSI till just den klientens reply, och vi parsar ut
        den för varje anchor i ordning.

        Returnerar rå-output med separator-rader `===<MAC>===` mellan ankarna.
        """
        parts = []
        for a in self.anchors:
            parts.append(f"echo '==={a['mac']}==='")
            # 1 paket, 1 s timeout. Om ankaret är offline expirerar pingen
            # och vi kommer ändå att läsa global RSSI — då sannolikt fel
            # värde (annan klient skickade senast). Analyssteget filtrerar.
            parts.append(f"ping -c 1 -W 1 {a['ip']} > /dev/null 2>&1")
            parts.append("iwpriv ra0 stat | grep '^RSSI'")
        remote_cmd = "; ".join(parts)

        client = self._paramiko.SSHClient()
        client.set_missing_host_key_policy(self._paramiko.AutoAddPolicy())
        try:
            client.connect(
                self.router_host,
                port=self.router_port,
                username=self.router_user,
                password=self.router_password,
                timeout=8,
                banner_timeout=8,
                auth_timeout=8,
                allow_agent=False,
                look_for_keys=False,
            )
            stdin, stdout, stderr = client.exec_command(remote_cmd, timeout=12)
            output = stdout.read().decode("utf-8", errors="replace")
        finally:
            try:
                client.close()
            except Exception:
                pass
        return output

    @staticmethod
    def _parse_router_output(output):
        """Tolka ping+iwpriv-output och returnera dict per MAC.

        Förväntad outputstruktur (upprepad per ankare):
            ===<MAC>===
            RSSI                            = -64 -66 0

        Mellan separator och RSSI-raden ligger ping-outputen som vi
        redirectat till /dev/null, så vi ser inte den. Bara rader som
        börjar med "RSSI" är intressanta — vi binder dem till senaste
        sett MAC-separator.
        """
        result = {}
        current_mac = None
        for line in output.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            m = ANCHOR_HEADER.match(stripped)
            if m:
                current_mac = m.group(1).upper()
                continue
            if current_mac is None:
                continue
            r = RSSI_LINE.match(stripped)
            if not r:
                continue
            rssi0 = int(r.group(1))
            rssi1 = int(r.group(2))
            # chain 2 är alltid 0 på 2-antenns-router → ignorera
            chains = [v for v in (rssi0, rssi1) if v != 0]
            rssi_avg = sum(chains) // len(chains) if chains else 0
            result[current_mac] = {
                "rssi0": rssi0,
                "rssi1": rssi1,
                "idle": 0,  # alltid färsk — vi pingade just innan läsningen
                "rssi_avg": rssi_avg,
            }
            current_mac = None  # vänta på nästa anchor-header
        return result

    # ---------- Hjälpmetoder ----------

    def _resolve_password(self, router_cfg):
        # Antingen direkt 'password:' (för testning) eller 'password_secret:' nyckel
        # som slås upp i secrets.yaml bredvid denna fil.
        direct = router_cfg.get("password")
        if direct:
            return str(direct)
        key = router_cfg.get("password_secret")
        if not key:
            return ""
        if not SECRETS_PATH.exists():
            self.error(f"FEL: hittar inte {SECRETS_PATH} (för password_secret)")
            return ""
        try:
            import yaml
            with SECRETS_PATH.open(encoding="utf-8") as f:
                secrets = yaml.safe_load(f) or {}
            return str(secrets.get(key, ""))
        except Exception as e:
            self.error(f"FEL: kunde inte läsa secrets.yaml: {e}")
            return ""

    def _csv_path(self):
        today = datetime.now().strftime("%Y-%m-%d")
        return self.data_dir / f"rssi_{today}.csv"

    def _ensure_csv_header(self, path):
        if not path.exists():
            with path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(CSV_COLUMNS)
