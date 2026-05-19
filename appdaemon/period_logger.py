"""period_logger.py — AppDaemon-app som loggar närvaroperioder.

Lyssnar på state-change på binary_sensor.rummet_bemannat_wifi (modellens
output). Samlar period-start och period-slut, filtrerar bort perioder
kortare än min_period_min och publicerar perioden till:
  1. MQTT-topic — för HA-konsumtion och loggning
  2. HA event bus (event: rummet_period) — azure_bridge plockar upp och
     forwardar till Azure IoT Hub.

Periodslut mäts på DETEKTOR-SPAN + en kort marginal, inte på när lampan
släcks. inference_app exponerar attributet detector_end_ts = senaste tick
där en detektor (A/B/C/D) faktiskt fyrade. Lampans latch (600 s) +
cooldown (120 s) håller annars lampan PÅ ~12 min efter sista verkliga
aktivitet, vilket skulle ge varje Azure-period en falsk svans och blåsa
upp korta besök till 12 min.

Periodslut = detector_end_ts + detector_grace_s. Marginalen (default
120 s) tar bort merparten av latch-svansen utan att perioden kollapsar
helt — en stillasittande person dröjer sig rimligen kvar en stund efter
sista RF-störningen. Latchen behövs för lampstyrningen, men närvarologgen
ska visa verklig vistelse, inte lamphistorik. Fix 2026-05-17.

Bryt-ut-designen (en Azure-klient i hela systemet) finns för att
IoT Hub bara tillåter en aktiv anslutning per device-identitet. Vi hade
en re-connect-storm 2026-05-12 när både den här appen och azure_bridge
kopplade upp samtidigt med samma connection string.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import appdaemon.plugins.hass.hassapi as hass


class PeriodLogger(hass.Hass):

    def initialize(self):
        self.watch_entity = self.args.get(
            "watch_entity", "binary_sensor.rummet_bemannat_wifi")
        self.min_period_min = float(self.args.get("min_period_min", 5))
        self.mqtt_topic = self.args.get(
            "mqtt_topic", "rummet/narvaro/period")
        self.heartbeat_entity = self.args.get(
            "heartbeat_entity", "sensor.period_logger_heartbeat")
        self.azure_event = self.args.get(
            "azure_event", "rummet_period")
        # Marginal som läggs på sista detektor-aktiviteten för att skatta
        # periodslut (se modul-docstring). 0 = rent detektor-span.
        self.detector_grace_s = float(self.args.get("detector_grace_s", 120))

        # State
        self.current_start = None
        self.current_detector = None
        self.periods_published = 0

        self.listen_state(self.on_state_change, self.watch_entity,
                          attribute="state")
        self.log(f"period_logger startad. Watching {self.watch_entity}, "
                 f"min_period={self.min_period_min} min, "
                 f"mqtt_topic={self.mqtt_topic}, azure_event={self.azure_event}")

        # Heartbeat var minut så vi vet att appen lever
        self.run_every(self._heartbeat, "now", 60)

    def _heartbeat(self, _):
        active = self.current_start is not None
        self.set_state(self.heartbeat_entity,
                       state=datetime.now().isoformat(),
                       attributes={
                           "active_period": active,
                           "current_start": self.current_start.isoformat() if active else None,
                           "periods_published": self.periods_published,
                       })

    def on_state_change(self, entity, attribute, old, new, kwargs):
        now = datetime.now().astimezone()

        if old == "off" and new == "on":
            self.current_start = now
            attrs = self.get_state(entity, attribute="all") or {}
            self.current_detector = attrs.get("attributes", {}).get("detector", "-")
            self.log(f"Period START kl {now.strftime('%H:%M:%S')} "
                     f"(via {self.current_detector})")
            return

        if old == "on" and new == "off":
            if self.current_start is None:
                self.log("WARN: period slut utan registrerad start", level="WARNING")
                return

            # Periodslut = senaste detektor-aktiva tick (detector_end_ts),
            # inte lampans släck-tid. Strippar latch + cooldown-svansen.
            attrs = (self.get_state(entity, attribute="all") or {}).get(
                "attributes", {})
            det_end_raw = attrs.get("detector_end_ts")
            slut = now
            if det_end_raw:
                try:
                    # detektor-span + marginal, dock aldrig efter lampans släck
                    slut = min(now, datetime.fromisoformat(det_end_raw)
                               + timedelta(seconds=self.detector_grace_s))
                except ValueError:
                    self.log(f"WARN: oläsbar detector_end_ts={det_end_raw!r} — "
                             f"faller tillbaka på lampans släck-tid",
                             level="WARNING")
            else:
                self.log("WARN: detector_end_ts saknas — faller tillbaka på "
                         "lampans släck-tid (latch-svans ej borttagen)",
                         level="WARNING")
            duration_min = max(
                0.0, (slut - self.current_start).total_seconds() / 60.0)

            if duration_min >= self.min_period_min:
                payload = {
                    "event_type": "narvaroperiod",
                    "start": self.current_start.isoformat(),
                    "slut": slut.isoformat(),
                    "varaktighet_min": round(duration_min, 1),
                    "via_detector": self.current_detector,
                    "source": "wifi_multivariate",
                }
                self.call_service("mqtt/publish",
                                  topic=self.mqtt_topic,
                                  payload=json.dumps(payload),
                                  retain=False)
                # Lämna över till azure_bridge för IoT Hub-forwarding
                self.fire_event(self.azure_event, **payload)
                self.periods_published += 1
                self.log(f"Period publicerad: {duration_min:.1f} min "
                         f"(via {self.current_detector}) — totalt {self.periods_published}")
            else:
                self.log(f"Period filtrerad bort: {duration_min:.1f} min < "
                         f"{self.min_period_min} min ({self.current_detector})")

            self.current_start = None
            self.current_detector = None
