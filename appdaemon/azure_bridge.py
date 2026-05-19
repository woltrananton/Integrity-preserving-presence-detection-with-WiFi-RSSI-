"""
AppDaemon-brygga: pushar närvaro-events till Azure IoT Hub.

Två strömmar genom samma Azure-klient (en device-identitet i IoT Hub):

1. occupancy_state — state-change på occupancy-entiteten (lampan).
   Payload: {"event_type": "occupancy_state", "ts", "occupied", "source"}

2. narvaroperiod — periodlogg från period_logger via HA event bus.
   Payload: {"event_type": "narvaroperiod", "start", "slut",
             "varaktighet_min", "via_detector", "source"}

Power BI / Stream Analytics filtrerar på event_type för att routea
strömmarna till olika dataset.

Att vi har en (1) Azure-klient i hela systemet är medvetet: IoT Hub
tillåter bara en aktiv anslutning per device-identitet, så två appar
som anslöt parallellt hamnade i en reconnect-storm (observerad 2026-05-12).

source är hardkodat till "wifi" sedan kameran kopplades bort från
lampstyrningen 2026-05-13. Tidigare lästes den från
input_select.narvarokalla, men den source-väljaren är borttagen.
Connection string läses från secrets.yaml (azure_iot_connection_string).

Kräver 'azure-iot-device' i AppDaemon-addonens python_packages-config.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import appdaemon.plugins.hass.hassapi as hass

logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("uamqp").setLevel(logging.WARNING)

SECRETS_PATH = Path(__file__).parent / "secrets.yaml"


class AzureBridge(hass.Hass):
    """Skickar närvaro-state-changes till Azure IoT Hub."""

    def initialize(self):
        self.occupancy_entity = self.args.get(
            "occupancy_entity", "binary_sensor.rummet_bemannat"
        )
        self.period_event = self.args.get(
            "period_event", "rummet_period"
        )

        conn_str = self._resolve_connection_string()
        if not conn_str:
            self.error(
                "FEL: azure_iot_connection_string saknas i secrets.yaml — "
                "Azure-bryggan startar inte."
            )
            return

        try:
            from azure.iot.device import IoTHubDeviceClient, Message
            self._Message = Message
            self._client = IoTHubDeviceClient.create_from_connection_string(conn_str)
            self._client.connect()
        except ImportError:
            self.error(
                "FEL: azure-iot-device saknas. Lägg till 'azure-iot-device' under "
                "python_packages i AppDaemon-addonens config och starta om."
            )
            return
        except Exception as e:
            self.error(f"FEL: kunde inte ansluta till IoT Hub: {e}")
            return

        self._sent = 0
        self._errors = 0

        self.listen_state(self._on_occupancy_change, self.occupancy_entity)
        self.listen_event(self._on_period_event, self.period_event)
        self.log(
            f"Azure-brygga startad — state-källa {self.occupancy_entity}, "
            f"period-event {self.period_event}"
        )

    def _send(self, payload_dict, log_label):
        """Skickar JSON-payload till IoT Hub och räknar resultatet."""
        try:
            msg = self._Message(json.dumps(payload_dict))
            msg.content_type = "application/json"
            msg.content_encoding = "utf-8"
            self._client.send_message(msg)
            self._sent += 1
            self.log(f"IoT Hub ← {log_label} (totalt skickat: {self._sent})")
        except Exception as e:
            self._errors += 1
            self.error(f"VARNING: kunde inte skicka till IoT Hub: {e}")

    def _on_occupancy_change(self, entity, attribute, old, new, kwargs):
        if new not in ("on", "off"):
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        occupied = new == "on"

        self._send(
            {
                "event_type": "occupancy_state",
                "ts": ts,
                "occupied": occupied,
                "source": "wifi",
            },
            f"occupancy_state occupied={occupied}",
        )

    def _on_period_event(self, event, data, kwargs):
        """Tar emot rummet_period från period_logger och forwardar till Azure."""
        # AppDaemon kan slå in payloaden olika beroende på version — normalisera
        if isinstance(data, dict) and "metadata" in data and "data" in data:
            data = data.get("data") or {}
        if not isinstance(data, dict):
            self.log(f"WARN: oväntad period-payload: {data!r}", level="WARNING")
            return

        payload = {
            "event_type": data.get("event_type", "narvaroperiod"),
            "start": data.get("start"),
            "slut": data.get("slut"),
            "varaktighet_min": data.get("varaktighet_min"),
            "via_detector": data.get("via_detector"),
            "source": data.get("source", "wifi_multivariate"),
        }
        self._send(
            payload,
            f"narvaroperiod {payload['varaktighet_min']} min via {payload['via_detector']}",
        )

    def _resolve_connection_string(self):
        direct = self.args.get("azure_iot_connection_string")
        if direct:
            return str(direct)
        if not SECRETS_PATH.exists():
            return ""
        try:
            import yaml
            with SECRETS_PATH.open(encoding="utf-8") as f:
                secrets = yaml.safe_load(f) or {}
            return str(secrets.get("azure_iot_connection_string", ""))
        except Exception as e:
            self.error(f"FEL: kunde inte läsa secrets.yaml: {e}")
            return ""

    def terminate(self):
        try:
            self._client.shutdown()
        except Exception:
            pass
