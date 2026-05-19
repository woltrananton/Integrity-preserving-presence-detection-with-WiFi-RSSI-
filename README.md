# IoT-närvarodetektering med WiFi RSSI

Projekt i kursen *IoT och molntjänster* (A331TG), VT2026 — Grupp 12.

Systemet detekterar om en labbsal är **tom eller bemannad** genom att
analysera hur människokroppar dämpar WiFi-signalen mellan en router och ett
antal anslutna IoT-enheter. Ingen kamera, inga bärbara taggar, ingen rådata
som lämnar det lokala nätet — närvaron härleds ur RSSI-fluktuationer som
redan finns i nätverket.

Det här är en **deploy-klar delmängd** av projektet: bara de filer som
faktiskt körs i drift, med rena namn och redo att installeras på samma
uppsättning som vi använde.

---

## Vad systemet gör

1. En **router** ses som mätstation. Var 10:e sekund läses RSSI-värdet för
   varje ansluten "ankarenhet" (smarta pluggar, en LED-slinga, en temp-sensor).
2. När en person rör sig i rummet skuggar och reflekterar kroppen
   signalen → RSSI-värdena fluktuerar.
3. En **Isolation Forest** (tränad enbart på säkert tomma helger) plus tre
   regelbaserade detektorer avgör om mönstret avviker från ett tomt rum.
4. Resultatet publiceras som `binary_sensor.rummet_bemannat_wifi` i Home
   Assistant, som tänder/släcker belysningen och loggar närvaroperioder
   vidare till Azure IoT Hub för analys i Power BI.

Modellen ger ett **binärt** svar (tom/bemannad) — den räknar inte personer
och positionerar dem inte. Det är ett medvetet designval: datan stödjer inte
mer, och det passar projektets *privacy by design*-narrativ.

---

## Arkitektur

```
  Asus-router
      │  SSH-poll var 10:e sekund (MAC-tabell → RSSI)
      ▼
  rssi_logger ........ AppDaemon-app          skriver CSV
      │                                       till /share/data/
      ▼
  inference_app ...... AppDaemon-app
      │  tail:ar CSV, bygger 8-dim delta-features
      ▼
  ml_server .......... Portainer-container, FastAPI :8765
      │  Isolation Forest → anomaly-score
      ▼
  inference_app ...... detektorer A/B/C/D + latch + watchdog
      │
      ▼
  binary_sensor.rummet_bemannat_wifi   (Home Assistant)
      │                         │
      ▼                         ▼
  automations.yaml          period_logger ..... AppDaemon-app
  (tänd/släck LED)          (närvaroperioder ≥ 5 min)
      │                         │
      └────────▶ azure_bridge ◀──┘   AppDaemon-app, ensam Azure-klient
                     │
                     ▼
            Azure IoT Hub ──▶ Storage ──▶ Power BI
```

Varför ML-inferensen ligger i en **egen container** och inte i AppDaemon:
scikit-learn går inte att installera i Home Assistants AppDaemon-add-on på
Raspberry Pi (Alpine/musl, inga `cp312-aarch64`-wheels). Containern bygger
på Debian/glibc där sklearn-wheels finns. AppDaemon-apparna klarar sig med
standard-Python och anropar containern över HTTP.

---

## Hårdvara

| Roll | Enhet i vår uppsättning |
|------|--------------------------|
| Mätstation | Asus RT-AC52U_B1 (MediaTek-chipset, SSH påslaget) |
| Server | Raspberry Pi 4 med Home Assistant OS |
| Ankare ×6 | Shelly Plus Plug S ×2, Shelly H&T, Cleverio LED, Cleverio Mini + Prokord (Tuya) |
| Belysning | Cleverio RGB LED-strip |
| Validering | Reolink RLC-410W (motion-only — loggas som ground truth, styr inget) |

Andra WiFi-enheter fungerar lika bra som ankare så länge de syns i routerns
MAC-tabell. Routern behöver SSH aktiverat (`Administration → System →
Enable SSH = LAN only`).

---

## Mappstruktur

```
deploy/
├── homeassistant/        # Läggs i /config/ på Home Assistant
│   ├── configuration.yaml
│   ├── automations.yaml
│   └── dashboard.yaml
├── appdaemon/            # Läggs i /config/appdaemon/apps/
│   ├── apps.yaml
│   ├── rssi_logger.py
│   ├── inference_app.py
│   ├── period_logger.py
│   ├── azure_bridge.py
│   └── secrets.yaml.example
└── ml_container/         # Läggs i /share/ml_inference/ på Pi:n
    ├── ml_server.py
    ├── Dockerfile
    ├── docker-compose.yml
    ├── requirements.txt
    └── models/
        ├── if_model.joblib
        └── if_model_metadata.yaml
```

---

## Installation

Förutsättningar: Home Assistant OS på en Raspberry Pi, samt add-onen
**AppDaemon**, **Samba share** (för att nå `/share/`) och en
Docker-/container-miljö (vi använde **Portainer**).

### 1. Home Assistant

Filerna i `homeassistant/` motsvarar `/config/` på Pi:n.

- `configuration.yaml` är efter modellpivoten i praktiken standard-HA —
  närvarosensorn skapas av AppDaemon, inte via YAML. Den finns med för
  fullständighet; lägg bara till dess rader om din egen `configuration.yaml`
  saknar dem.
- Kopiera in `automations.yaml` (tänder LED vid närvaro, släcker efter
  5 min frånvaro).
- Klistra in `dashboard.yaml` via *Inställningar → Dashboards → ⋮ → Raw
  configuration editor*.

Starta om Home Assistant.

### 2. AppDaemon

Kopiera **alla** filer i `appdaemon/` till `/config/appdaemon/apps/`.
AppDaemon laddar om automatiskt när filerna ändras.

Lägg till Python-beroenden i AppDaemon-add-onens konfiguration under
`python_packages`:

```yaml
python_packages:
  - paramiko          # SSH mot routern (rssi_logger)
  - azure-iot-device  # Azure IoT Hub (azure_bridge)
```

Öppna `apps.yaml` och anpassa:

- `router.host` — routerns IP (standard `192.168.1.1`).
- `anchors` — byt **platshållarvärdena** (`AA:BB:CC:00:00:0x` /
  `192.168.1.10x`) mot dina enheters riktiga MAC och IP. Hitta dem i
  routerns *Network Map* eller via SSH: `iwpriv ra0 get_mac_table`.
- `ml_url` — Raspberry Pi:ns IP plus port 8765 (standard
  `http://192.168.1.88:8765`).
- `motion_entity` / `occupancy_entity` / `light.*` — justera till dina
  egna entity-ID:n.

### 3. ML-container (Portainer)

Kopiera innehållet i `ml_container/` till `/share/ml_inference/` på Pi:n
(`ml_server.py` och mappen `models/` måste ligga där).

Deploya stacken i Portainer: *Stacks → Add stack → Web editor*, klistra in
`docker-compose.yml` och tryck **Deploy**. Containern installerar sina
beroenden vid första start och lyssnar sedan på port **8765**.

Alternativt kan man bygga en image från `Dockerfile` i stället.
`requirements.txt` listar samma pinnade versioner för lokal testkörning.

Kontroll: `curl http://<pi-ip>:8765/docs` ska svara med FastAPI-sidan.

### 4. secrets.yaml

`secrets.yaml` är medvetet **inte** med i repot (innehåller lösenord).
Kopiera mallen och fyll i dina värden:

```bash
cd appdaemon
cp secrets.yaml.example secrets.yaml
```

`secrets.yaml` ska ligga i `/config/appdaemon/apps/` bredvid `apps.yaml` —
men aldrig checkas in i git.

---

## Verifiering att allt går

- `sensor.rssi_logger_heartbeat` i HA ska räknas upp (`samples` ökar).
- ML-containern svarar på `http://<pi-ip>:8765`.
- `binary_sensor.rummet_bemannat_wifi` växlar `on`/`off` när någon rör sig
  i rummet, och LED-lampan följer med.
- `sensor.period_logger_heartbeat` uppdateras; närvaroperioder ≥ 5 min
  skickas vidare till Azure.

Dashboardet (`dashboard.yaml`) visar alla dessa i ett svep.

---

## Kända begränsningar

- **Stillasittande personer kan missas.** En orörlig kropp i en RF-skugga
  ger ingen mätbar fluktuation. Det är sensorfysik, inte en mjukvarubugg.
- **Latens.** Vid 10 s-sampling tar en ankomst någon minut att bekräfta —
  bra nog för retrospektiv rumsutnyttjandestatistik, för långsamt för
  ögonblicklig lampstyrning.
- Modellen i `models/` är tränad på vår lokal. Logga några säkert tomma
  dygn i din miljö och träna om för bästa resultat (se `if_model_metadata.yaml`).

---

## Integritet (privacy by design)

All rådata — RSSI-värden och kamerans motion-events — stannar på det lokala
nätet. Endast aggregerade binära närvaroperioder skickas till molnet.
Kameran används enbart som facit under valideringen och styr ingenting.
Designen följer GDPR Art. 5 (dataminimering, ändamålsbegränsning).


## Authors

- Anton Wolter ([@woltrananton](https://github.com/woltrananton))
- Elliot Persson ([@PerssonElliot05](https://github.com/PerssonElliot05))

## License

Released under the [MIT License](LICENSE).
