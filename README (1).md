# TheStatsAPI — Scanner HT Over 1.0 (xG + EV+)

Pipeline diario (GitHub Actions) que:

1. Consulta `GET /football/matches?date_from=hoy&date_to=hoy` (TheStatsAPI, Bearer token).
2. Por cada partido, agrega métricas del **primer tiempo** de la temporada actual
   (condición local/visitante) a partir de `/football/matches/{id}/stats`
   (`expected_goals.first_half`, `shots_on_target.first_half`) y goles HT
   (`score.half_time_*`): `xG_HT`, `xGA_HT`, tiros HT a favor/en contra y
   `% Over 1.0 HT`.
3. Obtiene la cuota del **mercado asiático Over 1.0** desde
   `/football/matches/{id}/odds` → `markets.total_goals["1.0"].over`.
4. Filtra con reglas de negocio: xG proyectado HT > 1.45 (ambas direcciones),
   frecuencia histórica > 65% en ambos equipos y edge ≥ 7% vs. la probabilidad
   implícita de la cuota.

Salidas: `ht_metrics_today.csv`, `ht_value_scan_resumen.csv`, `ht_value_alertas.csv`
(artifacts de cada ejecución + commit en `results/`).

## Configuración

1. **Secret**: en *Settings → Secrets and variables → Actions* crea
   `THESTATSAPI_KEY` con tu Bearer token.
2. **Horario**: edita el `cron` del workflow (hora UTC).
3. **Prueba sin credenciales**: *Actions → thestatsapi-daily-ht-scan → Run workflow*
   con `simulate = true`.

Documentación de la API: https://api.thestatsapi.com/llms.txt
