#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TheStatsAPI — Scanner diario de métricas HT + Alertas EV+ (Over 1.0 HT)
========================================================================
Pipeline completo, en un solo archivo, listo para GitHub Actions:

  1. GET /football/matches?date_from=hoy&date_to=hoy      -> partidos del día
  2. Por cada partido: historial HT de temporada actual (condición local/visit.)
     vía /football/matches?team_id=...&status=finished + /football/matches/{id}/stats
     (overview.expected_goals.first_half, shots.shots_on_target.first_half)
  3. Cuota Over 1.0 HT: GET /football/matches/{id}/odds -> total_goals["1.0"].over
  4. Filtros EV+ (xG proyectado > 1.45 ambas direcciones, freq > 65%, edge >= 7%)
  5. Salida: ht_metrics_today.csv, ht_value_alertas.csv, ht_value_scan_resumen.csv

Variables de entorno:
  THESTATSAPI_KEY     (requerida salvo simulación)
  THESTATSAPI_SIMULATE=true  -> omite la API y usa datos/cuotas simulados
                                (útil para probar el workflow sin credenciales)
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("thestatsapi_scan")

BASE_URL = "https://api.thestatsapi.com/api"
API_KEY = os.environ.get("THESTATSAPI_KEY", "")
SIMULATE = os.environ.get("THESTATSAPI_SIMULATE", "").lower() == "true"

# ---- umbrales de negocio ----
REQUEST_TIMEOUT = (5, 30)
MAX_RETRIES = 4
RETRY_BACKOFF = 1.5
RATE_LIMIT_SLEEP = 0.30
PER_PAGE = 100
PROYECCION_MIN_XG_HT = 1.45
FREQ_MIN_OVER_1_HT = 65.0
EDGE_MINIMO = 0.07
PESO_POISSON = 0.5
PESO_HISTORICO = 0.5
OVERROUND_CASA = 0.06


class TheStatsAPIError(Exception):
    pass


class TheStatsAPIHTTPError(TheStatsAPIError):
    def __init__(self, status_code: int, message: str, endpoint: str):
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(f"[{endpoint}] HTTP {status_code}: {message}")


# --------------------------------------------------------------------------- #
# Cliente HTTP con Bearer token, reintentos y paginación del envelope {data,meta}
# --------------------------------------------------------------------------- #
@dataclass
class TheStatsClient:
    api_key: str
    base_url: str = BASE_URL
    session: requests.Session = field(default_factory=requests.Session)

    def __post_init__(self) -> None:
        if not SIMULATE and not self.api_key:
            raise TheStatsAPIError("Define THESTATSAPI_KEY o THESTATSAPI_SIMULATE=true")
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}",
                                     "Accept": "application/json"})

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> dict:
        url = f"{self.base_url}{endpoint}"
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            except requests.exceptions.Timeout as exc:
                last_exc = exc
                log.warning("%s timeout (%d/%d)", endpoint, attempt, MAX_RETRIES)
            except requests.exceptions.ConnectionError as exc:
                last_exc = exc
                log.warning("%s conexión (%d/%d)", endpoint, attempt, MAX_RETRIES)
            else:
                if resp.status_code == 200:
                    time.sleep(RATE_LIMIT_SLEEP)
                    return resp.json()
                try:
                    msg = resp.json().get("error", {}).get("message", resp.text[:300])
                except ValueError:
                    msg = resp.text[:300]
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF ** attempt)
                    continue
                raise TheStatsAPIHTTPError(resp.status_code, msg, endpoint)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)
        raise TheStatsAPIError(f"{endpoint} falló tras {MAX_RETRIES} intentos: {last_exc}")

    def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        return self._request("GET", endpoint, params=params or {})

    def get_all_pages(self, endpoint: str, params: Optional[dict] = None) -> list:
        params = dict(params or {})
        params.setdefault("per_page", PER_PAGE)
        page, rows = 1, []
        while True:
            payload = self.get(endpoint, params={**params, "page": page})
            rows.extend(payload.get("data") or [])
            meta = payload.get("meta") or {}
            if page >= int(meta.get("total_pages", 1)):
                return rows
            page += 1


# --------------------------------------------------------------------------- #
# Caché y agregación de métricas HT por equipo (temporada actual, condición)
# --------------------------------------------------------------------------- #
class HTMetricsCache:
    def __init__(self, client: TheStatsClient):
        self.client = client
        self._matches_cache: dict = {}
        self._stats_cache: dict = {}
        self._season_cache: dict = {}

    def current_season_id(self, competition_id: str) -> Optional[str]:
        if competition_id in self._season_cache:
            return self._season_cache[competition_id]
        try:
            detail = self.client.get(f"/football/competitions/{competition_id}")
            season_id = (detail.get("data") or {}).get("current_season_id")
        except TheStatsAPIHTTPError as exc:
            season_id = None if exc.status_code == 404 else (_ for _ in ()).throw(exc)
        self._season_cache[competition_id] = season_id
        return season_id

    def team_finished_matches(self, team_id, competition_id, season_id, side) -> list:
        key = (team_id, season_id, side)
        if key in self._matches_cache:
            return self._matches_cache[key]
        matches = self.client.get_all_pages("/football/matches", {
            "team_id": team_id, "competition_id": competition_id,
            "season_id": season_id, "status": "finished"})
        side_key = "home_team" if side == "home" else "away_team"
        filtered = [m for m in matches if (m.get(side_key) or {}).get("id") == team_id]
        self._matches_cache[key] = filtered
        return filtered

    def match_stats(self, match_id: str) -> Optional[dict]:
        if match_id not in self._stats_cache:
            try:
                payload = self.client.get(f"/football/matches/{match_id}/stats")
                self._stats_cache[match_id] = payload.get("data")
            except TheStatsAPIHTTPError as exc:
                if exc.status_code != 404:
                    raise
                self._stats_cache[match_id] = None
        return self._stats_cache[match_id]

    @staticmethod
    def _fh(stats: dict, group: str, metric: str, side: str) -> Optional[float]:
        try:
            val = stats[group][metric]["first_half"][side]
        except (KeyError, TypeError):
            return None
        return float(val) if val is not None else None

    def ht_metrics(self, team_id, competition_id, season_id, side,
                   exclude_match_id=None) -> dict:
        matches = self.team_finished_matches(team_id, competition_id, season_id, side)
        xg_sum = xga_sum = sot_f_sum = sot_a_sum = 0.0
        n_xg = n_sot = over_1 = n_over = 0
        for m in matches:
            if m["id"] == exclude_match_id:
                continue
            stats = self.match_stats(m["id"])
            if not stats:
                continue
            opp = "away" if side == "home" else "home"
            xg, xga = self._fh(stats, "overview", "expected_goals", side), \
                      self._fh(stats, "overview", "expected_goals", opp)
            if xg is not None and xga is not None:
                xg_sum += xg; xga_sum += xga; n_xg += 1
            sf, sa = self._fh(stats, "shots", "shots_on_target", side), \
                     self._fh(stats, "shots", "shots_on_target", opp)
            if sf is not None and sa is not None:
                sot_f_sum += sf; sot_a_sum += sa; n_sot += 1
            gf = m.get("score", {}).get(f"half_time_{side}")
            ga = m.get("score", {}).get(f"half_time_{opp}")
            if gf is not None and ga is not None:
                n_over += 1
                if gf + ga > 1.0:
                    over_1 += 1
        return {
            "matches_sample": max(n_xg, n_sot, n_over),
            "xG_HT": xg_sum / n_xg if n_xg else None,
            "xGA_HT": xga_sum / n_xg if n_xg else None,
            "Shots_On_Target_HT_For": sot_f_sum / n_sot if n_sot else None,
            "Shots_On_Target_HT_Against": sot_a_sum / n_sot if n_sot else None,
            "Over_1.0_HT_Pct": (over_1 / n_over * 100.0) if n_over else None,
        }


# --------------------------------------------------------------------------- #
# Cuotas Over 1.0 HT (API real o simulación determinista)
# --------------------------------------------------------------------------- #
@dataclass
class OddsProvider:
    client: TheStatsClient
    bookmaker_pref: tuple = ("Bet365", "Pinnacle", "Betfair")

    def _fetch_from_api(self, match_id: str) -> Optional[float]:
        try:
            r = self.client.get(f"/football/matches/{match_id}/odds")
        except TheStatsAPIError as exc:
            log.warning("%s: odds no disponibles (%s)", match_id, exc)
            return None
        for bm in (r.get("data") or {}).get("bookmakers") or []:
            if bm.get("bookmaker") not in self.bookmaker_pref:
                continue
            line = (bm.get("markets", {}).get("total_goals", {}) or {}).get("1.0")
            over = (line or {}).get("over", {})
            cuota = over.get("last_seen") or over.get("opening")
            if cuota:
                return float(cuota)
        return None

    @staticmethod
    def _simulate(match_id: str, prob_modelo: float) -> float:
        seed = int(hashlib.sha256(match_id.encode()).hexdigest()[:8], 16)
        rng = np.random.default_rng(seed)
        return round((1.0 / max(prob_modelo, 1e-6))
                     * (1.0 + OVERROUND_CASA) * rng.uniform(0.94, 1.06), 2)

    def fetch_over_1_ht_odds(self, match_id: str, prob_modelo: float) -> tuple:
        if not SIMULATE and API_KEY:
            cuota = self._fetch_from_api(match_id)
            if cuota:
                return cuota, "api"
        return self._simulate(match_id, prob_modelo), "simulada"


# --------------------------------------------------------------------------- #
# Motor EV+
# --------------------------------------------------------------------------- #
class HTValueBetEngine:
    def __init__(self, odds: OddsProvider):
        self.odds = odds

    @staticmethod
    def _p_poisson(lam: float) -> float:
        lam = max(lam, 1e-6)
        return 1.0 - np.exp(-lam) * (1.0 + lam)

    def evaluar(self, df: pd.DataFrame) -> tuple:
        alertas, resumen = [], []
        for _, row in df.iterrows():
            mid = row["match_id"]
            ph = row["home_xG_HT"] + row["away_xGA_HT"]
            pa = row["away_xG_HT"] + row["home_xGA_HT"]
            lam = (ph + pa) / 2.0
            p_sis = PESO_POISSON * self._p_poisson(lam) \
                + PESO_HISTORICO * (row["home_Over_1.0_HT_Pct"]
                                    + row["away_Over_1.0_HT_Pct"]) / 200.0
            reg = {"match_id": mid,
                   "liga": row.get("competition_name") or row.get("competition_id"),
                   "local": row.get("home_name"), "visitante": row.get("away_name"),
                   "xg_proyectado_ht": round(lam, 2), "prob_sistema": round(p_sis, 4)}
            if not (ph > PROYECCION_MIN_XG_HT and pa > PROYECCION_MIN_XG_HT):
                reg.update(estado="DESCARTADO",
                           motivo=f"R1 xG HT {ph:.2f}/{pa:.2f} <= {PROYECCION_MIN_XG_HT}")
            elif not (row["home_Over_1.0_HT_Pct"] > FREQ_MIN_OVER_1_HT
                      and row["away_Over_1.0_HT_Pct"] > FREQ_MIN_OVER_1_HT):
                reg.update(estado="DESCARTADO",
                           motivo=f"R2 freq {row['home_Over_1.0_HT_Pct']:.1f}/"
                                  f"{row['away_Over_1.0_HT_Pct']:.1f}% <= {FREQ_MIN_OVER_1_HT}%")
            else:
                cuota, fuente = self.odds.fetch_over_1_ht_odds(mid, p_sis)
                edge = p_sis - 1.0 / cuota
                reg.update(cuota=cuota, fuente_cuota=fuente,
                           prob_implicita=round(1.0 / cuota, 4),
                           ventaja=round(edge * 100.0, 2))
                if edge >= EDGE_MINIMO:
                    reg["estado"] = "ALERTA_ALTO_VALOR"
                    alertas.append(reg)
                else:
                    reg.update(estado="SIN_VALOR",
                               motivo=f"R3 edge {edge*100:.2f}% < {EDGE_MINIMO*100:.0f}%")
            resumen.append(reg)
        return pd.DataFrame(alertas), pd.DataFrame(resumen)


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
def build_ht_dataframe(client, matches, exclude_self=True) -> pd.DataFrame:
    cache = HTMetricsCache(client)
    rows = []
    for m in matches:
        mid, comp = m["id"], m["competition_id"]
        season = m.get("season_id") or cache.current_season_id(comp)
        if not season:
            continue
        home, away = m.get("home_team") or {}, m.get("away_team") or {}
        exc = mid if exclude_self else None
        try:
            hm = cache.ht_metrics(home["id"], comp, season, "home", exc)
            am = cache.ht_metrics(away["id"], comp, season, "away", exc)
        except TheStatsAPIError as exc2:
            log.error("%s: %s", mid, exc2)
            continue
        row = {"match_id": mid, "date": m.get("utc_date"), "competition_id": comp,
               "season_id": season, "status": m.get("status"),
               "home_id": home.get("id"), "home_name": home.get("name"),
               "away_id": away.get("id"), "away_name": away.get("name")}
        row.update({f"home_{k}": v for k, v in hm.items()})
        row.update({f"away_{k}": v for k, v in am.items()})
        rows.append(row)
    df = pd.DataFrame(rows)
    if not df.empty:
        cols = [c for c in df.columns if c.startswith(("home_", "away_"))
                and not c.endswith(("id", "name"))]
        df[cols] = df[cols].astype("float64").round(3)
    return df


def run() -> int:
    client = TheStatsClient(api_key=API_KEY)
    hoy = datetime.now(timezone.utc).date().isoformat()
    log.info("=== Scan %s | simulate=%s ===", hoy, SIMULATE)

    if SIMULATE:
        matches = [{"id": f"mt_sim{i}", "competition_id": "comp_demo",
                    "season_id": "sn_demo", "status": "scheduled",
                    "home_team": {"id": f"tm_h{i}", "name": f"Local {i}"},
                    "away_team": {"id": f"tm_a{i}", "name": f"Visitante {i}"}}
                   for i in range(1, 5)]
        rng = np.random.default_rng(42)
        df_ht = pd.DataFrame([{
            "match_id": m["id"], "competition_id": m["competition_id"],
            "competition_name": "Demo League",
            "home_name": m["home_team"]["name"], "away_name": m["away_team"]["name"],
            "home_xG_HT": round(float(rng.uniform(0.5, 1.5)), 3),
            "home_xGA_HT": round(float(rng.uniform(0.3, 1.0)), 3),
            "away_xG_HT": round(float(rng.uniform(0.5, 1.5)), 3),
            "away_xGA_HT": round(float(rng.uniform(0.3, 1.0)), 3),
            "home_Over_1.0_HT_Pct": round(float(rng.uniform(55, 85)), 1),
            "away_Over_1.0_HT_Pct": round(float(rng.uniform(55, 85)), 1),
        } for m in matches])
    else:
        matches = client.get_all_pages("/football/matches",
                                       {"date_from": hoy, "date_to": hoy})
        log.info("%d partidos hoy", len(matches))
        df_ht = build_ht_dataframe(client, matches)

    df_ht.to_csv("ht_metrics_today.csv", index=False)
    alertas, resumen = HTValueBetEngine(OddsProvider(client)).evaluar(df_ht)
    resumen.to_csv("ht_value_scan_resumen.csv", index=False)
    alertas.to_csv("ht_value_alertas.csv", index=False)

    if alertas.empty:
        print(f"[{hoy}] Ningún partido supera los filtros de valor.")
    else:
        for _, r in alertas.iterrows():
            print(f"[{r['match_id']}] {r['liga']} | {r['local']} vs {r['visitante']} | "
                  f"xG Proyectado HT: {r['xg_proyectado_ht']:.2f} | "
                  f"Cuota: {r['cuota']:.2f} | Ventaja: {r['ventaja']:.1f}%")
    log.info("Scan completo: %d partidos | %d alertas EV+", len(df_ht), len(alertas))
    return 0


if __name__ == "__main__":
    sys.exit(run())
