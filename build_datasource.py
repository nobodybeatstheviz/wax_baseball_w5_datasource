"""build_datasource.py -- W5: the Keeping Score marts on Tableau Cloud, headless.

Three layers, never fused:
  Source        BigQuery `augmented-world-262319.wax_baseball_dbt` (the dbt marts)
  Analysis      this script -- the generator
  Presentation  build/* + the published data sources on
                10ax.online.tableau.com/#/site/nobodybeatstheviz -- disposable,
                rerun to regenerate, never patch in the GUI.

    py build_datasource.py --list                    # what would be built
    py build_datasource.py --build                   # BigQuery -> build/*.hyper (five single-table sources)
    py build_datasource.py --build --publish         # ...and publish (Overwrite)
    py build_datasource.py --verify                  # golden battery through VizQL Data Service
    py build_datasource.py --model [--publish]       # the relationship model (.hyper + generated .tds -> .tdsx)
    py build_datasource.py --verify-model            # binding + relationships + battery + the HOF arc

Auth: Tableau PAT in .env (gitignored; .env.example is the shape). BigQuery via
Application Default Credentials, same as wax_baseball_parity/scripts/export_bigquery.py.

Shape 1 (SINGLE): one single-table extract per mart the golden battery touches.
Each golden question hits exactly one grain -- the join-graph lesson the D360
SDM taught: metrics on unrelated fact tables do not combine, so they do not
share a data source here either.

Shape 2 (MODEL): one multi-table .hyper plus a generated .tds carrying the
object model (Tableau's relationships), zipped as a .tdsx. The graph mirrors
sdm/relationships.json in wax_baseball_datacloud_deploy, including its single
batter-only People->Plays edge, so the 281 -> 35 -> 44 Hall of Fame arc replays
against Tableau's relationship engine. Two deliberate differences: the SDM
leaves HOF_Sightings unrelated, but a Tableau object model must be one
connected graph, so HOF Sightings hangs off People; and People also carries
hof_category as a denormalized attribute, because that is the shape Tableau's
engine wants for a cross-fact measure (see the HOF arc below).

Everything below was measured 2026-09-01 against Tableau Cloud (REST 3.30):

  Why the .tds route
  - Hyper has only ASSUMED constraints (plain FKs: "Index support is disabled";
    named constraints unimplemented); an FK must reference the target's
    ASSUMED PRIMARY KEY, not merely a unique column.
  - Tableau Cloud infers relationships from those keys at publish ONLY for a
    star -- "it must contain exactly one fact table". Our graph has five leaf
    facts on two hubs, so the object model is written out explicitly. The XML
    grammar in model_tds() is what Tableau itself wrote for a published star,
    read back through the REST download.

  The collection-order binding (the two-pass publish in cmd_model)
  - The legacy <relation type='collection'> block must be present (without it
    every query is a 500) but Tableau binds the object at document position i
    to collection position sigma(i), where sigma is a fixed permutation of the
    object set that does not depend on collection order. So: publish once,
    read which physical table each object actually holds (VDS read-metadata +
    the <cols> map), invert, republish with the collection in sigma order.
    Object ids are deterministic (uuid5) so sigma cannot drift between passes.

  Relationship semantics through VDS (the HOF arc, H2a-H2d)
  - A row-level calculation spanning two sibling tables is refused: "Column
    can have only one upstream base table". D360's SDM accepted the same
    expression and silently fanned out (281); Tableau refuses instead.
  - A filter from one sibling table narrows a hub measure (plays.event_code=23
    -> 217 batters), but a filter that keeps every member of its field is a
    no-op that induces no join, and filters from two different siblings -- or
    a sibling filter once the query already spans a second table -- are
    dropped silently (23373 = all of People).
  - The model still holds the answer at grain level: people x hof.category x
    COUNT(plays), reduced client-side, gives 35 -- the same client-side
    transform pattern the parity harness uses for D360's missing ORDER/filter.
  - The engine-native one-call answer needs HOF-ness as an attribute of the hub:
    COUNTD(IIF([hof_category]='Player' AND [was_attended], [player_id], NULL))
    across the direct People->Plays edge returns 35.

--verify / --verify-model query through VizQL Data Service (VDS), the API the
Tableau MCP's query-datasource tool wraps: green here is green for the MCP.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
import uuid
import zipfile
from datetime import datetime
from xml.sax.saxutils import quoteattr

import requests
import tableauserverclient as TSC
from google.cloud import bigquery
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    NULLABLE,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)

HERE = pathlib.Path(__file__).resolve().parent
BUILD = HERE / "build"
PROJECT = "augmented-world-262319"
DATASET = "wax_baseball_dbt"
TABLEAU_PROJECT = "default"
MODEL_NAME = "Keeping Score - Model"
MODEL_STEM = "keeping_score_model"
SCHEMA = "Extract"
ID_NAMESPACE = uuid.UUID("6b1f0b1e-5a0e-4c8b-9d2a-3f3a2a6d5c11")  # any fixed namespace; ids must only be stable

# Shape 1: data source name -> mart. Names are the contract the MCP will see.
SINGLE: dict[str, str] = {
    "Keeping Score - Attended Games": "fct_attended_games",
    "Keeping Score - Plays": "fct_plays",
    "Keeping Score - Game Attendees": "fct_game_attendee",
    "Keeping Score - Team Games": "fct_attended_team_games",
    "Keeping Score - HOF Sightings": "fct_hof_sightings",
}

# Shape 2: the model. Order matters twice -- a referenced table must exist
# before its FK, and the first table to carry a column name keeps it plain in
# Tableau's field naming (later duplicates become "col (table)").
# (table, select SQL, primary key or None, [(fk column, referenced table, referenced column)])
_M = f"`{PROJECT}.{DATASET}`"
MODEL: list[tuple[str, str, str | None, list[tuple[str, str, str]]]] = [
    ("fct_attended_games", f"SELECT * FROM {_M}.fct_attended_games", "game_id", []),
    ("stg_lahman_people",
     f"SELECT pe.*, h.hof_category, h.hof_induction_year FROM {_M}.stg_lahman_people pe "
     f"LEFT JOIN (SELECT player_id, MAX(category) AS hof_category, MIN(induction_year) AS hof_induction_year "
     f"FROM {_M}.stg_lahman_hall_of_fame GROUP BY player_id) h ON h.player_id = pe.player_id",
     "player_id", []),
    ("fct_plays",
     f"SELECT p.*, pe.player_id AS batter_player_id FROM {_M}.fct_plays p "
     f"LEFT JOIN {_M}.stg_lahman_people pe ON pe.retro_id = p.batter_id",
     None, [("game_id", "fct_attended_games", "game_id"), ("batter_player_id", "stg_lahman_people", "player_id")]),
    ("fct_game_attendee",
     f"SELECT a.*, g.game_id FROM {_M}.fct_game_attendee a "
     f"LEFT JOIN {_M}.fct_attended_games g ON g.wax_game_id = a.wax_game_id",
     None, [("game_id", "fct_attended_games", "game_id")]),
    ("fct_attended_team_games", f"SELECT * FROM {_M}.fct_attended_team_games", None,
     [("game_id", "fct_attended_games", "game_id")]),
    ("stg_lahman_hall_of_fame", f"SELECT * FROM {_M}.stg_lahman_hall_of_fame", None,
     [("player_id", "stg_lahman_people", "player_id")]),
    ("fct_hof_sightings", f"SELECT * FROM {_M}.fct_hof_sightings", None,
     [("player_id", "stg_lahman_people", "player_id")]),
]

# BigQuery -> Hyper types (API objects for TableDefinition, SQL names for CREATE TABLE),
# and -> the .tds metadata-record triple (remote-type code, local-type, default aggregation)
# as Tableau wrote them for the star experiment.
HYPER_TYPES = {
    "STRING": SqlType.text, "INTEGER": SqlType.big_int, "INT64": SqlType.big_int,
    "FLOAT": SqlType.double, "FLOAT64": SqlType.double, "NUMERIC": lambda: SqlType.numeric(38, 9),
    "BOOLEAN": SqlType.bool, "BOOL": SqlType.bool, "DATE": SqlType.date,
    "TIMESTAMP": SqlType.timestamp_tz, "DATETIME": SqlType.timestamp,
}
HYPER_SQL = {
    "STRING": "TEXT", "INTEGER": "BIGINT", "INT64": "BIGINT", "FLOAT": "DOUBLE PRECISION",
    "FLOAT64": "DOUBLE PRECISION", "NUMERIC": "NUMERIC(38,9)", "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMPTZ", "DATETIME": "TIMESTAMP",
}
TDS_META = {
    "STRING": (129, "string", "Count"), "INTEGER": (20, "integer", "Sum"), "INT64": (20, "integer", "Sum"),
    "FLOAT": (5, "real", "Sum"), "FLOAT64": (5, "real", "Sum"), "NUMERIC": (5, "real", "Sum"),
    "BOOLEAN": (11, "boolean", "Count"), "BOOL": (11, "boolean", "Count"), "DATE": (133, "date", "Year"),
    "TIMESTAMP": (135, "datetime", "Year"), "DATETIME": (135, "datetime", "Year"),
}

# ---------------------------------------------------------------------------
# The golden battery, phrased for VDS. Same reference answers as
# wax_baseball_parity/scripts/parity_harness.py (recorded 2026-08-31 via MetricFlow).
# Each entry: label, data source, VDS query, checker(rows) -> (ok, detail),
# optionally an error checker(exc) -> (ok, detail) when a refusal IS the finding.
# `f(table, column)` renders a field caption: identity for single-table sources,
# the published <cols> map for the model (duplicate names get suffixed).
# ---------------------------------------------------------------------------

REF_G1 = {
    1984: 1, 1985: 1, 1986: 2, 1987: 3, 1988: 2, 1989: 1, 1990: 1, 1991: 1,
    1992: 3, 1993: 2, 1997: 5, 1998: 6, 1999: 8, 2000: 9, 2001: 17, 2002: 8,
    2003: 17, 2004: 6, 2005: 8, 2006: 9, 2007: 10, 2008: 12, 2009: 8,
    2010: 5, 2011: 11, 2012: 3, 2013: 3, 2014: 1, 2017: 1, 2018: 1,
    2022: 3, 2023: 1, 2024: 5, 2025: 4,
}
REF_G3 = [("Melissa", 57), ("Bergan", 27), ("Al", 26), ("solo", 16), ("Poppa", 14)]


def _calc(alias: str, formula: str) -> dict:
    return {"fieldCaption": alias, "calculation": formula}


def _set_filter(caption: str, *values) -> dict:
    return {"field": {"fieldCaption": caption}, "filterType": "SET", "values": list(values)}


def _check_scalar(expected):
    def check(rows):
        got = list(rows[0].values())[0] if rows else None
        return (got is not None and int(got) == expected), f"got {got}, want {expected}"
    return check


def _check_pair(a, b):
    def check(rows):
        r = rows[0] if rows else {}
        got = (r.get("games"), r.get("stadiums"))
        return (got == (a, b)), f"got {got}, want {(a, b)}"
    return check


def _check_years(rows):
    got = {int(r["yr"]): int(r["n"]) for r in rows}
    bad = {y: (got.get(y), REF_G1.get(y)) for y in sorted(set(got) | set(REF_G1)) if got.get(y) != REF_G1.get(y)}
    return (got == REF_G1), f"{sum(got.values())} games over {len(got)} years; mismatches: {bad}"


def _check_top5(rows):
    got = sorted(((r["k"], int(r["n"])) for r in rows), key=lambda t: (-t[1], t[0]))[:5]
    return (got == REF_G3), f"got {got}"


def _check_win_rate(rows):
    r = rows[0] if rows else {}
    got = (r.get("wins"), r.get("decided"))
    return (got == (90, 143)), f"got {got}, want (90, 143)"


def _record(note: str):
    """No assertion -- print what the engine says, with the reason it is only recorded."""
    def check(rows):
        got = list(rows[0].values())[0] if rows else None
        return True, f"engine says {got} -- {note}"
    return check


def _expect_refusal(substring: str):
    """The engine refusing is the finding: PASS when the error carries `substring`."""
    def check_error(exc: Exception):
        msg = str(exc)
        m = re.search(r'"message":"([^"]*)"', msg)
        return (substring in msg), f"refused: {m.group(1)[:120] if m else msg[:120]}"
    return check_error


def _battery(f) -> list[tuple]:
    return [
        ("G1 games attended by year", "Keeping Score - Attended Games",
         {"fields": [_calc("yr", f"YEAR([{f('fct_attended_games','game_date')}])"),
                     _calc("n", f"COUNT([{f('fct_attended_games','game_id')}])")]}, _check_years),
        ("G2 home runs witnessed", "Keeping Score - Plays",
         {"fields": [_calc("hr", f"SUM(IIF([{f('fct_plays','event_code')}] = 23, 1, 0))")]}, _check_scalar(400)),
        ("G3 top-5 attendees by games", "Keeping Score - Game Attendees",
         {"fields": [_calc("k", f"[{f('fct_game_attendee','attendee_name')}]"),
                     _calc("n", f"COUNT([{f('fct_game_attendee','game_attendee_key')}])")]}, _check_top5),
        ("G4 attended win rate (NYA spot-check)", "Keeping Score - Team Games",
         {"fields": [_calc("wins", f"SUM(IIF([{f('fct_attended_team_games','team_won')}], 1, 0))"),
                     _calc("decided", f"SUM(IIF([{f('fct_attended_team_games','is_decided')}], 1, 0))")],
          "filters": [_set_filter(f("fct_attended_team_games", "team_id"), "NYA")]},
         _check_win_rate),
        ("G5 Hall of Famers seen", "Keeping Score - HOF Sightings",
         {"fields": [_calc("hof", f"COUNTD([{f('fct_hof_sightings','player_id')}])")]}, _check_scalar(44)),
        ("G0a games attended / unique stadiums", "Keeping Score - Attended Games",
         {"fields": [_calc("games", f"COUNT([{f('fct_attended_games','game_id')}])"),
                     _calc("stadiums", f"COUNTD([{f('fct_attended_games','venue_wax')}])")]}, _check_pair(178, 22)),
        ("G0b runs witnessed", "Keeping Score - Plays",
         {"fields": [_calc("runs", f"SUM([{f('fct_plays','runs_on_play')}])")]}, _check_scalar(1706)),
    ]


def _hof_arc(f) -> list[tuple]:
    """The D360 lesson replayed: a measure joins only the objects its fields reference."""
    hof_pid, cat = f("stg_lahman_hall_of_fame", "player_id"), f("stg_lahman_hall_of_fame", "category")
    people_pid, hof_attr = f("stg_lahman_people", "player_id"), f("stg_lahman_people", "hof_category")
    attended, event, play_key = f("fct_plays", "was_attended"), f("fct_plays", "event_code"), f("fct_plays", "play_key")

    def reduce_grain(rows):
        got = sum(1 for r in rows if r.get(cat) == "Player" and (r.get("n") or 0) > 0)
        return got == 35, f"{len(rows)} grain rows -> {got} inducted Players with attended plays, want 35"

    return [
        ("H1 inducted players, no path forced", MODEL_NAME,
         {"fields": [_calc("n", f"COUNTD(IIF([{cat}] = 'Player', [{hof_pid}], NULL))")]},
         _record("D360 said 281 too: nothing forced the Plays path")),
        ("H2a D360's row-level IIF across sibling tables (HOF, Plays, People)", MODEL_NAME,
         {"fields": [_calc("n", f"COUNTD(IIF([{cat}] = 'Player' AND [{attended}], [{people_pid}], NULL))")]},
         _check_scalar(35), _expect_refusal("only one upstream base table")),
        ("H2b sibling filters HOF=Player + Plays.event_code=23 on COUNTD(People)", MODEL_NAME,
         {"fields": [_calc("n", f"COUNTD([{people_pid}])")], "filters": [_set_filter(cat, "Player"), _set_filter(event, 23)]},
         _record("filters from two different siblings are dropped; 23373 = all of People")),
        ("H2c grain query People x HOF.category x COUNT(Plays), reduced client-side", MODEL_NAME,
         {"fields": [{"fieldCaption": people_pid}, {"fieldCaption": cat}, _calc("n", f"COUNT([{play_key}])")]},
         reduce_grain),
        ("H2d Tableau's idiom: HOF as a People attribute, calc across the direct edge", MODEL_NAME,
         {"fields": [_calc("n", f"COUNTD(IIF([{hof_attr}] = 'Player' AND [{attended}], [{people_pid}], NULL))")]},
         _check_scalar(35)),
        ("H3 contract HOF seen (HOF Sightings mart)", MODEL_NAME,
         {"fields": [_calc("n", f"COUNTD([{f('fct_hof_sightings','player_id')}])")]}, _check_scalar(44)),
        ("H4 HR + HOF seen in ONE query (D360: NO_PATH_ERROR)", MODEL_NAME,
         {"fields": [_calc("hr", f"SUM(IIF([{event}] = 23, 1, 0))"),
                     _calc("hof", f"COUNTD([{f('fct_hof_sightings','player_id')}])")]},
         lambda rows: ((rows[0].get("hr"), rows[0].get("hof")) == (400, 44) if rows else False, f"got {rows[:1]}")),
    ]


# ---------------------------------------------------------------------------
# Plumbing
# ---------------------------------------------------------------------------

def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (HERE / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    missing = [k for k in ("TABLEAU_SERVER", "TABLEAU_SITE", "TABLEAU_PAT_NAME", "TABLEAU_PAT_SECRET") if k not in env]
    if missing:
        sys.exit(f".env is missing {missing} -- see .env.example")
    return env


def tableau_server(env: dict[str, str]) -> tuple[TSC.Server, TSC.PersonalAccessTokenAuth]:
    auth = TSC.PersonalAccessTokenAuth(env["TABLEAU_PAT_NAME"], env["TABLEAU_PAT_SECRET"], site_id=env["TABLEAU_SITE"])
    server = TSC.Server(env["TABLEAU_SERVER"], use_server_version=True)
    return server, auth


def _hyper_type(field_type: str):
    return HYPER_TYPES.get(field_type.upper(), SqlType.text)()


def _hyper_sql(field_type: str) -> str:
    return HYPER_SQL.get(field_type.upper(), "TEXT")


def _coerce(value, field_type: str):
    if value is None:
        return None
    if field_type.upper() == "DATE" and isinstance(value, datetime):
        return value.date()
    return value


def _insert(conn: Connection, table: TableName, result) -> int:
    schema = list(result.schema)
    n = 0
    with Inserter(conn, table) as ins:
        for row in result:
            ins.add_row([_coerce(row[f.name], f.field_type) for f in schema])
            n += 1
        ins.execute()
    return n


def build_hyper(client: bigquery.Client, mart: str, path: pathlib.Path, hyper: HyperProcess) -> int:
    """Shape 1: one mart as a single-table extract ([Extract].[Extract])."""
    result = client.query(f"SELECT * FROM `{PROJECT}.{DATASET}.{mart}`").result()
    table = TableDefinition(
        table_name=TableName(SCHEMA, "Extract"),
        columns=[TableDefinition.Column(f.name, _hyper_type(f.field_type), NULLABLE) for f in result.schema],
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with Connection(endpoint=hyper.endpoint, database=str(path), create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
        conn.catalog.create_schema_if_not_exists(table.table_name.schema_name)
        conn.catalog.create_table(table)
        return _insert(conn, table.table_name, result)


Built = list[tuple[str, list[tuple[str, str]], int]]  # (table, [(column, bigquery type)], rows)


def build_model_hyper(client: bigquery.Client, path: pathlib.Path, hyper: HyperProcess) -> Built:
    """Shape 2: every MODEL table in one .hyper (ASSUMED keys kept -- they document
    intent and would drive Tableau's inference on a star)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    out: Built = []
    with Connection(endpoint=hyper.endpoint, database=str(path), create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
        conn.execute_command(f'CREATE SCHEMA "{SCHEMA}"')
        for table, sql, pk, fks in MODEL:
            result = client.query(sql).result()
            cols = [f'"{f.name}" {_hyper_sql(f.field_type)}' for f in result.schema]
            if pk:
                cols.append(f'ASSUMED PRIMARY KEY ("{pk}")')
            for col, ref_table, ref_col in fks:
                cols.append(f'ASSUMED FOREIGN KEY ("{col}") REFERENCES "{SCHEMA}"."{ref_table}"("{ref_col}")')
            conn.execute_command(f'CREATE TABLE "{SCHEMA}"."{table}" ({", ".join(cols)})')
            out.append((table, [(f.name, f.field_type) for f in result.schema], _insert(conn, TableName(SCHEMA, table), result)))
    return out


def local_names(tables: Built) -> dict[tuple[str, str], str]:
    """Tableau's field-naming rule: first table to carry a column keeps it plain,
    later duplicates become 'col (table)'."""
    local: dict[tuple[str, str], str] = {}
    seen: set[str] = set()
    for table, cols, _ in tables:
        for col, _ in cols:
            local[(table, col)] = col if col not in seen else f"{col} ({table})"
            seen.add(col)
    return local


def object_id(table: str) -> str:
    return f"{table}_{uuid.uuid5(ID_NAMESPACE, f'{MODEL_NAME}/{table}').hex.upper()}"


def model_tds(tables: Built, hyper_rel_path: str, collection: list[str]) -> str:
    """The object model as Tableau writes it (grammar read back from a published
    star via REST download): a federated connection over one hyper, the legacy
    relation collection in `collection` order (see the module docstring on why
    that order is chosen empirically), a <cols> map giving every column a unique
    local name, metadata-records binding each column to its logical object, one
    datatype='table' column per object, and an object-graph whose relationships
    equate local names."""
    conn_name = MODEL_NAME
    local = local_names(tables)
    obj_id = {t: object_id(t) for t, _, _ in tables}

    def rel(table: str) -> str:
        return (f"<relation connection={quoteattr(conn_name)} name={quoteattr(table)} "
                f"table={quoteattr(f'[{SCHEMA}].[{table}]')} type='table' />")

    x = ["<?xml version='1.0' encoding='utf-8' ?>",
         f"<datasource formatted-name={quoteattr(MODEL_NAME)} inline='true' version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>",
         "  <connection class='federated'>",
         "    <named-connections>",
         f"      <named-connection name={quoteattr(conn_name)}>",
         f"        <connection class='hyper' dbname={quoteattr(hyper_rel_path)} extract-engine='true' schema={quoteattr(SCHEMA)} tablename='Extract' />",
         "      </named-connection>",
         "    </named-connections>",
         "    <relation type='collection'>"]
    x += [f"      {rel(t)}" for t in collection]
    x += ["    </relation>", "    <cols>"]
    x += [f"      <map key={quoteattr(f'[{name}]')} value={quoteattr(f'[{table}].[{col}]')} />"
          for (table, col), name in sorted(local.items(), key=lambda kv: kv[1])]
    x += ["    </cols>", "    <metadata-records>"]
    for table, cols, n in tables:
        for ordinal, (col, bq_type) in enumerate(cols):
            code, local_type, agg = TDS_META.get(bq_type.upper(), (129, "string", "Count"))
            x += ["      <metadata-record class='column'>",
                  f"        <remote-name>{col}</remote-name>",
                  f"        <remote-type>{code}</remote-type>",
                  f"        <local-name>[{local[(table, col)]}]</local-name>",
                  f"        <parent-name>[{table}]</parent-name>",
                  f"        <remote-alias>{col}</remote-alias>",
                  f"        <ordinal>{ordinal}</ordinal>",
                  f"        <local-type>{local_type}</local-type>",
                  f"        <aggregation>{agg}</aggregation>",
                  "        <contains-null>true</contains-null>",
                  f"        <object-id>[{obj_id[table]}]</object-id>",
                  "      </metadata-record>"]
    x += ["    </metadata-records>", "  </connection>",
          "  <column datatype='integer' name='[Number of Records]' role='measure' type='quantitative' user:auto-column='numrec'>",
          "    <calculation class='tableau' formula='1' />",
          "  </column>"]
    x += [f"  <column caption={quoteattr(t)} datatype='table' name={quoteattr(f'[__tableau_internal_object_id__].[{obj_id[t]}]')} role='measure' type='quantitative' />"
          for t, _, _ in tables]
    x += ["  <layout dim-ordering='alphabetic' measure-ordering='alphabetic' show-structure='true' />",
          "  <object-graph>", "    <objects>"]
    for t, _, _ in tables:
        x += [f"      <object caption={quoteattr(t)} id={quoteattr(obj_id[t])}>",
              "        <properties context=''>", f"          {rel(t)}", "        </properties>", "      </object>"]
    x += ["    </objects>", "    <relationships>"]
    for table, _, _, fks in MODEL:
        for col, ref_table, ref_col in fks:
            x += ["      <relationship>", "        <expression op='='>",
                  f"          <expression op={quoteattr(f'[{local[(table, col)]}]')} />",
                  f"          <expression op={quoteattr(f'[{local[(ref_table, ref_col)]}]')} />",
                  "        </expression>",
                  f"        <first-end-point object-id={quoteattr(obj_id[table])} />",
                  f"        <second-end-point object-id={quoteattr(obj_id[ref_table])} />",
                  "      </relationship>"]
    x += ["    </relationships>", "  </object-graph>", "</datasource>", ""]
    return "\n".join(x)


def package_tdsx(hyper_path: pathlib.Path, tds_xml: str, tdsx_path: pathlib.Path) -> None:
    (tdsx_path.parent / f"{tdsx_path.stem}.tds").write_text(tds_xml, encoding="utf-8")  # kept beside it for inspection
    with zipfile.ZipFile(tdsx_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{tdsx_path.stem}.tds", tds_xml)
        z.write(hyper_path, f"Data/{hyper_path.name}")


def publish(server: TSC.Server, name: str, path: pathlib.Path, description: str) -> TSC.DatasourceItem:
    project = next(p for p in TSC.Pager(server.projects) if p.name == TABLEAU_PROJECT)
    item = TSC.DatasourceItem(project_id=project.id, name=name)
    item.description = description
    return server.datasources.publish(item, str(path), mode=TSC.Server.PublishMode.Overwrite)


def _vds(server: TSC.Server, endpoint: str, body: dict) -> dict:
    """VizQL Data Service: the API the Tableau MCP's query/metadata tools wrap."""
    url = f"{server.server_address}/api/v1/vizql-data-service/{endpoint}"
    r = requests.post(url, json=body, headers={"X-Tableau-Auth": server.auth_token, "Content-Type": "application/json"}, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f"VDS {endpoint} {r.status_code}: {r.text[:400]}")
    return r.json()


def vds_query(server: TSC.Server, luid: str, query: dict) -> list[dict]:
    return _vds(server, "query-datasource", {"datasource": {"datasourceLuid": luid}, "query": query}).get("data", [])


def vds_metadata(server: TSC.Server, luid: str) -> list[dict]:
    return _vds(server, "read-metadata", {"datasource": {"datasourceLuid": luid}}).get("data", [])


def download_tds(server: TSC.Server, luid: str) -> str:
    path = pathlib.Path(server.datasources.download(luid, filepath=str(BUILD), include_extract=False))
    if path.suffix == ".tdsx":
        with zipfile.ZipFile(path) as z:
            return z.read(next(n for n in z.namelist() if n.endswith(".tds"))).decode("utf-8")
    return path.read_text(encoding="utf-8")


def relationships_in(xml: str) -> list[str]:
    names = {v: k for k, v in re.findall(r"<object caption='([^']*)' id='([^']*)'", xml)}
    out = []
    for rel in re.findall(r"<relationship>(.*?)</relationship>", xml, re.S):
        ops = re.findall(r"<expression op='\[([^\]]*)\]' />", rel)
        ends = re.findall(r"end-point object-id='([^']*)'", rel)
        out.append(f"{names.get(ends[0], ends[0])} -> {names.get(ends[1], ends[1])} on {' = '.join(ops)}")
    return out


def cols_in(xml: str) -> dict[tuple[str, str], str]:
    """(table, column) -> published local field name, from the <cols> map."""
    return {(t, c): k for k, t, c in re.findall(r"<map key='\[([^\]]*)\]' value='\[([^\]]*)\]\.\[([^\]]*)\]' />", xml)}


def detect_binding(server: TSC.Server, luid: str) -> tuple[dict[str, str], str]:
    """object caption -> the physical table whose columns it actually serves
    (VDS read-metadata joined to the published <cols> map). Returns the .tds too."""
    xml = download_tds(server, luid)
    by_local = {local: table for (table, _), local in cols_in(xml).items()}
    caption_of = {i: c for c, i in re.findall(r"<object caption='([^']*)' id='([^']*)'", xml)}
    holds: dict[str, str] = {}
    for m in vds_metadata(server, luid):
        oid, cap = m.get("logicalTableId"), m["fieldCaption"]
        if oid and cap in by_local:
            holds.setdefault(caption_of.get(oid, oid), by_local[cap])
    return holds, xml


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_list() -> None:
    print(f"Source: {PROJECT}.{DATASET}  ->  Tableau project '{TABLEAU_PROJECT}'")
    for name, mart in SINGLE.items():
        print(f"  {name!r:40}  <-  {mart}")
    print(f"  {MODEL_NAME!r:40}  <-  " + ", ".join(t for t, *_ in MODEL))


def cmd_build(do_publish: bool) -> None:
    client = bigquery.Client(project=PROJECT)
    env = load_env() if do_publish else None
    built: list[tuple[str, pathlib.Path, int]] = []
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        for name, mart in SINGLE.items():
            path = BUILD / f"{mart}.hyper"
            n = build_hyper(client, mart, path, hyper)
            built.append((name, path, n))
            print(f"built  {path.name:32} {n:>6} rows")
    if not do_publish:
        return
    server, auth = tableau_server(env)
    with server.auth.sign_in(auth):
        for name, path, n in built:
            desc = (f"W5 Keeping Score extract of {PROJECT}.{DATASET}.{path.stem} ({n} rows). "
                    f"Generated by wax_baseball_w5_datasource/build_datasource.py -- do not edit in the GUI; rerun the script.")
            item = publish(server, name, path, desc)
            print(f"published  {name!r:40} luid={item.id}")


def cmd_model(do_publish: bool) -> None:
    client = bigquery.Client(project=PROJECT)
    env = load_env() if do_publish else None
    hyper_path = BUILD / f"{MODEL_STEM}.hyper"
    tdsx_path = BUILD / f"{MODEL_STEM}.tdsx"
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        tables = build_model_hyper(client, hyper_path, hyper)
    for table, cols, n in tables:
        print(f"built  {table:32} {n:>6} rows  {len(cols):>3} cols")
    objects = [t for t, _, _ in tables]
    collection = list(objects)
    package_tdsx(hyper_path, model_tds(tables, f"Data/{hyper_path.name}", collection), tdsx_path)
    print(f"packaged  {tdsx_path.name}  ({tdsx_path.stat().st_size / 1e6:.1f} MB; .tds beside it for inspection)")
    if not do_publish:
        return
    desc = (f"W5 Keeping Score relationship model: {len(MODEL)} tables from {PROJECT}.{DATASET}; object model generated "
            f"as .tds (mirrors the D360 Keeping_Score SDM graph + HOF Sightings on People + hof_category on People). "
            f"Generated by wax_baseball_w5_datasource/build_datasource.py -- do not edit in the GUI; rerun the script.")
    server, auth = tableau_server(env)
    with server.auth.sign_in(auth):
        for attempt in (1, 2):
            item = publish(server, MODEL_NAME, tdsx_path, desc)
            holds, _ = detect_binding(server, item.id)
            if all(holds.get(o) == o for o in objects):
                print(f"published  {MODEL_NAME!r:40} luid={item.id}  (pass {attempt}: every object bound to its own table)")
                return
            # object i holds the table at collection position sigma(i); invert so pass 2 is the identity
            sigma = [collection.index(holds[o]) for o in objects]
            collection = [""] * len(objects)
            for i, o in enumerate(objects):
                collection[sigma[i]] = o
            print(f"pass {attempt}: objects bound by sigma={sigma}; republishing with collection order {collection}")
            package_tdsx(hyper_path, model_tds(tables, f"Data/{hyper_path.name}", collection), tdsx_path)
        sys.exit("binding still permuted after two passes -- sigma is not stable for this object set")


def _run(server: TSC.Server, cases, luids: dict[str, str]) -> int:
    failures = 0
    for label, ds, query, check, *rest in cases:
        check_error = rest[0] if rest else None
        try:
            ok, detail = check(vds_query(server, luids[ds], query))
        except Exception as e:  # noqa: BLE001 -- a refusal can be the finding
            ok, detail = check_error(e) if check_error else (False, f"ERROR {e}")
        failures += (not ok)
        print(f"{'PASS' if ok else 'FAIL'}  {label:72} {detail}")
    return failures


def cmd_verify() -> int:
    server, auth = tableau_server(load_env())
    with server.auth.sign_in(auth):
        luids = {d.name: d.id for d in TSC.Pager(server.datasources) if d.name in SINGLE}
        if missing := [n for n in SINGLE if n not in luids]:
            sys.exit(f"not published yet: {missing}")
        cases = _battery(lambda table, col: col)  # single-table sources: caption == column
        failures = _run(server, cases, luids)
    print(f"\n{len(cases) - failures}/{len(cases)} PASS through VizQL Data Service (single-table sources)")
    return failures


def cmd_verify_model() -> int:
    server, auth = tableau_server(load_env())
    with server.auth.sign_in(auth):
        luid = next((d.id for d in TSC.Pager(server.datasources) if d.name == MODEL_NAME), None)
        if not luid:
            sys.exit(f"{MODEL_NAME!r} is not published yet -- run --model --publish")
        holds, xml = detect_binding(server, luid)
        print("## object -> physical table it serves (VDS read-metadata + the published <cols> map)")
        bound_ok = True
        for o, t in holds.items():
            bound_ok &= o == t
            print(f"   {'ok ' if o == t else 'BAD'} {o:28} -> {t}")
        print("\n## relationships in the published model (REST download of its .tds)")
        rels = relationships_in(xml)
        for r in rels:
            print("  ", r)
        declared = sum(len(fks) for *_, fks in MODEL)
        print(f"  {len(rels)} relationships ({declared} declared in MODEL)")

        cols = cols_in(xml)
        f = lambda table, col: cols[(table, col)]  # noqa: E731
        print("\n## golden battery through the model")
        luids = {name: luid for name in SINGLE} | {MODEL_NAME: luid}
        failures = _run(server, _battery(f), luids)
        print("\n## the Hall of Fame arc (D360's 281 -> 35 -> 44, replayed on Tableau relationships)")
        failures += _run(server, _hof_arc(f), luids)
        failures += (not bound_ok)
    print(f"\n{'ALL PASS' if not failures else f'{failures} FAIL'} through VizQL Data Service (relationship model)")
    return failures


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--build", action="store_true", help="shape 1: five single-table extracts")
    ap.add_argument("--model", action="store_true", help="shape 2: the relationship model (.tdsx)")
    ap.add_argument("--publish", action="store_true", help="publish after --build / --model (Overwrite)")
    ap.add_argument("--verify", action="store_true", help="golden battery through VDS, single-table sources")
    ap.add_argument("--verify-model", action="store_true", help="binding + relationships + battery + HOF arc on the model")
    args = ap.parse_args()
    if args.list or not any((args.build, args.model, args.verify, args.verify_model)):
        cmd_list()
    if args.build:
        cmd_build(args.publish)
    if args.model:
        cmd_model(args.publish)
    failures = 0
    if args.verify:
        failures += cmd_verify()
    if args.verify_model:
        failures += cmd_verify_model()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
