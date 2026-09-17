"""build_workbook.py -- V2: a Tableau workbook over the W5 published sources, authored as XML and
published headless through the Tableau MCP.

Source:        the W5 published data sources on Tableau Cloud (build_datasource.py owns those).
Analysis:      this script -- a small declarative spec (SPECS) -> a .twb, validated locally against
               Tableau's published XSD, then published through the Tableau MCP's `publish-workbook`
               tool (which re-validates server-side before it uploads).
Presentation:  workbooks/*.twb (the git-tracked receipt the parity plan asks for) and the published
               workbook on the site -- disposable, regenerate, never patch in the GUI.

    py build_workbook.py --list                         # specs
    py build_workbook.py --setup-mcp                    # npm-install @tableau/mcp-server into .mcp/ + enable authoring-tools
    py build_workbook.py --build [--spec hr_by_year]    # write workbooks/<spec>.twb + local XSD validation
    py build_workbook.py --build --publish              # ...and publish via the Tableau MCP (falls back to REST with --rest)
    py build_workbook.py --receipt                      # fetch the published view's PNG + CSV, check the golden answer

Measured 2026-09-09/10 (the grammar rules this generator encodes):
  - Tableau Cloud REST 3.30 validates a TWB before publishing (the MCP's publish-workbook surfaces the
    errors as `status: invalid`). The 2026.2 XSD path (`version='26.2'` + `<ManifestByVersion/>`) is
    blocked by two errors in Tableau's own schema ("group 'Sort-G' must contain ... compositor") that
    no workbook content fixes. The legacy grammar (`version='18.1'`, a classic manifest) publishes --
    and it is what Tableau 2026.1 itself still writes, measured on a workbook downloaded from the site.
  - A published data source is a `class='sqlproxy'` connection: dbname = the source's content URL,
    server/channel/port = the site, plus a `<repository-location>` pointing at /t/<site>/datasources.
  - Field references are the source's column names in brackets; aggregations are `column-instance`s
    named `[sum:Field:qk]` / `[yr:Field:ok]`; shelves take `[<datasource name>].[<instance>]`.
  - The official XSD (tableau/tableau-document-schemas, 2026_2) does not compile in lxml as shipped:
    it references a `user:` attribute group and the W3C xml namespace without schemaLocations.
    schemas/twb_2026.2.0.patched.xsd adds both (user_stub.xsd, xml.xsd); local validation then agrees
    with the server on structure but is stricter than the 18.1 grammar Tableau accepts -- treat it as
    advisory, the server verdict is the gate.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys
import uuid

HERE = pathlib.Path(__file__).resolve().parent
WORKBOOKS = HERE / "workbooks"
MCP_DIR = HERE / ".mcp"
MCP_PACKAGE = "@tableau/mcp-server@4.8.1"
XSD = HERE / "schemas" / "twb_2026.2.0.patched.xsd"
TABLEAU_PROJECT = "default"

# ---------------------------------------------------------------- specs
# One spec = one workbook = one worksheet answering one golden question.
# datasource: the published source (content_url is what sqlproxy binds to).
# columns:    fields the sheet uses; calc columns carry a formula.
# rows/cols:  (column name, derivation) -- derivation in Tableau's own vocabulary (Sum, Year, None, ...).
SPECS: dict[str, dict] = {
    "hr_by_year": {
        "workbook_name": "Keeping Score - HR by Year",
        "sheet": "HR by Year",
        "golden": ("G2 home runs witnessed", 400),
        "datasource": {"content_url": "KeepingScore-Plays", "caption": "Keeping Score - Plays"},
        "columns": [
            {"name": "game_date", "datatype": "date", "role": "dimension", "type": "ordinal"},
            {"name": "event_code", "datatype": "integer", "role": "measure", "type": "quantitative"},
            {"name": "Calculation_HR", "caption": "Home Runs", "datatype": "integer", "role": "measure",
             "type": "quantitative", "formula": "IIF([event_code] = 23, 1, 0)"},
        ],
        "rows": [("Calculation_HR", "Sum")],
        "cols": [("game_date", "Year")],
        "mark": "Bar",
    },
}

# derivation -> (instance prefix, instance type suffix)
_DERIV = {"Sum": ("sum", "qk"), "Year": ("yr", "ok"), "None": ("none", "nk"), "Count": ("cnt", "qk"),
          "CountD": ("ctd", "qk"), "Avg": ("avg", "qk"), "Month": ("mn", "ok"), "Quarter": ("qr", "ok")}
_ITYPE = {"qk": "quantitative", "ok": "ordinal", "nk": "nominal"}


def _env() -> dict[str, str]:
    env = {}
    for line in (HERE / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    missing = [k for k in ("TABLEAU_SERVER", "TABLEAU_SITE", "TABLEAU_PAT_NAME", "TABLEAU_PAT_SECRET") if k not in env]
    if missing:
        sys.exit(f".env is missing {missing} -- see .env.example")
    return env


# ---------------------------------------------------------------- TWB authoring
def _column_xml(c: dict, indent: str) -> str:
    cap = f" caption='{c['caption']}'" if c.get("caption") else ""
    head = f"{indent}<column{cap} datatype='{c['datatype']}' name='[{c['name']}]' role='{c['role']}' type='{c['type']}'"
    if c.get("formula"):
        return f"{head}>\n{indent}  <calculation class='tableau' formula='{c['formula']}' />\n{indent}</column>\n"
    return head + " />\n"


CHART_TOKENS = pathlib.Path(os.environ.get("NBTV_CHART_TOKENS",
                                           pathlib.Path.home() / ".claude" / "skills" / "nbtv-design" / "tokens" / "chart.json"))


def _style_xml() -> str:
    """NBTV chart tokens as a worksheet <style> block (Build 2.5: brand is a ceiling, never a blocker --
    if chart.json is missing the sheet ships unstyled). Mark color + fonts; the server validates the grammar."""
    if not CHART_TOKENS.exists():
        return "        <style />\n"
    t = json.loads(CHART_TOKENS.read_text(encoding="utf-8"))
    bar, ink, muted = t["light"]["bar"], t["light"]["ink"], t["light"]["muted"]
    font = t["font"]["display"].split(",")[0].strip("'\" ")
    return f"""        <style>
          <style-rule element='mark'>
            <format attr='mark-color' value='{bar}' />
          </style-rule>
          <style-rule element='worksheet'>
            <format attr='font-family' value='{font}' />
            <format attr='color' value='{ink}' />
          </style-rule>
          <style-rule element='axis'>
            <format attr='font-family' value='{font}' />
            <format attr='color' value='{muted}' />
          </style-rule>
          <style-rule element='label'>
            <format attr='font-family' value='{font}' />
          </style-rule>
        </style>
"""


def _instance(col: str, deriv: str) -> tuple[str, str]:
    prefix, suffix = _DERIV[deriv]
    return f"[{prefix}:{col}:{suffix}]", _ITYPE[suffix]


def render_twb(spec: dict, env: dict[str, str]) -> str:
    server_host = env["TABLEAU_SERVER"].replace("https://", "").replace("http://", "").rstrip("/")
    site = env["TABLEAU_SITE"]
    ds = spec["datasource"]
    ds_name = ds["content_url"]
    sheet = spec["sheet"]
    sheet_uuid = "{%s}" % str(uuid.uuid4()).upper()

    columns = "".join(_column_xml(c, "      ") for c in spec["columns"])
    dep_columns = "".join(_column_xml(c, "            ") for c in spec["columns"])
    instances, shelves = "", {}
    for shelf in ("rows", "cols"):
        refs = []
        for col, deriv in spec[shelf]:
            name, itype = _instance(col, deriv)
            instances += (f"            <column-instance column='[{col}]' derivation='{deriv}' name='{name}' "
                          f"pivot='key' type='{itype}' />\n")
            refs.append(f"[{ds_name}].{name}")
        shelves[shelf] = " / ".join(refs)

    return f"""<?xml version='1.0' encoding='utf-8' ?>
<workbook original-version='18.1' source-build='2026.1.0 (20261.26.0401.1148)' source-platform='win' version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <document-format-change-manifest>
    <SheetIdentifierTracking />
    <WindowsPersistSimpleIdentifiers />
  </document-format-change-manifest>
  <preferences />
  <datasources>
    <datasource caption='{ds["caption"]}' inline='true' name='{ds_name}' version='18.1'>
      <repository-location id='{ds_name}' path='/t/{site}/datasources' revision='1.0' site='{site}' />
      <connection channel='https' class='sqlproxy' dbname='{ds_name}' directory='/dataserver' port='443' server='{server_host}' username=''>
        <relation name='sqlproxy' table='[sqlproxy]' type='table' />
      </connection>
{columns}    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='{sheet}'>
      <table>
        <view>
          <datasources>
            <datasource caption='{ds["caption"]}' name='{ds_name}' />
          </datasources>
          <datasource-dependencies datasource='{ds_name}'>
{dep_columns}{instances}          </datasource-dependencies>
          <aggregation value='true' />
        </view>
{_style_xml()}        <panes>
          <pane selection-relaxation-option='selection-relaxation-allow'>
            <view>
              <breakdown value='auto' />
            </view>
            <mark class='{spec["mark"]}' />
          </pane>
        </panes>
        <rows>{shelves["rows"]}</rows>
        <cols>{shelves["cols"]}</cols>
      </table>
      <simple-id uuid='{sheet_uuid}' />
    </worksheet>
  </worksheets>
  <windows>
    <window class='worksheet' maximized='true' name='{sheet}'>
      <cards>
        <edge name='left'>
          <strip size='160'>
            <card type='pages' />
            <card type='filters' />
            <card type='marks' />
          </strip>
        </edge>
        <edge name='top'>
          <strip size='31'>
            <card type='columns' />
          </strip>
          <strip size='31'>
            <card type='rows' />
          </strip>
          <strip size='31'>
            <card type='title' />
          </strip>
        </edge>
      </cards>
    </window>
  </windows>
</workbook>
"""


def validate_local(path: pathlib.Path) -> bool:
    """Advisory: the patched 2026.2 XSD is stricter than the 18.1 grammar the server accepts."""
    try:
        from lxml import etree
    except ImportError:
        print("  lxml not installed; local validation skipped")
        return True
    schema = etree.XMLSchema(etree.parse(str(XSD)))
    ok = schema.validate(etree.parse(str(path)))
    print(f"  local XSD (advisory): {'valid' if ok else 'not valid'}")
    for e in list(schema.error_log)[:5]:
        print(f"    line {e.line}: {e.message[:200]}")
    return ok


# ---------------------------------------------------------------- MCP
def setup_mcp() -> None:
    MCP_DIR.mkdir(exist_ok=True)
    if not (MCP_DIR / "package.json").exists():
        # not `npm init -y`: a directory named .mcp yields an invalid package name
        (MCP_DIR / "package.json").write_text(json.dumps({"name": "tableau-mcp-local", "private": True}, indent=2))
    subprocess.run(["npm", "install", MCP_PACKAGE, "--no-audit", "--no-fund", "--loglevel=error"],
                   cwd=MCP_DIR, check=True, shell=(os.name == "nt"))
    features = MCP_DIR / "node_modules" / "@tableau" / "mcp-server" / "build" / "features.json"
    flags = json.loads(features.read_text())
    flags["authoring-tools"] = True
    features.write_text(json.dumps(flags, indent=2))
    print(f"MCP installed at {MCP_DIR}; authoring-tools enabled in {features.relative_to(HERE)}")


def mcp_server_js() -> pathlib.Path:
    js = MCP_DIR / "node_modules" / "@tableau" / "mcp-server" / "build" / "index.js"
    if not js.exists():
        sys.exit("Tableau MCP not installed -- run: py build_workbook.py --setup-mcp")
    return js


def mcp_call(env: dict[str, str], tool: str, args: dict) -> dict:
    """Call one Tableau MCP tool over stdio, PAT auth from .env, only the tools we need exposed."""
    child_env = dict(os.environ)
    child_env.update({
        "SERVER": env["TABLEAU_SERVER"], "SITE_NAME": env["TABLEAU_SITE"],
        "PAT_NAME": env["TABLEAU_PAT_NAME"], "PAT_VALUE": env["TABLEAU_PAT_SECRET"],
        "INCLUDE_TOOLS": "list-projects,publish-workbook",
    })
    out = subprocess.run(
        [sys.executable, str(HERE / "tableau_mcp_client.py"), "--cmd", f"node {mcp_server_js()}",
         "--call", tool, "--args", json.dumps(args), "--json-only"],
        env=child_env, capture_output=True, text=True, encoding="utf-8")
    lines = [l for l in out.stdout.splitlines() if l.startswith("{")]
    if not lines:
        raise RuntimeError(f"MCP call produced no result:\n{out.stderr[-1500:]}")
    result = json.loads(lines[-1])
    return result.get("structuredContent") or json.loads(result["content"][0]["text"])


def project_id(env: dict[str, str]) -> str:
    res = mcp_call(env, "list-projects", {})
    projects = res if isinstance(res, list) else res.get("projects") or res.get("data") or []
    for p in projects:
        if p.get("name") == TABLEAU_PROJECT:
            return p["id"]
    raise RuntimeError(f"project {TABLEAU_PROJECT!r} not found in {projects}")


def publish_mcp(env: dict[str, str], spec: dict, twb: pathlib.Path) -> dict:
    res = mcp_call(env, "publish-workbook", {
        "name": spec["workbook_name"], "projectId": project_id(env),
        "workbookFilePath": str(twb), "overwrite": True,
    })
    status = res.get("status")
    print(f"  MCP publish-workbook: {status}")
    for e in res.get("errors", []):
        print(f"    ERROR line {e.get('line')} <{e.get('elementName')}>: {e.get('message')[:200]}")
    for w in res.get("warnings", []):
        print(f"    warning: {str(w)[:200]}")
    if status == "published":
        print(f"  url: {res.get('url')}")
    return res


def publish_rest(env: dict[str, str], spec: dict, twb: pathlib.Path) -> None:
    """Fallback: the same publish through tableauserverclient (the REST API the MCP wraps)."""
    import tableauserverclient as TSC
    auth = TSC.PersonalAccessTokenAuth(env["TABLEAU_PAT_NAME"], env["TABLEAU_PAT_SECRET"], site_id=env["TABLEAU_SITE"])
    server = TSC.Server(env["TABLEAU_SERVER"], use_server_version=True)
    with server.auth.sign_in(auth):
        proj = next(p for p in TSC.Pager(server.projects) if p.name == TABLEAU_PROJECT)
        item = TSC.WorkbookItem(project_id=proj.id, name=spec["workbook_name"], show_tabs=True)
        wb = server.workbooks.publish(item, str(twb), TSC.Server.PublishMode.Overwrite)
        print(f"  REST publish: {wb.name} {wb.id}")


# ---------------------------------------------------------------- receipt
def receipt(env: dict[str, str], spec: dict) -> bool:
    import tableauserverclient as TSC
    auth = TSC.PersonalAccessTokenAuth(env["TABLEAU_PAT_NAME"], env["TABLEAU_PAT_SECRET"], site_id=env["TABLEAU_SITE"])
    server = TSC.Server(env["TABLEAU_SERVER"], use_server_version=True)
    with server.auth.sign_in(auth):
        wb = next((w for w in TSC.Pager(server.workbooks) if w.name == spec["workbook_name"]), None)
        if wb is None:
            print("  workbook not found on the site")
            return False
        server.workbooks.populate_views(wb)
        view = next(v for v in wb.views if v.name == spec["sheet"])
        server.views.populate_image(view, TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High, maxage=1))
        server.views.populate_csv(view, TSC.CSVRequestOptions(maxage=1))
        png_bytes = view.image                      # lazy fetchers -- read inside the session
        csv = b"".join(view.csv).decode("utf-8-sig")
    stem = WORKBOOKS / spec["file_stem"]
    stem.with_suffix(".png").write_bytes(png_bytes)
    stem.with_suffix(".csv").write_text(csv, encoding="utf-8")
    rows = [r for r in csv.splitlines()[1:] if r.strip()]
    total = sum(float(r.rsplit(",", 1)[-1].strip('"')) for r in rows)
    label, want = spec["golden"]
    ok = total == want
    print(f"  {label}: {total:g} (want {want}) {'PASS' if ok else 'FAIL'} -- {len(rows)} marks; png+csv beside the twb")
    return ok


# ---------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--setup-mcp", action="store_true")
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--rest", action="store_true", help="publish through REST instead of the MCP")
    ap.add_argument("--receipt", action="store_true")
    ap.add_argument("--spec", default=None, help="one spec name; default all")
    a = ap.parse_args()

    if a.setup_mcp:
        setup_mcp()
    if a.list:
        for k, s in SPECS.items():
            print(f"{k}: {s['workbook_name']} -- {s['golden'][0]} = {s['golden'][1]}")
    if not (a.build or a.publish or a.receipt):
        return

    env = _env()
    WORKBOOKS.mkdir(exist_ok=True)
    names = [a.spec] if a.spec else list(SPECS)
    failures = 0
    for name in names:
        spec = dict(SPECS[name], file_stem=f"keeping-score-{name.replace('_', '-')}")
        twb = WORKBOOKS / f"{spec['file_stem']}.twb"
        print(f"== {name}")
        if a.build:
            twb.write_text(render_twb(spec, env), encoding="utf-8")
            print(f"  wrote {twb.relative_to(HERE)} ({twb.stat().st_size} bytes)")
            validate_local(twb)
        if a.publish:
            if a.rest:
                publish_rest(env, spec, twb)
            else:
                res = publish_mcp(env, spec, twb)
                if res.get("status") != "published":
                    failures += 1
                    continue
        if a.receipt:
            if not receipt(env, spec):
                failures += 1
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
