# wax_baseball_w5_datasource

The Keeping Score marts on Tableau Cloud, published headless from BigQuery -- W5 of the
Keeping Score parity plan (`wax-system/wax-baseball/keeping-score-parity-plan.md`).
No GUI step anywhere: a Personal Access Token, the Hyper API, and the REST API do it all.

## Why one home

The dbt marts in BigQuery are the Source. This script is the Analysis layer -- the generator.
Everything it produces (`build/*.hyper`, `build/*.tdsx`, the published data sources on
`10ax.online.tableau.com/#/site/nobodybeatstheviz`) is Presentation: disposable, regenerated
by rerunning, never patched in the Tableau GUI. If a number is wrong, fix the mart or the
script, then rerun.

## Run

```
py build_datasource.py --list                  # what would be built
py build_datasource.py --build --publish       # five single-table sources (shape 1)
py build_datasource.py --verify                # golden battery through VizQL Data Service
py build_datasource.py --model --publish       # the relationship model (shape 2), two-pass publish
py build_datasource.py --verify-model          # binding + relationships + battery + the HOF arc
```

Auth: `.env` holds the Tableau PAT (gitignored; `.env.example` is the shape; the token
expires 2027-02-28). BigQuery uses Application Default Credentials, like the parity harness.

## What gets published

| Data source | Source mart | Answers |
|---|---|---|
| Keeping Score - Attended Games | `fct_attended_games` (178) | games by year, games / unique stadiums |
| Keeping Score - Plays | `fct_plays` (14,406) | home runs witnessed, runs witnessed |
| Keeping Score - Game Attendees | `fct_game_attendee` (320) | top attendees |
| Keeping Score - Team Games | `fct_attended_team_games` (356) | a team's attended win rate |
| Keeping Score - HOF Sightings | `fct_hof_sightings` (44) | Hall of Famers seen |
| Keeping Score - Model | all of the above + Lahman `people`, `hall_of_fame` | the same seven, through Tableau relationships |

Shape 1 is one extract per grain, which is the D360 join-graph lesson applied: metrics on
unrelated fact tables do not combine, so they do not share a data source. Shape 2 is one
`.hyper` with seven tables plus a generated `.tds` object model, packaged as a `.tdsx`; its
graph mirrors the D360 `Keeping_Score` SDM (`wax_baseball_datacloud_deploy/sdm/relationships.json`).

`--verify` and `--verify-model` run the seven golden questions through VizQL Data Service,
the API the Tableau MCP's `query-datasource` tool wraps, so a green battery here is a green
battery for the MCP surface. Reference answers are the parity harness's (BigQuery via
MetricFlow, 2026-08-31): 178 games / 22 stadiums, 400 HR, 1,706 runs, 44 Hall of Famers,
NYA 90 of 143, Melissa 57.

## What was measured on the way (2026-09-01)

The module docstring in `build_datasource.py` carries the full list; the ones that change
how you build:

- **Hyper keys are `ASSUMED` only**, and an FK must reference the target's `ASSUMED PRIMARY
  KEY`. Tableau Cloud infers relationships from those keys at publish, but only for a star
  with exactly one fact table. A graph with several leaf facts needs the object model written
  out as `.tds` XML -- the grammar in `model_tds()` is what Tableau itself wrote for a
  published star, read back through the REST download.
- **The legacy `<relation type='collection'>` block binds objects to tables positionally**
  through a fixed permutation of the object set. `cmd_model` publishes once, reads which
  physical table each object actually serves, inverts, and republishes -- two passes, then the
  binding is the identity. Object ids are deterministic so the permutation cannot drift.
- **Tableau refuses what D360 silently fanned out.** A row-level calc spanning two sibling
  tables is rejected ("only one upstream base table"); D360's SDM accepted the same
  expression and returned 281. Filters from two different siblings are dropped silently. The
  engine-native answer for "Hall of Famers I saw bat" (35) needs HOF-ness as an attribute on
  People, which is why `stg_lahman_people` carries `hof_category` in the model.
