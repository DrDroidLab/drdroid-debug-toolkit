# Plan: Tool-call empty / failure emit parity (cloud toolkit)

**Repo:** `DrDroidLab/drdroid-debug-toolkit`  
**Branch:** `feat/tool-call-empty-failure-emitters`  
**PR:** https://github.com/DrDroidLab/drdroid-debug-toolkit/pull/169  
**Source of truth:** `drdroid-on-prem/drdroid-debug-toolkit`  
**Related consumer:** `prototype` (stamps `tool_call_status`; must pin this package)  
**Verified:** 2026-07-15  

> **This document is a plan only.** Emit work for shared managers is already on this branch — remaining items are merge/publish + optional product parity (Argo / Gardener / OpsGenie). Tool-call emit contract is complete; do not reopen TEXT→helper sweeps unless inventory regresses.

---

## 1. Goal

Every shared connector source manager that on-prem converted must:

| Outcome | Emit |
|---------|------|
| Zero rows / logs / metrics / widgets | `build_task_empty_result` → `failure_kind=empty_query_result` |
| Auth / HTTP / validation / unexpected fault | `build_task_failure_result` → `failure_kind=operational_error` |
| Real data | Existing success types (`TIMESERIES`, `TABLE`, `API_RESPONSE` 200, success `TEXT` where the product *is* text) |

So prototype’s write-time stamp can classify **`no_data` / `failure` / `success`** instead of painting zero-result **TEXT** as success.

---

## 2. Status: shared-manager emit contract — **DONE on this branch**

### 2.1 Foundation (merged into this branch)

| Artifact | Status |
|----------|--------|
| `core/integrations/utils/task_result_utils.py` | Byte-identical to on-prem |
| `core/integrations/utils/processor_result.py` | Byte-identical to on-prem |
| Unit tests for helpers | `tests/test_task_result_utils.py` |

### 2.2 Managers converted (19)

Datadog, Grafana, CloudWatch, Coralogix, Metabase, Jira, Elasticsearch, New Relic, Sentry, PostHog, SigNoz, GitHub, OpenSearch, VictoriaLogs, Azure, BigQuery, Grafana Loki, Mimir, **GCM**.

### 2.3 Shared-tree totals (cloud branch vs on-prem)

| Metric | On-prem | Cloud branch | Notes |
|--------|--------:|-------------:|-------|
| `build_task_empty_result(` | 73 | 77 | Cloud +4 (GCM reclassified → empty) |
| `build_task_failure_result(` | 198 | 194 | Cloud −4 (same GCM sites) |
| `PlaybookTaskResultType.TEXT` | 24 | 24 | Matched |

**Verdict:** No shared manager is behind on-prem on TEXT-as-success empties. Inventory guard: `tests/test_source_manager_emit_inventory.py`.

### 2.4 Intentional TEXT leftovers (both trees — do not “fix”)

| Manager | Why TEXT remains |
|---------|------------------|
| documentation / slack | Product output *is* TEXT |
| cloudwatch / coralogix / ES / signoz / opensearch | Success payloads, `result_type` metadata, or validation strings that also exist on on-prem |

### 2.5 GCM nuance (keep cloud semantics)

Cloud intentionally uses **empty** (not failure) for: no GCM logs, missing spreadsheet, empty sheet, unprocessable timeseries. Do **not** sync those back to on-prem’s `failure` sites. Also preserve cloud `_parse_gcm_timestamp` / `_strip_orphan_bool_ops` and `execute_mql(interval=…)`.

---

## 3. Direct answer: “is toolkit done with all source managers?”

| Question | Answer |
|----------|--------|
| All **shared** managers emit-parity with on-prem? | **Yes** (on this branch / PR #169) |
| Every manager that *exists on both* trees needs more emit work? | **No** |
| Is the package “fully identical” to on-prem? | **No** — product gaps remain (§4) |

Managers with **no** `build_task_*` on **either** tree (nothing to port for emit): api, argocd, bash, clickhouse, databricks, documentation, eks, github_actions, gke, jenkins, kubernetes, mongodb, ms_teams, ops_genie, pager_duty, postgres, render, rootly, slack, smtp, sql_database, zenduty, …

---

## 4. Remaining work (plans only — ordered)

### Wave TK-Ship (blocking consumer smoke)

1. Merge PR **#169** to `master`.
2. Retarget prototype pin from this feature branch → `@master` (or release tag).
3. Redeploy / restart prototype.
4. Smoke on a **new** investigation: Datadog empty log/metric → stamp / UI **no_data** (yellow); operational fault → **failure**; data → **success**.

Old investigations keep wrong historical stamps — out of scope unless a restamp job is added later (lives outside this repo).

### Wave TK-Product (optional — not emit contract)

| Item | Scope | Notes |
|------|-------|-------|
| **Argo Workflows** | Manager + API processor + metadata extractor + task/asset protos + `Source` enum wiring in consumers | On-prem only today; emit counts already 0/0 |
| **Gardener** | Same pattern | On-prem only |
| **OpsGenie task types** | Port `FETCH_TEAM_ROUTING_RULES`, `FETCH_ALERT_POLICIES`, `FETCH_NOTIFICATION_RULES` (+ processor methods) | Product features; neither side uses `build_task_*` for OpsGenie today |
| Proto packaging | Ensure generated `*_pb2.py` / `.proto` stay in sync with consumer prototype | Risk: mismatched enums break register() |

**Suggested PR split:** one PR per connector family (Argo, Gardener, OpsGenie). Do not mix with emit hotfixes.

### Wave TK-Polish (optional)

| Item | Note |
|------|------|
| VictoriaLogs “No data” still uses `failure` on **both** trees | Optional align to empty (on-prem first, then cloud) |
| Signoz remaining TEXT validation errors | Optional; identical on both |
| Continuous inventory CI | Run `test_source_manager_emit_inventory.py` in CI on every PR |

---

## 5. Non-goals

- Falcon / podracer UI or agent compaction.
- Historical log restamp (prototype/podracer concern).
- Force-sync GCM empty semantics down to on-prem.
- Blind `cp -r` of entire on-prem toolkit over cloud (breaks cloud-only MQL/datetime fixes).

---

## 6. Suggested acceptance checklist

- [x] Helpers present and tested  
- [x] Shared managers TEXT budgets ≤ on-prem  
- [x] Datadog / Grafana / Coralogix / GCM covered  
- [ ] #169 merged to master  
- [ ] Prototype pin updated + Datadog empty smoke green/yellow/red as expected  
- [ ] (Optional) Argo / Gardener / OpsGenie product ports tracked as separate issues  

---

## 7. Next concrete actions

1. Land / merge **#169**.  
2. Update prototype `requirements.txt` pin → master SHA/tag.  
3. Open tracking issues for TK-Product items if full monorepo parity is desired.  
4. Do **not** reopen shared-manager TEXT→helper ports unless inventory regresses.
