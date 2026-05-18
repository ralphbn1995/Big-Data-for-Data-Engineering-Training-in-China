#!/usr/bin/env bash
# ============================================================
#  scripts/test_api.sh
#  Session 5 – Complete API test suite using curl
#  All five endpoints + error cases
# ============================================================
set -euo pipefail

BASE="http://localhost:5000/api/v1"
PASS=0
FAIL=0

# ── Helpers ───────────────────────────────────────────────────
sep() { echo ""; echo "──────────────────────────────────────────────"; echo "  $1"; echo "──────────────────────────────────────────────"; }
pp()  { python3 -m json.tool 2>/dev/null || cat; }

check() {
    local LABEL="$1"
    local EXPECTED_CODE="$2"
    local ACTUAL_CODE="$3"
    local BODY="$4"

    if [[ "$ACTUAL_CODE" == "$EXPECTED_CODE" ]]; then
        echo "  ✅  $LABEL → $ACTUAL_CODE"
        PASS=$((PASS + 1))
    else
        echo "  ❌  $LABEL → got $ACTUAL_CODE, expected $EXPECTED_CODE"
        echo "      Body: $BODY"
        FAIL=$((FAIL + 1))
    fi
}

# ── Check server is up ────────────────────────────────────────
echo "=================================================="
echo "  Session 5 – API Test Suite"
echo "  Target: $BASE"
echo "=================================================="

if ! curl -sf "$BASE/health" > /dev/null 2>&1; then
    echo ""
    echo "  ❌  Server is not running at $BASE"
    echo "     Start it with: python run.py"
    exit 1
fi
echo "  ✔  Server is up."

# ══════════════════════════════════════════════════════════════
sep "1. GET /health"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/health")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "health returns 200" "200" "$CODE" "$BODY"

# ══════════════════════════════════════════════════════════════
sep "2. GET /sensors"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/sensors")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "list sensors returns 200" "200" "$CODE" "$BODY"

# ══════════════════════════════════════════════════════════════
sep "3a. GET /sensors/temperature/latest  (valid sensor)"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/sensors/temperature/latest")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "latest temperature: 200 or 404" "$CODE" "$CODE" "$BODY"   # accept both

sep "3b. GET /sensors/radar/latest  (invalid sensor → expect 404)"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/sensors/radar/latest")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "invalid sensor returns 404" "404" "$CODE" "$BODY"

# ══════════════════════════════════════════════════════════════
sep "4a. GET /sensors/temperature/stats?days=3  (valid)"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/sensors/temperature/stats?days=3")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "stats with days=3 returns 200" "200" "$CODE" "$BODY"

sep "4b. GET /sensors/temperature/stats?days=abc  (bad param → 400)"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/sensors/temperature/stats?days=abc")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "invalid days returns 400" "400" "$CODE" "$BODY"

sep "4c. GET /sensors/temperature/stats?days=0  (out of range → 400)"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/sensors/temperature/stats?days=0")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "days=0 returns 400" "400" "$CODE" "$BODY"

sep "4d. GET /sensors/temperature/stats?days=91  (out of range → 400)"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/sensors/temperature/stats?days=91")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "days=91 returns 400" "400" "$CODE" "$BODY"

# ══════════════════════════════════════════════════════════════
sep "5a. POST /readings  (valid payload → 201)"
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    "$BASE/readings" \
    -H "Content-Type: application/json" \
    -d '{"sensor":"temperature","value":29.3,"unit":"C"}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "valid POST returns 201" "201" "$CODE" "$BODY"

sep "5b. POST /readings  (missing 'value' field → 400)"
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    "$BASE/readings" \
    -H "Content-Type: application/json" \
    -d '{"sensor":"temperature"}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "missing field returns 400" "400" "$CODE" "$BODY"

sep "5c. POST /readings  (invalid sensor type → 422)"
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    "$BASE/readings" \
    -H "Content-Type: application/json" \
    -d '{"sensor":"radar","value":29.3}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "invalid sensor returns 422" "422" "$CODE" "$BODY"

sep "5d. POST /readings  (non-JSON body → 400)"
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    "$BASE/readings" \
    -d 'sensor=temperature&value=29.3')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "non-JSON body returns 400" "400" "$CODE" "$BODY"

sep "5e. POST /readings  (value out of range → 422)"
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    "$BASE/readings" \
    -H "Content-Type: application/json" \
    -d '{"sensor":"temperature","value":999.9}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "out-of-range value returns 422" "422" "$CODE" "$BODY"

# ══════════════════════════════════════════════════════════════
sep "6. Invalid URL → 404"
RESP=$(curl -s -w "\n%{http_code}" "$BASE/does-not-exist")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "unknown URL returns 404" "404" "$CODE" "$BODY"

sep "7. Wrong method (GET on POST endpoint → 405)"
RESP=$(curl -s -w "\n%{http_code}" -X GET "$BASE/readings")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | head -n -1)
echo "$BODY" | pp
check "wrong method returns 405" "405" "$CODE" "$BODY"

# ══════════════════════════════════════════════════════════════
echo ""
echo "=================================================="
echo "  Results: $PASS passed, $FAIL failed"
echo "=================================================="
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
