#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# test_simulate.sh — Simulation et tests END GATEWAY
#
# Usage : bash test_simulate.sh [BASE_URL] [PASSWORD]
#   BASE_URL   : URL du panel (défaut: http://localhost:5000)
#   PASSWORD   : Mot de passe admin (défaut: admin)
#
# Prérequis :
#   - DEBUG_MODE=true dans les env vars (bypass HMAC webhook)
#   - Application et worker Celery en cours d'exécution
#   - curl installé
# ─────────────────────────────────────────────────────────────────────────────

BASE="${1:-http://localhost:5000}"
PASS="${2:-admin}"
COOKIE_FILE="/tmp/endgw_test_cookies.txt"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'; BOLD='\033[1m'

ok()  { echo -e "${GREEN}✓${NC} $1"; }
err() { echo -e "${RED}✗${NC} $1"; }
hdr() { echo -e "\n${BOLD}${YELLOW}── $1 ──${NC}"; }

# ── Login ─────────────────────────────────────────────────────────────────────
hdr "1. Authentification"
LOGIN=$(curl -s -c "$COOKIE_FILE" -b "$COOKIE_FILE" \
  -X POST "$BASE/admin/login" \
  -d "password=$PASS" \
  -w "%{http_code}" -o /dev/null)
if [ "$LOGIN" = "200" ] || [ "$LOGIN" = "302" ]; then ok "Login OK"; else err "Login échoué (HTTP $LOGIN)"; fi

# ── État général ──────────────────────────────────────────────────────────────
hdr "2. État du système"
STATE=$(curl -s -b "$COOKIE_FILE" "$BASE/admin/state" -H "Accept: application/json")
echo "$STATE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"  Redis: {'✓' if d.get('redis_ok') else '✗'}  Worker: {'✓' if d.get('worker_ok') else '✗'}  Remaining: {d.get('remaining',0)}  Templates: {len(d.get('templates',[]))}\") " 2>/dev/null || echo "  (parse error)"

# ── Templates ─────────────────────────────────────────────────────────────────
hdr "3. Création de templates"
TMPL1=$(curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/templates/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "name=Test Campagne&text=Bonjour {{nom}} ! Voici notre offre : {{link}}&type=sms&category=campaign")
echo "  Campagne : $(echo $TMPL1 | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("msg","?"))' 2>/dev/null)"

TMPL2=$(curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/templates/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "name=Réponse auto 1&text=Merci {{number}} de nous avoir contacté ! Voici notre lien : {{link}}&type=sms&category=reply1")
echo "  Réponse1 : $(echo $TMPL2 | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("msg","?"))' 2>/dev/null)"

TMPL3=$(curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/templates/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "name=Réponse auto 2&text=Parfait {{number}}, à bientôt !&type=sms&category=reply2")
echo "  Réponse2 : $(echo $TMPL3 | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("msg","?"))' 2>/dev/null)"

# ── Lien global ───────────────────────────────────────────────────────────────
hdr "4. Configuration lien global {{link}}"
LINK_RES=$(curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/global_link/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "global_link=https://exemple.com/offre")
echo "  $(echo $LINK_RES | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("msg","?"))' 2>/dev/null)"

# ── Countdown réponse ──────────────────────────────────────────────────────────
hdr "5. Countdown réponse auto (test: 0 = immédiat)"
CD_RES=$(curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/reply_countdown/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "reply_countdown=0")
echo "  $(echo $CD_RES | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("msg","?"))' 2>/dev/null)"

# ── Vitesse d'envoi ───────────────────────────────────────────────────────────
hdr "6. Vitesse d'envoi"
SP_RES=$(curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/send_speed/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "send_speed=1-2")
echo "  $(echo $SP_RES | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("msg","?"))' 2>/dev/null)"

# ── Simulation message entrant ────────────────────────────────────────────────
hdr "7. Simulation message entrant (DEBUG_MODE=true requis)"
echo "  Envoi d'un faux SMS entrant depuis +33600000001..."
MSG_PAYLOAD='[{"ID":"test001","number":"+33600000001","deviceID":"1","message":"TEST"}]'
WEBHOOK_RES=$(curl -s -X POST "$BASE/sms_auto_reply" \
  -d "messages=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$MSG_PAYLOAD'))")" \
  -w "\n[HTTP %{http_code}]" 2>/dev/null)
echo "  Réponse webhook: $WEBHOOK_RES"
echo "  → Attends 3s puis vérifie les logs : $BASE/logs"

sleep 3

# ── Vérification logs ──────────────────────────────────────────────────────────
hdr "8. Dernières lignes de log"
curl -s -b "$COOKIE_FILE" "$BASE/logs" 2>/dev/null | tail -10 | sed 's/^/  /'

# ── Test auto-reply config ─────────────────────────────────────────────────────
hdr "9. Configuration auto-reply"
AR_RES=$(curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/autoreply/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "enabled=1&reply_mode=2&step0_text=Bonjour ! Voici le lien : {{link}}&step1_text=Merci {{number}}, à bientôt !&step0_delay=0&step1_delay=0&step0_type=sms&step1_type=sms")
echo "  $(echo $AR_RES | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("msg","?"))' 2>/dev/null)"

# ── Deuxième simulation ───────────────────────────────────────────────────────
hdr "10. 2ème simulation (doit déclencher réponse 1)"
MSG2='[{"ID":"test002","number":"+33600000001","deviceID":"1","message":"TEST2"}]'
curl -s -X POST "$BASE/sms_auto_reply" \
  -d "messages=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$MSG2'))")" \
  -w "[HTTP %{http_code}]\n" > /dev/null
echo "  Envoyé. Vérifier les logs pour voir l'auto-reply."

# ── Test batch planifié ───────────────────────────────────────────────────────
hdr "11. Résumé des routes disponibles"
cat << 'EOF'
  POST /admin/templates/save          — Créer/modifier template (name, text, type, category)
  POST /admin/templates/delete        — Supprimer (tmpl_id)
  POST /admin/global_link/save        — Lien global {{link}}
  POST /admin/reply_countdown/save    — Countdown réponse (ex: "60-180", "0", "30")
  POST /admin/send_speed/save         — Vitesse envoi (ex: "0", "1", "1-2")
  POST /admin/nl/send                 — Lancer campagne (+ delay_minutes pour planifier)
  POST /admin/batch/<id>/pause        — Mettre en pause
  POST /admin/batch/<id>/resume       — Reprendre
  POST /admin/batch/<id>/cancel       — Annuler
  POST /admin/device/relancer         — Relancer cycle + auto-dispatch campagne
  POST /admin/device/max_cycles/save  — Max cycles par device
  GET  /admin/state                   — État complet (JSON)
  GET  /logs                          — Logs en temps réel
EOF

# ── Rétablir countdown normal ─────────────────────────────────────────────────
hdr "12. Restaurer countdown 60-180s"
curl -s -b "$COOKIE_FILE" -X POST "$BASE/admin/reply_countdown/save" \
  -H "X-Requested-With: XMLHttpRequest" -H "Accept: application/json" \
  -d "reply_countdown=60-180" > /dev/null
ok "Countdown rétabli"

echo -e "\n${GREEN}${BOLD}Tests terminés.${NC}"
echo "Panel : $BASE/admin/settings"
echo "Logs  : $BASE/logs"
rm -f "$COOKIE_FILE"
