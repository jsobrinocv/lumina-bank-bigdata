#!/usr/bin/env bash
# =============================================================
# Entrega 3 - Produccion: entrada publica unica a la API
# Load Balancer HTTPS global + Cloud Armor (WAF y limite de solicitudes)
# Requiere un dominio propio:  DOMAIN=api.lumina.example bash api/deploy_lb_armor.sh
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh
: "${DOMAIN:?Define DOMAIN con el dominio publico de la API}"

gcloud compute network-endpoint-groups create lumina-api-neg --region="$REGION" \
  --network-endpoint-type=serverless --cloud-run-service=lumina-api
gcloud compute backend-services create lumina-api-backend --global \
  --load-balancing-scheme=EXTERNAL_MANAGED
gcloud compute backend-services add-backend lumina-api-backend --global \
  --network-endpoint-group=lumina-api-neg --network-endpoint-group-region="$REGION"

echo ">> Cloud Armor: bloquea inyeccion SQL y XSS, y limita 600 solicitudes por minuto por IP"
gcloud compute security-policies create lumina-api-armor --description="WAF API Lumina Bank"
gcloud compute security-policies rules create 1000 --security-policy=lumina-api-armor \
  --expression="evaluatePreconfiguredExpr('sqli-v33-stable')" --action=deny-403
gcloud compute security-policies rules create 1001 --security-policy=lumina-api-armor \
  --expression="evaluatePreconfiguredExpr('xss-v33-stable')" --action=deny-403
gcloud compute security-policies rules create 2000 --security-policy=lumina-api-armor \
  --src-ip-ranges="*" --action=throttle --rate-limit-threshold-count=600 \
  --rate-limit-threshold-interval-sec=60 --conform-action=allow --exceed-action=deny-429 \
  --enforce-on-key=IP
gcloud compute backend-services update lumina-api-backend --global \
  --security-policy=lumina-api-armor

gcloud compute url-maps create lumina-api-urlmap --default-service=lumina-api-backend
gcloud compute ssl-certificates create lumina-api-cert --domains="$DOMAIN" --global
gcloud compute target-https-proxies create lumina-api-proxy --url-map=lumina-api-urlmap \
  --ssl-certificates=lumina-api-cert
gcloud compute addresses create lumina-api-ip --global
gcloud compute forwarding-rules create lumina-api-fr --global \
  --load-balancing-scheme=EXTERNAL_MANAGED --address=lumina-api-ip \
  --target-https-proxy=lumina-api-proxy --ports=443

# Cloud Run deja de aceptar trafico directo desde internet: solo via el balanceador
gcloud run services update lumina-api --region="$REGION" \
  --ingress=internal-and-cloud-load-balancing

IP=$(gcloud compute addresses describe lumina-api-ip --global --format='value(address)')
echo ">> Apunta el registro DNS A de $DOMAIN a $IP; el certificado se activa en ~15-60 min."
