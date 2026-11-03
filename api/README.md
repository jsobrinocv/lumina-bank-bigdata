# Capa de exposición (Entrega 3 – pendiente)

Aquí irá la API sencilla que expone los datos procesados (requisito P3), por ejemplo:

- `GET /cliente/{id}/riesgo` → lee el perfil del cliente en Bigtable (con caché Memorystore).
- `GET /liquidez/{distrito}` → lee `lumina_gold.gold_plan_liquidez_v1` en BigQuery.
- `GET /kpis/{fecha}` → lee `lumina_gold.gold_kpi_operativo_diario_v1`.

Despliegue previsto: Cloud Run.
