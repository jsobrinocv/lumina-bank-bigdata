# Agente conversacional – Lumina Bank (Google ADK + Gemini)

Asistente de riesgo y operaciones construido con **Agent Development Kit (ADK)** y **Gemini en
Vertex AI**. No se conecta a las bases de datos: consume la **API de Lumina** en Cloud Run, como
cualquier otra aplicación consumidora (mismo control de acceso, mismas reglas).

Herramientas del agente: `consultar_perfil_cliente`, `listar_alertas`, `plan_de_liquidez`,
`kpis_operativos`, `listar_distritos`, `registrar_transaccion`.

## Ejecución en Cloud Shell

```bash
cd ~/lumina-bank-bigdata/agente
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp lumina_agente/.env.example lumina_agente/.env     # completar URL y clave de la API
adk web --port 8000
```

Luego **Vista previa en la Web → Cambiar puerto → 8000** y elegir `lumina_agente`.

Modelo: **Gemini 4 Hydrogen** (`gemini-4-hydrogen`) en la multirregión **`us`**: no está disponible en `us-central1`, y la multirregión mantiene el procesamiento dentro de EE. UU., igual que los datasets de BigQuery. Se descarta `global` porque no garantiza la ubicación del procesamiento. Con `LUMINA_RAZONAMIENTO=LOW` las respuestas son más rápidas en la demo.

Requisitos: API **Vertex AI** habilitada y rol **Usuario de Vertex AI** para quien ejecuta el agente. Usa una versión reciente de `google-adk` (`pip install -U google-adk`).

## Preguntas de ejemplo para la demo

- "¿Cuáles son las últimas 5 transacciones bloqueadas y por qué?"
- "¿Cuál es el último estado del cliente C1231006815?"
- "¿Cuánto efectivo debo provisionar esta semana en Valle Sur?"
- "Simula una transferencia de 9 000 del cliente C1231006815 que tiene saldo 9 000 y queda en 0."
