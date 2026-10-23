# SI7006 · Big Data Labs 2026-2

Laboratorios prácticos para el curso **Almacenamiento y Procesamiento de Grandes Datos** — Maestría en Ingeniería de Datos, EAFIT.

**Caso de uso:** PayTrack Colombia — plataforma de pagos digitales con 500K transacciones diarias, análisis de fraude en tiempo real y conciliación batch.

> El Lab 2 corre en AWS (S3, Glue, Kinesis, Athena) con Delta Lake, desplegado con Serverless Framework v3,
> y se puede desarrollar completo en local con Docker (Spark + Kafka).

---

## Laboratorios

| Lab | Tema | Infraestructura | Peso | Entrega |
|-----|------|-----------------|------|---------|
| [Lab 1](labs/01-modelado/) | Modelado OLTP → OLAP: de transacciones a esquema estrella (SCD2) | Docker + PostgreSQL | 15% | Vie 23 oct |
| [Lab 2](labs/02-lakehouse/) | Lakehouse end-to-end: batch (S3 → Medallion con Delta Lake en Glue → Athena) y streaming (Kafka / Kinesis → Spark Structured Streaming → Delta) | Docker (local) + AWS | 30% | Vie 6 nov |

## Cómo funcionan los labs

Cada lab es un **reto**: el código mecánico (conexiones, infraestructura, lectura y escritura de archivos) está dado, y las partes que aplican los temas del curso están marcadas con `TODO` para que las completes tú.

Cada lab incluye **autoevaluación**: tests, scripts de validación o validaciones dentro de los jobs que te dicen si tu solución es correcta (`✓`) o qué falla (`✗`). No los modifiques: son los mismos criterios con los que se califica la parte técnica. Las verificaciones confirman que tu código funciona; las decisiones de diseño y su justificación las evalúa tu informe.

---

## Requisitos previos

- Docker Desktop ≥ 4.x con WSL 2 (Windows), o Docker Desktop / colima (Mac)
- Python ≥ 3.11 y [`uv`](https://docs.astral.sh/uv/)
- Cuenta de AWS con CLI configurada (`aws configure`) — Lab 2
- Node.js ≥ 20 y Serverless Framework v3 (`npm install -g serverless@3`) — Lab 2

---

## Setup rápido

```bash
# 1. Clonar el repositorio
git clone https://github.com/asacrea/big-data-labs-2026-2.git
cd big-data-labs-2026-2

# 2. Instalar dependencias de Python (incluye pytest)
uv sync --extra dev

# 3. Levantar PostgreSQL y pgAdmin (Lab 1)
docker compose -f .docker/docker-compose.yml up -d postgres pgadmin

# Lab 2 (streaming): levantar Kafka
docker compose -f .docker/docker-compose.yml up -d kafka

# 4. Lab 2: variables de entorno de AWS
cp .envs/aws.env.example .envs/aws.env
# Edita .envs/aws.env con tus credenciales y tu STUDENT_ID
```

---

## Caso de uso: PayTrack Colombia

PayTrack es una fintech colombiana con los siguientes flujos de datos:

| Fuente | Volumen | Frecuencia |
|--------|---------|------------|
| Transacciones POS / app | 500K / día | En tiempo real |
| Eventos de fraude | 2K / día | En tiempo real |
| Conciliación bancaria | 1 archivo / banco | Batch nocturno |
| Reportes regulatorios | Mensual | Batch |

Los labs van construyendo la arquitectura completa de esta plataforma sesión a sesión.

---

## Estructura del repositorio

```
.
├── .docker/          # Dockerfile y docker-compose
├── .envs/            # Variables de entorno (no se suben a git)
├── labs/
│   ├── shared/       # Generador de datos sintéticos de PayTrack
│   ├── 01-modelado/  # Lab 1: OLTP → OLAP (PostgreSQL)
│   └── 02-lakehouse/ # Lab 2: Lakehouse batch y streaming (S3 + Delta + Glue + Kafka/Kinesis + Athena)
└── pyproject.toml    # Dependencias de Python (uv)
```

---

## Licencia

MIT — Andrés Sacre Alzate, EAFIT 2026
