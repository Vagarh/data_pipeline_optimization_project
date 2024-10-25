## Descripción del proyecto

Este repositorio contiene un pipeline de procesamiento de datos optimizado, diseñado para la gestión y análisis de datos de un sistema de alquiler de películas. El proyecto se desarrolla en cuatro fases, cada una implementando tecnologías y metodologías distintas para mejorar el rendimiento, escalabilidad y organización del proceso de datos.

## Estructura del pipeline

### Fase 1: Programación Lineal
Limpieza inicial y unificación de datos en una estructura lineal utilizando Pandas para asegurar la consistencia del conjunto de datos.

### Fase 2: Programación Orientada a Objetos (POO)
Reestructuración del código en clases y métodos, encapsulando la lógica de limpieza, combinación y almacenamiento en un modelo orientado a objetos.

### Fase 3: Adaptación a Apache Spark
Migración del código a PySpark para el procesamiento distribuido, aumentando la eficiencia en grandes volúmenes de datos.

### Fase 4: Arquitectura en AWS
Implementación de una arquitectura híbrida serverless en AWS, utilizando AWS DataSync, Glue, Athena, y SageMaker para orquestar y visualizar el flujo de datos.

## Objetivo del proyecto

El objetivo es crear un pipeline escalable, modular y fácilmente adaptable que permita responder preguntas de negocio clave, como tendencias de rentas, rentabilidad de películas y comportamiento de clientes. El proyecto utiliza Looker Studio y Python para visualización y análisis avanzado de los datos procesados.

## Estructura del Repositorio

```plaintext
project_name/
│
├── README.md                     # Descripción general del proyecto
├── requirements.txt              # Lista de dependencias del proyecto
├── .gitignore                    # Archivos y carpetas a ignorar por git
│
├── docs/                         # Documentación adicional
│   ├── arquitectura_aws.png      # Imagen de arquitectura híbrida en AWS
│   ├── diagrama_er.png           # Diagrama entidad-relación de las tablas
│   └── proyecto_reporte.pdf      # Informe detallado del proyecto (opcional)
│
├── data/                         # Almacenamiento de datos utilizados en el proyecto
│   ├── raw/                      # Datos sin procesar
│   └── processed/                # Datos procesados
│
├── notebooks/                    # Notebooks de Jupyter o Colab para cada fase
│   ├── fase_1_programacion_lineal.ipynb
│   ├── fase_2_poo.ipynb
│   ├── fase_3_spark.ipynb
│   └── fase_4_aws.ipynb
│
├── src/                          # Código fuente del proyecto
│   ├── fase_1_lineal/            # Código lineal de la fase 1
│   │   └── data_cleaning.py      # Script de limpieza de datos
│   ├── fase_2_poo/               # Código reestructurado en POO
│   │   └── film_data_processor.py
│   ├── fase_3_spark/             # Código en PySpark
│   │   └── spark_processing.py
│   └── fase_4_aws/               # Código y scripts de arquitectura en AWS
│       ├── glue_scripts/         # Scripts de Glue (ETL)
│       ├── step_functions/       # Definición de Step Functions
│       └── sagemaker/            # Scripts para SageMaker
│
├── config/                       # Configuración del proyecto (opcional)
│   ├── aws_config.json           # Configuración de AWS (ejemplo)
│   └── spark_config.json         # Configuración para Spark
│
└── tests/                        # Pruebas para las diferentes fases
    ├── test_data_cleaning.py     # Pruebas de limpieza de datos (fase 1)
    ├── test_poo_processing.py    # Pruebas para el procesamiento POO (fase 2)
    ├── test_spark_processing.py  # Pruebas para el procesamiento en Spark (fase 3)
    └── test_aws_architecture.py  # Pruebas para los scripts en AWS (fase 4)

