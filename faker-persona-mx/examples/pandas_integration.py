"""
Ejemplo de integración con pandas.
"""

import pandas as pd
from faker_persona_mx import PersonaGenerator


def main():
    print("=" * 60)
    print("Faker Persona MX - Integración con Pandas")
    print("=" * 60)

    # Generar datos
    print("\nGenerando 1000 personas...")
    generator = PersonaGenerator(seed=123)
    personas = generator.generate_batch(1000)

    # Convertir a DataFrame
    print("Convirtiendo a DataFrame...")
    df = generator.to_dataframe(personas)

    # Análisis de datos
    print("\n📊 Información del DataFrame:")
    print(f"   Dimensiones: {df.shape}")
    print(f"   Columnas: {list(df.columns)}")

    print("\n📊 Primeras 5 filas:")
    print(df[['nombre', 'apellido_paterno', 'apellido_materno', 'email']].head())

    # Análisis de teléfonos por lada
    print("\n📊 Top 10 ladas más comunes:")
    df['lada'] = df['telefono'].str[:2]
    ladas_count = df['lada'].value_counts().head(10)
    print(ladas_count)

    # Filtrar personas con lada de CDMX (55)
    df_cdmx = df[df['lada'] == '55']
    print(f"\n📊 Personas con teléfono de CDMX: {len(df_cdmx)}")

    # Guardar resultados
    print("\n💾 Guardando análisis...")
    df.to_csv("analisis_personas.csv", index=False)
    df_cdmx.to_csv("personas_cdmx.csv", index=False)

    print("\n✓ Archivos generados:")
    print("  - analisis_personas.csv (1000 registros)")
    print(f"  - personas_cdmx.csv ({len(df_cdmx)} registros)")

    print("\n" + "=" * 60)
    print("Ejemplo completado!")
    print("=" * 60)


if __name__ == "__main__":
    main()
