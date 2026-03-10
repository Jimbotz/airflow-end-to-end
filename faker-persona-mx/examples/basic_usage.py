"""
Ejemplo básico de uso de Faker Persona MX.
"""

from faker_persona_mx import PersonaGenerator


def main():
    print("=" * 60)
    print("Faker Persona MX - Ejemplo Básico")
    print("=" * 60)

    # 1. Crear generador
    print("\n1. Inicializando generador...")
    generator = PersonaGenerator(seed=42)

    # 2. Generar una persona
    print("\n2. Generando una persona:")
    persona = generator.generate_one()
    print(f"   Nombre completo: {persona.nombre_completo()}")
    print(f"   Email: {persona.email}")
    print(f"   Teléfono: {persona.telefono}")
    print(f"   CURP: {persona.curp}")
    print(f"   RFC: {persona.rfc}")

    # 3. Generar múltiples personas
    print("\n3. Generando 5 personas:")
    personas = generator.generate_batch(5)
    for i, p in enumerate(personas, 1):
        print(f"   {i}. {p.nombre_completo()} - {p.email}")

    # 4. Exportar a CSV
    print("\n4. Exportando 100 personas a CSV...")
    personas_csv = generator.generate_batch(100)
    generator.export_to_csv(personas_csv, "personas_ejemplo.csv")
    print("   ✓ Archivo 'personas_ejemplo.csv' creado")

    # 5. Exportar a JSON
    print("\n5. Exportando 50 personas a JSON...")
    personas_json = generator.generate_batch(50)
    generator.export_to_json(personas_json, "personas_ejemplo.json")
    print("   ✓ Archivo 'personas_ejemplo.json' creado")

    print("\n" + "=" * 60)
    print("Ejemplo completado!")
    print("=" * 60)


if __name__ == "__main__":
    main()
