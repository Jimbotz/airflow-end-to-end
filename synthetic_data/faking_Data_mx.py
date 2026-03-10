from faker_persona_mx import PersonaGenerator

generator = PersonaGenerator(seed=42)


persona = generator.generate_one()
print(persona.nombre_completo())  # "Juan Carlos García López"
print(persona.curp)               # "GALJ850815HDFRPN09"
print(persona.rfc)                # "GALJ850815ABC"
print(persona.email)              # "juan.carlos@example.com"
print(persona.telefono)           # "5512345678"



# Generar múltiples personas
personas = generator.generate_batch(100)

# Exportar a CSV
generator.export_to_csv(personas, "personas.csv")

# Exportar a JSON
generator.export_to_json(personas, "personas.json")

# Convertir a DataFrame
df = generator.to_dataframe(personas)