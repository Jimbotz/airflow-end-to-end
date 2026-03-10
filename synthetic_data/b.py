import faker_persona_mx

generator = faker_persona_mx.PersonaGenerator(seed=41)
personas = generator.generate_batch(10)

for persona in personas:
    print(f"Nombre: {persona.nombre}")
    print(f"Apellido Paterno: {persona.apellido_paterno}")
    print(f"Apellido Materno: {persona.apellido_materno}")
    print(f"CURP: {persona.curp}")
    print(f"RFC: {persona.rfc}")
    print(f"Email: {persona.email}")    
    print(f"Teléfono: {persona.telefono}\n")