import pandas as pd
import random
from faker_persona_mx import PersonaGenerator

def main():
    print("=" * 60)

    # Generar una semilla válida (entre 1 y el límite máximo de 32 bits) -- el cero a veces da errores y no se genera de manera correcta so 
    
    # Inicializar generador
    for _ in range(100):
        semilla_dinamica = random.randint(1, 4294967295)
        generator = PersonaGenerator(seed=semilla_dinamica)
        print(f"\nInicializando generador con seed: {semilla_dinamica}")
        # Generar una persona
        persona = generator.generate_one()
        print(f"Nombre: {persona.nombre}")  # "Juan Carlos García López"
        print(f"Apellido Paterno: {persona.apellido_paterno}")
        print(f"Apellido Materno: {persona.apellido_materno}")
        print(f"CURP: {persona.curp}")               # "GALJ850815HDFRPN09"
        print(f"RFC: {persona.rfc}")                # "GALJ850815ABC"
        print(f"Email: {persona.email}")              # "juan.carlos@example.com"
        print(f"Teléfono: {persona.telefono}")           # "5512345678"
        edad = random.randint(18, 90)
        print(f"Edad: {edad} años\n")

if __name__ == "__main__":
    main()