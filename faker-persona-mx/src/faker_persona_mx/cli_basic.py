"""
CLI básico para Faker Persona MX usando argparse.
"""

import argparse
import sys
from . import __version__
from .core.persona import PersonaGenerator


def main():
    parser = argparse.ArgumentParser(
        description="Generador de datos ficticios de personas mexicanas"
    )

    subparsers = parser.add_subparsers(dest="command", help="Comandos disponibles")

    # Comando generate
    gen_parser = subparsers.add_parser("generate", help="Generar personas ficticias")
    gen_parser.add_argument("count", type=int, nargs="?", default=10, help="Cantidad de personas a generar")
    gen_parser.add_argument("--output", "-o", help="Archivo de salida (CSV o JSON)")
    gen_parser.add_argument("--seed", "-s", type=int, default=0, help="Semilla para reproducibilidad")
    gen_parser.add_argument("--format", "-f", choices=["json", "csv"], default="json", help="Formato de salida")

    # Comando version
    ver_parser = subparsers.add_parser("version", help="Mostrar versión")

    # Comando info
    info_parser = subparsers.add_parser("info", help="Información de datasets")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "version":
        print(f"Faker Persona MX v{__version__}")
        return 0

    elif args.command == "info":
        from .data.loader import DataLoader
        from .utils.config import config

        print("\n=== Faker Persona MX - Información de Datasets ===\n")
        loader = DataLoader(cache_enabled=False)
        loader.load_all()

        print(f"Nombres:              {len(loader.nombres):,}")
        print(f"Apellidos Paternos:   {len(loader.apellidos_paternos):,}")
        print(f"Apellidos Maternos:   {len(loader.apellidos_maternos):,}")
        print(f"Dominios Email:       {len(loader.email_domains):,}")
        print(f"Usuarios Email:       {len(loader.email_usernames):,}")
        print(f"Estados (Ladas):      {len(loader.ladas_por_estado)}")
        print(f"\nDirectorio datos: {config.DATASETS_DIR}\n")
        return 0

    elif args.command == "generate":
        try:
            print(f"Generando {args.count} personas (seed={args.seed})...")

            generator = PersonaGenerator(seed=args.seed)
            personas = generator.generate_batch(args.count)

            if args.output:
                if args.format == "csv" or args.output.endswith('.csv'):
                    generator.export_to_csv(personas, args.output)
                else:
                    generator.export_to_json(personas, args.output)
                print(f"✓ {len(personas)} personas exportadas a {args.output}")
            else:
                # Mostrar primeras 5
                print("\nPersonas generadas:\n")
                for i, p in enumerate(personas[:5], 1):
                    print(f"{i}. {p.nombre_completo()}")
                    print(f"   Email: {p.email}")
                    print(f"   Tel: {p.telefono} | CURP: {p.curp} | RFC: {p.rfc}\n")

                if len(personas) > 5:
                    print(f"... y {len(personas) - 5} más\n")
                print(f"Total: {len(personas)} personas generadas")
                print("Usa --output para exportar a archivo\n")

            return 0

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
