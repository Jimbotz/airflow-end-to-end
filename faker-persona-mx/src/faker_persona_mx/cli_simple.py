"""
CLI simplificado para Faker Persona MX.
"""

import typer
from typing import Optional
from rich.console import Console

from .core.persona import PersonaGenerator

app = typer.Typer(help="Generador de datos ficticios de personas mexicanas")
console = Console()


@app.command()
def generate(
    count: int = 10,
    output: Optional[str] = None,
    seed: int = 0
):
    """Genera personas ficticias mexicanas."""
    try:
        console.print(f"[cyan]Generando {count} personas (seed={seed})...[/cyan]")

        generator = PersonaGenerator(seed=seed)
        personas = generator.generate_batch(count)

        if output:
            if output.endswith('.csv'):
                generator.export_to_csv(personas, output)
            else:
                generator.export_to_json(personas, output)
            console.print(f"[green]✓ {len(personas)} personas exportadas a {output}[/green]")
        else:
            # Mostrar primeras 5
            for i, p in enumerate(personas[:5], 1):
                console.print(f"{i}. {p.nombre_completo()} - {p.email} - {p.telefono}")
            if len(personas) > 5:
                console.print(f"[dim]... y {len(personas) - 5} más[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def version():
    """Muestra la versión."""
    from . import __version__
    console.print(f"Faker Persona MX v{__version__}")


def main():
    app()


if __name__ == "__main__":
    main()
