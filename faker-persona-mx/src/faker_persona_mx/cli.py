"""
CLI profesional para Faker Persona MX usando Typer.
"""

import typer
from typing import Optional, List
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import track
import sys

from .core.persona import PersonaGenerator
from .utils.logger import get_logger

app = typer.Typer(
    name="faker-persona-mx",
    help="Generador de datos ficticios de personas mexicanas",
    add_completion=True
)
console = Console()
logger = get_logger(__name__)


@app.command()
def generate(
    count: int = typer.Argument(10, help="Cantidad de personas a generar"),
    output: Optional[str] = typer.Option(None, help="Archivo de salida (CSV o JSON)"),
    format: str = typer.Option("json", help="Formato de salida: json, csv, o table"),
    seed: int = typer.Option(0, help="Semilla para reproducibilidad"),
    no_cache: bool = typer.Option(False, help="Deshabilitar caché de datasets")
):
    """
    Genera personas ficticias mexicanas.

    Ejemplos:

        # Generar 100 personas y mostrar en tabla
        $ faker-persona-mx generate 100 --format table

        # Generar 500 personas y exportar a CSV
        $ faker-persona-mx generate 500 -o personas.csv -f csv

        # Generar con semilla específica
        $ faker-persona-mx generate 50 -s 42 -o datos.json
    """
    try:
        # Validaciones
        if count <= 0:
            console.print("[red]Error: La cantidad debe ser mayor a 0[/red]")
            raise typer.Exit(1)

        if format not in ["json", "csv", "table"]:
            console.print(f"[red]Error: Formato '{format}' no válido. Use: json, csv, o table[/red]")
            raise typer.Exit(1)

        # Inicializar generador
        console.print(f"[cyan]Inicializando generador (seed={seed})...[/cyan]")
        generator = PersonaGenerator(
            seed=seed,
            cache_enabled=not no_cache
        )

        # Generar personas
        console.print(f"[green]Generando {count} personas...[/green]")
        show_progress = True  # Siempre mostrar progreso
        if show_progress:
            personas = []
            for persona in track(
                generator.generate_stream(count),
                total=count,
                description="Generando..."
            ):
                personas.append(persona)
        else:
            personas = generator.generate_batch(count)

        # Exportar o mostrar
        if output:
            _export_data(generator, personas, output, format)
        else:
            _display_data(personas, format)

        console.print(f"[bold green]✓ {len(personas)} personas generadas exitosamente[/bold green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Error en comando generate")
        raise typer.Exit(1)


@app.command()
def info():
    """Muestra información sobre los datasets disponibles."""
    try:
        from .utils.config import config

        console.print("\n[bold cyan]Faker Persona MX - Información de Datasets[/bold cyan]\n")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Dataset", style="cyan")
        table.add_column("Archivo", style="white")
        table.add_column("Registros", style="green", justify="right")

        # Cargar y mostrar info
        from .data.loader import DataLoader
        loader = DataLoader(cache_enabled=False)
        loader.load_all()

        table.add_row("Nombres", config.NOMBRES_FILE, str(len(loader.nombres)))
        table.add_row("Apellidos Paternos", config.APELLIDOS_PATERNOS_FILE, str(len(loader.apellidos_paternos)))
        table.add_row("Apellidos Maternos", config.APELLIDOS_MATERNOS_FILE, str(len(loader.apellidos_maternos)))
        table.add_row("Dominios Email", config.EMAIL_DOMAINS_FILE, str(len(loader.email_domains)))
        table.add_row("Usuarios Email", config.EMAIL_USERNAMES_FILE, str(len(loader.email_usernames)))
        table.add_row("Estados (Ladas)", config.LADAS_MEXICO_FILE, str(len(loader.ladas_por_estado)))

        console.print(table)
        console.print(f"\n[dim]Directorio de datos: {config.DATASETS_DIR}[/dim]\n")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def version():
    """Muestra la versión del paquete."""
    from . import __version__
    console.print(f"[bold cyan]Faker Persona MX[/bold cyan] versión [green]{__version__}[/green]")


@app.command()
def clear_cache(
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Eliminar caché sin confirmación"
    )
):
    """
    Elimina los archivos de caché generados.

    Los archivos de caché se crean para mejorar el rendimiento al generar
    grandes cantidades de datos con la misma semilla.

    Ejemplos:

        # Eliminar caché con confirmación
        $ faker-persona-mx clear-cache

        # Eliminar caché sin confirmación
        $ faker-persona-mx clear-cache --force
    """
    try:
        from .utils.config import config
        import shutil

        cache_dir = config.CACHE_DIR

        if not cache_dir.exists():
            console.print("[yellow]No existe directorio de caché[/yellow]")
            return

        # Contar archivos en caché
        cache_files = list(cache_dir.glob("*.csv"))

        if not cache_files:
            console.print("[yellow]No hay archivos de caché para eliminar[/yellow]")
            return

        # Confirmación
        if not force:
            console.print(f"\n[yellow]Se eliminarán {len(cache_files)} archivo(s) de caché:[/yellow]")
            for file in cache_files[:5]:  # Mostrar solo los primeros 5
                console.print(f"  - {file.name}")
            if len(cache_files) > 5:
                console.print(f"  ... y {len(cache_files) - 5} más")

            confirm = typer.confirm("\n¿Desea continuar?")
            if not confirm:
                console.print("[yellow]Operación cancelada[/yellow]")
                raise typer.Exit(0)

        # Eliminar archivos
        deleted = 0
        for file in cache_files:
            try:
                file.unlink()
                deleted += 1
            except Exception as e:
                console.print(f"[red]Error al eliminar {file.name}: {e}[/red]")

        console.print(f"[bold green]✓ {deleted} archivo(s) de caché eliminados exitosamente[/bold green]")
        console.print(f"[dim]Directorio de caché: {cache_dir}[/dim]")

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Error en comando clear-cache")
        raise typer.Exit(1)


def _export_data(generator: PersonaGenerator, personas: List, output: str, format: str):
    """Exporta datos al formato especificado."""
    try:
        if format == "csv" or output.endswith(".csv"):
            generator.export_to_csv(personas, output)
            console.print(f"[green]Datos exportados a {output}[/green]")
        else:  # json
            generator.export_to_json(personas, output)
            console.print(f"[green]Datos exportados a {output}[/green]")
    except Exception as e:
        console.print(f"[red]Error al exportar: {e}[/red]")
        raise


def _display_data(personas: List, format: str):
    """Muestra datos en consola."""
    if format == "table":
        _display_table(personas[:20])  # Limitar a 20 para legibilidad
        if len(personas) > 20:
            console.print(f"\n[dim]Mostrando 20 de {len(personas)} personas. Use -o para exportar todas.[/dim]")
    elif format == "json":
        import json
        data = [p.model_dump(mode='json') for p in personas]
        console.print_json(json.dumps(data, ensure_ascii=False, indent=2))
    elif format == "csv":
        # Mostrar como CSV en consola
        import io
        import pandas as pd
        df = pd.DataFrame([p.model_dump(mode='json') for p in personas])
        console.print(df.to_csv(index=False))


def _display_table(personas: List):
    """Muestra personas en formato tabla."""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Nombre Completo", style="cyan", width=25)
    table.add_column("Email", style="white", width=25)
    table.add_column("Teléfono", style="green", width=12)
    table.add_column("Ciudad", style="blue", width=15)
    table.add_column("Estado", style="yellow", width=15)
    table.add_column("Sexo", style="magenta", width=8)
    table.add_column("CURP", style="bright_black", width=18)

    for persona in personas:
        table.add_row(
            persona.nombre_completo(),
            persona.email,
            persona.telefono,
            persona.ciudad,
            persona.estado_nacimiento,
            persona.sexo,
            persona.curp
        )

    console.print(table)


def main():
    """Punto de entrada principal."""
    app()


if __name__ == "__main__":
    main()
