#!/usr/bin/env python3
"""Test CLI to debug Typer issue."""

import typer
from typing import Optional

app = typer.Typer()

@app.command()
def generate(
    count: int = typer.Argument(10, help="Cantidad de personas a generar"),
    output: Optional[str] = typer.Option(None, help="Archivo de salida (CSV o JSON)"),
    format: str = typer.Option("json", help="Formato de salida: json, csv, o table"),
):
    print(f"Count: {count}, Output: {output}, Format: {format}")

def main():
    app()

if __name__ == "__main__":
    main()
