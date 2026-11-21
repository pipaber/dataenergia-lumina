"""Arma docs/10_guia_implementacion.md a partir de book/guia_implementacion.plantilla.txt.

La plantilla tiene el texto de la guía y tres tipos de marcas, cada una en su propia línea:

  {{codigo ruta | desde: texto | hasta: texto | mas: 1 | lenguaje: bash}}
      Copia del archivo `ruta` desde la primera línea que contiene `desde` hasta la primera línea
      posterior que contiene `hasta` (más `mas` líneas). Así el fragmento siempre es el código real.
  {{salida nombre}}
      Inserta docs/evidencia/<nombre>.txt: la salida real de un comando o consulta.
  {{captura archivo.png | Pie de la imagen}}
      Inserta docs/img/guia/<archivo> si existe; si no, deja anotado que la captura está pendiente.

Uso:  python book/armar_guia.py
"""
from __future__ import annotations

import re
import textwrap
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
PLANTILLA = RAIZ / "book" / "guia_implementacion.plantilla.txt"
DESTINO = RAIZ / "docs" / "10_guia_implementacion.md"
AVISO = "<!-- Generado por book/armar_guia.py desde book/guia_implementacion.plantilla.txt: no editar aquí. -->\n\n"


def opciones(texto: str) -> tuple[str, dict[str, str]]:
    partes = [p.strip() for p in texto.split(" | ")]
    return partes[0], dict(p.split(": ", 1) for p in partes[1:])


def codigo(args: str) -> str:
    ruta, op = opciones(args)
    lineas = (RAIZ / ruta).read_text(encoding="utf-8").splitlines()
    ini = next((i for i, l in enumerate(lineas) if op["desde"] in l), None)
    if ini is None:
        raise SystemExit(f"{ruta}: no se encontró el inicio «{op['desde']}»")
    fin = next((i for i in range(ini, len(lineas)) if op["hasta"] in lineas[i]), None)
    if fin is None:
        raise SystemExit(f"{ruta}: no se encontró el final «{op['hasta']}»")
    fin += int(op.get("mas", 0))
    fragmento = textwrap.dedent("\n".join(lineas[ini:fin + 1]))
    return (f"```{op.get('lenguaje', 'bash')}\n{fragmento}\n```\n\n"
            f"*Fragmento de [`{ruta}`](../{ruta}), líneas {ini + 1} a {fin + 1}.*")


def salida(nombre: str) -> str:
    return "```text\n" + (RAIZ / "docs" / "evidencia" / f"{nombre.strip()}.txt").read_text(encoding="utf-8").rstrip() + "\n```"


def captura(args: str) -> str:
    archivo, pie = [p.strip() for p in args.split(" | ", 1)]
    if (RAIZ / "docs" / "img" / "guia" / archivo).exists():
        return f"![{pie}](img/guia/{archivo})"
    return f"*Captura pendiente: {pie}.*"


def main() -> None:
    marcas = {"codigo": codigo, "salida": salida, "captura": captura}
    pendientes = 0
    lineas = []
    for linea in PLANTILLA.read_text(encoding="utf-8").splitlines():
        m = re.fullmatch(r"\{\{(codigo|salida|captura) (.+)\}\}", linea.strip())
        bloque = marcas[m.group(1)](m.group(2)) if m else linea
        pendientes += bloque.startswith("*Captura pendiente")
        lineas.append(bloque)
    DESTINO.write_text(AVISO + "\n".join(lineas) + "\n", encoding="utf-8", newline="\n")
    print(f"[guía] {DESTINO.name} generada; capturas pendientes: {pendientes}")


if __name__ == "__main__":
    main()
