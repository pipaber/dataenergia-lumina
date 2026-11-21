"""Arma el documento de la Entrega 3 a partir de docs/10_guia_implementacion.md.

Le pone carátula y cambia los enlaces relativos por enlaces a GitHub, porque el Word se lee fuera
del repositorio. Las imágenes se quedan como rutas locales para que queden incrustadas.

Uso:  python book/armar_guia.py && python book/armar_entrega3.py
      cd docs/entrega3 && quarto pandoc entrega3_implementacion.md -o entrega3_implementacion.docx --toc --resource-path=..
"""
from __future__ import annotations

import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
GUIA = RAIZ / "docs" / "10_guia_implementacion.md"
DESTINO = RAIZ / "docs" / "entrega3" / "entrega3_implementacion.md"
GITHUB = "https://github.com/pipaber/dataenergia-lumina/blob/main"
CARATULA = """---
title: "DataEnergía Metropolitan · Lumina Energía"
subtitle: "Entrega 3: guía de implementación paso a paso, con evidencias"
author: "Yessenia Chirinos, Edgard Inga, Diego Sánchez y Piero Palacios"
date: 2026-11-21
lang: es
---
"""


def main() -> None:
    lineas = GUIA.read_text(encoding="utf-8").splitlines()
    cuerpo = "\n".join(l for l in lineas if not l.startswith(("<!-- Generado", "# Guía de implementación")))
    cuerpo = re.sub(r"\]\(\.\./([^)]+)\)", rf"]({GITHUB}/\1)", cuerpo)
    cuerpo = re.sub(r"(?<!!)\[([^\]]+)\]\((?!https?://|#)([^)]+)\)", rf"[\1]({GITHUB}/docs/\2)", cuerpo)
    DESTINO.write_text(CARATULA + cuerpo.lstrip("\n") + "\n", encoding="utf-8", newline="\n")
    print(f"[entrega 3] {DESTINO.name} generado")


if __name__ == "__main__":
    main()
