"""Baixa os jars de Delta Lake e s3a para o diretório de jars do Spark (build da imagem).

Uso: python baixar_jars.py <diretorio_jars>

As versões vêm de `cnpj_lakehouse.versoes` (fonte única, RNF-05). Cada jar é conferido com o
SHA-1 publicado no Maven Central. O script também confere se a versão do Hadoop embarcada no
PySpark é a esperada, para que `hadoop-aws` seja compatível.
"""

from __future__ import annotations

import hashlib
import sys
import urllib.request
from pathlib import Path

from cnpj_lakehouse.versoes import HADOOP_VERSION, JARS


def _baixar(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as resposta:  # noqa: S310 (URL fixa)
        conteudo: bytes = resposta.read()
    return conteudo


def main(destino: Path) -> None:
    hadoop_embarcado = destino / f"hadoop-client-api-{HADOOP_VERSION}.jar"
    if not hadoop_embarcado.exists():
        raise SystemExit(
            f"Hadoop {HADOOP_VERSION} não encontrado em {destino}; "
            "ajuste HADOOP_VERSION em cnpj_lakehouse/versoes.py"
        )
    for jar in JARS:
        conteudo = _baixar(jar.url)
        esperado = _baixar(jar.url + ".sha1").decode().split()[0].strip()
        obtido = hashlib.sha1(conteudo).hexdigest()  # noqa: S324 (checksum do Maven)
        if obtido != esperado:
            raise SystemExit(f"SHA-1 divergente para {jar.nome_jar}: {obtido} != {esperado}")
        (destino / jar.nome_jar).write_bytes(conteudo)
        print(f"ok {jar.coordenada} ({len(conteudo) // 1024} KiB)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("uso: python baixar_jars.py <diretorio_jars>")
    main(Path(sys.argv[1]))
