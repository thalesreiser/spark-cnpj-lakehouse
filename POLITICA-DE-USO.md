# Política de uso: spark-cnpj-lakehouse

Esta política explica, em linguagem direta, como este repositório pode ser usado. Os termos com valor jurídico estão no arquivo [`LICENSE`](LICENSE). Se houver diferença entre os dois, vale o `LICENSE`.

**Autor e titular dos direitos:** Thales Reiser Pedreira · [GitHub](https://github.com/thalesreiser) · [LinkedIn](https://www.linkedin.com/in/thalesreiser/)

## Finalidade do projeto

Este é um projeto de portfólio, criado para demonstrar competências de engenharia de dados com um processo de engenharia de requisitos documentado. Ele não é um produto e não se destina a uso em produção.

## O que você pode e não pode fazer

| ✅ Permitido sem pedir | ❌ Exige autorização por escrito do autor |
|---|---|
| Ler o código e a documentação | Copiar ou redistribuir o código, no todo ou em parte |
| Clonar e rodar localmente para avaliar o trabalho | Publicar versões modificadas ou obras derivadas |
| Usar em processo seletivo ou revisão técnica do autor | Uso comercial ou dentro de produtos e serviços |
| Citar trechos curtos com crédito e link | Apresentar o projeto como de sua autoria |
| Abrir issues com dúvidas e sugestões | Treinar ou avaliar modelos de IA com o código |

## Dados

- O repositório **não contém dados brutos**. Os dados são baixados das fontes públicas oficiais em tempo de execução, e apenas pequenas amostras para testes são versionadas.
- Os dados pertencem às respectivas fontes e seguem os termos de uso delas, que esta política não altera.
- Quem executa o projeto é responsável por usar os dados de acordo com a **LGPD (Lei nº 13.709/2018)** e com os termos das fontes.
- **Dados de CNPJ (Receita Federal):** os dados de sócios trazem CPF parcialmente mascarado pela própria fonte. É **proibido** usar este projeto para tentar reidentificar pessoas físicas, cruzar os dados com outras bases para esse fim ou montar perfis de pessoas (restrição RST-01 do projeto).
- O vínculo de sócios homônimos (RF-10) é um exercício técnico de *record linkage* com revisão amostral, não uma ferramenta de identificação de pessoas.

## Segurança

- Nenhum segredo, chave ou credencial deve ser versionado. Crie o seu `.env` (ignorado pelo Git) a partir do `.env.example`.
- O pre-commit roda o **gitleaks** para detectar segredos antes de cada commit.
- Se encontrar uma vulnerabilidade, **não abra issue pública**: fale direto com o autor pelo GitHub ou pelo LinkedIn.

## Contribuições

Sugestões e issues são bem-vindas. Ao contribuir, você concorda que o autor pode usar a sua contribuição livremente, conforme a cláusula 4 do [`LICENSE`](LICENSE).

## Pedidos de autorização

Para qualquer uso fora da coluna "Permitido", fale com o autor pelo [GitHub](https://github.com/thalesreiser) ou pelo [LinkedIn](https://www.linkedin.com/in/thalesreiser/) e descreva o uso pretendido.
