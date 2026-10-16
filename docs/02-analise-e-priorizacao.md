# 02 - Análise e priorização (MoSCoW)

## Requisitos funcionais
| ID | Requisito | Prior. | Critério de aceite | Verificação |
|---|---|---|---|---|
| RF-01 | Baixar e descompactar os arquivos de forma reprodutível e parametrizada por competência (mês) | M | Dado uma competência, quando executo `make download`, então os arquivos ficam em `data/raw/<competência>/` com checksum registrado | Teste de integração |
| RF-02 | Ingerir os CSVs na camada bronze (Delta) com schema explícito e metadados de carga | M | Dado um CSV, então a tabela bronze contém todas as linhas e as colunas `_ingest_ts` e `_source_file` | Contagem origem × destino |
| RF-03 | Gerar a camada silver tipada, deduplicada e normalizada (CNPJ, CNAE, datas) | M | Então não há CNPJ duplicado na chave de negócio e 100% das datas válidas estão tipadas como `date` | Testes unitários e quality checks |
| RF-04 | Aplicar carga incremental por MERGE ao chegar uma nova competência | M | Dada nova competência, quando reexecuto, então registros alterados são atualizados e novos inseridos sem duplicar | Teste de idempotência |
| RF-05 | Produzir tabelas gold: empresas por CNAE/UF/ano, porte, situação cadastral | M | Então os totais da gold batem com a silver (reconciliação) | Teste de reconciliação |
| RF-06 | Demonstrar time travel com consulta à versão anterior da tabela | S | Então consigo reproduzir o resultado de uma consulta da versão N-1 | Notebook documentado |
| RF-07 | Compactar arquivos pequenos e limpar versões antigas (OPTIMIZE / VACUUM) | S | Então o número de arquivos por partição cai e o tempo de leitura é medido antes e depois | Benchmark |
| RF-08 | Registrar benchmark antes e depois das otimizações (tempo, shuffle, spill, nº de arquivos) | M | Então existe `docs/benchmark.md` com tabela comparativa e prints do Spark UI | Revisão |
| RF-09 | Testes unitários das transformações com DataFrames pequenos | M | Então `pytest` executa sem acessar a base completa | CI |
| RF-10 | Vincular sócios homônimos por similaridade (record linkage simplificado) | C | Então pares candidatos são gerados com score e revisão amostral | Amostra manual |

## Requisitos não funcionais, regras e restrições
| ID | Tipo | Descrição |
|---|---|---|
| RNF-01 | Desempenho | Redução ≥ 30% do tempo do job principal após tuning (meta) |
| RNF-02 | Portabilidade | Ambiente completo via Docker Compose (Spark master e workers, MinIO), sem dependência de nuvem paga |
| RNF-03 | Escalabilidade | Parâmetro de amostragem para rodar com 16 GB de RAM e execução completa se houver recursos |
| RNF-04 | Manutenibilidade | Transformações em funções puras e testáveis, separadas da orquestração |
| RNF-05 | Reprodutibilidade | Versões de Spark, Delta e Python fixadas; `make up && make run` |
| RN-01 | Regra | A chave de negócio do estabelecimento é CNPJ básico + ordem + DV |
| RST-01 | Restrição (LGPD) | Dados públicos, com CPF de sócios parcialmente mascarado. Proibido reidentificar pessoas; dados brutos não são republicados |

## Riscos
| ID | Risco | Prob. | Impacto | Mitigação |
|---|---|---|---|---|
| R-01 | Escopo crescer e o projeto não terminar | Alta | Alto | MoSCoW rígido; "Could" só após todos os "Must" |
| R-03 | Falta de tempo | Alta | Alto | Entrega incremental: cada épico gera algo publicável |
| R-04 | Dados abertos mudarem de formato ou URL | Média | Médio | Script de download parametrizado e amostra versionada para testes |
| R-P2-01 | Volume exceder a memória local | — | — | Amostragem parametrizada; processar por competência e por partição |
| R-P2-02 | Ganho de 30% não ser atingido | — | — | Registrar o resultado real e o diagnóstico |
