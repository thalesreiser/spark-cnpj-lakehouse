# 01 - Visão e escopo

**Problema:** processar grandes volumes de dados cadastrais de forma distribuída, confiável e performática, com histórico auditável.

**Objetivo:** construir um lakehouse em camadas (bronze, silver, gold) com Spark e Delta Lake sobre os Dados Abertos de CNPJ da Receita Federal, documentando o ganho de desempenho obtido por otimização.

**Fonte de dados:** Dados Abertos CNPJ (Receita Federal): empresas, estabelecimentos, sócios e tabelas auxiliares. Confirmar volume, layout e URL atuais antes de começar (R-04).

| Dentro do escopo | Fora do escopo |
|---|---|
| Ingestão batch, camadas Delta, MERGE incremental, otimização, benchmark, testes | Streaming, dashboards, deploy em cluster gerenciado |

## Métricas de sucesso (meta)
- Pipeline completo reprodutível com um comando
- Redução ≥ 30% no tempo do job principal após tuning, comprovada no Spark UI
- ≥ 80% de cobertura nas transformações

## Stakeholders (papéis simulados)
| Papel | Interesse principal |
|---|---|
| Recrutador técnico / gestor | Evidência clara de stack, qualidade e autonomia, em 5 minutos de leitura |
| Engenheiro(a) revisor(a) | Código testado, organizado, reprodutível, com decisões justificadas |
| Analista consumidor(a) | Dados confiáveis, documentados e fáceis de consultar |
| Thales (dev e operador) | Custo baixo, execução reprodutível, manutenção simples |
