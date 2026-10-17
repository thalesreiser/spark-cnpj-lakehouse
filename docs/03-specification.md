# 03 - Specification

> Phase 3 (ISO/IEC/IEEE 29148). Requirements with ID, verifiable acceptance criteria and data model.

## User stories (INVEST + Gherkin)

```gherkin
Feature: <name>
  Scenario: <RF-xx>
    Given ...
    When ...
    Then ...
```

## Data model

The source tables (bronze) are described column by column in the [data dictionary](data-dictionary.md), which mirrors `cnpj_lakehouse/schemas.py`. The silver and gold models are added below as they are built.

| Table | Column | Type | Description | Rule |
|---|---|---|---|---|
