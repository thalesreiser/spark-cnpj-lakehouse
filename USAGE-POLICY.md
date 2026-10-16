# Usage policy: spark-cnpj-lakehouse

**Author:** Thales Reiser Pedreira · [GitHub](https://github.com/thalesreiser) · [LinkedIn](https://www.linkedin.com/in/thalesreiserpedreira)

## Code license

The code is released under the [MIT license](LICENSE). In short:

| ✅ You may | 📌 As long as | ⚠️ Keep in mind |
|---|---|---|
| Use, copy, modify and distribute the code | You keep the `Copyright (c) 2026 Thales Reiser Pedreira` notice and the license text in every copy or substantial portion | The software is provided without warranty of any kind |
| Use it in personal or commercial projects | You give credit to the author | The author is not liable for damages or costs arising from its use |

The [`LICENSE`](LICENSE) text is the legally binding one; this page is only a summary.

## Purpose of the project

This is a portfolio project, built to demonstrate data engineering skills through a documented requirements engineering process. It is not a product and was not designed for production use.

## Data

The MIT license applies to the **code**, not to the data it processes.

- The repository **contains no raw data**. Data is downloaded from the official public sources at run time, and only small samples for tests are versioned.
- The data belongs to its sources and follows their terms of use.
- Whoever runs the project is responsible for using the data in accordance with Brazil's data protection law (**LGPD, Law 13,709/2018**) and the terms of the sources.
- **CNPJ data (Brazilian Federal Revenue Service):** partner records come with CPFs (individual taxpayer IDs) partially masked by the source itself. **Do not** use this project to try to re-identify individuals, to cross-reference the data with other databases for that purpose, or to build profiles of people (project constraint RST-01; see also the LGPD).
- Matching partners with the same name (RF-10) is a technical *record linkage* exercise with sample-based review, not a tool for identifying people.

## Security

- No secret, key or credential may be committed. Create your own `.env` (ignored by Git) from `.env.example`.
- The pre-commit hooks run **gitleaks** to detect secrets before every commit.
- If you find a vulnerability, **do not open a public issue**: contact the author directly on GitHub or LinkedIn.

## Contributions

Issues and suggestions are welcome. Contributions submitted to this repository are licensed under the same MIT license.
