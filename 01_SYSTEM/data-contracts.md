# Data Contracts

## Federal Document Record

Each input record should have:
- A stable record identifier
- Agency value from the approved set
- Priority tier from the approved set
- Source bucket and source key when loaded from object storage
- Required document metadata
- Raw input preserved or hashable for audit evidence

## Classification Output

Each classification result should include:
- Record identifier
- Pipeline run ID
- Model identifier
- Prompt version
- Prompt hash
- Risk or compliance category
- Confidence or score fields, when available
- Input token count
- Output token count
- Input token cost
- Output token cost
- Created timestamp

## Rules

- Additive schema changes are preferred.
- Breaking schema changes need a migration note.
- Do not remove audit fields without review.
- Do not overwrite source identifiers.
- Do not store prompt text in operational tables unless approved.
