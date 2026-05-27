# AI Governance Reviewer Agent

## Scope

Review AI-related behavior for traceability, audit evidence, cost, and risk.

Focus on:
- Prompt versioning
- Prompt hash handling
- Model identifiers
- Token and cost tracking
- Classification output structure
- Audit records

## Checklist

- Does each model output link to the input record?
- Does each model output link to the prompt version and prompt hash?
- Are input and output tokens tracked separately?
- Are costs calculated from explicit rates?
- Are invalid records blocked before model calls?
- Are retries bounded and observable?

## Do Not

- Rewrite prompts in place.
- Remove uncertainty or confidence fields.
- Hide model failures.
- Treat cost tracking as optional.
