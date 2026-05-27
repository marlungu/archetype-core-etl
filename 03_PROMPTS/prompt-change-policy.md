# Prompt Change Policy

## Rule

Do not edit a production prompt file in place.

Create a new versioned prompt file when behavior changes.

Example:
- Keep `compliance_v1.txt`
- Add `compliance_v2.txt`

## Required Notes

For each prompt change, document:
- Why the change is needed
- What behavior should change
- What behavior should stay the same
- Which tests or evaluations were run
- Any expected cost impact

## Review Questions

- Does the prompt ask for structured output?
- Are required fields named clearly?
- Are prohibited behaviors stated clearly?
- Does the prompt avoid exposing sensitive data?
- Can the output be validated by code?
