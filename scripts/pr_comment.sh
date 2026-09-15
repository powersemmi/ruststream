#!/usr/bin/env bash
# Leave one report on a pull request: bash scripts/pr_comment.sh <marker> <file>
#
# The comment opens with a hidden marker, and a re-run rewrites the comment carrying it instead of
# adding another one to the thread. So a pull request measured ten times still reads as one report
# per subject, and the numbers under discussion are the current ones.
#
# The repository, the pull request, the token and whether the head is a fork come from the
# environment the workflow sets. A pull request from a fork gets a read-only token, and there the
# report stays in the job summary rather than failing the run.
set -euo pipefail

marker="$1"
body_file="$2"
hidden="<!-- $marker -->"
comments="repos/$GITHUB_REPOSITORY/issues/$PR_NUMBER/comments"

if [ "${PR_FORK:-false}" = "true" ]; then
  echo "A pull request from a fork gets a read-only token: $marker stays in the job summary."
  exit 0
fi

# The parentheses are not decoration: jq 1.7 reads an object value up to the first operator and
# rejects the concatenation without them.
payload=$(jq -n --arg hidden "$hidden" --rawfile body "$body_file" '{body: ($hidden + "\n" + $body)}')

# --slurp hands jq one array of pages, so a pull request past its first hundred comments still
# finds the comment it wrote earlier instead of opening a second one.
existing=$(gh api --paginate --slurp "$comments" \
  | jq -r --arg hidden "$hidden" 'flatten | map(select(.body | startswith($hidden))) | .[0].id // empty')

if [ -n "$existing" ]; then
  printf '%s' "$payload" \
    | gh api --method PATCH --input - "repos/$GITHUB_REPOSITORY/issues/comments/$existing" \
    > /dev/null
  echo "Updated the $marker comment on #$PR_NUMBER."
else
  printf '%s' "$payload" | gh api --method POST --input - "$comments" > /dev/null
  echo "Posted the $marker comment on #$PR_NUMBER."
fi
