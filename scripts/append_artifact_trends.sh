#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
TRENDS_ROOT="${DISTDB_TRENDS_ROOT:-$ARTIFACTS_ROOT/trends}"

SECURITY_ROOT="$ARTIFACTS_ROOT/security"
PERF_ROOT="${PERF_DATA_ROOT:-$ARTIFACTS_ROOT/perf}"
E2E_ROOT="${SPLIT_BRAIN_DATA_ROOT:-$ARTIFACTS_ROOT/e2e}"

SECURITY_LEDGER="$TRENDS_ROOT/security-trend.json"
PERF_LEDGER="$TRENDS_ROOT/nonfunctional-trend.json"
E2E_LEDGER="$TRENDS_ROOT/split-brain-trend.json"

SECURITY_LEDGER_JSONL_LEGACY="$TRENDS_ROOT/security-trend.jsonl"
PERF_LEDGER_JSONL_LEGACY="$TRENDS_ROOT/nonfunctional-trend.jsonl"
E2E_LEDGER_JSONL_LEGACY="$TRENDS_ROOT/split-brain-trend.jsonl"

mkdir -p "$TRENDS_ROOT"

latest_dir_by_pattern() {
  local root_dir="$1"
  local name_pattern="$2"
  find "$root_dir" -maxdepth 1 -type d -name "$name_pattern" -print0 2>/dev/null \
    | xargs -0 ls -dt 2>/dev/null \
    | head -n 1 \
    || true
}

extract_json_field() {
  local file_path="$1"
  local field_name="$2"
  sed -n "s/.*\"$field_name\":[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$file_path" | head -n 1
}

migrate_legacy_jsonl_if_needed() {
  local legacy_path="$1"
  local ledger_path="$2"

  [[ -f "$legacy_path" ]] || return 0
  if [[ -f "$ledger_path" ]]; then
    rm -f "$legacy_path"
    echo "[artifact-trends][ok] removed legacy ledger $legacy_path"
    return 0
  fi

  perl - "$legacy_path" "$ledger_path" <<'PERL'
use strict;
use warnings;
use JSON::PP;

my ($legacy_path, $ledger_path) = @ARGV;
my $json = JSON::PP->new->utf8->canonical;
my @rows;

open my $in, '<', $legacy_path or die "open $legacy_path: $!\n";
while (my $line = <$in>) {
  $line =~ s/^\s+//;
  $line =~ s/\s+$//;
  next if $line eq '';
  my $decoded = eval { $json->decode($line) };
  next if !$decoded;
  push @rows, $decoded;
}
close $in;

open my $out, '>', $ledger_path or die "open $ledger_path: $!\n";
print {$out} $json->pretty->encode(\@rows);
close $out;
PERL

  echo "[artifact-trends][ok] migrated legacy ledger legacy=$legacy_path ledger=$ledger_path"
  rm -f "$legacy_path"
  echo "[artifact-trends][ok] removed legacy ledger $legacy_path"
}

append_entry_to_json_array() {
  local ledger_path="$1"
  local entry_json="$2"

  perl - "$ledger_path" "$entry_json" <<'PERL'
use strict;
use warnings;
use JSON::PP;

my ($ledger_path, $entry_raw) = @ARGV;
my $json = JSON::PP->new->utf8->canonical;

my $entry = $json->decode($entry_raw);
my $rows = [];

if (-f $ledger_path && -s $ledger_path) {
  local $/;
  open my $in, '<', $ledger_path or die "open $ledger_path: $!\n";
  my $text = <$in>;
  close $in;
  my $decoded = eval { $json->decode($text) };
  if ($decoded && ref($decoded) eq 'ARRAY') {
    $rows = $decoded;
  }
}

push @{$rows}, $entry;

open my $out, '>', $ledger_path or die "open $ledger_path: $!\n";
print {$out} $json->pretty->encode($rows);
close $out;
PERL
}

dedupe_ledger_by_run_id() {
  local ledger_path="$1"

  [[ -f "$ledger_path" ]] || return 0

  perl - "$ledger_path" <<'PERL'
use strict;
use warnings;
use JSON::PP;

my ($ledger_path) = @ARGV;
my $json = JSON::PP->new->utf8->canonical;

local $/;
open my $in, '<', $ledger_path or die "open $ledger_path: $!\n";
my $text = <$in>;
close $in;

my $decoded = eval { $json->decode($text) };
exit 0 if !$decoded || ref($decoded) ne 'ARRAY';

my %seen;
my @deduped;

for my $entry (@{$decoded}) {
  my $run_id = '';
  if (ref($entry) eq 'HASH' && ref($entry->{manifest}) eq 'HASH') {
    $run_id = $entry->{manifest}->{run_id} // '';
  }

  if ($run_id ne '') {
    next if $seen{$run_id}++;
  }
  push @deduped, $entry;
}

open my $out, '>', $ledger_path or die "open $ledger_path: $!\n";
print {$out} $json->pretty->encode(\@deduped);
close $out;
PERL
}

append_manifest_if_new() {
  local manifest_path="$1"
  local ledger_path="$2"

  [[ -f "$manifest_path" ]] || return 0

  local run_id
  run_id="$(extract_json_field "$manifest_path" "run_id")"
  [[ -n "$run_id" ]] || return 0

  if [[ -f "$ledger_path" ]] && grep -q "\"run_id\"[[:space:]]*:[[:space:]]*\"$run_id\"" "$ledger_path"; then
    echo "[artifact-trends][skip] run_id already present in ledger run_id=$run_id ledger=$ledger_path"
    return 0
  fi

  local manifest_json entry_json
  manifest_json="$(tr -d '\n' < "$manifest_path")"
  entry_json="$(printf '{"ingested_at_utc":"%s","manifest_path":"%s","manifest":%s}' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$manifest_path" \
    "$manifest_json")"

  append_entry_to_json_array "$ledger_path" "$entry_json"

  echo "[artifact-trends][ok] appended run_id=$run_id ledger=$ledger_path"
}

migrate_legacy_jsonl_if_needed "$SECURITY_LEDGER_JSONL_LEGACY" "$SECURITY_LEDGER"
migrate_legacy_jsonl_if_needed "$PERF_LEDGER_JSONL_LEGACY" "$PERF_LEDGER"
migrate_legacy_jsonl_if_needed "$E2E_LEDGER_JSONL_LEGACY" "$E2E_LEDGER"

dedupe_ledger_by_run_id "$SECURITY_LEDGER"
dedupe_ledger_by_run_id "$PERF_LEDGER"
dedupe_ledger_by_run_id "$E2E_LEDGER"

security_dir="$(latest_dir_by_pattern "$SECURITY_ROOT" "security-baseline-*")"
if [[ -n "$security_dir" ]]; then
  append_manifest_if_new "$security_dir/manifest.json" "$SECURITY_LEDGER"
else
  echo "[artifact-trends][warn] no security artifact directory found under $SECURITY_ROOT"
fi

perf_dir="$(latest_dir_by_pattern "$PERF_ROOT" "nonfunctional-baseline-*")"
if [[ -n "$perf_dir" ]]; then
  append_manifest_if_new "$perf_dir/manifest.json" "$PERF_LEDGER"
else
  echo "[artifact-trends][warn] no non-functional artifact directory found under $PERF_ROOT"
fi

e2e_dir="$(latest_dir_by_pattern "$E2E_ROOT" "split-brain-evidence-*")"
if [[ -n "$e2e_dir" ]]; then
  append_manifest_if_new "$e2e_dir/manifest.json" "$E2E_LEDGER"
else
  echo "[artifact-trends][warn] no split-brain artifact directory found under $E2E_ROOT"
fi

echo "[artifact-trends][ok] trend append completed"
