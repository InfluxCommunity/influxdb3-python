#!/bin/bash

total_count=0
error_count=0

for (( i = 0; i < 100; i++ )); do
  result=$(pytest tests/test_polars.py::TestWritePolars::test_write_polars_batching)
  if [[ "$result" == *"expected call not found"* ]]; then
    echo "$result"
    ((error_count++))
  fi

  ((total_count++))
done

echo "Total run $total_count"
echo "Error count $error_count"
