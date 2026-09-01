#!/bin/sh
case "$1" in
  version) printf '%s\n' 'retrieval-provider-contract-fixture' ;;
  stdin)
    payload=$(cat)
    case "$payload" in
      *scanner-fixture-secret*) exit 1 ;;
      *) exit 0 ;;
    esac
    ;;
  *) exit 2 ;;
esac
