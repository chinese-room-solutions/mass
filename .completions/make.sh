#!/bin/bash

_make() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    opts="build build-libs build-web run proto test lint fmt tidy clean clean-all help"

    COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
}

# Function will be called by wrapper, no complete registration needed
