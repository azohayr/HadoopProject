#!/bin/bash

input_dir="C:\Users\admin\OneDrive\Bureau\HadoopSparkProjetPMN\input"

if [ -z "$(ls -A "$input_dir")" ]; then
    echo "Le répertoire input est vide."
else
    echo "Le répertoire input n'est pas vide."
fi
