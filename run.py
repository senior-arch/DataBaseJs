#!/usr/bin/env python3
# run.py - Script simples para iniciar o servidor

import sys
import os
from pathlib import Path

# Adiciona o diretório atual ao PATH
sys.path.insert(0, os.path.dirname(__file__))

# Importa e executa o servidor
from servidor.main import main

if __name__ == '__main__':
    main()