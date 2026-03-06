#!/usr/bin/env python3
# client.py - Cliente de exemplo para o servidor de banco de dados

import socket
import json
import sys
import os
from typing import Optional, Dict, Any

try:
    from colorama import init, Fore, Style
    init()
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False
    # Fallback para sem cores
    class Fore:
        GREEN = ''; RED = ''; YELLOW = ''; CYAN = ''; RESET = ''
    class Style:
        BRIGHT = ''; RESET_ALL = ''


class DatabaseClient:
    """Cliente simples para conectar ao servidor de banco de dados."""

    def __init__(self, host: str = 'localhost', port: int = 5432):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.connected = False
        self.authenticated = False

    def connect(self) -> bool:
        """Conecta ao servidor."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            self.connected = True
            print(f"{Fore.GREEN}✅ Conectado ao servidor {self.host}:{self.port}{Style.RESET_ALL}")
            return True
        except Exception as e:
            print(f"{Fore.RED}❌ Erro ao conectar: {e}{Style.RESET_ALL}")
            return False

    def close(self):
        """Fecha a conexão."""
        if self.sock:
            self.sock.close()
            self.connected = False
            self.authenticated = False

    def send_command(self, command: str) -> Optional[Dict[str, Any]]:
        """
        Envia um comando e retorna a resposta.
        """
        if not self.connected or not self.sock:
            print(f"{Fore.RED}❌ Não conectado{Style.RESET_ALL}")
            return None

        try:
            # Envia comando terminado por \n
            self.sock.sendall(f"{command}\n".encode('utf-8'))

            # Recebe resposta (até \n)
            data = b''
            while True:
                chunk = self.sock.recv(1)
                if not chunk:
                    self.connected = False
                    print(f"{Fore.RED}❌ Conexão fechada pelo servidor{Style.RESET_ALL}")
                    return None
                if chunk == b'\n':
                    break
                data += chunk

            # Decodifica JSON
            response = json.loads(data.decode('utf-8'))
            return response

        except (socket.error, ConnectionResetError, BrokenPipeError) as e:
            self.connected = False
            print(f"{Fore.RED}❌ Erro de conexão: {e}{Style.RESET_ALL}")
            return None
        except json.JSONDecodeError as e:
            print(f"{Fore.RED}❌ Resposta inválida do servidor: {e}{Style.RESET_ALL}")
            return None

    def authenticate(self, username: str, password: str) -> bool:
        """Autentica no servidor."""
        response = self.send_command(f"AUTH {username} {password}")
        if response and response.get('status') == 'ok':
            self.authenticated = True
            print(f"{Fore.GREEN}✅ {response.get('message', 'Autenticado')}{Style.RESET_ALL}")
            return True
        else:
            print(f"{Fore.RED}❌ {response.get('message', 'Falha na autenticação')}{Style.RESET_ALL}")
            return False

    def execute(self, sql: str) -> None:
        """Executa um comando SQL e mostra o resultado."""
        if not self.authenticated:
            print(f"{Fore.RED}❌ Autentique-se primeiro com AUTH{Style.RESET_ALL}")
            return

        response = self.send_command(sql)
        if not response:
            return

        if response.get('status') == 'ok':
            self._show_success(response)
        else:
            self._show_error(response)

    def _show_success(self, response: Dict):
        """Mostra resposta de sucesso formatada."""
        print(f"{Fore.GREEN}✅ OK{Style.RESET_ALL}")

        if 'message' in response:
            print(f"   {response['message']}")

        if 'rows' in response:
            rows = response['rows']
            print(f"\n{Fore.CYAN}📊 {len(rows)} registro(s):{Style.RESET_ALL}")
            for i, row in enumerate(rows, 1):
                print(f"\n{Style.BRIGHT}Registro #{i} (ID: {row.get('id', 'N/A')}){Style.RESET_ALL}")
                print(f"   Criado em: {row.get('criado_em', 'N/A')}")
                print("   Dados:")
                for k, v in row.get('data', {}).items():
                    print(f"     {k}: {v}")

        if 'tables' in response:
            tables = response['tables']
            print(f"\n{Fore.CYAN}📋 Tabelas:{Style.RESET_ALL}")
            for table in tables:
                print(f"   - {table}")

        if 'columns' in response:
            cols = response['columns']
            print(f"\n{Fore.CYAN}📋 Estrutura da tabela:{Style.RESET_ALL}")
            print(f"   Total de registros: {response.get('record_count', 0)}")
            print("\n   {:<15} {:<10} {:<9} {:<5} {:<5} {:<8} {}".format(
                "Coluna", "Tipo", "NOT NULL", "PK", "AI", "UNIQUE", "DEFAULT"))
            for col in cols:
                print("   {:<15} {:<10} {:<9} {:<5} {:<5} {:<8} {}".format(
                    col['name'],
                    col['type'],
                    "SIM" if col['not_null'] else "NÃO",
                    "SIM" if col['primary_key'] else "NÃO",
                    "SIM" if col['auto_increment'] else "NÃO",
                    "SIM" if col['unique'] else "NÃO",
                    col['default'] or "-"
                ))

        if 'ids' in response:
            print(f"   IDs inseridos: {response['ids']}")

    def _show_error(self, response: Dict):
        """Mostra resposta de erro."""
        print(f"{Fore.RED}❌ ERRO: {response.get('message', 'Erro desconhecido')}{Style.RESET_ALL}")

    def interactive(self):
        """Modo interativo (shell)."""
        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}🗄️  Cliente do Banco de Dados JSON{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"Conectado a {self.host}:{self.port}")
        print("Comandos especiais: SAIR, LIMPAR")
        print("Digite comandos SQL (terminados com ; ou ENTER)")
        print()

        buffer = []
        while True:
            try:
                if buffer:
                    prompt = "... "
                else:
                    prompt = "SQL> "

                linha = input(prompt).strip()

                if linha.upper() in ('SAIR', 'EXIT', 'QUIT'):
                    break
                if linha.upper() == 'LIMPAR':
                    os.system('cls' if os.name == 'nt' else 'clear')
                    continue
                if not linha:
                    continue

                buffer.append(linha)

                # Se a linha termina com ; ou se é um comando AUTH (que não precisa de ;)
                if linha.endswith(';') or linha.upper().startswith('AUTH'):
                    comando = ' '.join(buffer)
                    if comando.endswith(';'):
                        comando = comando[:-1]  # remove ;
                    self.execute(comando)
                    buffer = []

            except KeyboardInterrupt:
                print("\nUse SAIR para encerrar")
                buffer = []
            except EOFError:
                break

        print(f"\n{Fore.YELLOW}👋 Até logo!{Style.RESET_ALL}")


def main():
    """Função principal."""
    import argparse

    parser = argparse.ArgumentParser(description="Cliente do Banco de Dados JSON")
    parser.add_argument('--host', default='localhost', help='Host do servidor')
    parser.add_argument('--port', type=int, default=5432, help='Porta do servidor')
    parser.add_argument('--user', '-u', help='Nome de usuário (para login direto)')
    parser.add_argument('--password', '-p', help='Senha (para login direto)')
    parser.add_argument('--command', '-c', help='Executa um comando e sai')
    args = parser.parse_args()

    client = DatabaseClient(args.host, args.port)

    if not client.connect():
        sys.exit(1)

    try:
        # Se forneceu usuário/senha, autentica
        if args.user and args.password:
            if not client.authenticate(args.user, args.password):
                sys.exit(1)

        # Se forneceu comando, executa e sai
        if args.command:
            if not client.authenticated:
                print("É necessário autenticar primeiro. Use --user e --password")
                sys.exit(1)
            client.execute(args.command)
        else:
            # Modo interativo
            if not client.authenticated:
                # Tenta autenticar interativamente
                print("Autenticação necessária")
                user = input("Usuário: ").strip()
                import getpass
                password = getpass.getpass("Senha: ")
                if not client.authenticate(user, password):
                    sys.exit(1)
            client.interactive()

    finally:
        client.close()


if __name__ == '__main__':
    main()