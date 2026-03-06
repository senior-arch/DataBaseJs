import json
import socket
import threading
from pathlib import Path
from datetime import datetime
from Manager import JSONDatabase  # Importa sua classe existente


class JSONDatabaseServer:
    def __init__(self, host='localhost', port=5555):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Dicionário para manter uma instância do banco por cliente
        self.client_databases = {}

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"🚀 Servidor JSON DB rodando em {self.host}:{self.port}")
        print("✅ Pressione Ctrl+C para parar")

        while True:
            try:
                client_socket, address = self.server_socket.accept()
                print(f"📡 Nova conexão de {address}")

                # Cria uma nova instância do banco para este cliente
                db_instance = JSONDatabase()

                # Armazena no dicionário usando o socket como chave
                self.client_databases[client_socket] = db_instance

                # Inicia thread para o cliente
                thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address)
                )
                thread.daemon = True
                thread.start()

            except KeyboardInterrupt:
                print("\n👋 Servidor encerrado")
                break
            except Exception as e:
                print(f"❌ Erro no servidor: {e}")

    def handle_client(self, client_socket, address):
        """Processa requisições do cliente usando a mesma lógica do terminal"""
        try:
            # Pega a instância do banco para este cliente
            db = self.client_databases[client_socket]
            buffer = ""  # Buffer para acumular dados

            while True:
                # Recebe comando do cliente
                data = client_socket.recv(4096).decode('utf-8')
                if not data:
                    break

                # Acumula no buffer (pode vir em partes)
                buffer += data

                try:
                    # Tenta parsear o JSON
                    comando = json.loads(buffer)
                    buffer = ""  # Limpa buffer após parse bem sucedido

                    acao = comando.get('acao')
                    parametros = comando.get('parametros', {})

                    print(f"📨 Comando de {address}: {acao}")
                    if acao == 'criar_tabela':
                        print(f"   Tabela: {parametros.get('nome_tabela')}")
                        print(f"   Colunas: {len(parametros.get('colunas', []))}")

                    # Executa o comando na instância do banco
                    resultado = self.executar_comando(db, acao, parametros)

                    # Envia resposta
                    response = json.dumps(resultado) + '\n'
                    client_socket.send(response.encode('utf-8'))

                except json.JSONDecodeError:
                    # JSON incompleto, continua acumulando
                    continue

        except Exception as e:
            print(f"❌ Erro com cliente {address}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Limpa recursos do cliente
            if client_socket in self.client_databases:
                del self.client_databases[client_socket]
            client_socket.close()
            print(f"📴 Conexão encerrada: {address}")

    def executar_comando(self, db, acao, parametros):
        """Mapeia comandos para os métodos da sua classe"""
        try:
            if acao == 'criar_banco':
                return self.criar_banco_api(db, parametros)

            elif acao == 'criar_tabela':
                return self.criar_tabela_api(db, parametros)

            elif acao == 'inserir':
                return self.inserir_api(db, parametros)

            elif acao == 'consultar':
                return self.consultar_api(db)

            elif acao == 'listar_tabelas':
                return self.listar_tabelas_api(db)

            elif acao == 'selecionar_tabela':
                db.current_table = parametros.get('tabela')
                return {
                    'status': 'sucesso',
                    'mensagem': f"Tabela {db.current_table} selecionada",
                    'tabela_atual': db.current_table
                }

            else:
                return {'status': 'erro', 'mensagem': f'Comando não reconhecido: {acao}'}

        except Exception as e:
            return {'status': 'erro', 'mensagem': str(e)}

    def criar_banco_api(self, db, parametros):
        """Cria um novo banco de dados via API"""
        nome_banco = parametros.get('nome_banco')

        if not nome_banco:
            return {'status': 'erro', 'mensagem': 'Nome do banco é obrigatório'}

        db.db_name = nome_banco
        db.db_path = Path(nome_banco)

        if db.db_path.exists():
            return {
                'status': 'sucesso',
                'mensagem': f'Banco {nome_banco} conectado',
                'banco_atual': db.db_name
            }

        # Cria novo banco
        db.db_path.mkdir()
        metadata = {
            "nome": nome_banco,
            "criado_em": datetime.now().isoformat(),
            "tabelas": []
        }

        with open(db.db_path / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)

        return {
            'status': 'sucesso',
            'mensagem': f'Banco {nome_banco} criado com sucesso',
            'banco_atual': db.db_name
        }

    def criar_tabela_api(self, db, parametros):
        """Versão API do criar_tabela (sem input interativo)"""
        if not db.db_path:
            return {'status': 'erro', 'mensagem': 'Nenhum banco selecionado'}

        nome_tabela = parametros.get('nome_tabela')
        colunas = parametros.get('colunas', [])

        if not nome_tabela or not colunas:
            return {'status': 'erro', 'mensagem': 'Nome da tabela e colunas são obrigatórios'}

        tabela_path = db.db_path / nome_tabela

        if tabela_path.exists():
            return {'status': 'erro', 'mensagem': f'Tabela {nome_tabela} já existe'}

        # Criar estrutura da tabela
        tabela_path.mkdir()
        (tabela_path / "data").mkdir()

        # Schema da tabela
        schema = {
            "name": nome_tabela,
            "criado_em": datetime.now().isoformat(),
            "columns": colunas,
            "last_id": 0,
            "record_count": 0
        }

        # Salvar schema
        with open(tabela_path / "schema.json", "w") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)

        # Atualizar metadata do banco
        with open(db.db_path / "metadata.json", "r+") as f:
            metadata = json.load(f)
            if "tabelas" not in metadata:
                metadata["tabelas"] = []
            metadata["tabelas"].append(nome_tabela)
            f.seek(0)
            json.dump(metadata, f, indent=2, ensure_ascii=False)
            f.truncate()

        return {
            'status': 'sucesso',
            'mensagem': f'Tabela {nome_tabela} criada com sucesso',
            'schema': schema
        }

    def inserir_api(self, db, parametros):
        """Versão API do inserir_dados"""
        if not db.db_path:
            return {'status': 'erro', 'mensagem': 'Nenhum banco selecionado'}

        if not db.current_table:
            return {'status': 'erro', 'mensagem': 'Nenhuma tabela selecionada'}

        dados = parametros.get('dados', {})

        if not dados:
            return {'status': 'erro', 'mensagem': 'Nenhum dado fornecido'}

        tabela_path = db.db_path / db.current_table

        # Carregar schema
        with open(tabela_path / "schema.json", "r") as f:
            schema = json.load(f)

        # Validar dados contra o schema
        dados_validados = {}
        erros = []

        for coluna in schema["columns"]:
            if coluna.get("auto_increment"):
                continue

            nome_col = coluna["name"]

            if nome_col in dados:
                valor = dados[nome_col]

                # Pula se for None e campo não é obrigatório
                if valor is None and not coluna['not_null']:
                    dados_validados[nome_col] = None
                    continue

                # Validar tipo
                try:
                    if coluna['type'] == 'INTEGER':
                        if valor is not None:
                            dados_validados[nome_col] = int(valor)
                        else:
                            dados_validados[nome_col] = None
                    elif coluna['type'] == 'DECIMAL':
                        if valor is not None:
                            dados_validados[nome_col] = float(valor)
                        else:
                            dados_validados[nome_col] = None
                    elif coluna['type'] == 'BOOLEAN':
                        if valor is not None:
                            if isinstance(valor, bool):
                                dados_validados[nome_col] = valor
                            else:
                                dados_validados[nome_col] = str(valor).upper() == 'S' or str(valor).upper() == 'TRUE'
                        else:
                            dados_validados[nome_col] = None
                    else:  # VARCHAR, DATE, TEXT
                        dados_validados[nome_col] = str(valor) if valor is not None else None

                except ValueError as e:
                    erros.append(f"Tipo inválido para {nome_col}: esperado {coluna['type']}")

            elif coluna['not_null']:
                erros.append(f"Campo obrigatório: {nome_col}")
            else:
                dados_validados[nome_col] = coluna.get('default')

        if erros:
            return {'status': 'erro', 'mensagem': 'Erros de validação', 'erros': erros}

        # Criar registro
        novo_id = schema["last_id"] + 1
        registro = {
            "id": novo_id,
            "data": dados_validados,
            "criado_em": datetime.now().isoformat()
        }

        # Salvar
        registro_path = tabela_path / "data" / f"{novo_id}.json"
        with open(registro_path, "w") as f:
            json.dump(registro, f, indent=2, ensure_ascii=False)

        # Atualizar schema
        schema["last_id"] = novo_id
        schema["record_count"] += 1
        with open(tabela_path / "schema.json", "w") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)

        return {
            'status': 'sucesso',
            'mensagem': f'Registro inserido com ID: {novo_id}',
            'id': novo_id,
            'registro': registro
        }

    def consultar_api(self, db):
        """Consulta dados via API"""
        if not db.db_path:
            return {'status': 'erro', 'mensagem': 'Nenhum banco selecionado'}

        if not db.current_table:
            return {'status': 'erro', 'mensagem': 'Nenhuma tabela selecionada'}

        tabela_path = db.db_path / db.current_table
        data_path = tabela_path / "data"

        if not data_path.exists():
            return {
                'status': 'sucesso',
                'total': 0,
                'registros': []
            }

        registros = sorted(data_path.glob("*.json"))
        resultados = []

        for reg_file in registros:
            with open(reg_file, "r") as f:
                reg = json.load(f)
            resultados.append(reg)

        return {
            'status': 'sucesso',
            'total': len(resultados),
            'registros': resultados
        }

    def listar_tabelas_api(self, db):
        """Lista todas as tabelas via API"""
        if not db.db_path:
            return {'status': 'erro', 'mensagem': 'Nenhum banco selecionado'}

        try:
            with open(db.db_path / "metadata.json", "r") as f:
                metadata = json.load(f)

            tabelas = metadata.get("tabelas", [])

            # Busca informações adicionais de cada tabela
            info_tabelas = []
            for tabela in tabelas:
                schema_path = db.db_path / tabela / "schema.json"
                if schema_path.exists():
                    with open(schema_path, "r") as f:
                        schema = json.load(f)
                    info_tabelas.append({
                        "nome": tabela,
                        "registros": schema.get("record_count", 0),
                        "colunas": len(schema.get("columns", []))
                    })
                else:
                    info_tabelas.append({
                        "nome": tabela,
                        "registros": 0,
                        "colunas": 0
                    })

            return {
                'status': 'sucesso',
                'tabelas': info_tabelas,
                'total': len(info_tabelas)
            }

        except Exception as e:
            return {'status': 'erro', 'mensagem': f'Erro ao listar tabelas: {e}'}


if __name__ == "__main__":
    server = JSONDatabaseServer()
    server.start()