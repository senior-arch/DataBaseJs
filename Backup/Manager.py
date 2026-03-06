import json
import os
from pathlib import Path
from datetime import datetime


class JSONDatabase:
    def __init__(self):
        self.db_path = None
        self.current_table = None
        self.db_name = None

    def criar_banco(self):
        """Cria um novo banco de dados"""
        print("\n" + "=" * 50)
        self.db_name = input("🏦 Nome do banco de dados: ").strip()

        if not self.db_name:
            print("❌ Nome inválido!")
            return False

        self.db_path = Path(self.db_name)

        if self.db_path.exists():
            print(f"⚠️ Banco '{self.db_name}' já existe! Conectando...")
        else:
            self.db_path.mkdir()
            # Metadata do banco
            metadata = {
                "nome": self.db_name,
                "criado_em": datetime.now().isoformat(),
                "tabelas": []
            }
            with open(self.db_path / "metadata.json", "w") as f:
                json.dump(metadata, f, indent=2)
            print(f"✅ Banco '{self.db_name}' criado com sucesso!")

        return True

    def criar_tabela(self):
        """Cria uma nova tabela com propriedades definidas pelo usuário"""
        if not self.db_path:
            print("❌ Crie ou conecte a um banco primeiro!")
            return

        print("\n" + "=" * 50)
        print("📋 CRIAÇÃO DE TABELA")
        print("=" * 50)

        nome_tabela = input("Nome da tabela: ").strip()
        if not nome_tabela:
            print("❌ Nome inválido!")
            return

        tabela_path = self.db_path / nome_tabela
        if tabela_path.exists():
            print(f"❌ Tabela '{nome_tabela}' já existe!")
            return

        # Criar estrutura da tabela
        tabela_path.mkdir()
        (tabela_path / "data").mkdir()

        # Definir colunas
        print("\n📌 Defina as colunas da tabela (deixe nome vazio para terminar):")
        colunas = []
        contador = 1

        while True:
            print(f"\n--- Coluna {contador} ---")
            nome_col = input("Nome da coluna (ENTER para terminar): ").strip()
            if not nome_col:
                if contador == 1:
                    print("❌ Pelo menos uma coluna é necessária!")
                    continue
                break

            # Tipo da coluna
            print("Tipos disponíveis: VARCHAR, INTEGER, DECIMAL, DATE, BOOLEAN, TEXT")
            tipo = input("Tipo: ").strip().upper()
            if tipo not in ["VARCHAR", "INTEGER", "DECIMAL", "DATE", "BOOLEAN", "TEXT"]:
                tipo = "VARCHAR"
                print(f"⚠️ Tipo inválido, usando {tipo}")

            # Propriedades
            print("\nPropriedades (S/N):")
            not_null = input("NOT NULL? ").strip().upper() == "S"
            primary_key = input("PRIMARY KEY? ").strip().upper() == "S"
            auto_increment = input("AUTO INCREMENT? ").strip().upper() == "S"
            unique = input("UNIQUE? ").strip().upper() == "S"

            # Valor padrão
            default = input("Valor DEFAULT (ENTER para nenhum): ").strip()
            if default == "":
                default = None

            coluna = {
                "name": nome_col,
                "type": tipo,
                "not_null": not_null,
                "primary_key": primary_key,
                "auto_increment": auto_increment,
                "unique": unique,
                "default": default
            }

            colunas.append(coluna)
            contador += 1

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
        with open(self.db_path / "metadata.json", "r+") as f:
            metadata = json.load(f)
            metadata["tabelas"].append(nome_tabela)
            f.seek(0)
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        print(f"\n✅ Tabela '{nome_tabela}' criada com sucesso!")
        self.mostrar_estrutura_tabela(nome_tabela)

    def inserir_dados(self):
        """Insere dados na tabela atual"""
        if not self.current_table:
            print("❌ Selecione uma tabela primeiro!")
            return

        print("\n" + "=" * 50)
        print(f"📝 INSERINDO DADOS EM: {self.current_table}")
        print("=" * 50)

        tabela_path = self.db_path / self.current_table

        # Carregar schema
        with open(tabela_path / "schema.json", "r") as f:
            schema = json.load(f)

        print("\nDigite os dados (ENTER vazio para cancelar):")
        dados = {}

        for coluna in schema["columns"]:
            # Pula auto_increment (gerado automaticamente)
            if coluna.get("auto_increment"):
                continue

            while True:
                prompt = f"{coluna['name']} ({coluna['type']})"
                if not coluna['not_null']:
                    prompt += " [opcional]"
                if coluna['default']:
                    prompt += f" (default: {coluna['default']})"
                prompt += ": "

                valor = input(prompt).strip()

                # Se vazio e não obrigatório, usa default ou None
                if valor == "":
                    if not coluna['not_null']:
                        dados[coluna['name']] = coluna['default']
                        break
                    else:
                        print(f"❌ Campo obrigatório!")
                        continue

                # Converter conforme tipo
                try:
                    if coluna['type'] == 'INTEGER':
                        dados[coluna['name']] = int(valor)
                    elif coluna['type'] == 'DECIMAL':
                        dados[coluna['name']] = float(valor)
                    elif coluna['type'] == 'BOOLEAN':
                        dados[coluna['name']] = valor.upper() == 'S'
                    else:
                        dados[coluna['name']] = valor
                    break
                except ValueError:
                    print(f"❌ Tipo inválido! Deve ser {coluna['type']}")

        if not dados:
            print("❌ Nenhum dado inserido!")
            return

        # Criar registro
        novo_id = schema["last_id"] + 1
        registro = {
            "id": novo_id,
            "data": dados,
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

        print(f"✅ Registro inserido com ID: {novo_id}")

    def listar_tabelas(self):
        """Lista todas as tabelas do banco"""
        if not self.db_path:
            print("❌ Nenhum banco conectado!")
            return

        with open(self.db_path / "metadata.json", "r") as f:
            metadata = json.load(f)

        print("\n" + "=" * 50)
        print(f"📊 TABELAS DO BANCO: {self.db_name}")
        print("=" * 50)

        if not metadata["tabelas"]:
            print("Nenhuma tabela criada ainda.")
            return

        for i, tabela in enumerate(metadata["tabelas"], 1):
            try:
                with open(self.db_path / tabela / "schema.json", "r") as f:
                    schema = json.load(f)
                status = "✅ ATUAL" if tabela == self.current_table else "   "
                print(f"{i}. {status} {tabela} - {schema['record_count']} registros")
            except:
                print(f"{i}. ⚠️ {tabela} - erro ao carregar")

    def selecionar_tabela(self):
        """Seleciona uma tabela para operações"""
        self.listar_tabelas()

        if not self.db_path:
            return

        with open(self.db_path / "metadata.json", "r") as f:
            metadata = json.load(f)

        if not metadata["tabelas"]:
            print("❌ Nenhuma tabela disponível!")
            return

        nome = input("\n📋 Nome da tabela para usar: ").strip()

        if nome in metadata["tabelas"]:
            self.current_table = nome
            print(f"✅ Tabela '{nome}' selecionada!")
            self.mostrar_estrutura_tabela(nome)
        else:
            print(f"❌ Tabela '{nome}' não encontrada!")

    def mostrar_estrutura_tabela(self, nome_tabela=None):
        """Mostra a estrutura de uma tabela"""
        tabela = nome_tabela or self.current_table

        if not tabela:
            print("❌ Nenhuma tabela especificada!")
            return

        try:
            with open(self.db_path / tabela / "schema.json", "r") as f:
                schema = json.load(f)

            print(f"\n📋 ESTRUTURA DA TABELA: {tabela}")
            print("-" * 80)
            print(f"{'Coluna':<15} {'Tipo':<10} {'NOT NULL':<10} {'PK':<5} {'AI':<5} {'UNIQUE':<8} {'DEFAULT'}")
            print("-" * 80)

            for col in schema["columns"]:
                not_null = "SIM" if col.get("not_null") else "NÃO"
                pk = "SIM" if col.get("primary_key") else "NÃO"
                ai = "SIM" if col.get("auto_increment") else "NÃO"
                unique = "SIM" if col.get("unique") else "NÃO"
                default = col.get("default") or "-"

                print(f"{col['name']:<15} {col['type']:<10} {not_null:<10} {pk:<5} {ai:<5} {unique:<8} {default}")

            print(f"\nTotal de registros: {schema['record_count']}")

        except Exception as e:
            print(f"❌ Erro ao carregar tabela: {e}")

    def consultar_dados(self):
        """Consulta dados da tabela atual"""
        if not self.current_table:
            print("❌ Selecione uma tabela primeiro!")
            return

        print("\n" + "=" * 50)
        print(f"🔍 CONSULTANDO DADOS: {self.current_table}")
        print("=" * 50)

        tabela_path = self.db_path / self.current_table
        data_path = tabela_path / "data"

        registros = sorted(data_path.glob("*.json"))

        if not registros:
            print("📭 Nenhum registro encontrado.")
            return

        print(f"\n📊 Total de {len(registros)} registros:\n")

        for i, reg_file in enumerate(registros, 1):
            with open(reg_file, "r") as f:
                reg = json.load(f)

            print(f"Registro #{i} (ID: {reg['id']})")
            print(f"Criado em: {reg['criado_em']}")
            print("Dados:")
            for chave, valor in reg['data'].items():
                print(f"  {chave}: {valor}")
            print("-" * 40)


def main():
    db = JSONDatabase()

    while True:
        print("\n" + "=" * 60)
        print("🗄️  SISTEMA DE BANCO DE DADOS JSON")
        print("=" * 60)
        print(f"Banco atual: {db.db_name or 'Nenhum'}")
        print(f"Tabela atual: {db.current_table or 'Nenhuma'}")
        print("-" * 60)
        print("1. 📁 Criar/Conectar banco de dados")
        print("2. 📋 Criar nova tabela")
        print("3. 📂 Listar tabelas")
        print("4. 🔄 Selecionar tabela")
        print("5. 📝 Inserir dados na tabela atual")
        print("6. 🔍 Consultar dados da tabela atual")
        print("7. ℹ️  Mostrar estrutura da tabela atual")
        print("8. ❌ Sair")
        print("-" * 60)

        opcao = input("Escolha uma opção: ").strip()

        if opcao == "1":
            db.criar_banco()

        elif opcao == "2":
            if not db.db_path:
                print("❌ Crie um banco primeiro!")
            else:
                db.criar_tabela()

        elif opcao == "3":
            db.listar_tabelas()

        elif opcao == "4":
            if not db.db_path:
                print("❌ Crie um banco primeiro!")
            else:
                db.selecionar_tabela()

        elif opcao == "5":
            db.inserir_dados()

        elif opcao == "6":
            db.consultar_dados()

        elif opcao == "7":
            db.mostrar_estrutura_tabela()

        elif opcao == "8":
            print("\n👋 Até logo!")
            break

        else:
            print("❌ Opção inválida!")

        input("\nPressione ENTER para continuar...")


if __name__ == "__main__":
    main()