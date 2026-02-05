"""
Script de Teste de Conexão - MySQL
Teste rápido para verificar se a conexão está funcionando
"""

import sys

def testar_conexao():
    print("=" * 60)
    print("🔍 TESTE DE CONEXÃO MYSQL")
    print("=" * 60)
    
    # 1. Verificar se PyMySQL está instalado
    print("\n1. Verificando PyMySQL...")
    try:
        import pymysql
        print("   ✅ PyMySQL instalado")
    except ImportError:
        print("   ❌ PyMySQL NÃO instalado")
        print("   💡 Execute: pip install pymysql")
        return False
    
    # 2. Verificar SQLAlchemy
    print("\n2. Verificando SQLAlchemy...")
    try:
        from sqlalchemy import create_engine, text
        print("   ✅ SQLAlchemy instalado")
    except ImportError:
        print("   ❌ SQLAlchemy NÃO instalado")
        print("   💡 Execute: pip install sqlalchemy")
        return False
    
    # 3. Solicitar credenciais
    print("\n3. Configuração do MySQL:")
    print("   (Pressione ENTER para usar valores padrão)")
    
    host = input("   Host [localhost]: ").strip() or "localhost"
    port = input("   Porta [3306]: ").strip() or "3306"
    user = input("   Usuário [root]: ").strip() or "root"
    password = input("   Senha: ").strip()
    
    if not password:
        print("   ⚠️  Senha vazia - pode não funcionar")
    
    database = "crimes_curitiba"
    
    # 4. Tentar conectar
    print("\n4. Testando conexão...")
    try:
        connection_string = (
            f"mysql+pymysql://{user}:{password}"
            f"@{host}:{port}/{database}"
            f"?charset=utf8mb4"
        )
        
        engine = create_engine(connection_string, pool_pre_ping=True)
        
        with engine.connect() as conn:
            # Teste simples
            result = conn.execute(text("SELECT 1 AS test"))
            test_value = result.fetchone()[0]
            
            if test_value == 1:
                print("   ✅ Conexão estabelecida com sucesso!")
                
                # Verificar se o banco existe
                print("\n5. Verificando estrutura do banco...")
                try:
                    tables = conn.execute(text("""
                        SELECT TABLE_NAME 
                        FROM information_schema.TABLES 
                        WHERE TABLE_SCHEMA = :db
                        ORDER BY TABLE_NAME
                    """), {"db": database}).fetchall()
                    
                    if tables:
                        print(f"   ✅ Banco '{database}' encontrado com {len(tables)} tabelas:")
                        for table in tables:
                            print(f"      • {table[0]}")
                        
                        # Verificar registros
                        print("\n6. Verificando dados...")
                        try:
                            count = conn.execute(text(
                                "SELECT COUNT(*) FROM FATO_OCORRENCIA"
                            )).fetchone()[0]
                            
                            if count > 0:
                                print(f"   ✅ Banco populado: {count:,} ocorrências")
                            else:
                                print("   ⚠️  Banco vazio - execute coleta_mysql.py")
                        except:
                            print("   ⚠️  Tabela FATO_OCORRENCIA não encontrada")
                            print("   💡 Execute: setup_database.sql")
                    else:
                        print(f"   ⚠️  Banco '{database}' existe mas está vazio")
                        print("   💡 Execute: setup_database.sql")
                        
                except Exception as e:
                    print(f"   ❌ Erro ao verificar estrutura: {e}")
                
                return True
            else:
                print("   ❌ Conexão falhou no teste")
                return False
                
    except Exception as e:
        print(f"   ❌ Erro de conexão: {e}")
        print("\n💡 Possíveis causas:")
        print("   • MySQL não está rodando")
        print("   • Usuário ou senha incorretos")
        print("   • Banco 'crimes_curitiba' não foi criado")
        print("   • Firewall bloqueando a porta 3306")
        return False


def menu_principal():
    print("\n" + "=" * 60)
    print("📊 MENU DE TESTES")
    print("=" * 60)
    print("\n1. Testar conexão MySQL")
    print("2. Ver informações do sistema")
    print("3. Sair")
    
    escolha = input("\nEscolha uma opção: ").strip()
    
    if escolha == "1":
        testar_conexao()
    elif escolha == "2":
        mostrar_info_sistema()
    elif escolha == "3":
        print("\n👋 Até logo!")
        sys.exit(0)
    else:
        print("\n❌ Opção inválida")
    
    input("\nPressione ENTER para continuar...")
    menu_principal()


def mostrar_info_sistema():
    print("\n" + "=" * 60)
    print("💻 INFORMAÇÕES DO SISTEMA")
    print("=" * 60)
    
    # Python
    print(f"\nPython: {sys.version}")
    
    # Bibliotecas instaladas
    print("\nBibliotecas:")
    libs = [
        "pandas", "requests", "beautifulsoup4", 
        "sqlalchemy", "pymysql", "matplotlib", "seaborn"
    ]
    
    for lib in libs:
        try:
            mod = __import__(lib)
            version = getattr(mod, "__version__", "instalado")
            print(f"   ✅ {lib}: {version}")
        except ImportError:
            print(f"   ❌ {lib}: NÃO INSTALADO")


if __name__ == "__main__":
    try:
        menu_principal()
    except KeyboardInterrupt:
        print("\n\n👋 Teste interrompido pelo usuário.")
        sys.exit(0)
