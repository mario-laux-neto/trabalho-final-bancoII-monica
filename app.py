import psycopg2
import xml.etree.ElementTree as ET

# ==========================
# CONFIG DO POSTGRES
# ==========================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "trabalho_xml",
    "user": "postgres",
    "password": "postgres"
}

def get_connection():
    """
    Abre conexão com o PostgreSQL usando a config acima.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    # Se der problema de acentuação, isso ajuda:
    conn.set_client_encoding("UTF8")
    return conn


# ==========================
# CARREGAR DADOS DO POSTGRES
# ==========================
def carregar_dados_relacionais():
    """
    Lê Fornecedor, Peca e Projeto do banco e guarda em dicionários.
    As chaves vão ser F1, P1, J1 para casar DIRETO com o XML.
    """
    conn = get_connection()
    cur = conn.cursor()

    # ---------- Fornecedor ----------
    cur.execute("SELECT cod_fornec, fnome, status, cidade FROM Fornecedor;")
    fornecedores = {}
    for cod, nome, status, cidade in cur.fetchall():
        # cod é numeric, convertemos pra int e montamos F1, F2, ...
        chave = f"F{int(cod)}"
        fornecedores[chave] = {
            "codigo": int(cod),
            "nome": nome,
            "status": int(status) if status is not None else None,
            "cidade": cidade,
        }

    # ---------- Peca ----------
    cur.execute("SELECT cod_peca, pnome, cor, peso, cdade FROM Peca;")
    pecas = {}
    for cod, nome, cor, peso, cdade in cur.fetchall():
        chave = f"P{int(cod)}"
        pecas[chave] = {
            "codigo": int(cod),
            "nome": nome,
            "cor": cor,
            "peso": float(peso) if peso is not None else None,
            "cidade": cdade,
        }

    # ---------- Projeto ----------
    cur.execute("SELECT cod_proj, jnome, cidade FROM Projeto;")
    projetos = {}
    for cod, nome, cidade in cur.fetchall():
        chave = f"J{int(cod)}"
        projetos[chave] = {
            "codigo": int(cod),
            "nome": nome,
            "cidade": cidade,
        }

    cur.close()
    conn.close()
    return fornecedores, pecas, projetos


# ==========================
# CARREGAR FORNECIMENTOS DO XML
# ==========================
def carregar_fornecimentos_xml(caminho_xml: str):
    """
    Lê o arquivo fornecimento.xml no formato:

    <dados>
      <fornecimento>
        <Cod_Fornec>F1</Cod_Fornec>
        <Cod_Peca>P1</Cod_Peca>
        <Cod_Proj>J1</Cod_Proj>
        <Quantidade>200</Quantidade>
      </fornecimento>
      ...
    </dados>
    """
    tree = ET.parse(caminho_xml)
    root = tree.getroot()

    fornecimentos = []
    for f in root.findall("fornecimento"):
        cod_fornec = f.findtext("Cod_Fornec")
        cod_peca = f.findtext("Cod_Peca")
        cod_proj = f.findtext("Cod_Proj")
        qtd_text = f.findtext("Quantidade")

        quantidade = int(qtd_text) if qtd_text is not None else 0

        fornecimentos.append({
            "cod_fornec": cod_fornec,   # ex: F1
            "cod_peca": cod_peca,       # ex: P1
            "cod_proj": cod_proj,       # ex: J1
            "quantidade": quantidade,
        })

    return fornecimentos


# ==========================
# GERAR RELATÓRIO INTEGRADO
# ==========================
def gerar_relatorio(fornecedores, pecas, projetos, fornecimentos):
    """
    Para cada fornecimento do XML, faz a junção com os dados do Postgres.
    Só imprime quando fornecedor, peça e projeto existem no banco.
    """
    print("=== RELATÓRIO INTEGRANDO XML + POSTGRES ===\n")
    print(f"Total de fornecimentos no XML: {len(fornecimentos)}\n")

    for f in fornecimentos:
        cod_fornec_xml = f["cod_fornec"]  # F1, F2...
        cod_peca_xml = f["cod_peca"]      # P1, P2...
        cod_proj_xml = f["cod_proj"]      # J1, J2...
        qtd = f["quantidade"]

        # Se qualquer um não existir no banco, pula
        if cod_fornec_xml not in fornecedores:
            print(f"[IGNORADO] Fornecedor {cod_fornec_xml} não existe no banco.")
            continue

        if cod_peca_xml not in pecas:
            print(f"[IGNORADO] Peça {cod_peca_xml} não existe no banco.")
            continue

        if cod_proj_xml not in projetos:
            print(f"[IGNORADO] Projeto {cod_proj_xml} não existe no banco.")
            continue

        forn = fornecedores[cod_fornec_xml]
        peca = pecas[cod_peca_xml]
        proj = projetos[cod_proj_xml]

        print(
            f"Fornecedor {cod_fornec_xml} ({forn['nome']}, {forn['cidade']}) "
            f"forneceu {qtd} unidades da peça {cod_peca_xml} "
            f"({peca['nome']}, {peca['cor']}) "
            f"para o projeto {cod_proj_xml} ({proj['nome']}, {proj['cidade']})."
        )


def main():
    # 1) Carrega dados relacionais (Postgres)
    fornecedores, pecas, projetos = carregar_dados_relacionais()

    # 2) Carrega fornecimentos do XML
    # Certifique-se que "fornecimento.xml" está na MESMA PASTA do app.py
    fornecimentos = carregar_fornecimentos_xml("fornecimento.xml")

    # 3) Gera a junção / integração
    gerar_relatorio(fornecedores, pecas, projetos, fornecimentos)


if __name__ == "__main__":
    main()
