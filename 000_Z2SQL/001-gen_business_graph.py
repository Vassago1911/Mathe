import pandas as pd
import numpy as np
import duckdb
import cudf # Falls GPU vorhanden
import time
timestamp = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

def generate_realworld_data(n_carts=250_000, n_products=5000):
    import random
    # 1. Produkte mit Beliebtheits-Gewichtung (Zipf-Verteilung)
    # Ein paar Produkte sind extrem beliebt, die meisten kaum.
    product_ids = [f"P_{i:04d}" for i in range(n_products)]
    weights = 1 / (np.arange(1, n_products + 1) ** 0.8)
    weights /= weights.sum()

    # 2. "Rezepte" definieren (Strukturgeber für Homologie)
    # Wir definieren 50 feste Rezepte, die öfter zusammen gekauft werden
    rezepte = [
        np.random.choice(product_ids, size=np.random.randint(3, 7), p=weights).tolist()
        for _ in range(50)
    ]

    data = []
    for _ in range(n_carts):
        # Würfeln: Ist es ein strukturbasiertes "Rezept" oder Zufallskauf?
        if np.random.random() < 0.3:
            # Ein Rezept wird gekauft (evtl. unvollständig)
            base_rezept = random.choice(rezepte)
            cart = [p for p in base_rezept if np.random.random() > 0.2]
        else:
            # Zufälliger Warenkorb basierend auf Beliebtheit
            size = np.random.randint(2, 12)
            cart = np.random.choice(product_ids, size=size, p=weights, replace=False).tolist()
        
        # Sortieren simuliert den Laufweg / Scan-Reihenfolge
        cart.sort() 
        revenue = round(np.random.gamma(5, 10), 2)
        data.append({'cart': cart, 'revenue': revenue})
    
    return pd.DataFrame(data)

def regen_warenkoerbe():
    # Ausführung
    print(timestamp(), "Generiere 250k Real-World Warenkörbe...")
    df_real = generate_realworld_data()
    con.execute("CREATE OR REPLACE TABLE carts AS SELECT * FROM df_real")
    # Flache Tabelle für die schnellen Joins
    con.execute("""
        CREATE OR REPLACE TABLE cart_items AS 
        SELECT id, unnest(cart) as item, generate_subscripts(cart, 1) as pos 
        FROM (SELECT row_number() OVER () as id, cart FROM carts);
        CREATE INDEX idx_item ON cart_items (item);
    """)
    print(timestamp(), 'warenkoerbe generiert und in supermarket_complex.db als "cart_items" tabelliert')

def get_warenkorb_connection(con):
    try:
        _ = len( con.execute('select * from cart_items limit 1').df() )
        _ = len( con.execute('select * from carts limit 1').df() )
        print(timestamp(), 'found generated db with carts and cart_items table, returning that connection')    
    except Exception as e:
        print(timestamp(), 'loading carts and cart_items failed, regenerating them, takes a bit!')
        regen_warenkoerbe(con)

def get_edges_from_cart_items(con):
    # Wir extrahieren alle Kanten (Gap 1 bedeutet: Positionen liegen direkt beieinander)
    # Das ist die Basis für unser "Lifting"
    con.execute("""
        CREATE OR REPLACE TABLE edges_extracted AS
        SELECT 
            t1.item AS source, 
            t2.item AS target, 
            COUNT(*) as freq
        FROM cart_items t1
        JOIN cart_items t2 ON t1.id = t2.id AND t2.pos = t1.pos + 1
        GROUP BY 1, 2
        HAVING freq >= 5; -- Rauschen direkt filtern
    """)
    # 1. Daten als Arrow-Tabelle aus DuckDB holen (bleibt im RAM)
    edges_arrow = con.execute("SELECT source, target FROM edges_extracted").arrow()

    # 2. Direkt von Arrow in den GPU-VRAM schieben
    import cudf
    edges_gpu = cudf.from_arrow(edges_arrow)
    return edges_gpu


with duckdb.connect('supermarket_complex.db') as con:
    get_warenkorb_connection(con)


# # Wir extrahieren alle Kanten (Gap 1 bedeutet: Positionen liegen direkt beieinander)
# # Das ist die Basis für unser "Lifting"
# con.execute("""
#     CREATE OR REPLACE TABLE edges_extracted AS
#     SELECT 
#         t1.item AS source, 
#         t2.item AS target, 
#         COUNT(*) as freq
#     FROM cart_items t1
#     JOIN cart_items t2 ON t1.id = t2.id AND t2.pos = t1.pos + 1
#     GROUP BY 1, 2
#     HAVING freq >= 5; -- Rauschen direkt filtern
# """)

# # Jetzt ab zur GPU
# edges_for_gpu = con.execute("SELECT source, target FROM edges_extracted").df()
# import cudf
# edges_gpu = cudf.from_pandas(edges_for_gpu)

# # --- 3. TRANSFER ZUR GPU (cuDF) ---
# # Wir nutzen Arrow für maximale Geschwindigkeit
# arrow_table = con.table('edges_full').to_arrow_table()
# edges_gpu = cudf.DataFrame.from_arrow(arrow_table)

# print(timestamp(), f"GPU: {len(edges_gpu)} Kanten in den VRAM geladen.")

# print(timestamp(), f"generiere die dreiecke, zaehle haeufigkeit und summiere umsatz")
# # 1. Struktur-Check auf der GPU (geht blitzschnell)
# # Wir finden alle theoretisch möglichen Dreiecke (A->B, B->C, A->C)
# E = edges_gpu[['source', 'target']].drop_duplicates()
# triplets = E.merge(E, left_on='target', right_on='source', suffixes=('_uv', '_vw'))
# potential_simplices_gpu = triplets.merge(
#     E, 
#     left_on=['source_uv', 'target_vw'], 
#     right_on=['source', 'target']
# )[['source_uv', 'target_uv', 'target_vw']]
# potential_simplices_gpu.columns = ['u', 'v', 'w']

# # 2. ÜBERTRAGUNG: GPU -> DuckDB
# # Wir registrieren den GPU-DataFrame als temporäre Sicht in DuckDB
# potential_arrow = potential_simplices_gpu.to_arrow()
# con.register('potential_simplices', potential_arrow)

# # 3. STATISTIK: Flache Tabelle für Speed (Index-basiert)
# con.execute("""
#     CREATE OR REPLACE TABLE cart_items AS 
#     SELECT id, unnest(cart) as item, generate_subscripts(cart, 1) as pos 
#     FROM (SELECT row_number() OVER () as id, cart FROM carts);
    
#     -- Jetzt die eigentliche triangles-Tabelle mit Umsatz und Frequenz
#     CREATE OR REPLACE TABLE triangles AS
#     SELECT 
#         s.u, s.v, s.w,
#         COUNT(DISTINCT t1.id) as frequency,
#         SUM(c.revenue) as total_revenue
#     FROM potential_simplices s
#     JOIN cart_items t1 ON s.u = t1.item
#     JOIN cart_items t2 ON s.v = t2.item AND t1.id = t2.id AND t1.pos < t2.pos
#     JOIN cart_items t3 ON s.w = t3.item AND t2.id = t3.id AND t2.pos < t3.pos
#     JOIN (SELECT row_number() OVER () as id, revenue FROM carts) c ON t1.id = c.id
#     GROUP BY 1, 2, 3;
# """)
# print(timestamp(), f"dreiecke generiert")

# def find_max_dimension(con):
#     dim = 1
#     print(f"Dimension 0: {len(products)} Knoten")
    
#     # Start mit Kanten (Dimension 1)
#     con.execute("SELECT count(*) FROM edges_full WHERE gap = 1").fetchone()
    
#     while True:
#         # Wir bauen Dimension n+1 aus Dimension n
#         # Ein (n+1)-Simplex braucht einen n-Simplex plus einen neuen Knoten,
#         # der zu ALLEN Knoten des n-Simplex eine gerichtete Kante hat.
        
#         # Für das Beispiel hier prüfen wir einfach, ob noch Simplizes in der 
#         # Tabelle 'carts' existieren, die diese Länge haben:
#         count = con.execute(f"SELECT count(*) FROM carts WHERE len(cart) > {dim}").fetchone()[0]
        
#         if count == 0:
#             print(f"Schluss! Die maximale Dimension ist {dim-1}.")
#             break
#         else:
#             print(f"Dimension {dim}: {count} potenzielle Simplizes vorhanden.")
#             dim += 1
#     return dim

# print(timestamp(), 'suche maximal moegliche dimension einer maximalen clique')
# print(timestamp(), 'gefunden:',find_max_dimension(con))

