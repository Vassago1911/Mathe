def get_vertex_count(bin_string):
    import math
    L = len(bin_string)
    if L == 0:
        return 0
    # Die Formel liefert den exakten oder nächsthöheren V
    V = math.ceil((1 + math.sqrt(1 + 8 * L)) / 2)
    return int(V)

def get_stable_max_cliques(bin_string):
    # 1. Kantenliste stabil generieren
    # Wir bauen die Liste so auf, dass sie bei jedem V nur erweitert wird
    # (0,1) -> (0,2, 1,2) -> (0,3, 1,3, 2,3) ...
    n = len(bin_string)
    e_KV = []
    v_curr = 1
    while len(e_KV) < n:
        for u in range(v_curr):
            e_KV.append((u, v_curr))
        v_curr += 1
    
    # V ist der höchste Knotenindex + 1
    V = v_curr
    
    # 2. Nachbarschafts-Masken (nb_masks)
    nb_masks = [0] * V
    for bit_idx, char in enumerate(bin_string):
        if char == '1':
            u, v = e_KV[bit_idx]
            nb_masks[u] |= (1 << v)
            nb_masks[v] |= (1 << u)

    # 3. Bron-Kerbosch (einfache Version mit Bitmasks)
    # Wir finden maximale Cliquen
    max_cliques = []
    
    def find_cliques(R, P, X):
        if P == 0 and X == 0:
            if R != 0:
                nodes = [i for i in range(V) if (R >> i) & 1]
                max_cliques.append(nodes)
            return
        
        # Pivot-Wahl für Effizienz
        pivot = (P | X).bit_length() - 1
        P_without_neighbors_of_pivot = P & ~nb_masks[pivot]
        
        for v in range(V):
            if (P_without_neighbors_of_pivot >> v) & 1:
                find_cliques(R | (1 << v), P & nb_masks[v], X & nb_masks[v])
                P &= ~(1 << v)
                X |= (1 << v)

    find_cliques(0, (1 << V) - 1, 0)
    max_cliques = list(map(tuple,max_cliques))
    return sorted(max_cliques, key=lambda x: ( -len(x), x) )

def get_random_binary_str(density:float=0.2):
    from random import randint, random
    ln = randint(0,52)
    b = ''.join( list(map(str,[ ( 1 if random() > (1-density) else 0 ) for _ in range(ln) ])) )
    return b

# Test

bs = [ get_random_binary_str(0.5) for _ in range(100) ]
bs = sorted(bs, key= lambda x: (len(x),x))

for s in bs:
    print(f"Cliquen s1={s:>52}: {get_stable_max_cliques(s)}, V={get_vertex_count(s)}")
