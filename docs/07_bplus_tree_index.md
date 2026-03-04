# B+ Tree Index

## Rôle

Un scan séquentiel complet pour trouver une ligne par sa valeur est O(n). Le B+ Tree Index permet des recherches en **O(log n)** en maintenant les clés triées dans une structure arborescente équilibrée.

## Différence B-Tree vs B+ Tree

Dans un **B-Tree**, les données sont stockées dans tous les nœuds (internes et feuilles).

Dans un **B+ Tree** :
- Les **internal nodes** ne contiennent que des clés de navigation
- Les **leaf nodes** contiennent les clés ET les pointeurs vers les données (RID)
- Les leaf nodes sont **chaînés** entre eux → scans de plages efficaces

## Structures

```go
type Node interface {
    Search(key uint16) Node
}

type InternalNode struct {
    Keys   []uint16  // Clés de séparation
    Childs []Node    // len(Childs) == len(Keys) + 1
}

type LeafNode struct {
    Keys         []uint16           // Clés triées
    RID          []table_heap.RID   // Pointeurs vers les tuples
    NextLeafNode *LeafNode          // Chaînage pour les range scans
}

type BPlusTreeIndex struct {
    Root  Node
    Order uint8  // Nombre max de clés par nœud
}
```

**Invariant** : un `InternalNode` a toujours un enfant de plus que de clés.

```
InternalNode [15 | 30]
             /    |    \
           [A]   [B]   [C]
  (clés < 15) (15≤k<30) (clés ≥ 30)
```

## Recherche

```go
func (bpti *BPlusTreeIndex) Search(key uint16) Node
```

Descend l'arbre depuis la racine jusqu'à une feuille :

- Sur un `InternalNode` : compare la clé aux séparateurs pour choisir l'enfant
- Sur un `LeafNode` : vérifie si la clé est présente

```go
func (in *InternalNode) Search(key uint16) Node {
    if key < in.Keys[0] {
        return in.Childs[0]
    }
    for i := 0; i < len(in.Keys)-1; i++ {
        if key >= in.Keys[i] && key < in.Keys[i+1] {
            return in.Childs[i+1]
        }
    }
    return in.Childs[len(in.Childs)-1]
}
```

## Insertion

```go
func (bpti *BPlusTreeIndex) Insert(key uint16, RID table_heap.RID)
```

### Cas 1 : arbre vide

```go
if bpti.Root == nil {
    bpti.Root = NewLeafNode([]uint16{key}, []table_heap.RID{RID})
    return
}
```

### Cas 2 : insertion normale (pas de débordement)

1. Descend jusqu'au leaf node approprié (en mémorisant le chemin `nodes []*InternalNode`)
2. Trouve la position d'insertion triée avec `findNodeIndex`
3. Insère la clé et le RID avec `slices.Insert`

### Cas 3 : débordement → Split

Si après insertion `len(leafNode.Keys) >= Order` :

#### Split du leaf node

```
Avant (Order=3) :   [10 | 15 | 20]
Après split :
  Gauche : [10 | 15]
  Droite : [20]
  middleKey : 15  (remonte dans le parent)
  Gauche.NextLeafNode → Droite
```

```go
func splitLeafNode(ln *LeafNode) (uint16, *LeafNode) {
    middleIndex := len(ln.Keys) / 2
    middleKey := ln.Keys[middleIndex]
    newLeafNode := NewLeafNode(ln.Keys[middleIndex:], ln.RID[middleIndex:])
    ln.Keys = ln.Keys[:middleIndex]
    ln.RID = ln.RID[:middleIndex]
    ln.NextLeafNode = newLeafNode
    return middleKey, newLeafNode
}
```

**Différence cruciale** : dans un leaf node, la clé du milieu **reste** dans le nœud gauche ET remonte dans le parent (copie). Les données ne doivent jamais être perdues.

#### Split de l'internal node

```
Avant (Order=3) :   [15 | 30 | 45]
Après split :
  Gauche : [15]
  Droite : [45]
  middleKey : 30  (remonte, NE reste PAS dans les nœuds)
```

```go
func splitInternalNode(in *InternalNode) (uint16, *InternalNode) {
    middleIndex := len(in.Keys) / 2
    middleKey := in.Keys[middleIndex]
    newNode := NewInternalNode(in.Keys[middleIndex+1:], in.Childs[middleIndex+1:])
    in.Keys = in.Keys[:middleIndex]
    in.Childs = in.Childs[:middleIndex+1]  // un enfant de plus que de clés
    return middleKey, newNode
}
```

**Différence cruciale** : dans un internal node, la clé du milieu **monte uniquement** — elle ne reste ni à gauche ni à droite.

### Propagation des splits en cascade

Après un split, la `middleKey` doit être insérée dans le parent. Si ce parent déborde à son tour, on le splitte aussi, et ainsi de suite jusqu'à la racine.

Le chemin `nodes []*InternalNode` (collecté lors de la descente) permet de remonter avec une boucle :

```go
rootSplit := true
for i := len(nodes) - 1; i >= 0; i-- {
    parent := nodes[i]
    // insère middleKey dans parent
    if len(parent.Keys) < int(bpti.Order) {
        rootSplit = false
        break
    }
    middleKey, newInternalNode = splitInternalNode(parent)
}

if rootSplit {
    // la racine a splitté → nouvelle racine
    bpti.Root = NewInternalNode([]uint16{middleKey}, []Node{nodes[0], newInternalNode})
}
```

Si même la racine splitte, l'arbre gagne un niveau.

## Exemple complet

Arbre d'ordre 3, insertions : 10, 20, 30

```
Insert 10 :         [10]

Insert 20 :         [10|20]

Insert 30 :         [10|20|30]  → overflow, split !
  middleKey = 20
  Leaf gauche : [10]   Leaf droit : [30]
  Nouvelle racine : InternalNode[20]
                        /    \
                     [10]   [30]  (leaf nodes chaînés)
```

## Chaînage des feuilles pour les range scans

```
Racine [20]
       /   \
   [10]  →  [20|30]  →  [40|50]
```

Pour un range scan "clés entre 15 et 45" :
1. Descend jusqu'à la première feuille contenant ≥ 15 → `[20|30]`
2. Parcourt via `NextLeafNode` jusqu'à dépasser 45

Complexité O(log n + k) où k est le nombre de résultats.
