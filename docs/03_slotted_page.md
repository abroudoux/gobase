# Slotted Page

## Rôle

Une page de 4096 octets doit pouvoir stocker plusieurs tuples de **taille variable**. La technique de la slotted page résout ce problème avec un layout ingénieux.

## Layout d'une page

```
+--------+--------+-----------+-----------+----------+--- ... ---+-------+-------+
| nSlots | freeEnd| nextPageID| prevPageID|  Slot 0  |  espace   | Data2 | Data1 |
|  2B    |   2B   |    2B     |    2B     |  4B      |   libre   |       |       |
+--------+--------+-----------+-----------+----------+--- ... ---+-------+-------+
^                                                                               ^
offset 0                                                                   offset 4095
```

- **Header** (8 octets) : métadonnées de la page
  - `numSlots` (2B) : nombre de slots utilisés
  - `freeEnd` (2B) : offset où commence l'espace libre depuis la fin
  - `nextPageID` (2B) : page suivante dans la liste chaînée
  - `prevPageID` (2B) : page précédente
- **Slot directory** : grandit depuis le début (après le header)
- **Données** : grandissent depuis la fin vers le début
- **Espace libre** : entre les deux

## Structure Slot

```go
type Slot struct {
    Offset uint16  // Position du tuple dans la page
    Length uint16  // Taille du tuple (0 = supprimé)
}
```

Chaque slot fait 4 octets et pointe vers un tuple dans la page.

## Insertion d'un tuple

```
Avant insertion de "Alice" (5B) :
+--------+--------+----+----+--- libre (4080B) ---+----------+
| nS=1   | fE=4091| .. | .. | Slot0 (4B)         | "Bob"(3B)|
+--------+--------+----+----+--- libre (4080B) ---+----------+

Après insertion de "Alice" :
+--------+--------+----+----+-------+--- libre ---+----------+----------+
| nS=2   | fE=4086| .. | .. | Slot0 | Slot1 (4B) | "Alice"  | "Bob"(3B)|
+--------+--------+----+----+-------+--- libre ---+----------+----------+
                                                  ^4086       ^4091
```

Algorithme :
1. Vérifie l'espace libre : `freeEnd - headerSize - numSlots*4 >= len(tuple) + 4`
2. Écrit le tuple à `freeEnd - len(tuple)`
3. Met à jour `freeEnd`
4. Crée un nouveau slot avec l'offset et la longueur
5. Incrémente `numSlots`

## Suppression — Lazy Deletion

```go
func (sp *SlottedPage) DeleteTuple(slotID uint16) error {
    // Marque simplement le slot comme supprimé
    slot.Length = 0
}
```

La suppression ne libère pas immédiatement l'espace — elle marque juste le slot avec `Length = 0`. C'est la **lazy deletion** : simple à implémenter, mais l'espace n'est pas récupéré tant qu'on ne fait pas de compaction.

## Accès à un tuple

```go
func (sp *SlottedPage) GetTuple(slotID uint16) (shared.Tuple, error)
```

1. Lit le slot à `headerSize + slotID * 4`
2. Vérifie que `Length > 0` (non supprimé)
3. Lit `Length` octets à `Offset` dans la page

## Pages chaînées

Quand une table déborde sur plusieurs pages, les pages sont chaînées en liste doublement chaînée :

```go
func (sp *SlottedPage) SetNextPageID(pageID uint16)
func (sp *SlottedPage) GetNextPageID() uint16
func (sp *SlottedPage) SetPrevPageID(pageID uint16)
func (sp *SlottedPage) GetPrevPageID() uint16
```

```
Page 1  ←→  Page 2  ←→  Page 3
(nextPageID = 2)  (nextPageID = 3)  (nextPageID = 0 = fin)
```

## Sérialisation binaire

Toutes les valeurs numériques sont encodées en **little-endian** avec `binary.LittleEndian` :

```go
binary.LittleEndian.PutUint16(sp.data[0:2], numSlots)
binary.LittleEndian.PutUint16(sp.data[2:4], freeEnd)
```

Le contenu brut de `sp.data` ([]byte de 4096 octets) est exactement ce qui est écrit sur le disque par le DiskManager.
