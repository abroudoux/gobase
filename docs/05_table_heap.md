# Table Heap

## Rôle

Le Table Heap gère le stockage d'une table complète répartie sur **plusieurs pages**. Il s'appuie sur les pages chaînées du slotted page et le buffer pool manager pour offrir des opérations CRUD de base.

## Row Identifier (RID)

Chaque tuple stocké est identifié de façon unique par un RID :

```go
type RID struct {
    pageID uint16
    slotID uint16
}
```

Un RID permet de retrouver n'importe quel tuple directement, sans scanner toute la table. C'est ce que stocke le B+ tree index pour faire le lien entre une clé et un tuple.

## Structure

```go
type TableHeap struct {
    bpm         *buffer_pool_manager.BufferPoolManager
    firstPageID uint16
    lastPageID  uint16
}
```

- `firstPageID` : point d'entrée pour les scans séquentiels
- `lastPageID` : page où insérer les nouveaux tuples (évite de scanner pour trouver de la place)

## Insertion

```go
func (th *TableHeap) Insert(tuple shared.Tuple) (RID, error)
```

1. Fetch la dernière page via le BPM
2. Tente d'insérer le tuple dans cette page
3. Si la page est pleine :
   - Alloue une nouvelle page via `bpm.NewPage()`
   - Chaîne les pages : `lastPage.SetNextPageID(newPageID)`, `newPage.SetPrevPageID(lastPageID)`
   - Insère dans la nouvelle page
   - Met à jour `lastPageID`
4. Unpin la page (avec `isDirty = true`)
5. Retourne le RID `{pageID, slotID}`

## Lecture

```go
func (th *TableHeap) Get(rid RID) (shared.Tuple, error)
```

1. Fetch la page `rid.pageID`
2. Appelle `slottedPage.GetTuple(rid.slotID)`
3. Unpin la page

## Suppression

```go
func (th *TableHeap) Delete(rid RID) error
```

Délègue à `slottedPage.DeleteTuple()` — lazy deletion, l'espace n'est pas récupéré immédiatement.

## Scan séquentiel

```go
func (th *TableHeap) Scan() *TableIterator
```

Retourne un itérateur qui parcourt toutes les pages chaînées et tous les slots non supprimés.

```go
type TableIterator struct {
    th            *TableHeap
    currentPageID uint16
    currentSlotID uint16
}
```

Algorithme de l'itérateur :
1. Commence à `firstPageID`, slot 0
2. Pour chaque slot de la page courante, retourne le tuple s'il n'est pas supprimé
3. Quand tous les slots d'une page sont épuisés, suit `nextPageID` pour passer à la page suivante
4. Unpin chaque page après avoir terminé de la lire

## Schéma d'une table sur plusieurs pages

```
TableHeap
firstPageID=1  lastPageID=3

Page 1 (SlottedPage)          Page 2 (SlottedPage)          Page 3 (SlottedPage)
+------------------+          +------------------+          +------------------+
| next=2  prev=0   |    →     | next=3  prev=1   |    →     | next=0  prev=2   |
| Slot0: "Alice"   |          | Slot0: "Charlie" |          | Slot0: "Eve"     |
| Slot1: "Bob"     |          | Slot1: [deleted] |          | Slot1: "Frank"   |
+------------------+          +------------------+          +------------------+
  RID(1,0) RID(1,1)             RID(2,0)                      RID(3,0) RID(3,1)
```

Un scan complet parcourra Page1 → Page2 → Page3 et retournera Alice, Bob, Charlie, Eve, Frank (en sautant le slot supprimé).
