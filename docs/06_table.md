# Table

## Rôle

La couche Table est l'**API de haut niveau** exposée à l'utilisateur. Elle combine le Catalog (sérialisation) et le TableHeap (stockage) pour offrir une interface travaillant avec des valeurs Go typées plutôt que des octets bruts.

## Structure

```go
type Table struct {
    Name   string
    Schema *catalog.Schema
    Heap   *table_heap.TableHeap
}
```

## Opérations

### Insertion

```go
func (t *Table) Insert(values ...any) (table_heap.RID, error)
```

1. Encode les valeurs avec `catalog.EncodeTuple(t.Schema, values)`
2. Insère le tuple binaire dans `t.Heap`
3. Retourne le RID

```go
rid, err := table.Insert(1, "Alice", 30)
// → encode [1, "Alice", 30] en bytes
// → stocke dans le heap
// → retourne RID{pageID, slotID}
```

### Lecture par RID

```go
func (t *Table) GetByRID(rid table_heap.RID) ([]any, error)
```

1. Fetch le tuple binaire depuis `t.Heap`
2. Décode avec `catalog.DecodeTuple(t.Schema, tuple)`
3. Retourne les valeurs Go

### Suppression

```go
func (t *Table) Delete(rid table_heap.RID) error
```

Délègue directement au heap.

### Scan

```go
func (t *Table) Scan() *TableScanner
```

Retourne un scanner qui wrape l'itérateur du heap et décode automatiquement chaque tuple.

```go
type TableScanner struct {
    schema *catalog.Schema
    iter   *table_heap.TableIterator
}
```

Usage typique :

```go
scanner := table.Scan()
for {
    values, rid, err := scanner.Next()
    if err != nil { break }
    fmt.Println(rid, values)
}
```

## Position dans l'architecture

```
Utilisateur
    ↓
Table (encode/decode, API typée)
    ↓
Catalog ←→ TableHeap
              ↓
        BufferPoolManager
              ↓
         DiskManager
              ↓
           Fichier
```

La Table est le point d'entrée "user-friendly". En dessous, tout est bytes.
