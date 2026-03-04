# Catalog

## Rôle

Le Catalog gère le **schéma** des tables : quelles colonnes existent, de quel type elles sont, et comment sérialiser/désérialiser les valeurs Go en octets pour le stockage.

## Types supportés

```go
type ColumnType uint8

const (
    TypeInt      ColumnType = iota  // 4 octets
    TypeSmallInt                    // 2 octets
    TypeBoolean                     // 1 octet
    TypeVarchar                     // 2B longueur + N octets
)
```

## Schéma

```go
type Column struct {
    Name     string
    Type     ColumnType
    Size     uint16  // Utilisé pour VARCHAR : taille max
    Nullable bool
}

type Schema struct {
    Columns []Column
}
```

Exemple de schéma pour une table `users` :

```go
schema := catalog.NewSchema([]catalog.Column{
    {Name: "id",   Type: catalog.TypeInt},
    {Name: "name", Type: catalog.TypeVarchar, Size: 50},
    {Name: "age",  Type: catalog.TypeSmallInt},
})
```

## Encodage d'un tuple

```go
func EncodeTuple(schema *Schema, values []any) (shared.Tuple, error)
```

Convertit une liste de valeurs Go en slice d'octets. Les colonnes sont encodées séquentiellement :

| Type      | Encodage                              |
|-----------|---------------------------------------|
| INT       | 4 octets, little-endian               |
| SMALLINT  | 2 octets, little-endian               |
| BOOLEAN   | 1 octet (0 = false, 1 = true)         |
| VARCHAR   | 2 octets longueur + bytes de la chaîne|

**Exemple** : encodage de `(1, "Alice", 30)` avec le schéma ci-dessus :

```
INT(1)       → 01 00 00 00
VARCHAR("Alice") → 05 00 41 6C 69 63 65   (longueur=5, puis "Alice")
SMALLINT(30) → 1E 00
```

Résultat : `01 00 00 00 05 00 41 6C 69 63 65 1E 00` (13 octets)

## Décodage d'un tuple

```go
func DecodeTuple(schema *Schema, tuple shared.Tuple) ([]any, error)
```

Opération inverse : lit les octets séquentiellement en suivant le schéma pour reconstruire les valeurs Go.

Pour VARCHAR, lit d'abord 2 octets pour connaître la longueur, puis lit ce nombre d'octets.

## Recherche de colonne

```go
func (s *Schema) GetColumnIndex(name string) (int, error)
```

Retourne l'index d'une colonne par son nom. Utile pour accéder à une valeur spécifique après décodage.

## Position dans l'architecture

Le Catalog est utilisé par la couche `table` pour abstraire le stockage binaire. Les couches inférieures (slotted page, table heap) ne voient que des `[]byte` — elles ne savent pas ce qu'ils contiennent. C'est le Catalog qui fait le pont entre les types Go et les octets.

```
table.Insert(1, "Alice", 30)
    → catalog.EncodeTuple(schema, [1, "Alice", 30])
    → []byte{01 00 00 00 05 00 41 6C 69 63 65 1E 00}
    → table_heap.Insert(tuple)
    → slotted_page.InsertTuple(tuple)
```
